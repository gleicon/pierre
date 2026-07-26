//! Static bearer-token auth (DECISIONS.md "Auth model for v1") — deliberately not
//! federated/full auth. Checks `Authorization: Bearer <token>` against a configured
//! list, matching what real collectors (Promtail/Alloy/Vector's `bearer_token`
//! client config) already send by default, so pointing an existing collector at
//! Pierre "just works" without inventing a new credential scheme. Not a security
//! boundary for anything beyond small/trusted-network/test deployments: no
//! rotation, no per-client scoping, no expiry. If unconfigured (empty list), auth
//! is off entirely — this is the default, matching every deployment before this
//! feature existed.

use std::collections::HashSet;
use std::sync::Arc;

use axum::extract::{Extension, Request};
use axum::http::{header, StatusCode};
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::Router;
use subtle::ConstantTimeEq;

#[derive(Clone)]
pub struct AuthTokens(Arc<HashSet<String>>);

impl AuthTokens {
    pub fn new(tokens: Vec<String>) -> Self {
        AuthTokens(Arc::new(tokens.into_iter().collect()))
    }

    fn is_enabled(&self) -> bool {
        !self.0.is_empty()
    }

    /// Constant-time against every configured token — `HashSet::contains`'s
    /// hash-then-lookup already dampens the classic "byte comparison bails on
    /// the first mismatch" timing signal, but doesn't eliminate it, so this
    /// closes it properly: compares against *every* entry, never short-
    /// circuits on a match, and each individual comparison runs in time
    /// independent of where the bytes first differ. `[u8]::ct_eq` (`subtle`,
    /// already resolved in the dependency tree via the RustCrypto chain
    /// several other dependencies pull in — zero net new compiled crate)
    /// short-circuits only on a length mismatch, which is fine: token
    /// *length* isn't the secret here, only its content is.
    fn is_valid(&self, token: &str) -> bool {
        let token = token.as_bytes();
        self.0
            .iter()
            .fold(subtle::Choice::from(0u8), |acc, candidate| {
                acc | candidate.as_bytes().ct_eq(token)
            })
            .into()
    }

    /// Checks a raw `Authorization` header value (e.g. `"Bearer <token>"`) against
    /// the configured tokens — `true` if auth is off, or the header carries a
    /// valid bearer token. Shared by the axum middleware below (HTTP surfaces)
    /// and the OTLP gRPC interceptor (`listener/otlp.rs`, which reads the same
    /// header out of gRPC metadata instead of an HTTP header) so the "off when
    /// unconfigured, else require a matching Bearer token" rule has exactly one
    /// implementation instead of drifting between transports.
    pub fn check_header(&self, header_value: Option<&str>) -> bool {
        if !self.is_enabled() {
            return true;
        }
        header_value
            .and_then(|v| v.strip_prefix("Bearer "))
            .is_some_and(|token| self.is_valid(token))
    }
}

/// Applies the shared HTTP hardening every axum-based listener needs — the
/// bearer-token check (`Extension` must wrap *outside* `from_fn`, since axum
/// layers added later run first and the middleware needs the extension already
/// inserted), plus `CatchPanicLayer`: a panic inside a plain axum handler (loki,
/// es_bulk, OTLP/HTTP, query_api) would otherwise abort the whole HTTP/1.1
/// connection instead of returning a clean 500.
///
/// Does **not** cover a panic inside an MCP tool call (`src/mcp.rs`, mounted
/// through `listener/mcp.rs`'s `nest_service`) — verified empirically, not
/// assumed: `rmcp`'s `StreamableHttpService` dispatches each tool call through
/// its own internal task, off the future this layer wraps, so `catch_unwind`
/// here never sees it. Reproduced directly: temporarily reintroducing the
/// `hex_decode` bug this comment used to cite (a crafted `doc_id` panicking a
/// `str` byte-slice mid-UTF-8-character) still hung the client indefinitely
/// with this layer in place — the fix had to be "don't panic on untrusted
/// input" at the source, not a layer here. Kept anyway: real protection for
/// every other listener, and one place to get the auth check right instead of
/// each repeating it.
pub fn layer<S>(router: Router<S>, tokens: AuthTokens) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    router
        .layer(middleware::from_fn(require_bearer_token))
        .layer(Extension(tokens))
        .layer(tower_http::catch_panic::CatchPanicLayer::new())
}

async fn require_bearer_token(
    Extension(tokens): Extension<AuthTokens>,
    request: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let provided = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok());

    if tokens.check_header(provided) {
        Ok(next.run(request).await)
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::routing::get;
    use tower::ServiceExt;

    /// Proves `CatchPanicLayer` actually does what `layer`'s doc comment claims
    /// for a plain axum handler (the class of listener it *does* protect —
    /// loki/es_bulk/OTLP-HTTP/query_api, not MCP's tool calls, see that comment):
    /// a panic must come back as a clean 500, not a dropped/hung connection.
    #[tokio::test]
    async fn a_panicking_handler_returns_a_clean_500_not_a_dropped_connection() {
        async fn panics() -> &'static str {
            panic!("boom");
        }
        let router: Router = layer(
            Router::new().route("/boom", get(panics)),
            AuthTokens::new(vec![]),
        );
        let response = router
            .oneshot(
                Request::builder()
                    .uri("/boom")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    /// Correctness for the constant-time rewrite of `is_valid`/`check_header` —
    /// timing itself isn't asserted here (notoriously flaky in CI), but every
    /// functional case the old `HashSet::contains` handled must still work
    /// identically: empty config means auth off, the right token among several
    /// configured ones passes, a wrong or empty-string token fails, and a
    /// missing/malformed `Authorization` header fails.
    #[test]
    fn constant_time_check_matches_hashset_contains_semantics() {
        let disabled = AuthTokens::new(vec![]);
        assert!(disabled.check_header(None));
        assert!(disabled.check_header(Some("Bearer anything")));

        let tokens = AuthTokens::new(vec!["alpha".to_string(), "beta".to_string()]);
        assert!(tokens.check_header(Some("Bearer alpha")));
        assert!(tokens.check_header(Some("Bearer beta")));
        assert!(!tokens.check_header(Some("Bearer gamma")));
        assert!(
            !tokens.check_header(Some("Bearer alph")),
            "must not prefix-match"
        );
        assert!(
            !tokens.check_header(Some("Bearer alphaX")),
            "must not prefix-match the other way"
        );
        assert!(
            !tokens.check_header(Some("Bearer ")),
            "empty token must not match"
        );
        assert!(
            !tokens.check_header(Some("alpha")),
            "missing Bearer prefix must not match"
        );
        assert!(!tokens.check_header(None));
    }
}
