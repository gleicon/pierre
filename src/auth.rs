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
use axum::middleware::Next;
use axum::response::Response;

#[derive(Clone)]
pub struct AuthTokens(Arc<HashSet<String>>);

impl AuthTokens {
    pub fn new(tokens: Vec<String>) -> Self {
        AuthTokens(Arc::new(tokens.into_iter().collect()))
    }

    fn is_enabled(&self) -> bool {
        !self.0.is_empty()
    }

    fn is_valid(&self, token: &str) -> bool {
        self.0.contains(token)
    }
}

/// Apply via `.layer(middleware::from_fn(auth::require_bearer_token)).layer(Extension(tokens))`
/// — the `Extension` layer must be added *after* (so it wraps *outside*) the
/// `from_fn` layer, since axum layers added later run first and this middleware
/// needs the extension already inserted when it runs.
pub async fn require_bearer_token(
    Extension(tokens): Extension<AuthTokens>,
    request: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    if !tokens.is_enabled() {
        return Ok(next.run(request).await);
    }

    let provided = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "));

    match provided {
        Some(token) if tokens.is_valid(token) => Ok(next.run(request).await),
        _ => Err(StatusCode::UNAUTHORIZED),
    }
}
