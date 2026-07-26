use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::streamable_http_server::{StreamableHttpServerConfig, StreamableHttpService};

use crate::auth::AuthTokens;
use crate::mcp::PierreMcpServer;
use crate::storage::Storage;

/// Mounts the MCP server at `/mcp` via the Streamable HTTP transport — the same
/// axum-service pattern every other Pierre listener uses (`StreamableHttpService`
/// implements `tower::Service`, so `Router::nest_service` just works), sharing the
/// same bearer-token auth middleware as the rest of the query surface. PRD A-1's
/// "auth is a scoped token bound to a label subset" is not implemented — this is
/// the same flat allow/deny every other Pierre HTTP surface already has, not a
/// per-token label restriction. Read-only by construction: `PierreMcpServer`'s
/// tools never call a mutating `Storage` method, so there is nothing to gate here
/// beyond who can reach the surface at all.
pub fn router(
    storage: Arc<Storage>,
    allowed_fields: Arc<Vec<String>>,
    textindex_bucket_duration: Duration,
    auth_tokens: AuthTokens,
) -> Router {
    let mcp_server = PierreMcpServer::new(storage, allowed_fields, textindex_bucket_duration);
    let service = StreamableHttpService::new(
        move || Ok(mcp_server.clone()),
        LocalSessionManager::default().into(),
        StreamableHttpServerConfig::default(),
    );

    let router = Router::new().nest_service("/mcp", service);
    crate::auth::layer(router, auth_tokens)
}

pub async fn serve(
    addr: &str,
    storage: Arc<Storage>,
    allowed_fields: Arc<Vec<String>>,
    textindex_bucket_duration: Duration,
    auth_tokens: AuthTokens,
) -> anyhow::Result<()> {
    let app = router(
        storage,
        allowed_fields,
        textindex_bucket_duration,
        auth_tokens,
    );
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
