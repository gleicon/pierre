use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::Router;
use prost::Message;
use tonic::service::Interceptor;
use tonic::{Request, Response, Status};

use crate::auth::AuthTokens;
use crate::otlpproto::opentelemetry::proto::collector::logs::v1::logs_service_server::{
    LogsService, LogsServiceServer,
};
use crate::otlpproto::opentelemetry::proto::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use crate::rollup::RollupHandle;
use crate::stats::IngestStats;
use crate::storage::Storage;
use crate::textindex::TextIndexHandle;

/// Shared committer for both transports (gRPC and HTTP): decodes the batch OTLP
/// always sends (real exporters batch resource/scope/record hierarchies, never
/// one record per request) via `otlpproto::decode_export_request`, then commits
/// each through the same `ingest::commit` path every other listener uses.
struct Ingester {
    storage: Arc<Storage>,
    allowed_fields: Arc<Vec<String>>,
    rollup: Option<RollupHandle>,
    textindex: Option<TextIndexHandle>,
    stats: IngestStats,
}

impl Ingester {
    async fn ingest(&self, request: &ExportLogsServiceRequest) -> anyhow::Result<usize> {
        let records = crate::otlpproto::decode_export_request(request);
        let count = records.len();
        for wire in records {
            crate::ingest::commit(
                &self.storage,
                wire,
                &self.allowed_fields,
                self.rollup.as_ref(),
                self.textindex.as_ref(),
            )
            .await?;
            self.stats.record_commit();
        }
        Ok(count)
    }
}

/// gRPC `LogsService` — the primary OTLP transport (most exporters default to
/// gRPC over HTTP). A separate listen address from the HTTP variant: real OTel
/// Collectors also run gRPC and HTTP on two distinct ports (4317/4318
/// conventionally), and Pierre's own native protocol already claims 4317, so an
/// operator pointing a real OTel SDK/Collector at Pierre sets its exporter
/// endpoint explicitly either way — normal practice, not a Pierre-specific step.
struct GrpcLogsService {
    ingester: Ingester,
}

#[tonic::async_trait]
impl LogsService for GrpcLogsService {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        match self.ingester.ingest(request.get_ref()).await {
            Ok(_) => Ok(Response::new(ExportLogsServiceResponse {
                partial_success: None,
            })),
            Err(e) => Err(Status::internal(e.to_string())),
        }
    }
}

/// Checks gRPC metadata's `authorization` entry against the same `AuthTokens`
/// every HTTP surface uses (`AuthTokens::check_header`) — gRPC carries it as
/// ordinary HTTP/2 header metadata, same value shape (`"Bearer <token>"`), just
/// read through `tonic::Request::metadata()` instead of axum's `HeaderMap`.
#[derive(Clone)]
struct AuthInterceptor(AuthTokens);

impl Interceptor for AuthInterceptor {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        let provided = request
            .metadata()
            .get("authorization")
            .and_then(|v| v.to_str().ok());
        if self.0.check_header(provided) {
            Ok(request)
        } else {
            Err(Status::unauthenticated("missing or invalid bearer token"))
        }
    }
}

pub async fn serve_grpc(
    addr: &str,
    storage: Arc<Storage>,
    allowed_fields: Arc<Vec<String>>,
    rollup: Option<RollupHandle>,
    textindex: Option<TextIndexHandle>,
    auth_tokens: AuthTokens,
    stats: IngestStats,
) -> anyhow::Result<()> {
    let service = GrpcLogsService {
        ingester: Ingester {
            storage,
            allowed_fields,
            rollup,
            textindex,
            stats,
        },
    };
    let service = LogsServiceServer::with_interceptor(service, AuthInterceptor(auth_tokens));
    tonic::transport::Server::builder()
        .add_service(service)
        .serve(addr.parse()?)
        .await?;
    Ok(())
}

/// `POST /v1/logs`, the OTLP/HTTP path — protobuf body only
/// (`Content-Type: application/x-protobuf`). OTLP/JSON is a deliberate scope cut,
/// not an oversight: it needs its own field-casing (lowerCamelCase) and bytes
/// encoding (base64) rules distinct from `serde_json`'s default derive, which
/// `prost`'s generated types don't carry — real additional work for the less
/// common of OTLP/HTTP's two content types (most OTLP/HTTP exporters default to
/// protobuf). A JSON body gets a clear 415, not a silent wrong parse.
pub fn router(
    storage: Arc<Storage>,
    allowed_fields: Arc<Vec<String>>,
    rollup: Option<RollupHandle>,
    textindex: Option<TextIndexHandle>,
    auth_tokens: AuthTokens,
    stats: IngestStats,
) -> Router {
    let router = Router::new()
        .route("/v1/logs", post(http_logs_handler))
        .with_state(Arc::new(Ingester {
            storage,
            allowed_fields,
            rollup,
            textindex,
            stats,
        }));
    crate::auth::layer(router, auth_tokens)
}

pub async fn serve_http(
    addr: &str,
    storage: Arc<Storage>,
    allowed_fields: Arc<Vec<String>>,
    rollup: Option<RollupHandle>,
    textindex: Option<TextIndexHandle>,
    auth_tokens: AuthTokens,
    stats: IngestStats,
) -> anyhow::Result<()> {
    let app = router(
        storage,
        allowed_fields,
        rollup,
        textindex,
        auth_tokens,
        stats,
    );
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn http_logs_handler(
    State(ingester): State<Arc<Ingester>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<StatusCode, StatusCode> {
    let is_protobuf = headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|ct| ct.starts_with("application/x-protobuf"));
    if !is_protobuf {
        return Err(StatusCode::UNSUPPORTED_MEDIA_TYPE);
    }

    let request = ExportLogsServiceRequest::decode(body).map_err(|_| StatusCode::BAD_REQUEST)?;
    ingester
        .ingest(&request)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(StatusCode::OK)
}
