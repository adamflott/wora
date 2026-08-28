use axum::Router;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::get;
use prometheus::{Encoder, Registry, TextEncoder};

use crate::ExporterError;

async fn metrics(State(registry): State<Registry>) -> impl IntoResponse {
    let encoder = TextEncoder::new();
    let mut body = Vec::new();
    match encoder.encode(&registry.gather(), &mut body) {
        Ok(()) => (StatusCode::OK, [(header::CONTENT_TYPE, encoder.format_type().to_owned())], body),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            [(header::CONTENT_TYPE, "text/plain".to_owned())],
            error.to_string().into_bytes(),
        ),
    }
}

/// Serve the registry at `GET /metrics` until the server is stopped.
pub async fn serve_prometheus_metrics(listener: tokio::net::TcpListener, registry: Registry) -> Result<(), ExporterError> {
    let app = Router::new().route("/metrics", get(metrics)).with_state(registry);
    axum::serve(listener, app).await.map_err(ExporterError::Io)
}
