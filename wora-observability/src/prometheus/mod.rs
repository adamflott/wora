//! Prometheus metrics sink and optional HTTP endpoint.

mod registry;
mod sink;

#[cfg(feature = "prometheus-http")]
mod http;

#[cfg(feature = "prometheus-http")]
pub use http::serve_prometheus_metrics;
pub use sink::{PrometheusSink, PrometheusSinkBuilder};
