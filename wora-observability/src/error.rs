use thiserror::Error;

/// Error returned while constructing or serving an observability exporter.
#[derive(Debug, Error)]
pub enum ExporterError {
    /// Prometheus collector or encoding error.
    #[error("prometheus: {0}")]
    Prometheus(String),
    /// OpenTelemetry provider or exporter error.
    #[error("opentelemetry: {0}")]
    OpenTelemetry(String),
    /// An I/O operation failed.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    /// Exporter configuration is invalid.
    #[error("invalid configuration: {0}")]
    InvalidConfiguration(String),
}

#[cfg(feature = "prometheus")]
impl From<::prometheus::Error> for ExporterError {
    fn from(value: ::prometheus::Error) -> Self {
        Self::Prometheus(value.to_string())
    }
}
