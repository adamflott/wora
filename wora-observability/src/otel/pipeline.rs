use opentelemetry::metrics::MeterProvider;
use opentelemetry_otlp::{MetricExporter, WithExportConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::metrics::SdkMeterProvider;

use super::OpenTelemetrySink;
use crate::ExporterError;

/// Builder for a periodic OTLP/gRPC metrics exporter and its WORA sink.
pub struct OpenTelemetryPipelineBuilder {
    service_name: String,
    endpoint: Option<String>,
}

impl OpenTelemetryPipelineBuilder {
    /// Create a builder with service name `wora` and the exporter default endpoint.
    pub fn new() -> Self {
        Self {
            service_name: "wora".into(),
            endpoint: None,
        }
    }

    /// Set the OpenTelemetry `service.name` resource attribute.
    pub fn with_service_name(mut self, name: impl Into<String>) -> Self {
        self.service_name = name.into();
        self
    }

    /// Set the OTLP collector endpoint, such as `http://localhost:4317`.
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    /// Install a periodic exporter using Tokio and return a sink that owns the provider.
    pub fn install(self) -> Result<OpenTelemetrySink, ExporterError> {
        if self.service_name.trim().is_empty() {
            return Err(ExporterError::InvalidConfiguration("service name cannot be empty".into()));
        }
        if self.endpoint.as_deref().is_some_and(|endpoint| endpoint.trim().is_empty()) {
            return Err(ExporterError::InvalidConfiguration("OTLP endpoint cannot be empty".into()));
        }
        let mut builder = MetricExporter::builder().with_tonic();
        if let Some(endpoint) = self.endpoint {
            builder = builder.with_endpoint(endpoint);
        }
        let exporter = builder.build().map_err(|error| ExporterError::OpenTelemetry(error.to_string()))?;
        let resource = Resource::builder().with_service_name(self.service_name.clone()).build();
        let provider = SdkMeterProvider::builder().with_resource(resource).with_periodic_exporter(exporter).build();
        let meter = provider.meter("wora-observability");
        let mut sink = OpenTelemetrySink::new(meter);
        sink.provider = Some(provider);
        Ok(sink)
    }
}

impl Default for OpenTelemetryPipelineBuilder {
    fn default() -> Self {
        Self::new()
    }
}
