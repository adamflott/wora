//! OpenTelemetry metrics sink and optional OTLP pipeline.

mod sink;

#[cfg(feature = "otlp")]
mod pipeline;

#[cfg(feature = "otlp")]
pub use pipeline::OpenTelemetryPipelineBuilder;
pub use sink::OpenTelemetrySink;
