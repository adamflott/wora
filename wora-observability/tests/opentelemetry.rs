#![cfg(feature = "opentelemetry")]

use opentelemetry::metrics::MeterProvider;
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, SdkMeterProvider};
use wora::o11y::{O11ySink, o11y_new_ev_status};
use wora_observability::otel::OpenTelemetrySink;

#[tokio::test]
async fn status_event_exports_metrics_and_attributes() -> Result<(), Box<dyn std::error::Error>> {
    let exporter = InMemoryMetricExporter::default();
    let provider = SdkMeterProvider::builder().with_periodic_exporter(exporter.clone()).build();
    let mut sink = OpenTelemetrySink::new(provider.meter("wora-test")).with_metric_attribute("app", "test");

    sink.handle_event(&o11y_new_ev_status::<()>(3, 8)).await?;
    provider.force_flush()?;

    let exported = exporter.get_finished_metrics()?;
    let metrics = exported
        .iter()
        .flat_map(|resource| resource.scope_metrics())
        .flat_map(|scope| scope.metrics())
        .collect::<Vec<_>>();
    let names = metrics.iter().map(|metric| metric.name()).collect::<Vec<_>>();
    assert!(names.contains(&"wora.o11y.events"));
    assert!(names.contains(&"wora.runtime.event_backlog.capacity"));
    assert!(names.contains(&"wora.runtime.event_backlog.max_capacity"));
    assert!(metrics.iter().all(|metric| match metric.data() {
        AggregatedMetrics::F64(data) => has_app_attribute(data),
        AggregatedMetrics::U64(data) => has_app_attribute(data),
        AggregatedMetrics::I64(data) => has_app_attribute(data),
    }));

    provider.shutdown()?;
    Ok(())
}

fn has_app_attribute<T>(data: &MetricData<T>) -> bool {
    match data {
        MetricData::Gauge(data) => data.data_points().all(|point| attributes_include_app(point.attributes())),
        MetricData::Sum(data) => data.data_points().all(|point| attributes_include_app(point.attributes())),
        MetricData::Histogram(data) => data.data_points().all(|point| attributes_include_app(point.attributes())),
        MetricData::ExponentialHistogram(data) => data.data_points().all(|point| attributes_include_app(point.attributes())),
    }
}

fn attributes_include_app<'a>(mut attributes: impl Iterator<Item = &'a opentelemetry::KeyValue>) -> bool {
    attributes.any(|attribute| attribute.key.as_str() == "app" && attribute.value.as_str() == "test")
}
