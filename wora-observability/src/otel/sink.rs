use std::fmt::Debug;

use opentelemetry::KeyValue;
use opentelemetry::metrics::{Counter, Gauge, Meter};
use wora::o11y::{O11yEvent, O11yEventKind, O11ySink, O11ySinkError};

use crate::labels::{health_value, leadership_value, readiness_value};

struct Instruments {
    events: Counter<u64>,
    restart_count: Gauge<u64>,
    backlog_capacity: Gauge<u64>,
    backlog_max_capacity: Gauge<u64>,
    health: Gauge<i64>,
    readiness: Gauge<i64>,
    leadership: Gauge<i64>,
    process_memory: Gauge<u64>,
    process_virtual_memory: Gauge<u64>,
    process_cpu_ratio: Gauge<f64>,
    process_start_time: Gauge<u64>,
    host_memory_total: Gauge<u64>,
    host_memory_used: Gauge<u64>,
    host_swap_total: Gauge<u64>,
    host_swap_used: Gauge<u64>,
    host_load1: Gauge<f64>,
    host_load5: Gauge<f64>,
    host_load15: Gauge<f64>,
}

impl Instruments {
    fn new(meter: &Meter) -> Self {
        Self {
            events: meter
                .u64_counter("wora.o11y.events")
                .with_description("WORA observability events received")
                .build(),
            restart_count: meter.u64_gauge("wora.runtime.restart_count").build(),
            backlog_capacity: meter.u64_gauge("wora.runtime.event_backlog.capacity").build(),
            backlog_max_capacity: meter.u64_gauge("wora.runtime.event_backlog.max_capacity").build(),
            health: meter.i64_gauge("wora.runtime.health.state").build(),
            readiness: meter.i64_gauge("wora.runtime.readiness.state").build(),
            leadership: meter.i64_gauge("wora.runtime.leadership.state").build(),
            process_memory: meter.u64_gauge("wora.process.memory").with_unit("By").build(),
            process_virtual_memory: meter.u64_gauge("wora.process.virtual_memory").with_unit("By").build(),
            process_cpu_ratio: meter.f64_gauge("wora.process.cpu.usage").with_unit("1").build(),
            process_start_time: meter.u64_gauge("wora.process.start_time").with_unit("s").build(),
            host_memory_total: meter.u64_gauge("wora.host.memory.total").with_unit("By").build(),
            host_memory_used: meter.u64_gauge("wora.host.memory.used").with_unit("By").build(),
            host_swap_total: meter.u64_gauge("wora.host.swap.total").with_unit("By").build(),
            host_swap_used: meter.u64_gauge("wora.host.swap.used").with_unit("By").build(),
            host_load1: meter.f64_gauge("wora.host.load1").build(),
            host_load5: meter.f64_gauge("wora.host.load5").build(),
            host_load15: meter.f64_gauge("wora.host.load15").build(),
        }
    }
}

/// A synchronous metrics sink backed by an OpenTelemetry [`Meter`].
pub struct OpenTelemetrySink {
    instruments: Instruments,
    attributes: Vec<KeyValue>,
    // The OTLP builder stores its provider here so the pipeline remains alive.
    pub(crate) provider: Option<opentelemetry_sdk::metrics::SdkMeterProvider>,
}

impl OpenTelemetrySink {
    /// Create a sink using an application-provided meter.
    pub fn new(meter: Meter) -> Self {
        Self {
            instruments: Instruments::new(&meter),
            attributes: Vec::new(),
            provider: None,
        }
    }

    /// Add a static attribute to every recorded metric.
    pub fn with_metric_attribute(mut self, key: &'static str, value: impl Into<String>) -> Self {
        self.attributes.push(KeyValue::new(key, value.into()));
        self
    }
}

fn kind_name<T>(kind: &O11yEventKind<T>) -> &'static str {
    match kind {
        O11yEventKind::Init(_) => "init",
        O11yEventKind::Finish => "finish",
        O11yEventKind::Flush => "flush",
        O11yEventKind::Clear => "clear",
        O11yEventKind::Reconnect => "reconnect",
        O11yEventKind::Status(_, _) => "status",
        O11yEventKind::HostInfo(_) => "host_info",
        O11yEventKind::HostStats(_) => "host_stats",
        O11yEventKind::ProcessStats(_) => "process_stats",
        O11yEventKind::RuntimeMetrics(_) => "runtime_metrics",
        O11yEventKind::Span(_, _) => "span",
        O11yEventKind::Log(_, _, _) => "log",
        O11yEventKind::App(_) => "app",
    }
}

#[async_trait::async_trait]
impl<T: Debug + Send + Sync + 'static> O11ySink<T> for OpenTelemetrySink {
    async fn handle_event(&mut self, event: &O11yEvent<T>) -> Result<(), O11ySinkError> {
        let mut event_attributes = self.attributes.clone();
        event_attributes.push(KeyValue::new("event.kind", kind_name(&event.kind)));
        self.instruments.events.add(1, &event_attributes);
        let attrs = &self.attributes;
        match &event.kind {
            O11yEventKind::Status(capacity, maximum) => {
                self.instruments.backlog_capacity.record(*capacity as u64, attrs);
                self.instruments.backlog_max_capacity.record(*maximum as u64, attrs);
            }
            O11yEventKind::RuntimeMetrics(value) => {
                self.instruments.restart_count.record(u64::from(value.restart_count), attrs);
                self.instruments.backlog_capacity.record(value.event_backlog_capacity as u64, attrs);
                self.instruments.backlog_max_capacity.record(value.event_backlog_max_capacity as u64, attrs);
                self.instruments.health.record(health_value(&value.health), attrs);
                self.instruments.readiness.record(readiness_value(&value.readiness), attrs);
                self.instruments.leadership.record(leadership_value(&value.leadership), attrs);
            }
            O11yEventKind::ProcessStats(value) => {
                self.instruments.process_memory.record(value.memory, attrs);
                self.instruments.process_virtual_memory.record(value.virtual_memory, attrs);
                self.instruments.process_cpu_ratio.record(f64::from(value.cpu_usage) / 100.0, attrs);
                self.instruments.process_start_time.record(value.start_time, attrs);
            }
            O11yEventKind::HostStats(value) => {
                self.instruments.host_memory_total.record(value.memory.total, attrs);
                self.instruments.host_memory_used.record(value.memory.used, attrs);
                self.instruments.host_swap_total.record(value.swap.total, attrs);
                self.instruments.host_swap_used.record(value.swap.used, attrs);
                self.instruments.host_load1.record(value.load.one, attrs);
                self.instruments.host_load5.record(value.load.five, attrs);
                self.instruments.host_load15.record(value.load.fifteen, attrs);
            }
            _ => {}
        }
        Ok(())
    }

    async fn flush(&mut self) -> Result<(), O11ySinkError> {
        if let Some(provider) = &self.provider {
            provider.force_flush().map_err(|error| O11ySinkError::Backend(error.to_string()))?;
        }
        Ok(())
    }
}
