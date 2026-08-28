use std::collections::HashMap;
use std::fmt::Debug;

use prometheus::Registry;
use wora::o11y::{O11yEvent, O11yEventKind, O11ySink, O11ySinkError};

use super::registry::Metrics;
use crate::ExporterError;
use crate::labels::{health_value, leadership_value, readiness_value};

/// A stateful Prometheus sink which exposes the latest WORA snapshots.
pub struct PrometheusSink {
    metrics: Metrics,
}

impl PrometheusSink {
    /// Create a fallible builder which registers all collectors during build.
    pub fn builder(registry: Registry) -> PrometheusSinkBuilder {
        PrometheusSinkBuilder {
            registry,
            prefix: "wora".into(),
            static_labels: HashMap::new(),
        }
    }
}

/// Builder for a [`PrometheusSink`].
pub struct PrometheusSinkBuilder {
    registry: Registry,
    prefix: String,
    static_labels: HashMap<String, String>,
}

impl PrometheusSinkBuilder {
    /// Set the metric-name prefix. A trailing underscore is removed.
    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into().trim_end_matches('_').to_owned();
        self
    }

    /// Add a sparse, static Prometheus label to all collectors.
    pub fn static_label(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.static_labels.insert(key.into(), value.into());
        self
    }

    /// Validate configuration and register all metric collectors.
    pub fn build(self) -> Result<PrometheusSink, ExporterError> {
        if self.prefix.is_empty() {
            return Err(ExporterError::InvalidConfiguration("Prometheus prefix cannot be empty".into()));
        }
        let metrics = Metrics::register(&self.registry, &self.prefix, &self.static_labels)?;
        Ok(PrometheusSink { metrics })
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
impl<T: Debug + Send + Sync + 'static> O11ySink<T> for PrometheusSink {
    async fn handle_event(&mut self, event: &O11yEvent<T>) -> Result<(), O11ySinkError> {
        let metrics = &self.metrics;
        metrics.events.with_label_values(&[kind_name(&event.kind)]).inc();
        match &event.kind {
            O11yEventKind::Status(capacity, maximum) => {
                metrics.backlog_capacity.set(*capacity as i64);
                metrics.backlog_max_capacity.set(*maximum as i64);
            }
            O11yEventKind::RuntimeMetrics(value) => {
                metrics.restart_count.set(i64::from(value.restart_count));
                metrics.backlog_capacity.set(value.event_backlog_capacity as i64);
                metrics.backlog_max_capacity.set(value.event_backlog_max_capacity as i64);
                metrics.health.set(health_value(&value.health));
                metrics.readiness.set(readiness_value(&value.readiness));
                metrics.leadership.set(leadership_value(&value.leadership));
            }
            O11yEventKind::ProcessStats(value) => {
                metrics.process_memory.set(value.memory as i64);
                metrics.process_virtual_memory.set(value.virtual_memory as i64);
                metrics.process_cpu_ratio.set(f64::from(value.cpu_usage) / 100.0);
                metrics.process_start_time.set(value.start_time as i64);
            }
            O11yEventKind::HostStats(value) => {
                metrics.host_memory_total.set(value.memory.total as i64);
                metrics.host_memory_used.set(value.memory.used as i64);
                metrics.host_swap_total.set(value.swap.total as i64);
                metrics.host_swap_used.set(value.swap.used as i64);
                metrics.host_load1.set(value.load.one);
                metrics.host_load5.set(value.load.five);
                metrics.host_load15.set(value.load.fifteen);
            }
            _ => {}
        }
        Ok(())
    }

    async fn clear(&mut self) -> Result<(), O11ySinkError> {
        let metrics = &self.metrics;
        metrics.events.reset();
        metrics.restart_count.set(0);
        metrics.backlog_capacity.set(0);
        metrics.backlog_max_capacity.set(0);
        metrics.health.set(0);
        metrics.readiness.set(0);
        metrics.leadership.set(0);
        metrics.process_memory.set(0);
        metrics.process_virtual_memory.set(0);
        metrics.process_cpu_ratio.set(0.0);
        metrics.process_start_time.set(0);
        metrics.host_memory_total.set(0);
        metrics.host_memory_used.set(0);
        metrics.host_swap_total.set(0);
        metrics.host_swap_used.set(0);
        metrics.host_load1.set(0.0);
        metrics.host_load5.set(0.0);
        metrics.host_load15.set(0.0);
        Ok(())
    }
}
