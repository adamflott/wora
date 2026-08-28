use std::collections::HashMap;

use prometheus::{CounterVec, Gauge, IntGauge, Opts, Registry};

use crate::ExporterError;

pub(crate) struct Metrics {
    pub events: CounterVec,
    pub restart_count: IntGauge,
    pub backlog_capacity: IntGauge,
    pub backlog_max_capacity: IntGauge,
    pub health: IntGauge,
    pub readiness: IntGauge,
    pub leadership: IntGauge,
    pub process_memory: IntGauge,
    pub process_virtual_memory: IntGauge,
    pub process_cpu_ratio: Gauge,
    pub process_start_time: IntGauge,
    pub host_memory_total: IntGauge,
    pub host_memory_used: IntGauge,
    pub host_swap_total: IntGauge,
    pub host_swap_used: IntGauge,
    pub host_load1: Gauge,
    pub host_load5: Gauge,
    pub host_load15: Gauge,
}

impl Metrics {
    pub fn register(registry: &Registry, prefix: &str, labels: &HashMap<String, String>) -> Result<Self, ExporterError> {
        let opts = |name: &str, help: &str| Opts::new(format!("{prefix}_{name}"), help).const_labels(labels.clone());
        let events = CounterVec::new(opts("o11y_events_total", "WORA observability events received."), &["kind"])?;
        let restart_count = IntGauge::with_opts(opts("runtime_restart_count", "Latest runtime restart count."))?;
        let backlog_capacity = IntGauge::with_opts(opts("runtime_event_backlog_capacity", "Current remaining event channel capacity."))?;
        let backlog_max_capacity = IntGauge::with_opts(opts("runtime_event_backlog_max_capacity", "Maximum event channel capacity."))?;
        let health = IntGauge::with_opts(opts(
            "runtime_health_state",
            "Health state: unknown=0, ok=1, suspended=2, try_again=3, failed=4.",
        ))?;
        let readiness = IntGauge::with_opts(opts(
            "runtime_readiness_state",
            "Readiness state: unknown=0, not_ready=1, ready=2, stopping=3, draining=4.",
        ))?;
        let leadership = IntGauge::with_opts(opts("runtime_leadership_state", "Leadership state: unknown=0, follower=1, leader=2."))?;
        let process_memory = IntGauge::with_opts(opts("process_memory_bytes", "Resident process memory in bytes."))?;
        let process_virtual_memory = IntGauge::with_opts(opts("process_virtual_memory_bytes", "Virtual process memory in bytes."))?;
        let process_cpu_ratio = Gauge::with_opts(opts("process_cpu_usage_ratio", "Process CPU usage as a ratio."))?;
        let process_start_time = IntGauge::with_opts(opts("process_start_time_unix_seconds", "Process start time in Unix seconds."))?;
        let host_memory_total = IntGauge::with_opts(opts("host_memory_total_bytes", "Total host memory in bytes."))?;
        let host_memory_used = IntGauge::with_opts(opts("host_memory_used_bytes", "Used host memory in bytes."))?;
        let host_swap_total = IntGauge::with_opts(opts("host_swap_total_bytes", "Total host swap in bytes."))?;
        let host_swap_used = IntGauge::with_opts(opts("host_swap_used_bytes", "Used host swap in bytes."))?;
        let host_load1 = Gauge::with_opts(opts("host_load1", "One-minute host load average."))?;
        let host_load5 = Gauge::with_opts(opts("host_load5", "Five-minute host load average."))?;
        let host_load15 = Gauge::with_opts(opts("host_load15", "Fifteen-minute host load average."))?;

        macro_rules! register {
            ($($metric:expr),+ $(,)?) => { $(registry.register(Box::new($metric.clone()))?;)+ };
        }
        register!(
            events,
            restart_count,
            backlog_capacity,
            backlog_max_capacity,
            health,
            readiness,
            leadership,
            process_memory,
            process_virtual_memory,
            process_cpu_ratio,
            process_start_time,
            host_memory_total,
            host_memory_used,
            host_swap_total,
            host_swap_used,
            host_load1,
            host_load5,
            host_load15
        );
        Ok(Self {
            events,
            restart_count,
            backlog_capacity,
            backlog_max_capacity,
            health,
            readiness,
            leadership,
            process_memory,
            process_virtual_memory,
            process_cpu_ratio,
            process_start_time,
            host_memory_total,
            host_memory_used,
            host_swap_total,
            host_swap_used,
            host_load1,
            host_load5,
            host_load15,
        })
    }
}
