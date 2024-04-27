use std::collections::HashMap;
use std::fmt::Debug;
use std::path::PathBuf;

use chrono::{DateTime, Local};
use derive_builder::Builder;
use derive_getters::Getters;
use procfs;
use procfs::ProcError;
use serde::Serialize;
use sysinfo::{Networks, System};
use thiserror::Error;
use tokio::sync::mpsc::Sender;
use tracing::{error, Level};
use tracing_subscriber::Layer;

pub struct MetricEvent<T> {
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub kind: MetricEventKind<T>,
}

pub fn meinit<T>() -> MetricEvent<T> {
    MetricEvent {
        timestamp: chrono::Utc::now(),
        kind: MetricEventKind::Init,
    }
}
pub fn mefinish<T>() -> MetricEvent<T> {
    MetricEvent {
        timestamp: chrono::Utc::now(),
        kind: MetricEventKind::Finish,
    }
}
pub fn meflush<T>() -> MetricEvent<T> {
    MetricEvent {
        timestamp: chrono::Utc::now(),
        kind: MetricEventKind::Flush,
    }
}
pub fn mestatus<T>(cap: usize, max_cap: usize) -> MetricEvent<T> {
    MetricEvent {
        timestamp: chrono::Utc::now(),
        kind: MetricEventKind::Status(cap, max_cap),
    }
}
pub fn melog<T>(lvl: Level, target: String, name: String) -> MetricEvent<T> {
    MetricEvent {
        timestamp: chrono::Utc::now(),
        kind: MetricEventKind::Log(lvl, target, name),
    }
}
pub fn meapp<T>(m: T) -> MetricEvent<T> {
    MetricEvent {
        timestamp: chrono::Utc::now(),
        kind: MetricEventKind::App(m),
    }
}
pub enum MetricEventKind<T> {
    Init,
    Finish,
    Flush,
    Reconnect,
    Status(usize, usize),

    HostInfo(HostInfo),
    HostStats(HostStats),

    Log(Level, String, String),

    App(T),
}

#[derive(Debug)]
pub enum MetricValue {
    Counter(u64),
}

impl Default for MetricValue {
    fn default() -> Self {
        MetricValue::Counter(0)
    }
}

impl MetricValue {
    pub fn inc(&mut self) {
        match self {
            MetricValue::Counter(v) => *v += 1,
        }
    }
}

#[derive(Debug, Builder, Getters)]
pub struct MetricsProcessorOptions<T> {
    sender: Sender<MetricEvent<T>>,
    flush_interval: std::time::Duration,
    status_interval: std::time::Duration,
    host_stats_interval: std::time::Duration,
}

struct MEVisitor<T>(Level, Sender<MetricEvent<T>>);

impl<T> tracing::field::Visit for MEVisitor<T> {
    fn record_error(&mut self, field: &tracing::field::Field, value: &(dyn std::error::Error + 'static)) {
        let _ = self.1.try_send(melog(self.0, "".to_string(), format!("{} {:?}", field.name(), value)));
    }

    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        let _ = self.1.try_send(melog(self.0, "".to_string(), format!("{} {:?}", field.name(), value)));
    }
}
pub struct Observability<T> {
    pub tx: Sender<MetricEvent<T>>,
    pub level: Level,
}

impl<S, T: Send + Sync + 'static> Layer<S> for Observability<T>
where
    S: tracing::Subscriber,
    S: for<'lookup> tracing_subscriber::registry::LookupSpan<'lookup>,
{
    fn on_event(&self, event: &tracing::Event<'_>, ctx: tracing_subscriber::layer::Context<'_, S>) {
        // TODO support spans
        if event.metadata().is_span() {
            match ctx.event_span(event) {
                None => {}
                Some(_parent_span) => {}
            }

            match ctx.event_scope(event) {
                None => {}
                Some(scope) => for _span in scope.from_root() {},
            }
        } else {
            // TODO add support for fields?
            for _field in event.fields() {}
            let lvl = event.metadata().level().clone();
            let _ = self
                .tx
                .try_send(melog(lvl.clone(), event.metadata().target().to_string(), event.metadata().name().to_string()));

            let mut visitor = MEVisitor(lvl, self.tx.clone());
            event.record(&mut visitor);
        }
    }
}

#[derive(Error, Debug)]
#[error(transparent)]
pub enum MetricError {
    #[error("procfs")]
    ProcFs(#[from] ProcError),
    #[error("unsupported os {0}")]
    UnsupportedOS(String),
}
#[derive(Default, Clone, Debug, Serialize)]
pub enum SupportedOSes {
    Linux,
    OSX,
    #[default]
    Unknown,
}

#[derive(Clone, Default, Debug, Serialize)]
pub struct Cpu {
    name: String,
    brand: String,
    freq: u64,
    usage: f32,
}

#[derive(Clone, Default, Debug, Serialize)]
pub struct MemStats {
    pub total: u64,
    pub free: u64,
    pub used: u64,
}

#[derive(Clone, Default, Debug, Serialize)]
pub struct SwapStats {
    pub total: u64,
    pub used: u64,
    pub free: u64,
}

#[derive(Clone, Default, Debug, Serialize)]
pub struct LoadAvg {
    pub one: f64,
    pub five: f64,
    pub fifteen: f64,
}

#[derive(Clone, Debug, Serialize)]
pub struct Disk {
    pub name: String,
    pub kind: String,
    pub file_system: String,
    pub mount_point: PathBuf,
    pub total_space: u64,
    pub available_space: u64,
    pub is_removable: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct NetIO {
    pub received: u64,
    pub total_received: u64,
    pub transmitted: u64,
    pub total_transmitted: u64,
    pub packets_received: u64,
    pub total_packets_received: u64,
    pub packets_transmitted: u64,
    pub total_packets_transmitted: u64,
    pub errors_on_received: u64,
    pub total_errors_on_received: u64,
    pub errors_on_transmitted: u64,
    pub total_errors_on_transmitted: u64,
}

#[derive(Debug, Getters)]
pub struct Host {
    sys: System,
    pub info: HostInfo,
    pub stats: HostStats,
}

impl Host {
    pub fn new() -> Result<Self, MetricError> {
        let mut sys = sysinfo::System::new_all();
        sys.refresh_all();

        let info = HostInfo::new(&sys)?;
        let stats = HostStats::new(&sys);

        Ok(Self { sys, info, stats })
    }
}
/// System stats/information from `sysinfo`
#[derive(Default, Debug, Serialize, Getters)]
pub struct HostStats {
    pub cpu: Vec<Cpu>,
    pub memory: MemStats,
    pub load: LoadAvg,
    pub swap: SwapStats,
    pub fs: Vec<Disk>,
    pub net_io: HashMap<String, NetIO>,
}

impl HostStats {
    pub fn new(sys: &System) -> Self {
        let mut cpus = vec![];
        for cpu in sys.cpus() {
            cpus.push(Cpu {
                name: cpu.name().to_string(),
                brand: cpu.brand().to_string(),
                freq: cpu.frequency(),
                usage: cpu.cpu_usage(),
            })
        }
        let fs = vec![];

        let mut net_io = HashMap::new();
        let networks = Networks::new_with_refreshed_list();
        for (if_name, net_data) in &networks {
            net_io.insert(
                if_name.to_string(),
                NetIO {
                    received: net_data.received(),
                    total_received: net_data.total_received(),
                    transmitted: net_data.transmitted(),
                    total_transmitted: net_data.total_transmitted(),
                    packets_received: net_data.packets_received(),
                    total_packets_received: net_data.total_packets_received(),
                    packets_transmitted: net_data.packets_transmitted(),
                    total_packets_transmitted: net_data.total_packets_transmitted(),
                    errors_on_received: net_data.errors_on_received(),
                    total_errors_on_received: net_data.total_errors_on_received(),
                    errors_on_transmitted: net_data.errors_on_transmitted(),
                    total_errors_on_transmitted: net_data.total_errors_on_transmitted(),
                },
            );
        }

        let mem_total = sys.total_memory();
        let mem_free = sys.free_memory();
        let mem_used = sys.used_memory();
        let load_avg = System::load_average();
        let swap_total = sys.total_swap();
        let swap_used = sys.used_swap();
        let swap_free = sys.free_swap();

        HostStats {
            cpu: cpus,
            memory: MemStats {
                total: mem_total,
                free: mem_free,
                used: mem_used,
            },
            load: LoadAvg {
                one: load_avg.one,
                five: load_avg.five,
                fifteen: load_avg.fifteen,
            },
            swap: SwapStats {
                total: swap_total,
                used: swap_used,
                free: swap_free,
            },
            fs,
            net_io,
        }
    }

    pub fn update(&mut self) -> Result<(), MetricError> {
        Ok(())
    }
}
#[derive(Default, Debug, Serialize, Getters)]
pub struct HostInfo {
    pub os_type: SupportedOSes,
    pub os_name: String,
    pub os_version: Option<String>,
    pub kernel_version: Option<String>,
    pub architecture: Option<String>,
    pub hostname: Option<String>,
    pub ncpus: usize,
    pub maxcpus: usize,
    pub boot_time: DateTime<Local>,
    pub boot_kernel_cmd: Option<Vec<String>>,
    pub ticks_per_sec: u64,
    pub current_process_arp_entries: Vec<procfs::net::ARPEntry>,
    pub current_process_routes: Vec<procfs::net::RouteEntry>,
    pub current_process_tcp: Vec<procfs::net::TcpNetEntry>,
    pub current_process_tcp6: Vec<procfs::net::TcpNetEntry>,
    pub current_process_udp: Vec<procfs::net::UdpNetEntry>,
    pub current_process_udp6: Vec<procfs::net::UdpNetEntry>,
    pub current_process_unix: Vec<procfs::net::UnixNetEntry>,
}

impl HostInfo {
    pub fn new(sys: &System) -> Result<Self, MetricError> {
        let os_type = match System::distribution_id().as_str() {
            "ubuntu" | "linux" | "macos" | "nixos" => SupportedOSes::Linux,
            unsupported => return Err(MetricError::UnsupportedOS(unsupported.to_string())),
        };

        let osinfo = os_info::get();

        let boot_time = procfs::boot_time()?;
        let boot_kernel_cmd = procfs::cmdline()?;
        let ticks_per_sec = procfs::ticks_per_second();

        let current_process_arp_entries = procfs::net::arp()?;
        let current_process_routes = procfs::net::route()?;
        let current_process_tcp = procfs::net::tcp()?;
        let current_process_tcp6 = procfs::net::tcp6()?;
        let current_process_udp = procfs::net::udp()?;
        let current_process_udp6 = procfs::net::udp6()?;
        let current_process_unix = procfs::net::unix()?;

        Ok(Self {
            os_type,
            os_name: System::distribution_id(),
            os_version: System::os_version(),
            kernel_version: System::kernel_version(),
            architecture: osinfo.architecture().map(|v| v.to_string()),
            hostname: System::host_name(),
            ncpus: sys.physical_core_count().unwrap_or(0),
            maxcpus: sys.cpus().len(),
            boot_time,
            boot_kernel_cmd: Some(boot_kernel_cmd),
            ticks_per_sec,
            current_process_arp_entries,
            current_process_routes,
            current_process_tcp,
            current_process_tcp6,
            current_process_udp,
            current_process_udp6,
            current_process_unix,
        })
    }

    pub fn update(&mut self, sys: &System) -> Result<(), MetricError> {
        self.ncpus = sys.physical_core_count().unwrap_or(0);
        self.maxcpus = sys.cpus().len();
        self.ticks_per_sec = procfs::ticks_per_second();
        self.current_process_arp_entries = procfs::net::arp()?;
        self.current_process_routes = procfs::net::route()?;
        self.current_process_tcp = procfs::net::tcp()?;
        self.current_process_tcp6 = procfs::net::tcp6()?;
        self.current_process_udp = procfs::net::udp()?;
        self.current_process_udp6 = procfs::net::udp6()?;
        self.current_process_unix = procfs::net::unix()?;

        Ok(())
    }
}
