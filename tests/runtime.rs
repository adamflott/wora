use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use chrono::Utc;
use tokio::sync::mpsc::Sender;
use wora::prelude::*;

type TestO11yReceiver = tokio::sync::mpsc::Receiver<O11yEvent<()>>;

fn unique_test_dir(name: &str) -> PathBuf {
    let suffix = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos();
    std::env::temp_dir().join(format!("wora-{name}-{}-{suffix}", std::process::id()))
}

fn test_dirs(root: PathBuf) -> Dirs {
    Dirs {
        root_dir: root.clone(),
        log_root_dir: root.join("log"),
        metadata_root_dir: root.join("metadata"),
        data_root_dir: root.join("data"),
        runtime_root_dir: root.join("runtime"),
        cache_root_dir: root.join("cache"),
        secrets_root_dir: root.join("secrets"),
    }
}

fn test_o11y() -> Result<O11yProcessorOptions<()>, Box<dyn std::error::Error>> {
    let (tx, _rx) = tokio::sync::mpsc::channel::<O11yEvent<()>>(16);
    let interval = Duration::from_secs(60);
    O11yProcessorOptionsBuilder::default()
        .sender(tx)
        .flush_interval(interval)
        .status_interval(interval)
        .host_stats_interval(interval)
        .build()
        .map_err(|err| std::io::Error::other(err.to_string()).into())
}

fn test_o11y_with_receiver(interval: Duration) -> Result<(O11yProcessorOptions<()>, TestO11yReceiver), Box<dyn std::error::Error>> {
    let (tx, rx) = tokio::sync::mpsc::channel::<O11yEvent<()>>(64);
    let o11y = O11yProcessorOptionsBuilder::default()
        .sender(tx)
        .flush_interval(interval)
        .status_interval(interval)
        .host_stats_interval(interval)
        .build()
        .map_err(|err| std::io::Error::other(err.to_string()))?;
    Ok((o11y, rx))
}

#[cfg(target_family = "unix")]
fn unique_socket_path(name: &str) -> PathBuf {
    let suffix = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos();
    std::env::temp_dir().join(format!("wora-{}-{}.sock", name.replace('_', "-"), suffix))
}

#[cfg(target_os = "linux")]
fn unique_abstract_socket_name(name: &str) -> String {
    let suffix = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos();
    format!("wora-{}-{}-{suffix}", name.replace('_', "-"), std::process::id())
}

#[cfg(target_family = "unix")]
fn skip_permission_denied_socket_test(err: &std::io::Error) -> bool {
    if err.kind() == std::io::ErrorKind::PermissionDenied {
        eprintln!("skipping Unix datagram notification test: {err}");
        true
    } else {
        false
    }
}

#[tokio::test]
async fn physical_vfs_creates_nested_directories() -> Result<(), Box<dyn std::error::Error>> {
    let root = unique_test_dir("vfs");
    let nested = root.join("a").join("b").join("c");
    let fs = PhysicalVFS::new();

    fs.create_dir(&nested).await?;

    assert!(nested.is_dir());
    Ok(())
}

#[derive(Clone, Debug)]
struct TestExec {
    dirs: Dirs,
}

#[async_trait]
impl AsyncExecutor<(), ()> for TestExec {
    fn id(&self) -> &'static str {
        "test"
    }

    fn dirs(&self) -> &Dirs {
        &self.dirs
    }

    async fn setup(&mut self, _wora: &Wora<(), ()>, fs: impl WFS) -> Result<(), SetupFailure> {
        for dir in [
            &self.dirs.root_dir,
            &self.dirs.log_root_dir,
            &self.dirs.metadata_root_dir,
            &self.dirs.data_root_dir,
            &self.dirs.runtime_root_dir,
            &self.dirs.cache_root_dir,
            &self.dirs.secrets_root_dir,
        ] {
            fs.create_dir(dir).await?;
        }
        Ok(())
    }

    async fn is_ready(&self, _wora: &Wora<(), ()>, _fs: impl WFS) -> bool {
        true
    }

    async fn end(&self, _wora: &Wora<(), ()>, _fs: impl WFS) {}
}

#[derive(Default)]
struct TestConfig {
    enabled: bool,
}

impl Config for TestConfig {
    type ConfigT = TestConfig;

    fn parse_main_config_file(data: String) -> Result<Self::ConfigT, Box<dyn std::error::Error>> {
        Ok(TestConfig {
            enabled: data.trim() == "enabled = true",
        })
    }
}

struct ConfiguredRestartApp {
    configured: bool,
    calls: Arc<Mutex<u8>>,
}

struct AlwaysRestartApp {
    name: &'static str,
    calls: Arc<Mutex<u8>>,
}

#[derive(Clone, Debug)]
struct ReadyMarkerExec {
    dirs: Dirs,
    ready_file: PathBuf,
}

#[async_trait]
impl AsyncExecutor<(), ()> for ReadyMarkerExec {
    fn id(&self) -> &'static str {
        "ready-marker"
    }

    fn dirs(&self) -> &Dirs {
        &self.dirs
    }

    async fn setup(&mut self, _wora: &Wora<(), ()>, fs: impl WFS) -> Result<(), SetupFailure> {
        for dir in [
            &self.dirs.root_dir,
            &self.dirs.log_root_dir,
            &self.dirs.metadata_root_dir,
            &self.dirs.data_root_dir,
            &self.dirs.runtime_root_dir,
            &self.dirs.cache_root_dir,
            &self.dirs.secrets_root_dir,
        ] {
            fs.create_dir(dir).await?;
        }
        Ok(())
    }

    async fn on_runtime_ready(&self, app_name: &str, _dirs: &Dirs, fs: impl WFS) -> Result<(), SetupFailure> {
        fs.write(&self.ready_file, format!("ready:{app_name}").as_bytes()).await?;
        Ok(())
    }

    async fn is_ready(&self, _wora: &Wora<(), ()>, _fs: impl WFS) -> bool {
        true
    }

    async fn end(&self, _wora: &Wora<(), ()>, _fs: impl WFS) {}
}

#[async_trait]
impl App<(), ()> for AlwaysRestartApp {
    type AppConfig = NoConfig;
    type AppSecrets = NoSecrets;
    type Setup = ();

    fn name(&self) -> &'static str {
        self.name
    }

    async fn setup(
        &mut self,
        _wora: &Wora<(), ()>,
        _exec: impl AsyncExecutor<(), ()>,
        _fs: impl WFS + 'static,
        _metrics: Sender<O11yEvent<()>>,
        _is_first_boot: bool,
    ) -> Result<Self::Setup, Box<dyn std::error::Error>> {
        Ok(())
    }

    async fn main(
        &mut self,
        _wora: &mut Wora<(), ()>,
        _exec: impl AsyncExecutor<(), ()>,
        _fs: impl WFS + 'static,
        _metrics: Sender<O11yEvent<()>>,
    ) -> MainRetryAction {
        let Ok(mut calls) = self.calls.lock() else {
            return MainRetryAction::UseExitCode(3);
        };
        *calls += 1;
        MainRetryAction::UseRestartPolicy
    }

    async fn end(&mut self, _wora: &Wora<(), ()>, _exec: impl AsyncExecutor<(), ()>, _fs: impl WFS + 'static, _metrics: Sender<O11yEvent<()>>) {}
}

#[async_trait]
impl App<(), ()> for ConfiguredRestartApp {
    type AppConfig = TestConfig;
    type AppSecrets = NoSecrets;
    type Setup = ();

    fn name(&self) -> &'static str {
        "configured_restart"
    }

    async fn configure(&mut self, config: TestConfig) -> Result<(), Box<dyn std::error::Error>> {
        self.configured = config.enabled;
        Ok(())
    }

    async fn setup(
        &mut self,
        _wora: &Wora<(), ()>,
        _exec: impl AsyncExecutor<(), ()>,
        _fs: impl WFS + 'static,
        _metrics: Sender<O11yEvent<()>>,
        _is_first_boot: bool,
    ) -> Result<Self::Setup, Box<dyn std::error::Error>> {
        Ok(())
    }

    async fn main(
        &mut self,
        _wora: &mut Wora<(), ()>,
        _exec: impl AsyncExecutor<(), ()>,
        _fs: impl WFS + 'static,
        _metrics: Sender<O11yEvent<()>>,
    ) -> MainRetryAction {
        if !self.configured {
            return MainRetryAction::UseExitCode(2);
        }

        let Ok(mut calls) = self.calls.lock() else {
            return MainRetryAction::UseExitCode(3);
        };
        *calls += 1;
        if *calls == 1 {
            MainRetryAction::UseRestartPolicy
        } else {
            MainRetryAction::Success
        }
    }

    async fn end(&mut self, _wora: &Wora<(), ()>, _exec: impl AsyncExecutor<(), ()>, _fs: impl WFS + 'static, _metrics: Sender<O11yEvent<()>>) {}
}

struct ControlDrivenApp {
    name: &'static str,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum SignalMappedEvent {
    ReloadAll,
}

struct SignalMappedApp;

struct DeferredReadyApp;

struct HealthFailureApp {
    calls: Arc<Mutex<u8>>,
}

#[derive(Default)]
struct ReloadingConfig {
    enabled: bool,
}

struct ReloadingSecrets;

struct ReloadingApp {
    initial_config_loaded: bool,
    current_enabled: bool,
    current_secret: String,
}

struct HelperLoopApp {
    current_enabled: bool,
}

struct VirtualWatcherApp {
    current_enabled: bool,
}

struct MetricsApp;

struct BootTrackingApp {
    calls: Arc<Mutex<Vec<bool>>>,
}

struct DelayedShutdownApp {
    delay: Duration,
}

#[derive(Clone, Debug)]
struct DrainingTrackerExec {
    dirs: Dirs,
    phases: Arc<Mutex<Vec<&'static str>>>,
    control_events: Arc<tokio::sync::Mutex<Option<tokio::sync::mpsc::Receiver<ControlEvent>>>>,
}

#[derive(Clone)]
struct DeterministicRuntimeEnvironment {
    host_info: HostInfo,
    host_stats: HostStats,
    process: Option<ProcessStats>,
}

impl RuntimeEnvironment for DeterministicRuntimeEnvironment {
    fn initial_host(&self) -> Result<Host, O11yError> {
        Ok(Host::from_parts(self.host_info.clone(), self.host_stats.clone()))
    }

    fn initial_process_stats(&self) -> Option<ProcessStats> {
        self.process.clone()
    }

    fn refresh_host_stats(&self) -> Result<Option<HostStats>, O11yError> {
        Ok(Some(self.host_stats.clone()))
    }

    fn refresh_process_stats(&self) -> Option<ProcessStats> {
        self.process.clone()
    }
}

impl DrainingTrackerExec {
    fn with_control_event_receiver(mut self, receiver: tokio::sync::mpsc::Receiver<ControlEvent>) -> Self {
        self.control_events = Arc::new(tokio::sync::Mutex::new(Some(receiver)));
        self
    }
}

#[async_trait]
impl AsyncExecutor<(), ()> for DrainingTrackerExec {
    fn id(&self) -> &'static str {
        "draining-tracker"
    }

    fn dirs(&self) -> &Dirs {
        &self.dirs
    }

    async fn setup(&mut self, _wora: &Wora<(), ()>, fs: impl WFS) -> Result<(), SetupFailure> {
        for dir in [
            &self.dirs.root_dir,
            &self.dirs.log_root_dir,
            &self.dirs.metadata_root_dir,
            &self.dirs.data_root_dir,
            &self.dirs.runtime_root_dir,
            &self.dirs.cache_root_dir,
            &self.dirs.secrets_root_dir,
        ] {
            fs.create_dir(dir).await?;
        }
        Ok(())
    }

    async fn spawn_runtime_event_sources(&self, sender: Sender<Event<()>>) -> Result<Vec<tokio::task::JoinHandle<()>>, SetupFailure> {
        let controls = self.control_events.clone();
        Ok(vec![tokio::spawn(async move {
            let Some(mut receiver) = controls.lock().await.take() else {
                return;
            };

            while let Some(event) = receiver.recv().await {
                if sender.send(Event::Control(event)).await.is_err() {
                    break;
                }
            }
        })])
    }

    async fn on_runtime_draining(&self, _app_name: &str, _dirs: &Dirs, _fs: impl WFS) -> Result<(), SetupFailure> {
        let Ok(mut phases) = self.phases.lock() else {
            return Err(SetupFailure::IO(std::io::Error::other("draining tracker lock poisoned")));
        };
        phases.push("draining");
        Ok(())
    }

    async fn is_ready(&self, _wora: &Wora<(), ()>, _fs: impl WFS) -> bool {
        true
    }

    async fn on_runtime_stopping(&self, _app_name: &str, _dirs: &Dirs, _fs: impl WFS) -> Result<(), SetupFailure> {
        let Ok(mut phases) = self.phases.lock() else {
            return Err(SetupFailure::IO(std::io::Error::other("draining tracker lock poisoned")));
        };
        phases.push("stopping");
        Ok(())
    }

    async fn end(&self, _wora: &Wora<(), ()>, _fs: impl WFS) {}
}

#[async_trait]
impl App<(), ()> for BootTrackingApp {
    type AppConfig = NoConfig;
    type AppSecrets = NoSecrets;
    type Setup = ();

    fn name(&self) -> &'static str {
        "boot_tracking"
    }

    async fn setup(
        &mut self,
        _wora: &Wora<(), ()>,
        _exec: impl AsyncExecutor<(), ()>,
        _fs: impl WFS + 'static,
        _metrics: Sender<O11yEvent<()>>,
        is_first_boot: bool,
    ) -> Result<Self::Setup, Box<dyn std::error::Error>> {
        let Ok(mut calls) = self.calls.lock() else {
            return Err(std::io::Error::other("boot tracking app lock poisoned").into());
        };
        calls.push(is_first_boot);
        Ok(())
    }

    async fn main(
        &mut self,
        _wora: &mut Wora<(), ()>,
        _exec: impl AsyncExecutor<(), ()>,
        _fs: impl WFS + 'static,
        _metrics: Sender<O11yEvent<()>>,
    ) -> MainRetryAction {
        MainRetryAction::Success
    }

    async fn end(&mut self, _wora: &Wora<(), ()>, _exec: impl AsyncExecutor<(), ()>, _fs: impl WFS + 'static, _metrics: Sender<O11yEvent<()>>) {}
}

fn deterministic_host_info() -> HostInfo {
    HostInfo {
        os_type: SupportedOSes::Linux,
        os_name: "test-os".to_string(),
        os_version: Some("1.0".to_string()),
        kernel_version: Some("1.0.0".to_string()),
        architecture: Some("aarch64".to_string()),
        hostname: Some("deterministic-host".to_string()),
        ncpus: 4,
        maxcpus: 8,
        boot_time: Utc::now(),
        #[cfg(target_os = "linux")]
        boot_kernel_cmd: Some(Vec::new()),
        #[cfg(target_os = "linux")]
        ticks_per_sec: 100,
        #[cfg(target_os = "linux")]
        current_process_arp_entries: Vec::new(),
        #[cfg(target_os = "linux")]
        current_process_routes: Vec::new(),
        #[cfg(target_os = "linux")]
        current_process_tcp: Vec::new(),
        #[cfg(target_os = "linux")]
        current_process_tcp6: Vec::new(),
        #[cfg(target_os = "linux")]
        current_process_udp: Vec::new(),
        #[cfg(target_os = "linux")]
        current_process_udp6: Vec::new(),
        #[cfg(target_os = "linux")]
        current_process_unix: Vec::new(),
    }
}

fn deterministic_host_stats() -> HostStats {
    HostStats {
        cpu: Vec::new(),
        memory: MemStats { total: 10, free: 4, used: 6 },
        load: LoadAvg {
            one: 0.1,
            five: 0.2,
            fifteen: 0.3,
        },
        swap: SwapStats { total: 20, used: 5, free: 15 },
        fs: Vec::new(),
        net_io: std::collections::HashMap::new(),
    }
}

impl Config for ReloadingConfig {
    type ConfigT = ReloadingConfig;

    fn parse_main_config_file(data: String) -> Result<Self::ConfigT, Box<dyn std::error::Error>> {
        Ok(ReloadingConfig {
            enabled: data.trim() == "enabled = true",
        })
    }
}

impl Secrets for ReloadingSecrets {
    type SecretT = String;

    fn parse_secret_file(_file_path: PathBuf, data: Vec<u8>) -> Result<Self::SecretT, Box<dyn std::error::Error>> {
        Ok(String::from_utf8(data)?.trim().to_string())
    }
}

#[async_trait]
impl App<(), ()> for ControlDrivenApp {
    type AppConfig = NoConfig;
    type AppSecrets = NoSecrets;
    type Setup = ();

    fn name(&self) -> &'static str {
        self.name
    }

    async fn setup(
        &mut self,
        _wora: &Wora<(), ()>,
        _exec: impl AsyncExecutor<(), ()>,
        _fs: impl WFS + 'static,
        _metrics: Sender<O11yEvent<()>>,
        _is_first_boot: bool,
    ) -> Result<Self::Setup, Box<dyn std::error::Error>> {
        Ok(())
    }

    async fn main(
        &mut self,
        wora: &mut Wora<(), ()>,
        _exec: impl AsyncExecutor<(), ()>,
        _fs: impl WFS + 'static,
        _metrics: Sender<O11yEvent<()>>,
    ) -> MainRetryAction {
        while let Some(event) = wora.receiver.recv().await {
            if matches!(event, Event::Control(ControlEvent::Shutdown(_))) {
                return MainRetryAction::Success;
            }
        }

        MainRetryAction::UseExitCode(9)
    }

    async fn end(&mut self, _wora: &Wora<(), ()>, _exec: impl AsyncExecutor<(), ()>, _fs: impl WFS + 'static, _metrics: Sender<O11yEvent<()>>) {}
}

#[async_trait]
impl App<SignalMappedEvent, ()> for SignalMappedApp {
    type AppConfig = NoConfig;
    type AppSecrets = NoSecrets;
    type Setup = ();

    fn name(&self) -> &'static str {
        "signal_mapped"
    }

    async fn setup(
        &mut self,
        _wora: &Wora<SignalMappedEvent, ()>,
        _exec: impl AsyncExecutor<SignalMappedEvent, ()>,
        _fs: impl WFS + 'static,
        _metrics: Sender<O11yEvent<()>>,
        _is_first_boot: bool,
    ) -> Result<Self::Setup, Box<dyn std::error::Error>> {
        Ok(())
    }

    async fn main(
        &mut self,
        wora: &mut Wora<SignalMappedEvent, ()>,
        _exec: impl AsyncExecutor<SignalMappedEvent, ()>,
        _fs: impl WFS + 'static,
        _metrics: Sender<O11yEvent<()>>,
    ) -> MainRetryAction {
        while let Some(event) = wora.receiver.recv().await {
            if matches!(event, Event::App(SignalMappedEvent::ReloadAll)) {
                return MainRetryAction::Success;
            }
        }

        MainRetryAction::UseExitCode(21)
    }

    async fn end(
        &mut self,
        _wora: &Wora<SignalMappedEvent, ()>,
        _exec: impl AsyncExecutor<SignalMappedEvent, ()>,
        _fs: impl WFS + 'static,
        _metrics: Sender<O11yEvent<()>>,
    ) {
    }
}

#[async_trait]
impl App<(), ()> for DelayedShutdownApp {
    type AppConfig = NoConfig;
    type AppSecrets = NoSecrets;
    type Setup = ();

    fn name(&self) -> &'static str {
        "delayed_shutdown"
    }

    async fn setup(
        &mut self,
        _wora: &Wora<(), ()>,
        _exec: impl AsyncExecutor<(), ()>,
        _fs: impl WFS + 'static,
        _metrics: Sender<O11yEvent<()>>,
        _is_first_boot: bool,
    ) -> Result<Self::Setup, Box<dyn std::error::Error>> {
        Ok(())
    }

    async fn main(
        &mut self,
        wora: &mut Wora<(), ()>,
        _exec: impl AsyncExecutor<(), ()>,
        _fs: impl WFS + 'static,
        _metrics: Sender<O11yEvent<()>>,
    ) -> MainRetryAction {
        while let Some(event) = wora.receiver.recv().await {
            if matches!(event, Event::Control(ControlEvent::Shutdown(_))) {
                tokio::time::sleep(self.delay).await;
                return MainRetryAction::Success;
            }
        }

        MainRetryAction::UseExitCode(13)
    }

    async fn end(&mut self, _wora: &Wora<(), ()>, _exec: impl AsyncExecutor<(), ()>, _fs: impl WFS + 'static, _metrics: Sender<O11yEvent<()>>) {}
}

#[async_trait]
impl App<(), ()> for DeferredReadyApp {
    type AppConfig = NoConfig;
    type AppSecrets = NoSecrets;
    type Setup = ();

    fn name(&self) -> &'static str {
        "deferred_ready"
    }

    async fn setup(
        &mut self,
        wora: &Wora<(), ()>,
        _exec: impl AsyncExecutor<(), ()>,
        _fs: impl WFS + 'static,
        _metrics: Sender<O11yEvent<()>>,
        _is_first_boot: bool,
    ) -> Result<Self::Setup, Box<dyn std::error::Error>> {
        wora.report_readiness(ReadinessState::NotReady);
        let status = wora.status_handle();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            status.report_readiness(ReadinessState::Ready);
        });
        Ok(())
    }

    async fn main(
        &mut self,
        _wora: &mut Wora<(), ()>,
        _exec: impl AsyncExecutor<(), ()>,
        _fs: impl WFS + 'static,
        _metrics: Sender<O11yEvent<()>>,
    ) -> MainRetryAction {
        tokio::time::sleep(Duration::from_millis(40)).await;
        MainRetryAction::Success
    }

    async fn end(&mut self, _wora: &Wora<(), ()>, _exec: impl AsyncExecutor<(), ()>, _fs: impl WFS + 'static, _metrics: Sender<O11yEvent<()>>) {}
}

#[async_trait]
impl App<(), ()> for HealthFailureApp {
    type AppConfig = NoConfig;
    type AppSecrets = NoSecrets;
    type Setup = ();

    fn name(&self) -> &'static str {
        "health_failure"
    }

    async fn setup(
        &mut self,
        wora: &Wora<(), ()>,
        _exec: impl AsyncExecutor<(), ()>,
        _fs: impl WFS + 'static,
        _metrics: Sender<O11yEvent<()>>,
        _is_first_boot: bool,
    ) -> Result<Self::Setup, Box<dyn std::error::Error>> {
        let status = wora.status_handle();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            status.report_health(HealthState::Failed);
        });
        Ok(())
    }

    async fn main(
        &mut self,
        wora: &mut Wora<(), ()>,
        _exec: impl AsyncExecutor<(), ()>,
        _fs: impl WFS + 'static,
        _metrics: Sender<O11yEvent<()>>,
    ) -> MainRetryAction {
        {
            let Ok(mut calls) = self.calls.lock() else {
                return MainRetryAction::UseExitCode(3);
            };
            *calls += 1;
        }

        while let Some(event) = wora.receiver.recv().await {
            if matches!(event, Event::Control(ControlEvent::Shutdown(_))) {
                return MainRetryAction::UseRestartPolicy;
            }
        }

        MainRetryAction::UseExitCode(8)
    }

    async fn end(&mut self, _wora: &Wora<(), ()>, _exec: impl AsyncExecutor<(), ()>, _fs: impl WFS + 'static, _metrics: Sender<O11yEvent<()>>) {}
}

#[async_trait]
impl App<(), ()> for ReloadingApp {
    type AppConfig = ReloadingConfig;
    type AppSecrets = ReloadingSecrets;
    type Setup = ();

    fn name(&self) -> &'static str {
        "reloading"
    }

    async fn reload_config(&mut self, reload: ConfigReload<ReloadingConfig>) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(config) = reload.main {
            self.initial_config_loaded = true;
            self.current_enabled = config.enabled;
        }
        Ok(())
    }

    async fn reload_secrets(&mut self, reload: SecretReload<String>) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(secret) = reload.files.into_iter().find(|file| file.key == "api_key") {
            self.current_secret = secret.value;
        }
        Ok(())
    }

    async fn setup(
        &mut self,
        _wora: &Wora<(), ()>,
        _exec: impl AsyncExecutor<(), ()>,
        _fs: impl WFS + 'static,
        _metrics: Sender<O11yEvent<()>>,
        _is_first_boot: bool,
    ) -> Result<Self::Setup, Box<dyn std::error::Error>> {
        assert!(self.initial_config_loaded);
        assert_eq!(self.current_secret, "alpha");
        Ok(())
    }

    async fn main(
        &mut self,
        wora: &mut Wora<(), ()>,
        _exec: impl AsyncExecutor<(), ()>,
        fs: impl WFS + 'static,
        _metrics: Sender<O11yEvent<()>>,
    ) -> MainRetryAction {
        let metadata_file = wora.dirs.metadata_root_dir.join("reloading.toml");
        let secret_file = wora.dirs.secrets_root_dir.join("api_key");
        let sender = wora.sender.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            let _ = tokio::fs::write(&metadata_file, "enabled = true").await;
            let _ = tokio::fs::write(&secret_file, "bravo").await;
            let config_event = ConfigChange::new(ChangeKind::Modified, metadata_file.clone(), vec![metadata_file]);
            let secret_event = SecretChange::new(ChangeKind::Modified, vec![secret_file]);
            let _ = sender.send(Event::ConfigChanged(config_event)).await;
            let _ = sender.send(Event::SecretChanged(secret_event)).await;
        });

        while let Some(event) = wora.receiver.recv().await {
            let _ = wora.apply_reload_event(self, fs.clone(), &event).await;
            if self.current_enabled && self.current_secret == "bravo" {
                return MainRetryAction::Success;
            }
        }

        MainRetryAction::UseExitCode(10)
    }

    async fn end(&mut self, _wora: &Wora<(), ()>, _exec: impl AsyncExecutor<(), ()>, _fs: impl WFS + 'static, _metrics: Sender<O11yEvent<()>>) {}
}

#[async_trait]
impl App<(), ()> for HelperLoopApp {
    type AppConfig = ReloadingConfig;
    type AppSecrets = NoSecrets;
    type Setup = ();

    fn name(&self) -> &'static str {
        "helper_loop"
    }

    async fn reload_config(&mut self, reload: ConfigReload<ReloadingConfig>) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(config) = reload.main {
            self.current_enabled = config.enabled;
        }
        Ok(())
    }

    async fn setup(
        &mut self,
        wora: &Wora<(), ()>,
        _exec: impl AsyncExecutor<(), ()>,
        _fs: impl WFS + 'static,
        _metrics: Sender<O11yEvent<()>>,
        _is_first_boot: bool,
    ) -> Result<Self::Setup, Box<dyn std::error::Error>> {
        let metadata_file = wora.dirs.metadata_root_dir.join("helper_loop.toml");
        let sender = wora.sender.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            let _ = tokio::fs::write(&metadata_file, "enabled = true").await;
            let event = ConfigChange::new(ChangeKind::Modified, metadata_file.clone(), vec![metadata_file]);
            let _ = sender.send(Event::ConfigChanged(event)).await;
        });
        Ok(())
    }

    async fn main(
        &mut self,
        wora: &mut Wora<(), ()>,
        _exec: impl AsyncExecutor<(), ()>,
        fs: impl WFS + 'static,
        _metrics: Sender<O11yEvent<()>>,
    ) -> MainRetryAction {
        match wora
            .run_event_loop(self, fs, |app, _wora, event| match event {
                Event::ConfigChanged(change) if app.current_enabled && change.main_config_changed => EventLoopAction::Exit(MainRetryAction::Success),
                _ => EventLoopAction::Continue,
            })
            .await
        {
            Ok(action) => action,
            Err(_) => MainRetryAction::UseExitCode(11),
        }
    }

    async fn end(&mut self, _wora: &Wora<(), ()>, _exec: impl AsyncExecutor<(), ()>, _fs: impl WFS + 'static, _metrics: Sender<O11yEvent<()>>) {}
}

#[async_trait]
impl App<(), ()> for VirtualWatcherApp {
    type AppConfig = ReloadingConfig;
    type AppSecrets = NoSecrets;
    type Setup = ();

    fn name(&self) -> &'static str {
        "virtual_watcher"
    }

    async fn reload_config(&mut self, reload: ConfigReload<ReloadingConfig>) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(config) = reload.main {
            self.current_enabled = config.enabled;
        }
        Ok(())
    }

    async fn setup(
        &mut self,
        _wora: &Wora<(), ()>,
        _exec: impl AsyncExecutor<(), ()>,
        _fs: impl WFS + 'static,
        _metrics: Sender<O11yEvent<()>>,
        _is_first_boot: bool,
    ) -> Result<Self::Setup, Box<dyn std::error::Error>> {
        Ok(())
    }

    async fn main(
        &mut self,
        wora: &mut Wora<(), ()>,
        _exec: impl AsyncExecutor<(), ()>,
        fs: impl WFS + 'static,
        _metrics: Sender<O11yEvent<()>>,
    ) -> MainRetryAction {
        let metadata_file = wora.dirs.metadata_root_dir.join("virtual_watcher.toml");
        let write_fs = fs.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            let _ = write_fs.write(&metadata_file, b"enabled = true").await;
        });

        match wora
            .run_event_loop(self, fs, |app, _wora, event| match event {
                Event::ConfigChanged(change) if app.current_enabled && change.main_config_changed => EventLoopAction::Exit(MainRetryAction::Success),
                _ => EventLoopAction::Continue,
            })
            .await
        {
            Ok(action) => action,
            Err(_) => MainRetryAction::UseExitCode(78),
        }
    }

    async fn end(&mut self, _wora: &Wora<(), ()>, _exec: impl AsyncExecutor<(), ()>, _fs: impl WFS + 'static, _metrics: Sender<O11yEvent<()>>) {}
}

#[async_trait]
impl App<(), ()> for MetricsApp {
    type AppConfig = NoConfig;
    type AppSecrets = NoSecrets;
    type Setup = ();

    fn name(&self) -> &'static str {
        "metrics_app"
    }

    async fn setup(
        &mut self,
        _wora: &Wora<(), ()>,
        _exec: impl AsyncExecutor<(), ()>,
        _fs: impl WFS + 'static,
        _metrics: Sender<O11yEvent<()>>,
        _is_first_boot: bool,
    ) -> Result<Self::Setup, Box<dyn std::error::Error>> {
        Ok(())
    }

    async fn main(
        &mut self,
        _wora: &mut Wora<(), ()>,
        _exec: impl AsyncExecutor<(), ()>,
        _fs: impl WFS + 'static,
        _metrics: Sender<O11yEvent<()>>,
    ) -> MainRetryAction {
        tokio::time::sleep(Duration::from_millis(35)).await;
        MainRetryAction::Success
    }

    async fn end(&mut self, _wora: &Wora<(), ()>, _exec: impl AsyncExecutor<(), ()>, _fs: impl WFS + 'static, _metrics: Sender<O11yEvent<()>>) {}
}

#[tokio::test]
async fn runner_loads_initial_config_and_applies_restart_policy() -> Result<(), Box<dyn std::error::Error>> {
    let root = unique_test_dir("runner");
    let dirs = test_dirs(root.clone());
    std::fs::create_dir_all(&dirs.metadata_root_dir)?;
    std::fs::create_dir_all(&dirs.runtime_root_dir)?;
    std::fs::write(dirs.metadata_root_dir.join("configured_restart.toml"), "enabled = true")?;

    let calls = Arc::new(Mutex::new(0));
    let app = ConfiguredRestartApp {
        configured: false,
        calls: calls.clone(),
    };
    let exec = TestExec { dirs };
    let fs = PhysicalVFS::new();
    let o11y = test_o11y()?;

    exec_async_runner_with_options(
        exec,
        app,
        fs,
        o11y,
        RunnerOptions::new()
            .with_boot_dir(root.join("boot"))
            .with_restart_options(RestartPolicyOptions {
                policy: WorkloadRestartPolicy::RetryInstantly,
                pause: Duration::from_millis(1),
                ..RestartPolicyOptions::default()
            }),
    )
    .await
    .map_err(|err| std::io::Error::other(err.to_string()))?;

    let calls = calls.lock().map_err(|err| std::io::Error::other(err.to_string()))?;
    assert_eq!(*calls, 2);
    Ok(())
}

#[tokio::test]
async fn executor_runtime_event_sources_can_drive_control_flow() -> Result<(), Box<dyn std::error::Error>> {
    let root = unique_test_dir("control-event-runtime-sources");
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    let exec = ContainerExecutor::new("control_event").await.with_control_event_receiver(rx);

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(10)).await;
        let _ = tx.send(ControlEvent::Shutdown(None)).await;
    });

    tokio::time::timeout(
        Duration::from_secs(2),
        exec_async_runner_with_options(
            exec,
            ControlDrivenApp {
                name: "control_event_runtime_sources",
            },
            PhysicalVFS::new(),
            test_o11y()?,
            RunnerOptions::new()
                .with_boot_dir(root.join("boot"))
                .with_lock_backend(InMemoryLockBackend::default()),
        ),
    )
    .await
    .map_err(|_| std::io::Error::other("runtime event source test timed out"))?
    .map_err(|err| std::io::Error::other(err.to_string()))?;

    Ok(())
}

#[tokio::test]
async fn runner_options_map_runtime_signals_to_app_events() -> Result<(), Box<dyn std::error::Error>> {
    let root = unique_test_dir("signal-mapper");
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    let exec = ContainerExecutor::new("signal_mapped").await.with_signal_receiver(rx);

    tx.send(RuntimeSignal::Hangup).await?;
    drop(tx);

    exec_async_runner_with_options(
        exec,
        SignalMappedApp,
        PhysicalVFS::new(),
        test_o11y()?,
        RunnerOptions::new()
            .with_boot_dir(root.join("boot"))
            .with_signal_mapper(|signal| match signal {
                RuntimeSignal::Hangup => Some(Event::App(SignalMappedEvent::ReloadAll)),
                RuntimeSignal::Terminate | RuntimeSignal::Interrupt | RuntimeSignal::Quit => {
                    Some(Event::Control(ControlEvent::Shutdown(Some(Utc::now().naive_utc()))))
                }
                RuntimeSignal::User1 | RuntimeSignal::User2 => None,
            })
            .with_lock_backend(InMemoryLockBackend::default()),
    )
    .await
    .map_err(|err| std::io::Error::other(err.to_string()))?;

    Ok(())
}

#[tokio::test]
async fn delayed_readiness_reports_trigger_executor_ready_hook() -> Result<(), Box<dyn std::error::Error>> {
    let root = unique_test_dir("deferred-ready");
    let ready_file = root.join("runtime").join("ready.marker");
    let dirs = test_dirs(root.clone());
    std::fs::create_dir_all(&dirs.runtime_root_dir)?;

    exec_async_runner_with_options(
        ReadyMarkerExec {
            dirs,
            ready_file: ready_file.clone(),
        },
        DeferredReadyApp,
        PhysicalVFS::new(),
        test_o11y()?,
        RunnerOptions::new().with_boot_dir(root.join("boot")),
    )
    .await
    .map_err(|err| std::io::Error::other(err.to_string()))?;

    assert_eq!(std::fs::read_to_string(ready_file)?, "ready:deferred_ready");
    Ok(())
}

#[tokio::test]
async fn failed_health_can_trigger_restart_policy() -> Result<(), Box<dyn std::error::Error>> {
    let root = unique_test_dir("health-restart");
    let dirs = test_dirs(root.clone());
    std::fs::create_dir_all(&dirs.runtime_root_dir)?;

    let calls = Arc::new(Mutex::new(0));
    let rc = exec_async_runner_with_options(
        TestExec { dirs },
        HealthFailureApp { calls: calls.clone() },
        PhysicalVFS::new(),
        test_o11y()?,
        RunnerOptions::new()
            .with_boot_dir(root.join("boot"))
            .with_restart_options(RestartPolicyOptions {
                policy: WorkloadRestartPolicy::RetryInstantly,
                max_retries: Some(1),
                supervision: SupervisionOptions {
                    shutdown_grace_period: Duration::from_millis(50),
                    unhealthy_action: UnhealthyAction::UseRestartPolicy,
                    ..SupervisionOptions::default()
                },
                ..RestartPolicyOptions::default()
            }),
    )
    .await;

    let calls = calls.lock().map_err(|err| std::io::Error::other(err.to_string()))?;
    assert!(matches!(rc, Err(MainEarlyReturn::UseExitCode(1))));
    assert_eq!(*calls, 2);
    Ok(())
}

#[tokio::test]
async fn shutdown_request_enters_draining_before_stopping() -> Result<(), Box<dyn std::error::Error>> {
    let root = unique_test_dir("draining-before-stopping");
    let dirs = test_dirs(root.clone());
    let phases = Arc::new(Mutex::new(Vec::new()));
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    let exec = DrainingTrackerExec {
        dirs,
        phases: phases.clone(),
        control_events: Arc::new(tokio::sync::Mutex::new(None)),
    }
    .with_control_event_receiver(rx);
    let fs = PhysicalVFS::new();

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(10)).await;
        let _ = tx.send(ControlEvent::Shutdown(None)).await;
    });

    let start = std::time::Instant::now();
    exec_async_runner_with_options(
        exec,
        DelayedShutdownApp {
            delay: Duration::from_millis(40),
        },
        fs,
        test_o11y()?,
        RunnerOptions::new()
            .with_boot_dir(root.join("boot"))
            .with_restart_options(RestartPolicyOptions {
                supervision: SupervisionOptions {
                    drain_grace_period: Duration::from_millis(20),
                    shutdown_grace_period: Duration::from_millis(50),
                    ..SupervisionOptions::default()
                },
                ..RestartPolicyOptions::default()
            })
            .with_lock_backend(InMemoryLockBackend::default()),
    )
    .await
    .map_err(|err| std::io::Error::other(err.to_string()))?;

    let phases = phases.lock().map_err(|err| std::io::Error::other(err.to_string()))?;
    assert_eq!(&*phases, &["draining", "stopping"]);
    assert!(start.elapsed() >= Duration::from_millis(20));

    Ok(())
}

#[tokio::test]
async fn typed_config_and_secret_reload_helpers_apply_runtime_changes() -> Result<(), Box<dyn std::error::Error>> {
    let root = unique_test_dir("typed-reload");
    let dirs = test_dirs(root.clone());
    std::fs::create_dir_all(&dirs.metadata_root_dir)?;
    std::fs::create_dir_all(&dirs.secrets_root_dir)?;
    std::fs::create_dir_all(&dirs.runtime_root_dir)?;
    std::fs::write(dirs.metadata_root_dir.join("reloading.toml"), "enabled = false")?;
    std::fs::write(dirs.secrets_root_dir.join("api_key"), "alpha")?;

    exec_async_runner_with_options(
        TestExec { dirs },
        ReloadingApp {
            initial_config_loaded: false,
            current_enabled: false,
            current_secret: String::new(),
        },
        PhysicalVFS::new(),
        test_o11y()?,
        RunnerOptions::new().with_boot_dir(root.join("boot")),
    )
    .await
    .map_err(|err| std::io::Error::other(err.to_string()))?;

    Ok(())
}

#[tokio::test]
async fn run_event_loop_auto_applies_typed_reload_before_handler() -> Result<(), Box<dyn std::error::Error>> {
    let root = unique_test_dir("helper-loop");
    let dirs = test_dirs(root.clone());
    std::fs::create_dir_all(&dirs.metadata_root_dir)?;
    std::fs::create_dir_all(&dirs.runtime_root_dir)?;
    std::fs::write(dirs.metadata_root_dir.join("helper_loop.toml"), "enabled = false")?;

    exec_async_runner_with_options(
        TestExec { dirs },
        HelperLoopApp { current_enabled: false },
        PhysicalVFS::new(),
        test_o11y()?,
        RunnerOptions::new().with_boot_dir(root.join("boot")),
    )
    .await
    .map_err(|err| std::io::Error::other(err.to_string()))?;

    Ok(())
}

#[tokio::test]
async fn apply_reload_event_supports_in_memory_vfs() -> Result<(), Box<dyn std::error::Error>> {
    let dirs = test_dirs(PathBuf::from("/virtual-runtime"));
    let fs = InMemoryVFS::new();
    fs.create_dir(&dirs.metadata_root_dir).await?;
    fs.create_dir(&dirs.secrets_root_dir).await?;
    fs.write(dirs.metadata_root_dir.join("reloading.toml"), b"enabled = true").await?;
    fs.write(dirs.secrets_root_dir.join("api_key"), b"bravo").await?;

    let wora: Wora<(), ()> = Wora::new(&dirs, "reloading".to_string(), 8, test_o11y()?)?;
    let mut app = ReloadingApp {
        initial_config_loaded: false,
        current_enabled: false,
        current_secret: String::new(),
    };

    let config_path = dirs.metadata_root_dir.join("reloading.toml");
    let secret_path = dirs.secrets_root_dir.join("api_key");
    let config_event = ConfigChange::new(ChangeKind::Modified, config_path.clone(), vec![config_path]);
    let secret_event = SecretChange::new(ChangeKind::Modified, vec![secret_path]);

    assert_eq!(
        wora.apply_reload_event(&mut app, fs.clone(), &Event::ConfigChanged(config_event)).await?,
        ReloadHandling::ConfigApplied
    );
    assert_eq!(
        wora.apply_reload_event(&mut app, fs.clone(), &Event::SecretChanged(secret_event)).await?,
        ReloadHandling::SecretsApplied
    );
    assert!(app.current_enabled);
    assert_eq!(app.current_secret, "bravo");

    Ok(())
}

#[tokio::test]
async fn runner_reports_already_running_when_lock_is_held() -> Result<(), Box<dyn std::error::Error>> {
    let root = unique_test_dir("lock-contention");
    let dirs = test_dirs(root.clone());
    let fs = InMemoryVFS::new();
    let o11y = test_o11y()?;
    let lock_backend = InMemoryLockBackend::default();
    let held_path = dirs.runtime_root_dir.join("configured_restart.lock");
    let _guard = lock_backend.try_lock(&held_path)?;

    let result = exec_async_runner_with_options(
        TestExec { dirs },
        ConfiguredRestartApp {
            configured: false,
            calls: Arc::new(Mutex::new(0)),
        },
        fs,
        o11y,
        RunnerOptions::new().with_boot_dir(root.join("boot")).with_lock_backend(lock_backend),
    )
    .await;

    match result {
        Err(MainEarlyReturn::AlreadyRunning(path)) => assert_eq!(path, held_path),
        other => panic!("unexpected runner result: {other:?}"),
    }

    Ok(())
}

#[tokio::test]
async fn runner_marks_only_the_first_invocation_as_first_boot() -> Result<(), Box<dyn std::error::Error>> {
    let root = unique_test_dir("boot-tracking");
    let dirs = test_dirs(root.clone());
    let fs = InMemoryVFS::new();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let boot_root = PathBuf::from("/boot-state");

    exec_async_runner_with_options(
        TestExec { dirs: dirs.clone() },
        BootTrackingApp { calls: calls.clone() },
        fs.clone(),
        test_o11y()?,
        RunnerOptions::new()
            .with_boot_dir(boot_root.clone())
            .with_lock_backend(InMemoryLockBackend::default()),
    )
    .await
    .map_err(|err| std::io::Error::other(err.to_string()))?;

    exec_async_runner_with_options(
        TestExec { dirs },
        BootTrackingApp { calls: calls.clone() },
        fs,
        test_o11y()?,
        RunnerOptions::new().with_boot_dir(boot_root).with_lock_backend(InMemoryLockBackend::default()),
    )
    .await
    .map_err(|err| std::io::Error::other(err.to_string()))?;

    let calls = calls.lock().map_err(|err| std::io::Error::other(err.to_string()))?;
    assert_eq!(&*calls, &[true, false]);
    Ok(())
}

#[tokio::test]
async fn runner_rejects_directory_boot_markers() -> Result<(), Box<dyn std::error::Error>> {
    let root = unique_test_dir("boot-marker-invalid");
    let dirs = test_dirs(root);
    let fs = InMemoryVFS::new();
    let boot_root = PathBuf::from("/boot-state");
    fs.create_dir(&boot_root).await?;
    fs.create_dir(boot_root.join(".boot_tracking.booted")).await?;

    let result = exec_async_runner_with_options(
        TestExec { dirs },
        BootTrackingApp {
            calls: Arc::new(Mutex::new(Vec::new())),
        },
        fs,
        test_o11y()?,
        RunnerOptions::new().with_boot_dir(boot_root).with_lock_backend(InMemoryLockBackend::default()),
    )
    .await;

    match result {
        Err(MainEarlyReturn::WoraSetup(WoraSetupError::BootMarker(_))) => {}
        other => panic!("unexpected runner result: {other:?}"),
    }

    Ok(())
}

#[tokio::test]
async fn exec_async_runner_supports_in_memory_vfs_watchers() -> Result<(), Box<dyn std::error::Error>> {
    let root = unique_test_dir("virtual-watch-runner");
    let dirs = test_dirs(root.clone());
    let fs = InMemoryVFS::new();

    fs.create_dir(&dirs.metadata_root_dir).await?;
    fs.write(dirs.metadata_root_dir.join("virtual_watcher.toml"), b"enabled = false").await?;

    exec_async_runner_with_options(
        TestExec { dirs },
        VirtualWatcherApp { current_enabled: false },
        fs,
        test_o11y()?,
        RunnerOptions::new()
            .with_boot_dir(root.join("boot"))
            .with_lock_backend(InMemoryLockBackend::default()),
    )
    .await
    .map_err(|err| std::io::Error::other(err.to_string()))?;

    Ok(())
}

#[tokio::test]
async fn o11y_processor_fans_out_to_memory_and_json_sinks() -> Result<(), Box<dyn std::error::Error>> {
    let root = unique_test_dir("o11y-processor");
    std::fs::create_dir_all(&root)?;
    let json_path = root.join("o11y.jsonl");
    let entries = Arc::new(Mutex::new(Vec::new()));
    let processor = O11yProcessor::new(vec![
        Box::new(O11yMemorySink::new(entries.clone())),
        Box::new(O11yJsonLinesSink::new(json_path.clone()).with_name("test")),
    ]);
    let (tx, rx) = tokio::sync::mpsc::channel(8);
    let task = processor.spawn(rx);

    tx.send(o11y_new_ev_flush::<()>()).await?;
    tx.send(o11y_new_ev_finish::<()>()).await?;
    drop(tx);

    task.await.map_err(|err| std::io::Error::other(err.to_string()))??;

    let entries = entries.lock().map_err(|err| std::io::Error::other(err.to_string()))?;
    assert!(entries.iter().any(|entry| entry.contains("flush")));
    assert!(entries.iter().any(|entry| entry.contains("finish")));

    let file = std::fs::read_to_string(json_path)?;
    assert!(file.contains("\"kind\":\"flush\""));
    assert!(file.contains("\"sink\":\"test\""));
    Ok(())
}

#[tokio::test]
async fn o11y_processor_flushes_buffered_json_lines_on_channel_close() -> Result<(), Box<dyn std::error::Error>> {
    let root = unique_test_dir("o11y-close-flush");
    std::fs::create_dir_all(&root)?;
    let json_path = root.join("o11y.jsonl");
    let processor = O11yProcessor::new(vec![Box::new(O11yJsonLinesSink::new(json_path.clone()).with_name("test"))]);
    let (tx, rx) = tokio::sync::mpsc::channel(8);
    let task = processor.spawn(rx);

    tx.send(o11y_new_ev_status::<()>(1, 8)).await?;
    drop(tx);

    task.await.map_err(|err| std::io::Error::other(err.to_string()))??;

    let file = std::fs::read_to_string(json_path)?;
    assert!(file.contains("\"kind\":\"status\""));
    assert!(file.contains("\"sink\":\"test\""));
    Ok(())
}

#[tokio::test]
async fn runner_emits_host_process_and_runtime_metrics() -> Result<(), Box<dyn std::error::Error>> {
    let root = unique_test_dir("o11y-metrics");
    let dirs = test_dirs(root.clone());
    std::fs::create_dir_all(&dirs.runtime_root_dir)?;
    let (o11y, mut rx) = test_o11y_with_receiver(Duration::from_millis(10))?;

    exec_async_runner_with_options(
        TestExec { dirs },
        MetricsApp,
        PhysicalVFS::new(),
        o11y,
        RunnerOptions::new().with_boot_dir(root.join("boot")),
    )
    .await
    .map_err(|err| std::io::Error::other(err.to_string()))?;

    let mut saw_host_stats = false;
    let mut saw_process_stats = false;
    let mut saw_runtime_metrics = false;

    while let Ok(event) = tokio::time::timeout(Duration::from_millis(10), rx.recv()).await {
        let Some(event) = event else {
            break;
        };
        match event.kind {
            O11yEventKind::HostStats(_) => saw_host_stats = true,
            O11yEventKind::ProcessStats(stats) => {
                saw_process_stats = true;
                assert!(stats.pid > 0);
            }
            O11yEventKind::RuntimeMetrics(metrics) => {
                saw_runtime_metrics = true;
                assert_eq!(metrics.app_name, "metrics_app");
            }
            _ => {}
        }
    }

    assert!(saw_host_stats);
    assert!(saw_process_stats);
    assert!(saw_runtime_metrics);
    Ok(())
}

#[tokio::test]
async fn runner_uses_injected_runtime_environment() -> Result<(), Box<dyn std::error::Error>> {
    let root = unique_test_dir("o11y-metrics-deterministic");
    let dirs = test_dirs(root.clone());
    std::fs::create_dir_all(&dirs.runtime_root_dir)?;
    let (o11y, mut rx) = test_o11y_with_receiver(Duration::from_millis(10))?;
    let runtime_environment = DeterministicRuntimeEnvironment {
        host_info: deterministic_host_info(),
        host_stats: deterministic_host_stats(),
        process: Some(ProcessStats {
            pid: 4242,
            memory: 64,
            virtual_memory: 128,
            cpu_usage: 5.0,
            accumulated_cpu_time: 12,
            run_time: 7,
            start_time: 9,
            read_bytes: 1,
            total_read_bytes: 2,
            written_bytes: 3,
            total_written_bytes: 4,
        }),
    };

    exec_async_runner_with_options(
        TestExec { dirs },
        MetricsApp,
        PhysicalVFS::new(),
        o11y,
        RunnerOptions::new()
            .with_boot_dir(root.join("boot"))
            .with_runtime_environment(runtime_environment),
    )
    .await
    .map_err(|err| std::io::Error::other(err.to_string()))?;

    let mut saw_host_stats = false;
    let mut saw_process_stats = false;

    while let Ok(event) = tokio::time::timeout(Duration::from_millis(10), rx.recv()).await {
        let Some(event) = event else {
            break;
        };
        match event.kind {
            O11yEventKind::HostStats(stats) => {
                saw_host_stats = true;
                assert_eq!(stats.memory.used, 6);
                assert!(stats.cpu.is_empty());
                assert_eq!(stats.swap.used, 5);
            }
            O11yEventKind::ProcessStats(stats) => {
                saw_process_stats = true;
                assert_eq!(stats.pid, 4242);
                assert_eq!(stats.total_written_bytes, 4);
            }
            _ => {}
        }
    }

    assert!(saw_host_stats);
    assert!(saw_process_stats);
    Ok(())
}

#[derive(Default)]
struct MissingSecretsApp {
    reloaded_secret_count: usize,
}

struct MissingSecretsParser;

impl Secrets for MissingSecretsParser {
    type SecretT = String;

    fn parse_secret_file(_file_path: PathBuf, data: Vec<u8>) -> Result<Self::SecretT, Box<dyn std::error::Error>> {
        Ok(String::from_utf8(data)?)
    }
}

#[async_trait]
impl App<(), ()> for MissingSecretsApp {
    type AppConfig = TestConfig;
    type AppSecrets = MissingSecretsParser;
    type Setup = ();

    fn name(&self) -> &'static str {
        "missing_secrets"
    }

    async fn reload_secrets(&mut self, reload: SecretReload<String>) -> Result<(), Box<dyn std::error::Error>> {
        self.reloaded_secret_count += reload.files.len();
        Ok(())
    }

    async fn setup(
        &mut self,
        _wora: &Wora<(), ()>,
        _exec: impl AsyncExecutor<(), ()>,
        _fs: impl WFS + 'static,
        _metrics: Sender<O11yEvent<()>>,
        _is_first_boot: bool,
    ) -> Result<Self::Setup, Box<dyn std::error::Error>> {
        Ok(())
    }

    async fn main(
        &mut self,
        _wora: &mut Wora<(), ()>,
        _exec: impl AsyncExecutor<(), ()>,
        _fs: impl WFS + 'static,
        _metrics: Sender<O11yEvent<()>>,
    ) -> MainRetryAction {
        assert_eq!(self.reloaded_secret_count, 0);
        MainRetryAction::Success
    }

    async fn end(&mut self, _wora: &Wora<(), ()>, _exec: impl AsyncExecutor<(), ()>, _fs: impl WFS + 'static, _metrics: Sender<O11yEvent<()>>) {}
}

#[tokio::test]
async fn initial_secret_load_skips_missing_secret_directory() -> Result<(), Box<dyn std::error::Error>> {
    let root = unique_test_dir("missing-secrets");
    let dirs = test_dirs(root.clone());
    std::fs::create_dir_all(&dirs.metadata_root_dir)?;
    std::fs::create_dir_all(&dirs.runtime_root_dir)?;
    std::fs::write(dirs.metadata_root_dir.join("missing_secrets.toml"), "enabled = true")?;

    exec_async_runner_with_options(
        TestExec { dirs },
        MissingSecretsApp::default(),
        PhysicalVFS::new(),
        test_o11y()?,
        RunnerOptions::new().with_boot_dir(root.join("boot")),
    )
    .await
    .map_err(|err| std::io::Error::other(err.to_string()))?;

    Ok(())
}

#[cfg(target_family = "unix")]
#[tokio::test]
async fn systemd_executor_sends_ready_and_stopping_notifications() -> Result<(), Box<dyn std::error::Error>> {
    use std::os::unix::net::UnixDatagram;

    let root = unique_test_dir("systemd-notify");
    std::fs::create_dir_all(&root)?;

    #[cfg(target_os = "linux")]
    let (receiver, notify_socket) = {
        use std::os::linux::net::SocketAddrExt;
        use std::os::unix::net::SocketAddr;

        let socket_name = unique_abstract_socket_name("systemd-notify");
        let addr = SocketAddr::from_abstract_name(socket_name.as_bytes())?;
        let receiver = match UnixDatagram::bind_addr(&addr) {
            Ok(receiver) => receiver,
            Err(err) if skip_permission_denied_socket_test(&err) => return Ok(()),
            Err(err) => return Err(err.into()),
        };
        (receiver, format!("@{socket_name}"))
    };

    #[cfg(not(target_os = "linux"))]
    let (receiver, notify_socket) = {
        let socket_path = unique_socket_path("systemd-notify");
        let receiver = match UnixDatagram::bind(&socket_path) {
            Ok(receiver) => receiver,
            Err(err) if skip_permission_denied_socket_test(&err) => return Ok(()),
            Err(err) => return Err(err.into()),
        };
        (receiver, socket_path.to_string_lossy().to_string())
    };

    receiver.set_read_timeout(Some(Duration::from_secs(1)))?;

    let exec = SystemdExecutor::system("notify_app").await.with_notify_socket(notify_socket);
    let wora: Wora<(), ()> = Wora::new(
        <SystemdExecutor as AsyncExecutor<(), ()>>::dirs(&exec),
        "notify_app".to_string(),
        16,
        test_o11y()?,
    )?;

    match <SystemdExecutor as AsyncExecutor<(), ()>>::on_runtime_ready(&exec, "notify_app", &wora.dirs, PhysicalVFS::new()).await {
        Ok(()) => {}
        Err(SetupFailure::IO(err)) if skip_permission_denied_socket_test(&err) => return Ok(()),
        Err(err) => return Err(err.into()),
    }
    let mut buf = [0u8; 256];
    let ready_size = receiver.recv(&mut buf)?;
    let ready_message = std::str::from_utf8(&buf[..ready_size])?;
    assert!(ready_message.contains("READY=1"));
    assert!(ready_message.contains("notify_app ready"));

    match <SystemdExecutor as AsyncExecutor<(), ()>>::on_runtime_draining(&exec, "notify_app", &wora.dirs, PhysicalVFS::new()).await {
        Ok(()) => {}
        Err(SetupFailure::IO(err)) if skip_permission_denied_socket_test(&err) => return Ok(()),
        Err(err) => return Err(err.into()),
    }
    let draining_size = receiver.recv(&mut buf)?;
    let draining_message = std::str::from_utf8(&buf[..draining_size])?;
    assert!(draining_message.contains("STATUS=notify_app draining"));

    match <SystemdExecutor as AsyncExecutor<(), ()>>::on_runtime_stopping(&exec, "notify_app", &wora.dirs, PhysicalVFS::new()).await {
        Ok(()) => {}
        Err(SetupFailure::IO(err)) if skip_permission_denied_socket_test(&err) => return Ok(()),
        Err(err) => return Err(err.into()),
    }
    let stopping_size = receiver.recv(&mut buf)?;
    let stopping_message = std::str::from_utf8(&buf[..stopping_size])?;
    assert!(stopping_message.contains("STOPPING=1"));
    assert!(stopping_message.contains("notify_app stopping"));

    Ok(())
}

#[tokio::test]
async fn container_executor_manages_readiness_and_termination_files() -> Result<(), Box<dyn std::error::Error>> {
    let readiness_path = PathBuf::from("/virtual/status/ready");
    let termination_path = PathBuf::from("/virtual/status/termination.log");
    let fs = InMemoryVFS::new();
    let exec = ContainerExecutor::new("container_app")
        .await
        .with_readiness_file(&readiness_path)
        .with_termination_log(&termination_path);
    let wora: Wora<(), ()> = Wora::new(
        <ContainerExecutor as AsyncExecutor<(), ()>>::dirs(&exec),
        "container_app".to_string(),
        16,
        test_o11y()?,
    )?;

    <ContainerExecutor as AsyncExecutor<(), ()>>::on_runtime_ready(&exec, "container_app", &wora.dirs, fs.clone()).await?;
    assert_eq!(fs.read_to_string(&readiness_path).await?, "ready:container_app\n");

    <ContainerExecutor as AsyncExecutor<(), ()>>::on_runtime_draining(&exec, "container_app", &wora.dirs, fs.clone()).await?;
    assert!(!fs.dir_exists(&readiness_path).await?);

    <ContainerExecutor as AsyncExecutor<(), ()>>::on_runtime_stopping(&exec, "container_app", &wora.dirs, fs.clone()).await?;
    assert_eq!(fs.read_to_string(&termination_path).await?, "stopping:container_app\n");

    Ok(())
}

#[tokio::test]
async fn container_executor_accepts_injected_control_events() -> Result<(), Box<dyn std::error::Error>> {
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    let exec = ContainerExecutor::new("container_control").await.with_control_event_receiver(rx);
    let fs = InMemoryVFS::new();

    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(10)).await;
        let _ = tx.send(ControlEvent::Shutdown(None)).await;
    });

    exec_async_runner_with_options(
        exec,
        ControlDrivenApp {
            name: "container_control_runtime_sources",
        },
        fs,
        test_o11y()?,
        RunnerOptions::new().with_lock_backend(InMemoryLockBackend::default()),
    )
    .await
    .map_err(|err| std::io::Error::other(err.to_string()))?;

    Ok(())
}

#[tokio::test]
async fn launchd_agent_constructor_produces_distinct_layout() -> Result<(), Box<dyn std::error::Error>> {
    let exec = LaunchdExecutor::agent("launchd_app").await?.with_socket_name("listener");

    assert_eq!(<LaunchdExecutor as AsyncExecutor<(), ()>>::id(&exec), "launchd-agent");
    assert!(
        <LaunchdExecutor as AsyncExecutor<(), ()>>::dirs(&exec).metadata_root_dir != <LaunchdExecutor as AsyncExecutor<(), ()>>::dirs(&exec).runtime_root_dir
    );
    assert!(<LaunchdExecutor as AsyncExecutor<(), ()>>::dirs(&exec).cache_root_dir != <LaunchdExecutor as AsyncExecutor<(), ()>>::dirs(&exec).secrets_root_dir);
    assert_eq!(exec.socket_names(), &["listener".to_string()]);

    #[cfg(target_os = "macos")]
    {
        let job = exec.launchd_job("com.example.launchd-app", "/usr/local/bin/launchd-app", vec!["--foreground".to_string()])?;
        let mut xml = Vec::new();
        job.to_writer_xml(&mut xml)?;
        let xml = String::from_utf8(xml)?;
        assert!(xml.contains("com.example.launchd-app"));
        assert!(xml.contains("WORA_EXECUTOR"));
        assert!(xml.contains("listener"));
        assert!(xml.contains("stdout.log"));
    }

    Ok(())
}

#[tokio::test]
async fn retry_instantly_stops_at_max_retries() -> Result<(), Box<dyn std::error::Error>> {
    let root = unique_test_dir("retry-instantly");
    let dirs = test_dirs(root.clone());
    std::fs::create_dir_all(&dirs.runtime_root_dir)?;

    let calls = Arc::new(Mutex::new(0));
    let app = AlwaysRestartApp {
        name: "retry_instantly_limit",
        calls: calls.clone(),
    };
    let rc = exec_async_runner_with_options(
        TestExec { dirs },
        app,
        PhysicalVFS::new(),
        test_o11y()?,
        RunnerOptions::new()
            .with_boot_dir(root.join("boot"))
            .with_restart_options(RestartPolicyOptions {
                policy: WorkloadRestartPolicy::RetryInstantly,
                max_retries: Some(2),
                ..RestartPolicyOptions::default()
            }),
    )
    .await;

    assert!(matches!(rc, Err(MainEarlyReturn::UseExitCode(1))));
    let calls = calls.lock().map_err(|err| std::io::Error::other(err.to_string()))?;
    assert_eq!(*calls, 3);
    Ok(())
}

#[tokio::test]
async fn retry_pause_stops_at_max_retries() -> Result<(), Box<dyn std::error::Error>> {
    let root = unique_test_dir("retry-pause");
    let dirs = test_dirs(root.clone());
    std::fs::create_dir_all(&dirs.runtime_root_dir)?;

    let calls = Arc::new(Mutex::new(0));
    let app = AlwaysRestartApp {
        name: "retry_pause_limit",
        calls: calls.clone(),
    };
    let rc = exec_async_runner_with_options(
        TestExec { dirs },
        app,
        PhysicalVFS::new(),
        test_o11y()?,
        RunnerOptions::new()
            .with_boot_dir(root.join("boot"))
            .with_restart_options(RestartPolicyOptions {
                policy: WorkloadRestartPolicy::RetryPause,
                pause: Duration::ZERO,
                max_retries: Some(1),
                ..RestartPolicyOptions::default()
            }),
    )
    .await;

    assert!(matches!(rc, Err(MainEarlyReturn::UseExitCode(1))));
    let calls = calls.lock().map_err(|err| std::io::Error::other(err.to_string()))?;
    assert_eq!(*calls, 2);
    Ok(())
}

#[test]
fn exponential_backoff_respects_max_backoff() {
    let options = RestartPolicyOptions {
        policy: WorkloadRestartPolicy::ExponentialBackoff,
        pause: Duration::from_secs(5),
        max_backoff: Some(Duration::from_secs(12)),
        ..RestartPolicyOptions::default()
    };

    assert_eq!(options.pause_for_retry(1), Duration::from_secs(5));
    assert_eq!(options.pause_for_retry(2), Duration::from_secs(10));
    assert_eq!(options.pause_for_retry(3), Duration::from_secs(12));
}

#[tokio::test]
async fn exit_with_workload_return_exits_immediately() -> Result<(), Box<dyn std::error::Error>> {
    let root = unique_test_dir("exit-with-workload-return");
    let dirs = test_dirs(root.clone());
    std::fs::create_dir_all(&dirs.runtime_root_dir)?;

    let calls = Arc::new(Mutex::new(0));
    let app = AlwaysRestartApp {
        name: "exit_with_workload_return",
        calls: calls.clone(),
    };
    let rc = exec_async_runner_with_options(
        TestExec { dirs },
        app,
        PhysicalVFS::new(),
        test_o11y()?,
        RunnerOptions::new()
            .with_boot_dir(root.join("boot"))
            .with_restart_options(RestartPolicyOptions::new(WorkloadRestartPolicy::ExitWithWorkloadReturn)),
    )
    .await;

    assert!(matches!(rc, Err(MainEarlyReturn::UseExitCode(1))));
    let calls = calls.lock().map_err(|err| std::io::Error::other(err.to_string()))?;
    assert_eq!(*calls, 1);
    Ok(())
}
