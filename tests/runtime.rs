use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;
use wora::prelude::*;

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

#[cfg(target_family = "unix")]
fn unique_socket_path(name: &str) -> PathBuf {
    let suffix = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos();
    std::env::temp_dir().join(format!("wora-{}-{}.sock", name.replace('_', "-"), suffix))
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
        let mut file = fs.create_file(&self.ready_file).await?;
        use tokio::io::AsyncWriteExt;
        file.write_all(format!("ready:{app_name}").as_bytes()).await?;
        Ok(())
    }

    async fn is_ready(&self, _wora: &Wora<(), ()>, _fs: impl WFS) -> bool {
        true
    }

    async fn end(&self, _wora: &Wora<(), ()>, _fs: impl WFS) {}
}

#[derive(Clone, Debug)]
struct ControlEventExec {
    dirs: Dirs,
}

#[async_trait]
impl AsyncExecutor<(), ()> for ControlEventExec {
    fn id(&self) -> &'static str {
        "control-event"
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

    async fn spawn_runtime_event_sources(&self, sender: Sender<Event<()>>) -> Result<Vec<JoinHandle<()>>, SetupFailure> {
        Ok(vec![tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let _ = sender.send(Event::Control(ControlEvent::Shutdown(None))).await;
        })])
    }

    async fn is_ready(&self, _wora: &Wora<(), ()>, _fs: impl WFS) -> bool {
        true
    }

    async fn end(&self, _wora: &Wora<(), ()>, _fs: impl WFS) {}
}

#[async_trait]
impl App<(), ()> for AlwaysRestartApp {
    type AppConfig = NoConfig;
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

    async fn is_healthy(&mut self) -> HealthState {
        HealthState::Ok
    }

    async fn end(&mut self, _wora: &Wora<(), ()>, _exec: impl AsyncExecutor<(), ()>, _fs: impl WFS + 'static, _metrics: Sender<O11yEvent<()>>) {}
}

#[async_trait]
impl App<(), ()> for ConfiguredRestartApp {
    type AppConfig = TestConfig;
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

    async fn is_healthy(&mut self) -> HealthState {
        HealthState::Ok
    }

    async fn end(&mut self, _wora: &Wora<(), ()>, _exec: impl AsyncExecutor<(), ()>, _fs: impl WFS + 'static, _metrics: Sender<O11yEvent<()>>) {}
}

struct ControlDrivenApp;

struct DeferredReadyApp;

struct HealthFailureApp {
    calls: Arc<Mutex<u8>>,
}

#[async_trait]
impl App<(), ()> for ControlDrivenApp {
    type AppConfig = NoConfig;
    type Setup = ();

    fn name(&self) -> &'static str {
        "control_driven"
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

    async fn is_healthy(&mut self) -> HealthState {
        HealthState::Ok
    }

    async fn end(&mut self, _wora: &Wora<(), ()>, _exec: impl AsyncExecutor<(), ()>, _fs: impl WFS + 'static, _metrics: Sender<O11yEvent<()>>) {}
}

#[async_trait]
impl App<(), ()> for DeferredReadyApp {
    type AppConfig = NoConfig;
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

    async fn is_healthy(&mut self) -> HealthState {
        HealthState::Ok
    }

    async fn end(&mut self, _wora: &Wora<(), ()>, _exec: impl AsyncExecutor<(), ()>, _fs: impl WFS + 'static, _metrics: Sender<O11yEvent<()>>) {}
}

#[async_trait]
impl App<(), ()> for HealthFailureApp {
    type AppConfig = NoConfig;
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

    async fn is_healthy(&mut self) -> HealthState {
        HealthState::Ok
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

    exec_async_runner_with_restart_policy(
        exec,
        app,
        fs,
        o11y,
        Some(root.join("boot")),
        WorkloadRestartPolicy::RetryInstantly,
        Duration::from_millis(1),
    )
    .await
    .map_err(|err| std::io::Error::other(err.to_string()))?;

    let calls = calls.lock().map_err(|err| std::io::Error::other(err.to_string()))?;
    assert_eq!(*calls, 2);
    Ok(())
}

#[tokio::test]
async fn executor_runtime_event_sources_can_drive_control_flow() -> Result<(), Box<dyn std::error::Error>> {
    let root = unique_test_dir("control-event-source");
    let dirs = test_dirs(root.clone());
    std::fs::create_dir_all(&dirs.runtime_root_dir)?;

    exec_async_runner(
        ControlEventExec { dirs },
        ControlDrivenApp,
        PhysicalVFS::new(),
        test_o11y()?,
        Some(root.join("boot")),
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

    exec_async_runner(
        ReadyMarkerExec {
            dirs,
            ready_file: ready_file.clone(),
        },
        DeferredReadyApp,
        PhysicalVFS::new(),
        test_o11y()?,
        Some(root.join("boot")),
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
    let rc = exec_async_runner_with_restart_options(
        TestExec { dirs },
        HealthFailureApp { calls: calls.clone() },
        PhysicalVFS::new(),
        test_o11y()?,
        Some(root.join("boot")),
        RestartPolicyOptions {
            policy: WorkloadRestartPolicy::RetryInstantly,
            max_retries: Some(1),
            supervision: SupervisionOptions {
                shutdown_grace_period: Duration::from_millis(50),
                unhealthy_action: UnhealthyAction::UseRestartPolicy,
                ..SupervisionOptions::default()
            },
            ..RestartPolicyOptions::default()
        },
    )
    .await;

    let calls = calls.lock().map_err(|err| std::io::Error::other(err.to_string()))?;
    assert!(matches!(rc, Err(MainEarlyReturn::UseExitCode(1))));
    assert_eq!(*calls, 2);
    Ok(())
}

#[cfg(target_family = "unix")]
#[tokio::test]
async fn systemd_executor_sends_ready_and_stopping_notifications() -> Result<(), Box<dyn std::error::Error>> {
    use std::os::unix::net::UnixDatagram;

    let root = unique_test_dir("systemd-notify");
    std::fs::create_dir_all(&root)?;
    let socket_path = unique_socket_path("systemd-notify");
    let receiver = UnixDatagram::bind(&socket_path)?;
    receiver.set_read_timeout(Some(Duration::from_secs(1)))?;

    let exec = SystemdExecutor::system("notify_app")
        .await
        .with_notify_socket(socket_path.to_string_lossy().to_string());
    let wora: Wora<(), ()> = Wora::new(
        <SystemdExecutor as AsyncExecutor<(), ()>>::dirs(&exec),
        "notify_app".to_string(),
        16,
        test_o11y()?,
    )?;

    <SystemdExecutor as AsyncExecutor<(), ()>>::on_runtime_ready(&exec, "notify_app", &wora.dirs, PhysicalVFS::new()).await?;
    let mut buf = [0u8; 256];
    let ready_size = receiver.recv(&mut buf)?;
    let ready_message = std::str::from_utf8(&buf[..ready_size])?;
    assert!(ready_message.contains("READY=1"));
    assert!(ready_message.contains("notify_app ready"));

    <SystemdExecutor as AsyncExecutor<(), ()>>::on_runtime_stopping(&exec, "notify_app", &wora.dirs, PhysicalVFS::new()).await?;
    let stopping_size = receiver.recv(&mut buf)?;
    let stopping_message = std::str::from_utf8(&buf[..stopping_size])?;
    assert!(stopping_message.contains("STOPPING=1"));
    assert!(stopping_message.contains("notify_app stopping"));

    Ok(())
}

#[tokio::test]
async fn container_executor_manages_readiness_and_termination_files() -> Result<(), Box<dyn std::error::Error>> {
    let root = unique_test_dir("container-exec");
    let readiness_path = root.join("status").join("ready");
    let termination_path = root.join("status").join("termination.log");
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

    <ContainerExecutor as AsyncExecutor<(), ()>>::on_runtime_ready(&exec, "container_app", &wora.dirs, PhysicalVFS::new()).await?;
    assert!(readiness_path.is_file());
    assert_eq!(std::fs::read_to_string(&readiness_path)?, "ready:container_app\n");

    <ContainerExecutor as AsyncExecutor<(), ()>>::on_runtime_stopping(&exec, "container_app", &wora.dirs, PhysicalVFS::new()).await?;
    assert!(!readiness_path.exists());
    assert_eq!(std::fs::read_to_string(&termination_path)?, "stopping:container_app\n");

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
    let rc = exec_async_runner_with_restart_options(
        TestExec { dirs },
        app,
        PhysicalVFS::new(),
        test_o11y()?,
        Some(root.join("boot")),
        RestartPolicyOptions {
            policy: WorkloadRestartPolicy::RetryInstantly,
            max_retries: Some(2),
            ..RestartPolicyOptions::default()
        },
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
    let rc = exec_async_runner_with_restart_options(
        TestExec { dirs },
        app,
        PhysicalVFS::new(),
        test_o11y()?,
        Some(root.join("boot")),
        RestartPolicyOptions {
            policy: WorkloadRestartPolicy::RetryPause,
            pause: Duration::ZERO,
            max_retries: Some(1),
            ..RestartPolicyOptions::default()
        },
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
    let rc = exec_async_runner_with_restart_options(
        TestExec { dirs },
        app,
        PhysicalVFS::new(),
        test_o11y()?,
        Some(root.join("boot")),
        RestartPolicyOptions::new(WorkloadRestartPolicy::ExitWithWorkloadReturn),
    )
    .await;

    assert!(matches!(rc, Err(MainEarlyReturn::UseExitCode(1))));
    let calls = calls.lock().map_err(|err| std::io::Error::other(err.to_string()))?;
    assert_eq!(*calls, 1);
    Ok(())
}
