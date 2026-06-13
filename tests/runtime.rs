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
