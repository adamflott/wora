use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use tokio::sync::mpsc::Sender;
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
    let (tx, _rx) = tokio::sync::mpsc::channel::<O11yEvent<()>>(16);
    let interval = Duration::from_secs(60);
    let o11y = O11yProcessorOptionsBuilder::default()
        .sender(tx)
        .flush_interval(interval)
        .status_interval(interval)
        .host_stats_interval(interval)
        .build()
        .map_err(|err| std::io::Error::other(err.to_string()))?;

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
