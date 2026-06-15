use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock;

use async_trait::async_trait;
use clap::{Parser, ValueEnum};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::Sender;
use tracing::{Level, debug, error, info};
use tracing_subscriber::prelude::*;

use wora::prelude::*;

#[derive(Clone, Debug, ValueEnum, Serialize, Deserialize)]
pub enum RunMode {
    Sys,
    User,
}

#[derive(Clone, Debug, Parser, Serialize, Deserialize)]
#[command(name = "async_daemon")]
#[command(author, version, about = "async wora daemon example", long_about = None)]
#[command(propagate_version = true)]
pub struct DaemonArgs {
    /// change default run mode
    #[arg(short, long, value_enum, default_value_t=RunMode::User)]
    pub run_mode: RunMode,
}

#[derive(Default, Deserialize)]
#[allow(dead_code)]
struct Obj {
    t_or_f: bool,
    list: Vec<String>,
}

#[derive(Default, Deserialize)]
#[allow(dead_code)]
pub struct DaemonConfig {
    str: String,
    num: Option<u16>,
    obj: Obj,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DaemonState {}

type DaemonSharedState = Arc<RwLock<DaemonState>>;
struct DaemonApp {
    args: DaemonArgs,
    #[allow(dead_code)]
    state: DaemonSharedState,
    config: DaemonConfig,
}

impl Config for DaemonConfig {
    type ConfigT = DaemonConfig;
    fn parse_main_config_file(data: String) -> Result<DaemonConfig, Box<dyn std::error::Error>> {
        match toml::from_str(&data) {
            Ok(v) => Ok(v),
            Err(err) => Err(Box::new(err)),
        }
    }
    fn parse_supplemental_config_file(_file_path: PathBuf, data: String) -> Result<DaemonConfig, Box<dyn std::error::Error>> {
        match toml::from_str(&data) {
            Ok(v) => Ok(v),
            Err(err) => Err(Box::new(err)),
        }
    }
}

#[async_trait]
impl App<(), ()> for DaemonApp {
    type AppConfig = DaemonConfig;
    type AppSecrets = NoSecrets;
    type Setup = ();
    fn name(&self) -> &'static str {
        "async_daemon"
    }

    async fn reload_config(&mut self, reload: ConfigReload<DaemonConfig>) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(config) = reload.main {
            self.config = config;
        }
        Ok(())
    }

    async fn setup(
        &mut self,
        wora: &Wora<(), ()>,
        exec: impl AsyncExecutor<(), ()>,
        _fs: impl WFS,
        _o11y: Sender<O11yEvent<()>>,
        _is_first_boot: bool,
    ) -> Result<Self::Setup, Box<dyn std::error::Error>> {
        debug!("{:?}", wora.stats_from_start());

        let args = DaemonArgs::parse();
        self.args = args;

        debug!("{:?}", exec.disable_core_dumps());

        Ok(())
    }

    async fn main(
        &mut self,
        wora: &mut Wora<(), ()>,
        _exec: impl AsyncExecutor<(), ()>,
        fs: impl WFS + 'static,
        _o11y: Sender<O11yEvent<()>>,
    ) -> MainRetryAction {
        info!("waiting for events...");
        match wora
            .run_event_loop(self, fs, |app, _wora, ev| {
                info!("event: {:?}", &ev);
                let action = match ev {
                    Event::Control(control) => match control {
                        ControlEvent::ReloadConfiguration => {
                            info!("control: reload configuration");
                            EventLoopAction::Continue
                        }
                        ControlEvent::Shutdown(dt) => {
                            info!("shutting down at {:?}", dt);
                            EventLoopAction::Exit(MainRetryAction::Success)
                        }
                        ControlEvent::Suspend(dt) => {
                            info!("suspending at {:?}", dt);
                            EventLoopAction::Continue
                        }
                        ControlEvent::LogRotation => {
                            info!("rotating log");
                            EventLoopAction::Continue
                        }
                    },
                    Event::SystemResource(_) => EventLoopAction::Continue,
                    Event::ConfigChanged(change) => {
                        info!("config changed main_config_changed={} paths={:?}", change.main_config_changed, change.paths);
                        EventLoopAction::Continue
                    }
                    Event::SecretChanged(change) => {
                        info!("secret changed paths={:?}", change.paths);
                        EventLoopAction::Continue
                    }
                    Event::LeadershipChanged(old_state, new_state) => {
                        info!("leadership has changed from state {:?} to {:?}", old_state, new_state);
                        EventLoopAction::Continue
                    }
                    Event::App(_) => EventLoopAction::Continue,
                    _ => EventLoopAction::Continue,
                };

                let _ = &app.config;

                action
            })
            .await
        {
            Ok(action) => action,
            Err(err) => {
                error!("event loop reload error: {}", err);
                MainRetryAction::UseExitCode(78)
            }
        }
    }

    async fn end(&mut self, _wora: &Wora<(), ()>, _exec: impl AsyncExecutor<(), ()>, _fs: impl WFS, _o11y: Sender<O11yEvent<()>>) {}
}

#[tokio::main]
async fn main() -> Result<(), MainEarlyReturn> {
    let args = DaemonArgs::parse();

    let app_state = DaemonState {};

    let app = DaemonApp {
        args: args.clone(),
        state: Arc::new(RwLock::new(app_state)),
        config: DaemonConfig::default(),
    };

    let (tx, mut rx) = tokio::sync::mpsc::channel::<O11yEvent<()>>(10);
    let _o11y_consumer_task = tokio::spawn(async move {
        while let Some(res) = rx.recv().await {
            match res.kind {
                O11yEventKind::Status(cap, sz) => {
                    println!("{}: status cap:{} max:{}", res.timestamp, cap, sz);
                }
                O11yEventKind::App(_o11y) => {}
                O11yEventKind::HostInfo(_hi) => {}
                O11yEventKind::HostStats(_hs) => {}
                O11yEventKind::ProcessStats(_ps) => {}
                O11yEventKind::RuntimeMetrics(_rm) => {}
                O11yEventKind::Flush => {
                    println!("{}: flush", res.timestamp);
                }
                O11yEventKind::Finish => {
                    println!("{}: finish", res.timestamp);
                }
                O11yEventKind::Init(log_dir) => {
                    println!("{}: init log_dir:{:?}", res.timestamp, log_dir);
                }
                O11yEventKind::Log(level, target, name) => {
                    println!("{}: {} target:{} name:{}", res.timestamp, level, target, name);
                }
                O11yEventKind::Reconnect => {}
                O11yEventKind::Clear => {}
                O11yEventKind::Span(_, _) => {}
            }
        }
    });

    let wob = Observability {
        tx: tx.clone(),
        level: Level::INFO,
    };

    tracing_subscriber::registry().with(wob).init();

    let fs = PhysicalVFS::new();

    let interval = std::time::Duration::from_secs(5);
    let o11y = O11yProcessorOptionsBuilder::default()
        .sender(tx)
        .flush_interval(interval)
        .status_interval(interval)
        .host_stats_interval(interval)
        .build()
        .map_err(|err| MainEarlyReturn::WoraSetup(WoraSetupError::Str(err.to_string())))?;

    match &args.run_mode {
        RunMode::Sys => {
            let exec = UnixLikeSystem::new(app.name()).await;
            exec_async_runner(exec, app, fs, o11y, None).await?
        }
        RunMode::User => match UnixLikeUser::new(app.name(), fs.clone()).await {
            Ok(exec) => exec_async_runner(exec, app, fs.clone(), o11y, None).await?,
            Err(exec_err) => {
                error!("exec error:{}", exec_err);
                return Err(MainEarlyReturn::Vfs(exec_err));
            }
        },
    }

    Ok(())
}
