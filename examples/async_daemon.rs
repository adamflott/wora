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

    let fs = PhysicalVFS::new();

    let interval = std::time::Duration::from_secs(5);
    let pipeline = O11yPipeline::builder()
        .capacity(10)
        .sink("stdout", O11yStdoutSink)
        .flush_interval(interval)
        .status_interval(interval)
        .host_stats_interval(interval)
        .build()
        .map_err(|err| MainEarlyReturn::WoraSetup(WoraSetupError::Str(err.to_string())))?;
    tracing_subscriber::registry().with(pipeline.tracing_layer(Level::INFO)).init();

    match &args.run_mode {
        RunMode::Sys => {
            let exec = UnixLikeSystem::new(app.name()).await;
            exec_async_runner(exec, app, fs, pipeline).await?
        }
        RunMode::User => match UnixLikeUser::new(app.name(), fs.clone()).await {
            Ok(exec) => exec_async_runner(exec, app, fs.clone(), pipeline).await?,
            Err(exec_err) => {
                error!("exec error:{}", exec_err);
                return Err(MainEarlyReturn::Vfs(exec_err));
            }
        },
    }

    Ok(())
}
