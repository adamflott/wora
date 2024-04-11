use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock;

use async_trait::async_trait;
use clap::{Parser, ValueEnum};
use libc::{SIGHUP, SIGINT, SIGQUIT, SIGTERM, SIGUSR1};
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info, trace};
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::{filter, prelude::*, reload, Registry};

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
struct Obj {
    t_or_f: bool,
    list: Vec<String>,
}

#[derive(Default, Deserialize)]
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
    state: DaemonSharedState,
    log_reload_handle: reload::Handle<filter::LevelFilter, Registry>,
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
    fn parse_supplemental_config_file(
        _file_path: PathBuf,
        data: String,
    ) -> Result<DaemonConfig, Box<dyn std::error::Error>> {
        match toml::from_str(&data) {
            Ok(v) => Ok(v),
            Err(err) => Err(Box::new(err)),
        }
    }
}

#[async_trait]
impl App<()> for DaemonApp {
    type AppMetricsProducer = MetricsProducerStdout;
    type AppConfig = DaemonConfig;
    type Setup = ();
    fn name(&self) -> &'static str {
        "async_daemon"
    }

    async fn setup(
        &mut self,
        wora: &Wora<()>,
        exec: &(dyn Executor + Send + Sync),
        _fs: impl WFS,
        _metrics: &(dyn MetricProcessor + Send + Sync),
    ) -> Result<Self::Setup, Box<dyn std::error::Error>> {
        debug!("{:?}", wora.stats_from_start());

        let args = DaemonArgs::parse();
        self.args = args;

        debug!("{:?}", exec.disable_core_dumps());

        Ok(())
    }

    async fn main(
        &mut self,
        wora: &mut Wora<()>,
        _exec: &(dyn Executor + Send + Sync),
        fs: impl WFS,
        _metrics: &mut (dyn MetricProcessor + Send + Sync),
    ) -> MainRetryAction {
        info!("waiting for events...");
        while let Some(ev) = wora.receiver.recv().await {
            info!("event: {:?}", &ev);
            match ev {
                Event::UnixSignal(signum) => match signum {
                    SIGTERM | SIGINT | SIGQUIT => return MainRetryAction::UseExitCode(1),
                    SIGHUP => {
                        info!("sighup!");
                    }
                    SIGUSR1 => {
                        self.log_reload_handle.modify(|filter| {
                            *filter = filter::LevelFilter::TRACE;
                        });
                        trace!("changed log level to trace");
                    }
                    _ => {}
                },
                Event::Shutdown(dt) => {
                    info!("shutting down at {:?}", dt);
                    return MainRetryAction::Success;
                }
                Event::SystemResource(_) => {}
                Event::ConfigChange(event) => {
                    for pathbuf in event.paths {
                        match fs.read_to_string(pathbuf).await {
                            Ok(data) => match DaemonConfig::parse_main_config_file(data) {
                                Ok(cfg) => {
                                    info!("config changed");
                                    self.config = cfg;
                                }
                                Err(err) => {
                                    error!("failed to parse config{:?}", err);
                                }
                            },
                            Err(_) => {}
                        }
                    }
                }
                Event::Suspended(dt) => {
                    info!("suspending at {:?}", dt);
                }
                Event::LogRotation => {
                    info!("rotating log");
                }
                Event::LeadershipChange(old_state, new_state) => {
                    info!(
                        "leadership has changed from state {:?} to {:?}",
                        old_state, new_state
                    );
                }
                Event::App(_) => {}
                _ => {}
            }
        }

        MainRetryAction::Success
    }

    async fn is_healthy(&mut self) -> HealthState {
        HealthState::Ok
    }

    async fn end(
        &mut self,
        _wora: &Wora<()>,
        _exec: &(dyn Executor + Send + Sync),
        _fs: impl WFS,
        _metrics: &(dyn MetricProcessor + Send + Sync),
    ) {
        ()
    }
}

#[tokio::main]
async fn main() -> Result<(), MainEarlyReturn> {
    let filter = filter::LevelFilter::TRACE;
    let (filter, reload_handle) = reload::Layer::new(filter);

    let format = tracing_subscriber::fmt::format()
        .with_file(true)
        .with_line_number(true)
        .with_level(true) // don't include levels in formatted output
        .with_target(true) // don't include targets
        .with_thread_ids(true) // include the thread ID of the current thread
        .with_thread_names(true); // include the name of the current thread

    let span_layer = tracing_subscriber::fmt::layer()
        .event_format(format)
        .with_span_events(FmtSpan::CLOSE | FmtSpan::ENTER);

    tracing_subscriber::registry().with(filter).with(span_layer).init();

    let args = DaemonArgs::parse();

    let app_state = DaemonState {};

    let app = DaemonApp {
        args: args.clone(),
        state: Arc::new(RwLock::new(app_state)),
        log_reload_handle: reload_handle,
        config: DaemonConfig::default(),
    };

    let metrics = MetricsProducerStdout::new().await;
    let fs = PhysicalVFS::new();
    match &args.run_mode {
        RunMode::Sys => {
            let exec = UnixLikeSystem::new(app.name()).await;
            exec_async_runner(exec, app, fs, metrics).await?
        }
        RunMode::User => match UnixLikeUser::new(app.name(), fs.clone()).await {
            Ok(exec) => exec_async_runner(exec, app, fs.clone(), metrics).await?,
            Err(exec_err) => {
                error!("exec error:{}", exec_err);
                return Err(MainEarlyReturn::Vfs(exec_err));
            }
        },
    }

    Ok(())
}
