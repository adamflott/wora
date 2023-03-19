use std::sync::Arc;
use std::sync::RwLock;

use async_trait::async_trait;
use clap::{Parser, ValueEnum};
use libc::{SIGHUP, SIGINT, SIGQUIT, SIGTERM, SIGUSR1, SIGUSR2};
use log::{debug, error, info, trace, warn};
use serde::{Deserialize, Serialize};

use wora::errors::{MainEarlyReturn, SetupFailure};
use wora::events::Event;
use wora::restart_policy::MainRetryAction;
use wora::*;
use wora::{Metric, UnixLikeSystem, UnixLikeUser};

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

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DaemonState {
    peers: Vec<std::net::SocketAddr>,
}

type DaemonSharedState = Arc<RwLock<DaemonState>>;
struct DaemonApp {
    args: DaemonArgs,
    state: DaemonSharedState,
}

#[async_trait]
impl App for DaemonApp {
    type AppMetricsProducer = MetricsProducerStdout;

    fn name(&self) -> &'static str {
        "async_daemon"
    }

    async fn setup(
        &mut self,
        wora: &Wora,
        exec: &(dyn Executor + Send + Sync),
        _metrics: &(dyn MetricProcessor + Send + Sync),
    ) -> Result<(), SetupFailure> {
        let l = fern::Dispatch::new()
            .level(log::LevelFilter::Trace)
            .format(|out, message, record| {
                out.finish(format_args!(
                    "{} {} {} {}",
                    chrono::Local::now().format("%Y-%m-%d %H:%M:%S:%f"),
                    record.target(),
                    record.level(),
                    message
                ))
            })
            .chain(std::io::stdout());

        l.apply()?;

        debug!("{:?}", wora.stats_from_start());

        let args = DaemonArgs::parse();
        self.args = args;

        debug!("{:?}", exec.disable_core_dumps());

        Ok(())
    }

    async fn main(
        &mut self,
        wora: &mut Wora,
        _exec: &(dyn Executor + Send + Sync),
        metrics: &mut (dyn MetricProcessor + Send + Sync),
    ) -> MainRetryAction {
        info!("waiting for events...");
        while let Some(ev) = wora.receiver.recv().await {
            info!("event: {:?}", &ev);
            match ev {
                Event::UnixSignal(signum) => match signum {
                    SIGTERM => return MainRetryAction::Success,
                    SIGHUP => {
                        info!("sighup!");
                    }
                    SIGINT => return MainRetryAction::Success,
                    SIGUSR1 => {
                        info!("got sigusr1, incrementing counter");
                        metrics.add(Metric::Counter("sighup".to_string())).await;
                    }
                    SIGUSR2 => {}
                    SIGQUIT => {}
                    _ => {}
                },
                Event::Shutdown => return MainRetryAction::Success,
                _ => {}
            }
        }

        MainRetryAction::Success
    }

    async fn end(
        &mut self,
        _wora: &Wora,
        _exec: &(dyn Executor + Send + Sync),
        _metrics: &(dyn MetricProcessor + Send + Sync),
    ) {
        ()
    }
}

#[tokio::main]
async fn main() -> Result<(), MainEarlyReturn> {
    let args = DaemonArgs::parse();

    let app_state = DaemonState { peers: vec![] };

    let app = DaemonApp {
        args: args.clone(),
        state: Arc::new(RwLock::new(app_state)),
    };

    let metrics = MetricsProducerStdout::new().await;

    match &args.run_mode {
        RunMode::Sys => {
            let exec = UnixLikeSystem::new(app.name()).await;
            exec_async_runner(exec, app, metrics).await?
        }
        RunMode::User => {
            let exec = UnixLikeUser::new(app.name()).await;
            exec_async_runner(exec, app, metrics).await?
        }
    }

    Ok(())
}
