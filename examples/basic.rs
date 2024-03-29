use async_trait::async_trait;
use clap::Parser;
use log::{debug, error, info, trace, warn};
use tracing_subscriber;
use vfs::async_vfs::{AsyncFileSystem, AsyncPhysicalFS};

use wora::errors::*;
use wora::exec::*;
use wora::exec_unix::*;
use wora::metrics::*;
use wora::restart_policy::MainRetryAction;
use wora::*;

#[derive(Clone, Debug, Parser)]
#[command(
    author,
    version,
    about,
    long_about = "A basic wora example to show off various features"
)]
struct BasicAppOpts {
    /// start app counter at n
    #[arg(short, long, default_value_t = 0)]
    counter: u32,

    /// logging level
    #[arg(short, long, default_value_t=log::LevelFilter::Trace)]
    level: log::LevelFilter,
}

#[derive(Debug)]
struct BasicApp {
    args: BasicAppOpts,
    counter: u32,
}


#[async_trait]
impl App<()> for BasicApp {
    type AppMetricsProducer = MetricsProducerStdout;
    type AppConfig = NoConfig;
    type Setup = ();
    fn name(&self) -> &'static str {
        "wora_basic"
    }

    async fn setup(
        &mut self,
        _wora: &Wora<()>,
        _exec: &(dyn Executor + Send + Sync),
        _fs: &impl AsyncFileSystem,
        _metrics: &(dyn MetricProcessor + Send + Sync),
    ) -> Result<Self::Setup, Box<dyn std::error::Error>> {
        debug!("command args: {:?}", self.args);
        Ok(())
    }

    async fn main(
        &mut self,
        _wora: &mut Wora<()>,
        _exec: &(dyn Executor + Send + Sync),
        _fs: &impl AsyncFileSystem,
        _metrics: &mut (dyn MetricProcessor + Send + Sync),
    ) -> MainRetryAction {
        trace!("Trace message");
        debug!("Debug message");
        info!("Info message");
        warn!("Warning message");
        error!("Error message");
        self.counter += 1;

        MainRetryAction::Success
    }

    async fn is_healthy() -> HealthState {
        HealthState::Ok
    }

    async fn end(
        &mut self,
        _wora: &Wora<()>,
        _exec: &(dyn Executor + Send + Sync),
        _fs: &impl AsyncFileSystem,
        _metrics: &(dyn MetricProcessor + Send + Sync),
    ) {
        info!("Final count: {}", self.counter);
    }
}

#[tokio::main]
async fn main() -> Result<(), MainEarlyReturn> {
    tracing_subscriber::fmt::init();

    let app_name = "wora_basic";

    let args = BasicAppOpts::parse();

    let app = BasicApp {
        args: args,
        counter: 1,
    };

    let fs = AsyncPhysicalFS::new("/");
    let metrics = MetricsProducerStdout::new().await;
    match UnixLikeUser::new(app_name, &fs).await {
        Ok(exec) => exec_async_runner(exec, app, fs, metrics).await?,
        Err(exec_err) => {
            error!("exec error:{}", exec_err);
            return Err(MainEarlyReturn::Vfs(exec_err));
        }
    }

    Ok(())
}
