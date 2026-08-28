use async_trait::async_trait;
use clap::Parser;
use log::{debug, error, info, trace, warn};
use tokio::sync::mpsc::Sender;
use tracing::Level;

use wora::prelude::*;

#[derive(Clone, Debug, Parser)]
#[command(author, version, about, long_about = "A basic wora example to show off various features")]
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
impl App<(), ()> for BasicApp {
    type AppConfig = NoConfig;
    type AppSecrets = NoSecrets;
    type Setup = ();
    fn name(&self) -> &'static str {
        "wora_basic"
    }

    async fn setup(
        &mut self,
        _wora: &Wora<(), ()>,
        _exec: impl AsyncExecutor<(), ()>,
        _fs: impl WFS,
        _o11y: Sender<O11yEvent<()>>,
        _is_first_boot: bool,
    ) -> Result<Self::Setup, Box<dyn std::error::Error>> {
        debug!("command args: {:?}", self.args);
        Ok(())
    }

    async fn main(&mut self, _wora: &mut Wora<(), ()>, _exec: impl AsyncExecutor<(), ()>, _fs: impl WFS, _o11y: Sender<O11yEvent<()>>) -> MainRetryAction {
        trace!("Trace message");
        debug!("Debug message");
        info!("Info message");
        warn!("Warning message");
        error!("Error message");
        self.counter += 1;

        MainRetryAction::Success
    }

    async fn end(&mut self, _wora: &Wora<(), ()>, _exec: impl AsyncExecutor<(), ()>, _fs: impl WFS, _o11y: Sender<O11yEvent<()>>) {
        info!("Final count: {}", self.counter);
    }
}

#[tokio::main]
async fn main() -> Result<(), MainEarlyReturn> {
    let app_name = "wora_basic";

    let args = BasicAppOpts::parse();

    let app = BasicApp { args, counter: 1 };

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
    match UnixLikeUser::new(app_name, fs.clone()).await {
        Ok(exec) => exec_async_runner(exec, app, fs.clone(), pipeline).await?,
        Err(exec_err) => {
            error!("exec error:{}", exec_err);
            return Err(MainEarlyReturn::Vfs(exec_err));
        }
    }

    Ok(())
}
