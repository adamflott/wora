use async_trait::async_trait;
use clap::Parser;
use log::{debug, error, info, trace, warn};
use tokio::sync::mpsc::Sender;
use tracing::Level;
use tracing_subscriber;

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
    type Setup = ();
    fn name(&self) -> &'static str {
        "wora_basic"
    }

    async fn setup(
        &mut self,
        _wora: &Wora<(), ()>,
        _exec: impl AsyncExecutor<(), ()>,
        _fs: impl WFS,
        _metrics: Sender<MetricEvent<()>>,
    ) -> Result<Self::Setup, Box<dyn std::error::Error>> {
        debug!("command args: {:?}", self.args);
        Ok(())
    }

    async fn main(&mut self, _wora: &mut Wora<(), ()>, _exec: impl AsyncExecutor<(), ()>, _fs: impl WFS, _metrics: Sender<MetricEvent<()>>) -> MainRetryAction {
        trace!("Trace message");
        debug!("Debug message");
        info!("Info message");
        warn!("Warning message");
        error!("Error message");
        self.counter += 1;

        MainRetryAction::Success
    }

    async fn is_healthy(&mut self) -> HealthState {
        HealthState::Ok
    }

    async fn end(&mut self, _wora: &Wora<(), ()>, _exec: impl AsyncExecutor<(), ()>, _fs: impl WFS, _metrics: Sender<MetricEvent<()>>) {
        info!("Final count: {}", self.counter);
    }
}

#[tokio::main]
async fn main() -> Result<(), MainEarlyReturn> {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<MetricEvent<()>>(10);
    let _metrics_consumer_task = tokio::spawn(async move {
        while let Some(res) = rx.recv().await {
            match res.kind {
                MetricEventKind::Status(cap, sz) => {
                    println!("{}: status cap:{} max:{}", res.timestamp, cap, sz);
                }
                MetricEventKind::App(_metric) => {}
                MetricEventKind::HostInfo(_hi) => {}
                MetricEventKind::HostStats(_hs) => {}
                MetricEventKind::Flush => {
                    println!("{}: flush", res.timestamp);
                }
                MetricEventKind::Finish => {
                    println!("{}: finish", res.timestamp);
                }
                MetricEventKind::Init => {
                    println!("{}: init", res.timestamp);
                }
                MetricEventKind::Log(level, target, name) => {
                    println!("{}: {} target:{} name:{}", res.timestamp, level, target, name);
                }
                MetricEventKind::Reconnect => {}
            }
        }
    });

    let wob = Observability {
        tx: tx.clone(),
        level: Level::INFO,
    };

    tracing_subscriber::registry().with(wob).init();

    let app_name = "wora_basic";

    let args = BasicAppOpts::parse();

    let app = BasicApp { args: args, counter: 1 };

    let fs = PhysicalVFS::new();
    let interval = std::time::Duration::from_secs(5);

    let metrics = MetricsProcessorOptionsBuilder::default()
        .sender(tx)
        .flush_interval(interval.clone())
        .status_interval(interval.clone())
        .host_stats_interval(interval.clone())
        .build()
        .unwrap();
    match UnixLikeUser::new(app_name, fs.clone()).await {
        Ok(exec) => exec_async_runner(exec, app, fs.clone(), metrics).await?,
        Err(exec_err) => {
            error!("exec error:{}", exec_err);
            return Err(MainEarlyReturn::Vfs(exec_err));
        }
    }

    Ok(())
}
