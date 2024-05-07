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
        _o11y: Sender<O11yEvent<()>>,
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

    async fn is_healthy(&mut self) -> HealthState {
        HealthState::Ok
    }

    async fn end(&mut self, _wora: &Wora<(), ()>, _exec: impl AsyncExecutor<(), ()>, _fs: impl WFS, _o11y: Sender<O11yEvent<()>>) {
        info!("Final count: {}", self.counter);
    }
}

#[tokio::main]
async fn main() -> Result<(), MainEarlyReturn> {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<O11yEvent<()>>(10);
    let _o11y_consumer_task = tokio::spawn(async move {
        while let Some(res) = rx.recv().await {
            match res.kind {
                O11yEventKind::Status(cap, sz) => {
                    println!("{}: status cap:{} max:{}", res.timestamp, cap, sz);
                }
                O11yEventKind::App(_O11y) => {}
                O11yEventKind::HostInfo(_hi) => {}
                O11yEventKind::HostStats(_hs) => {}
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

    let app_name = "wora_basic";

    let args = BasicAppOpts::parse();

    let app = BasicApp { args: args, counter: 1 };

    let fs = PhysicalVFS::new();
    let interval = std::time::Duration::from_secs(5);

    let o11y = O11yProcessorOptionsBuilder::default()
        .sender(tx)
        .flush_interval(interval.clone())
        .status_interval(interval.clone())
        .host_stats_interval(interval.clone())
        .build()
        .unwrap();
    match UnixLikeUser::new(app_name, fs.clone()).await {
        Ok(exec) => exec_async_runner(exec, app, fs.clone(), o11y).await?,
        Err(exec_err) => {
            error!("exec error:{}", exec_err);
            return Err(MainEarlyReturn::Vfs(exec_err));
        }
    }

    Ok(())
}
