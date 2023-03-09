use async_trait::async_trait;
use clap::{Parser, ValueEnum};
use log::{debug, error, info, trace, warn};

use wora::errors::EnvVarsParseError;
use wora::errors::{MainEarlyReturn, SetupFailure};
use wora::restart_policy::MainRetryAction;
use wora::*;
use wora::{UnixLikeSystem, UnixLikeUser};

#[derive(Clone, Debug, ValueEnum)]
enum RunMode {
    Sys,
    User,
}

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

    /// change default run mode
    #[arg(short, long, value_enum, default_value_t=RunMode::User)]
    run_mode: RunMode,
}

#[derive(Clone, Debug)]
struct BasicEnvVars {
    counter: Option<u32>,
}

#[derive(Clone, Debug)]
enum BasicEvents {}

fn new_env_vars_parser() -> Result<BasicEnvVars, EnvVarsParseError> {
    todo!()
}

#[derive(Debug)]
struct BasicApp {
    args: BasicAppOpts,
    counter: u32,
}

#[async_trait]
impl App for BasicApp {
    fn name(&self) -> &'static str {
        "wora_basic"
    }
    async fn setup(
        &mut self,
        wora: &Wora,
        _exec: &(dyn Executor + Send + Sync),
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

        //info!("{:?}", wora.stats_from_start());

        Ok(())
    }

    async fn main(
        &mut self,
        wora: &mut Wora,
        _exec: &(dyn Executor + Send + Sync),
    ) -> MainRetryAction {
        trace!("Trace message");
        debug!("Debug message");
        info!("Info message");
        warn!("Warning message");
        error!("Error message");
        self.counter += 1;

        let ten_millis = std::time::Duration::from_millis(10);
        let _now = std::time::Instant::now();

        std::thread::sleep(ten_millis);

        MainRetryAction::Success
    }

    async fn end(&mut self, wora: &Wora, _exec: &(dyn Executor + Send + Sync)) {
        info!("Final count: {}", self.counter);
    }
}

#[tokio::main]
async fn main() -> Result<(), MainEarlyReturn> {
    let app_name = "wora_basic";

    let args = BasicAppOpts::parse();

    //let env_vars = new_env_vars_parser()?;
    let app = BasicApp {
        args: args.clone(),
        counter: 1,
    };

    match &args.run_mode {
        RunMode::Sys => {
            let exec = UnixLikeSystem::new(app_name).await;
            exec_async_runner(exec, app).await?
        }
        RunMode::User => {
            let exec = UnixLikeUser::new(app_name).await;
            exec_async_runner(exec, app).await?
        }
    }

    Ok(())
}
