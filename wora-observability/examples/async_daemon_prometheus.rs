//! An async WORA daemon exporting its runtime metrics through Prometheus.

use std::net::SocketAddr;
use std::path::PathBuf;

use async_trait::async_trait;
use clap::{Parser, ValueEnum};
use prometheus::Registry;
use serde::Deserialize;
use tokio::sync::mpsc::Sender;
use tracing::{Level, error, info};
use tracing_subscriber::prelude::*;
use wora::prelude::*;
use wora_observability::prometheus::{PrometheusSink, serve_prometheus_metrics};

const APP_NAME: &str = "async_daemon_prometheus";

#[derive(Clone, Debug, ValueEnum)]
enum RunMode {
    Sys,
    User,
}

#[derive(Clone, Debug, Parser)]
#[command(name = "async_daemon_prometheus")]
#[command(about = "Async WORA daemon with a Prometheus metrics endpoint")]
struct DaemonArgs {
    /// Executor mode used by the daemon.
    #[arg(short, long, value_enum, default_value_t = RunMode::User)]
    run_mode: RunMode,

    /// Address on which GET /metrics is served.
    #[arg(long, default_value = "127.0.0.1:9090")]
    metrics_addr: SocketAddr,
}

#[derive(Default, Deserialize)]
struct DaemonConfig {
    #[serde(default)]
    observability: ObservabilityConfig,
}

#[derive(Deserialize)]
struct ObservabilityConfig {
    #[serde(default = "default_interval_secs")]
    flush_secs: u64,
    #[serde(default = "default_interval_secs")]
    status_secs: u64,
    #[serde(default = "default_interval_secs")]
    host_stats_secs: u64,
    #[serde(default = "default_true")]
    prometheus_enabled: bool,
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            flush_secs: default_interval_secs(),
            status_secs: default_interval_secs(),
            host_stats_secs: default_interval_secs(),
            prometheus_enabled: true,
        }
    }
}

fn default_interval_secs() -> u64 {
    5
}

fn default_true() -> bool {
    true
}

impl Config for DaemonConfig {
    type ConfigT = Self;

    fn parse_main_config_file(data: String) -> Result<Self, Box<dyn std::error::Error>> {
        toml::from_str(&data).map_err(Into::into)
    }

    fn parse_supplemental_config_file(_file_path: PathBuf, data: String) -> Result<Self, Box<dyn std::error::Error>> {
        toml::from_str(&data).map_err(Into::into)
    }
}

struct DaemonApp {
    config: DaemonConfig,
    o11y_control: O11yControlHandle,
}

#[async_trait]
impl App<(), ()> for DaemonApp {
    type AppConfig = DaemonConfig;
    type AppSecrets = NoSecrets;
    type Setup = ();

    fn name(&self) -> &'static str {
        APP_NAME
    }

    async fn reload_config(&mut self, reload: ConfigReload<Self::AppConfig>) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(config) = reload.main {
            let settings = &config.observability;
            self.o11y_control
                .apply(
                    O11ySettingsPatch::new()
                        .flush_interval(std::time::Duration::from_secs(settings.flush_secs))
                        .status_interval(std::time::Duration::from_secs(settings.status_secs))
                        .host_stats_interval(std::time::Duration::from_secs(settings.host_stats_secs)),
                )
                .await?;
            if settings.prometheus_enabled {
                self.o11y_control.enable_sink("prometheus").await?;
            } else {
                self.o11y_control.disable_sink("prometheus").await?;
            }
            self.config = config;
        }
        Ok(())
    }

    async fn setup(
        &mut self,
        _wora: &Wora<(), ()>,
        _exec: impl AsyncExecutor<(), ()>,
        _fs: impl WFS,
        _o11y: Sender<O11yEvent<()>>,
        _is_first_boot: bool,
    ) -> Result<Self::Setup, Box<dyn std::error::Error>> {
        Ok(())
    }

    async fn main(
        &mut self,
        wora: &mut Wora<(), ()>,
        _exec: impl AsyncExecutor<(), ()>,
        fs: impl WFS + 'static,
        _o11y: Sender<O11yEvent<()>>,
    ) -> MainRetryAction {
        info!("daemon running; waiting for events");
        match wora
            .run_event_loop(self, fs, |_app, _wora, event| match event {
                Event::Control(ControlEvent::Shutdown(at)) => {
                    info!(?at, "shutdown requested");
                    EventLoopAction::Exit(MainRetryAction::Success)
                }
                event => {
                    info!(?event, "runtime event");
                    EventLoopAction::Continue
                }
            })
            .await
        {
            Ok(action) => action,
            Err(error) => {
                error!(%error, "event loop failed");
                MainRetryAction::UseExitCode(78)
            }
        }
    }

    async fn end(&mut self, _wora: &Wora<(), ()>, _exec: impl AsyncExecutor<(), ()>, _fs: impl WFS, _o11y: Sender<O11yEvent<()>>) {}
}

#[tokio::main]
async fn main() -> Result<(), MainEarlyReturn> {
    let args = DaemonArgs::parse();
    let registry = Registry::new();
    let sink = PrometheusSink::builder(registry.clone())
        .static_label("app", APP_NAME)
        .build()
        .map_err(|error| MainEarlyReturn::WoraSetup(WoraSetupError::Str(error.to_string())))?;
    let listener = tokio::net::TcpListener::bind(args.metrics_addr)
        .await
        .map_err(|error| MainEarlyReturn::WoraSetup(WoraSetupError::Str(error.to_string())))?;
    let interval = std::time::Duration::from_secs(5);
    let pipeline = O11yPipeline::builder()
        .capacity(64)
        .sink("prometheus", sink)
        .flush_interval(interval)
        .status_interval(interval)
        .host_stats_interval(interval)
        .service("Prometheus HTTP endpoint", serve_prometheus_metrics(listener, registry))
        .build()
        .map_err(|error| MainEarlyReturn::WoraSetup(WoraSetupError::Str(error.to_string())))?;
    let o11y_control = pipeline.control_handle();
    let app = DaemonApp {
        config: DaemonConfig::default(),
        o11y_control,
    };

    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stdout))
        .with(pipeline.tracing_layer(Level::INFO))
        .init();
    info!(address = %args.metrics_addr, "serving Prometheus metrics at /metrics");

    let fs = PhysicalVFS::new();

    match args.run_mode {
        RunMode::Sys => exec_async_runner(UnixLikeSystem::new(app.name()).await, app, fs, pipeline).await?,
        RunMode::User => {
            let exec = UnixLikeUser::new(app.name(), fs.clone()).await.map_err(MainEarlyReturn::Vfs)?;
            exec_async_runner(exec, app, fs, pipeline).await?;
        }
    }
    Ok(())
}
