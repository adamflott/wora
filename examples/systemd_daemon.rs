#[cfg(target_os = "linux")]
mod linux_example {
    use std::path::PathBuf;
    use std::time::Duration;

    use tokio::sync::mpsc::Sender;
    use tracing_subscriber::prelude::*;
    use wora::prelude::*;

    #[derive(Debug, Default, serde::Deserialize)]
    struct SystemdConfig {
        message: String,
        interval_ms: u64,
    }

    impl Config for SystemdConfig {
        type ConfigT = Self;

        fn parse_main_config_file(data: String) -> Result<Self::ConfigT, Box<dyn std::error::Error>> {
            Ok(toml::from_str(&data)?)
        }

        fn parse_supplemental_config_file(_file_path: PathBuf, data: String) -> Result<Self::ConfigT, Box<dyn std::error::Error>> {
            Ok(toml::from_str(&data)?)
        }
    }

    #[derive(Clone, Debug)]
    enum DaemonEvent {
        Tick,
    }

    struct SystemdDaemon {
        config: SystemdConfig,
    }

    #[async_trait]
    impl App<DaemonEvent, ()> for SystemdDaemon {
        type AppConfig = SystemdConfig;
        type AppSecrets = NoSecrets;
        type Setup = ();

        fn name(&self) -> &'static str {
            "systemd_daemon"
        }

        async fn reload_config(&mut self, reload: ConfigReload<SystemdConfig>) -> Result<(), Box<dyn std::error::Error>> {
            if let Some(config) = reload.main {
                self.config = config;
            }
            Ok(())
        }

        async fn setup(
            &mut self,
            wora: &Wora<DaemonEvent, ()>,
            _exec: impl AsyncExecutor<DaemonEvent, ()>,
            _fs: impl WFS + 'static,
            _metrics: Sender<O11yEvent<()>>,
            _is_first_boot: bool,
        ) -> Result<Self::Setup, Box<dyn std::error::Error>> {
            let sender = wora.sender.clone();
            let status = wora.status_handle();
            let interval = Duration::from_millis(self.config.interval_ms.max(100));
            tokio::spawn(async move {
                status.report_readiness(ReadinessState::Ready);
                let mut ticker = tokio::time::interval(interval);
                loop {
                    ticker.tick().await;
                    if sender.send(Event::App(DaemonEvent::Tick)).await.is_err() {
                        break;
                    }
                }
            });
            Ok(())
        }

        async fn main(
            &mut self,
            wora: &mut Wora<DaemonEvent, ()>,
            _exec: impl AsyncExecutor<DaemonEvent, ()>,
            fs: impl WFS + 'static,
            _metrics: Sender<O11yEvent<()>>,
        ) -> MainRetryAction {
            match wora
                .run_event_loop(self, fs, |app, _wora, event| match event {
                    Event::App(DaemonEvent::Tick) => {
                        tracing::info!("tick message={}", app.config.message);
                        EventLoopAction::Continue
                    }
                    Event::Control(ControlEvent::ReloadConfiguration) => {
                        tracing::info!("systemd requested configuration reload");
                        EventLoopAction::Continue
                    }
                    Event::Control(ControlEvent::LogRotation) => {
                        tracing::info!("systemd requested log rotation");
                        EventLoopAction::Continue
                    }
                    Event::Control(ControlEvent::Shutdown(deadline)) => {
                        tracing::info!("systemd requested shutdown deadline={deadline:?}");
                        EventLoopAction::Exit(MainRetryAction::Success)
                    }
                    _ => EventLoopAction::Continue,
                })
                .await
            {
                Ok(action) => action,
                Err(err) => {
                    tracing::error!("event loop failed: {}", err);
                    MainRetryAction::UseExitCode(78)
                }
            }
        }

        async fn is_healthy(&mut self) -> HealthState {
            HealthState::Ok
        }

        async fn end(
            &mut self,
            _wora: &Wora<DaemonEvent, ()>,
            _exec: impl AsyncExecutor<DaemonEvent, ()>,
            _fs: impl WFS + 'static,
            _metrics: Sender<O11yEvent<()>>,
        ) {
        }
    }

    #[tokio::main]
    pub async fn main() -> Result<(), MainEarlyReturn> {
        let (tx, _rx) = tokio::sync::mpsc::channel::<O11yEvent<()>>(64);
        tracing_subscriber::registry()
            .with(Observability {
                tx: tx.clone(),
                level: tracing::Level::INFO,
            })
            .init();

        let o11y = O11yProcessorOptionsBuilder::default()
            .sender(tx)
            .flush_interval(Duration::from_secs(30))
            .status_interval(Duration::from_secs(30))
            .host_stats_interval(Duration::from_secs(30))
            .build()
            .map_err(|err| MainEarlyReturn::WoraSetup(WoraSetupError::Str(err.to_string())))?;

        let app = SystemdDaemon {
            config: SystemdConfig {
                message: "hello from systemd".to_string(),
                interval_ms: 1000,
            },
        };

        let exec = SystemdExecutor::system(app.name()).await;

        tracing::info!(
            "starting under systemd; write config to {} and use systemctl reload/stop to drive control events",
            <SystemdExecutor as AsyncExecutor<DaemonEvent, ()>>::dirs(&exec)
                .metadata_root_dir
                .join("systemd_daemon.toml")
                .display()
        );

        exec_async_runner(exec, app, PhysicalVFS::new(), o11y, None).await
    }
}

#[cfg(target_os = "linux")]
fn main() -> Result<(), wora::errors::MainEarlyReturn> {
    linux_example::main()
}

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("examples/systemd_daemon.rs is a Linux-only runtime example. It is kept buildable on non-Linux targets for development.");
}
