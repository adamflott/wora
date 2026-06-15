use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc::Sender;
use tracing::Level;
use tracing_subscriber::prelude::*;
use wora::prelude::*;

#[derive(Clone, Debug)]
enum OnboardingEvent {
    Tick,
}

#[derive(Default, Debug)]
struct OnboardingConfig {
    greeting: String,
}

impl Config for OnboardingConfig {
    type ConfigT = OnboardingConfig;

    fn parse_main_config_file(data: String) -> Result<Self::ConfigT, Box<dyn std::error::Error>> {
        Ok(Self {
            greeting: data.trim().to_string(),
        })
    }
}

struct OnboardingApp {
    greeting: String,
}

#[async_trait]
impl App<OnboardingEvent, ()> for OnboardingApp {
    type AppConfig = OnboardingConfig;
    type AppSecrets = NoSecrets;
    type Setup = ();

    fn name(&self) -> &'static str {
        "onboarding"
    }

    async fn reload_config(&mut self, reload: ConfigReload<OnboardingConfig>) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(config) = reload.main {
            self.greeting = config.greeting;
        }
        Ok(())
    }

    async fn setup(
        &mut self,
        wora: &Wora<OnboardingEvent, ()>,
        _exec: impl AsyncExecutor<OnboardingEvent, ()>,
        _fs: impl WFS + 'static,
        _metrics: Sender<O11yEvent<()>>,
        _is_first_boot: bool,
    ) -> Result<Self::Setup, Box<dyn std::error::Error>> {
        wora.report_readiness(ReadinessState::Ready);

        let sender = wora.sender.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(25)).await;
            let _ = sender.send(Event::App(OnboardingEvent::Tick)).await;
            tokio::time::sleep(Duration::from_millis(25)).await;
            let _ = sender.send(Event::Control(ControlEvent::Shutdown(None))).await;
        });

        Ok(())
    }

    async fn main(
        &mut self,
        wora: &mut Wora<OnboardingEvent, ()>,
        _exec: impl AsyncExecutor<OnboardingEvent, ()>,
        fs: impl WFS + 'static,
        _metrics: Sender<O11yEvent<()>>,
    ) -> MainRetryAction {
        match wora
            .run_event_loop(self, fs, |app, _wora, event| match event {
                Event::App(OnboardingEvent::Tick) => {
                    println!("{}", app.greeting);
                    EventLoopAction::Continue
                }
                Event::Control(ControlEvent::Shutdown(_)) => EventLoopAction::Exit(MainRetryAction::Success),
                _ => EventLoopAction::Continue,
            })
            .await
        {
            Ok(action) => action,
            Err(err) => {
                eprintln!("reload error: {}", err);
                MainRetryAction::UseExitCode(78)
            }
        }
    }

    async fn end(
        &mut self,
        _wora: &Wora<OnboardingEvent, ()>,
        _exec: impl AsyncExecutor<OnboardingEvent, ()>,
        _fs: impl WFS + 'static,
        _metrics: Sender<O11yEvent<()>>,
    ) {
    }
}

#[tokio::main]
async fn main() -> Result<(), MainEarlyReturn> {
    let entries = Arc::new(Mutex::new(Vec::new()));
    let log_path = std::env::temp_dir().join("wora-onboarding.jsonl");
    let processor = O11yProcessor::new(vec![
        Box::new(O11yMemorySink::new(entries.clone())),
        Box::new(O11yJsonLinesSink::new(log_path.clone()).with_name("onboarding")),
    ]);
    let (tx, rx) = tokio::sync::mpsc::channel::<O11yEvent<()>>(64);
    let _processor_task = processor.spawn(rx);

    tracing_subscriber::registry()
        .with(Observability {
            tx: tx.clone(),
            level: Level::INFO,
        })
        .init();

    let fs = PhysicalVFS::new();
    let o11y = O11yProcessorOptionsBuilder::default()
        .sender(tx)
        .flush_interval(Duration::from_millis(25))
        .status_interval(Duration::from_millis(25))
        .host_stats_interval(Duration::from_millis(25))
        .build()
        .map_err(|err| MainEarlyReturn::WoraSetup(WoraSetupError::Str(err.to_string())))?;

    let app_name = "onboarding";
    let exec = UnixLikeUser::new(app_name, fs.clone()).await.map_err(MainEarlyReturn::Vfs)?;
    let config_path: PathBuf = <UnixLikeUser as AsyncExecutor<OnboardingEvent, ()>>::dirs(&exec)
        .metadata_root_dir
        .join("onboarding.toml");
    if let Some(parent) = config_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    tokio::fs::write(&config_path, "hello from wora").await?;

    let app = OnboardingApp {
        greeting: "hello from onboarding".to_string(),
    };

    exec_async_runner(exec, app, fs, o11y, None).await?;

    let entries = entries.lock().map_err(|err| MainEarlyReturn::WoraSetup(WoraSetupError::Str(err.to_string())))?;
    println!("captured {} observability events", entries.len());
    println!("json-lines sink path: {}", log_path.display());
    Ok(())
}
