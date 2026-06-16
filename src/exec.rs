use async_trait::async_trait;
#[cfg(target_os = "linux")]
use caps::CapSet;
#[cfg(target_os = "linux")]
use libc::RLIM_INFINITY;
use nix::sys::{
    mman::{MlockAllFlags, mlockall},
    resource::{Resource, setrlimit},
};
use nix::unistd::{Group, Uid, User, setgid, setuid};
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;
use tracing::info;

use crate::dirs::Dirs;
use crate::errors::SetupFailure;
use crate::events::{ControlEvent, Event};
use crate::{WFS, Wora};

/// Platform signal understood by WORA's built-in Unix-like executors.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum RuntimeSignal {
    /// Hangup, normally `SIGHUP`.
    Hangup,
    /// Interrupt, normally `SIGINT`.
    Interrupt,
    /// Quit, normally `SIGQUIT`.
    Quit,
    /// Termination request, normally `SIGTERM`.
    Terminate,
    /// User-defined signal 1, normally `SIGUSR1`.
    User1,
    /// User-defined signal 2, normally `SIGUSR2`.
    User2,
}

/// Mapper used by the runner to translate platform signals into WORA events.
pub type SignalMapper<AppEv> = std::sync::Arc<dyn Fn(RuntimeSignal) -> Option<Event<AppEv>> + Send + Sync + 'static>;

/// Return WORA's default Unix-like signal mapping.
///
/// The default preserves the historical built-in executor behavior:
/// `SIGHUP` requests configuration reload, `SIGTERM`/`SIGINT`/`SIGQUIT`
/// request shutdown, `SIGUSR1` requests log rotation, and `SIGUSR2` is ignored.
pub fn default_signal_mapper<AppEv>() -> SignalMapper<AppEv> {
    std::sync::Arc::new(|signal| match signal {
        RuntimeSignal::Hangup => Some(Event::Control(ControlEvent::ReloadConfiguration)),
        RuntimeSignal::Interrupt | RuntimeSignal::Quit | RuntimeSignal::Terminate => {
            Some(Event::Control(ControlEvent::Shutdown(Some(chrono::Utc::now().naive_utc()))))
        }
        RuntimeSignal::User1 => Some(Event::Control(ControlEvent::LogRotation)),
        RuntimeSignal::User2 => None,
    })
}

/// Runtime environment adapter used by `exec_async_runner`.
///
/// Implementations provide directory layout, setup, readiness, and teardown
/// behavior for a target environment. The built-in executors cover Unix-like
/// system, user, and bare `/tmp` layouts.
#[async_trait]
pub trait AsyncExecutor<AppEv: Send + 'static, AppMetric>: Send + Sync + Clone {
    /// Executor's unique identifier
    fn id(&self) -> &'static str;
    /// Executor's set of directory paths
    fn dirs(&self) -> &Dirs;

    /// Prepare the target environment before application setup runs.
    async fn setup(&mut self, wora: &Wora<AppEv, AppMetric>, fs: impl WFS) -> Result<(), SetupFailure>;
    /// Start environment-specific runtime event sources.
    ///
    /// Executors use this hook to translate platform-specific controls such as
    /// signals, service-manager notifications, or admin APIs into WORA events.
    /// The default implementation does nothing.
    async fn spawn_runtime_event_sources(&self, _sender: Sender<Event<AppEv>>) -> Result<Vec<JoinHandle<()>>, SetupFailure> {
        Ok(vec![])
    }

    /// Start runtime event sources with a caller-supplied signal mapper.
    ///
    /// Custom executors that do not consume OS signals can keep the default
    /// implementation. Signal-aware executors should use `signal_mapper` to
    /// translate platform signals into app-specific events.
    async fn spawn_runtime_event_sources_with_signal_mapper(
        &self,
        sender: Sender<Event<AppEv>>,
        _signal_mapper: SignalMapper<AppEv>,
    ) -> Result<Vec<JoinHandle<()>>, SetupFailure> {
        self.spawn_runtime_event_sources(sender).await
    }
    /// Notify the target environment that the runtime is ready.
    ///
    /// Executors can use this hook to send service-manager specific readiness
    /// signals such as `sd_notify(READY=1)` or to materialize readiness state
    /// for container orchestrators.
    async fn on_runtime_ready(&self, _app_name: &str, _dirs: &Dirs, _fs: impl WFS) -> Result<(), SetupFailure> {
        Ok(())
    }
    /// Notify the target environment that the runtime is draining.
    ///
    /// Executors can use this hook to withdraw readiness before the shutdown
    /// grace period starts, allowing service managers and load balancers to
    /// stop sending new work while existing requests drain.
    async fn on_runtime_draining(&self, _app_name: &str, _dirs: &Dirs, _fs: impl WFS) -> Result<(), SetupFailure> {
        Ok(())
    }
    /// Report whether the executor is ready for application main execution.
    async fn is_ready(&self, wora: &Wora<AppEv, AppMetric>, fs: impl WFS) -> bool;
    /// Notify the target environment that the runtime is stopping.
    async fn on_runtime_stopping(&self, _app_name: &str, _dirs: &Dirs, _fs: impl WFS) -> Result<(), SetupFailure> {
        Ok(())
    }
    /// Clean up executor state after the application lifecycle completes.
    async fn end(&self, wora: &Wora<AppEv, AppMetric>, fs: impl WFS);

    /// Disable memory limits
    fn disable_memory_limits(&self) -> Result<(), SetupFailure> {
        info!("disabling memory limits");
        #[cfg(target_os = "linux")]
        setrlimit(Resource::RLIMIT_MEMLOCK, RLIM_INFINITY, RLIM_INFINITY)?;
        Ok(())
    }

    /// Disable paging memory to swap.
    fn disable_paging_mem_to_swap(&self) -> Result<(), SetupFailure> {
        info!("disabling paging memory to swap");
        mlockall(MlockAllFlags::all())?;
        Ok(())
    }

    /// Disable core files.
    fn disable_core_dumps(&self) -> Result<(), SetupFailure> {
        info!("disabling core dumps");
        setrlimit(Resource::RLIMIT_CORE, 0, 0)?;
        Ok(())
    }

    /// Switch to a non-root user and group.
    fn run_as_user_and_group(&self, user_name: &str, group_name: &str) -> Result<(), SetupFailure> {
        let new_group = Group::from_name(group_name)?.ok_or_else(|| SetupFailure::UnknownSystemUser(group_name.to_string()))?;
        let new_user = User::from_name(user_name)?.ok_or_else(|| SetupFailure::UnknownSystemUser(user_name.to_string()))?;

        setgid(new_group.gid)?;
        info!("process now runs as group: {} (id: {})", group_name, new_group.gid);

        setuid(new_user.uid)?;
        info!("process now runs as user: {} (id: {})", user_name, new_user.uid);

        Ok(())
    }

    #[cfg(target_os = "linux")]
    /// Return whether the current process has no effective Linux capabilities.
    fn has_no_caps(&self) -> Result<bool, SetupFailure> {
        let effective = caps::read(None, CapSet::Effective)?;
        Ok(effective.is_empty())
    }

    /// Return whether the current effective user is root.
    fn is_running_as_root(&self) -> bool {
        Uid::effective().is_root()
    }
}
