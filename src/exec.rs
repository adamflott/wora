use async_trait::async_trait;
#[cfg(target_os = "linux")]
use caps::CapSet;
#[cfg(target_os = "linux")]
use libc::RLIM_INFINITY;
use nix::sys::{
    mman::{MlockAllFlags, mlockall},
    resource::{Resource, setrlimit},
};
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;
use tracing::info;
use users::switch::set_both_gid;
use users::{get_effective_username, get_group_by_name, get_user_by_name, switch::set_both_uid};

use crate::dirs::Dirs;
use crate::errors::SetupFailure;
use crate::events::Event;
use crate::{WFS, Wora};

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
    /// Notify the target environment that the runtime is ready.
    ///
    /// Executors can use this hook to send service-manager specific readiness
    /// signals such as `sd_notify(READY=1)` or to materialize readiness state
    /// for container orchestrators.
    async fn on_runtime_ready(&self, _wora: &Wora<AppEv, AppMetric>, _fs: impl WFS) -> Result<(), SetupFailure> {
        Ok(())
    }
    /// Report whether the executor is ready for application main execution.
    async fn is_ready(&self, wora: &Wora<AppEv, AppMetric>, fs: impl WFS) -> bool;
    /// Notify the target environment that the runtime is stopping.
    async fn on_runtime_stopping(&self, _wora: &Wora<AppEv, AppMetric>, _fs: impl WFS) -> Result<(), SetupFailure> {
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
        let new_group = get_group_by_name(group_name).ok_or(SetupFailure::UnknownSystemUser(user_name.to_string()))?;
        set_both_gid(new_group.gid(), new_group.gid())?;
        info!("process now runs as group: {} (id: {})", group_name, new_group.gid());

        let new_user = get_user_by_name(user_name).ok_or(SetupFailure::UnknownSystemUser(user_name.to_string()))?;
        set_both_uid(new_user.uid(), new_user.uid())?;
        info!("process now runs as user: {} (id: {})", user_name, new_user.uid());

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
        get_effective_username().unwrap_or("".into()) == "root"
    }
}
