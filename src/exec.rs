use async_trait::async_trait;
#[cfg(target_os = "linux")]
use caps::CapSet;
#[cfg(target_os = "linux")]
use libc::RLIM_INFINITY;
use nix::sys::{
    mman::{mlockall, MlockAllFlags},
    resource::{setrlimit, Resource},
};
use tracing::info;
use users::switch::set_both_gid;
use users::{get_effective_username, get_group_by_name, get_user_by_name, switch::set_both_uid};
use vfs::async_vfs::AsyncFileSystem;

use crate::dirs::Dirs;
use crate::errors::SetupFailure;
use crate::metrics::*;
use crate::Wora;

/// Common methods for all `Executors`
pub trait Executor {
    /// Executor's unique identifier
    fn id(&self) -> &'static str;

    fn dirs(&self) -> &Dirs;

    /// Disable memory limits
    fn disable_memory_limits(&self) -> Result<(), SetupFailure> {
        info!("disabling memory limits");
        #[cfg(target_os = "linux")]
        setrlimit(Resource::RLIMIT_MEMLOCK, RLIM_INFINITY, RLIM_INFINITY)?;
        Ok(())
    }

    //  Disable paging memory to swap
    fn disable_paging_mem_to_swap(&self) -> Result<(), SetupFailure> {
        info!("disabling paging memory to swap");
        mlockall(MlockAllFlags::all())?;
        Ok(())
    }

    //  Disable core files
    fn disable_core_dumps(&self) -> Result<(), SetupFailure> {
        info!("disabling core dumps");
        setrlimit(Resource::RLIMIT_CORE, 0, 0)?;
        Ok(())
    }

    // Switch to a non-root user
    fn run_as_user_and_group(&self, user_name: &str, group_name: &str) -> Result<(), SetupFailure> {
        let new_user = get_user_by_name(user_name)
            .ok_or(SetupFailure::UnknownSystemUser(user_name.to_string()))?;
        set_both_uid(new_user.uid(), new_user.uid())?;
        info!(
            "process now runs as user: {} (id: {})",
            user_name,
            new_user.uid()
        );

        let new_group = get_group_by_name(group_name)
            .ok_or(SetupFailure::UnknownSystemUser(user_name.to_string()))?;
        set_both_gid(new_group.gid(), new_group.gid())?;
        info!(
            "process now runs as group: {} (id: {})",
            group_name,
            new_group.gid()
        );

        Ok(())
    }

    #[cfg(target_os = "linux")]
    fn has_no_caps(&self) -> Result<bool, SetupFailure> {
        let effective = caps::read(None, CapSet::Effective)?;
        Ok(effective.is_empty())
    }

    fn is_running_as_root(&self) -> bool {
        get_effective_username().unwrap_or("".into()) == "root"
    }
}

#[async_trait]
pub trait AsyncExecutor<T>: Executor {
    type Setup;
    async fn setup(
        &mut self,
        wora: &Wora<T>,
        fs: &(dyn AsyncFileSystem),
        metrics: &(dyn MetricProcessor + Send + Sync),
    ) -> Result<Self::Setup, SetupFailure>;
    async fn is_ready(&self, wora: &Wora<T>, metrics: &(dyn MetricProcessor + Send + Sync))
        -> bool;
    async fn end(&self, wora: &Wora<T>, metrics: &(dyn MetricProcessor + Send + Sync));
}
