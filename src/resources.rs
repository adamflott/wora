//! Count admission for runner-owned work. These limits do not measure heap/RSS.

use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// Caller-configured count budgets, validated by [`RuntimeResources::new`].
///
/// Queues are limited per runner; task/provider/sink/service admission is shared
/// across clones of one [`RuntimeResources`] handle, including overlapping runs.
/// Payload bytes, pending public senders, custom buffers and thread stacks are
/// outside these counts. Defaults are count policies, not measured heap ceilings.
/// Use `Default` and assign fields; the non-exhaustive shape permits future budgets.
#[derive(Clone, Copy, Debug)]
#[non_exhaustive]
pub struct RuntimeResourceLimits {
    /// Records in the application's event queue.
    pub app_event_records: usize,
    /// Records in the runner's dispatch queue.
    pub runtime_event_records: usize,
    /// Records in the runner's supervision queue.
    pub supervision_records: usize,
    /// Maximum accepted capacity of an already-built observability event queue.
    pub observability_records: usize,
    /// Maximum accepted capacity of an already-built observability command queue.
    pub command_records: usize,
    /// Concurrent application-scheduled loops; leases last until their futures drop.
    pub scheduled_tasks: usize,
    /// Concurrent blocking provider jobs admitted by runners using this handle.
    pub provider_workers: usize,
    /// Total registered sinks in runners sharing this handle.
    pub sinks: usize,
    /// Total registered exporter services in runners sharing this handle.
    pub services: usize,
}

impl Default for RuntimeResourceLimits {
    fn default() -> Self {
        Self {
            app_event_records: 1024,
            runtime_event_records: 1024,
            supervision_records: 1024,
            observability_records: 64,
            command_records: 32,
            scheduled_tasks: 64,
            provider_workers: 1,
            sinks: 16,
            services: 16,
        }
    }
}

impl RuntimeResourceLimits {
    /// Reject zero/excessive counts before constructing channels or semaphores.
    pub fn validate(&self) -> Result<(), ResourceError> {
        for (name, value) in [
            ("app_event_records", self.app_event_records),
            ("runtime_event_records", self.runtime_event_records),
            ("supervision_records", self.supervision_records),
            ("observability_records", self.observability_records),
            ("command_records", self.command_records),
            ("scheduled_tasks", self.scheduled_tasks),
            ("provider_workers", self.provider_workers),
            ("sinks", self.sinks),
            ("services", self.services),
        ] {
            if !(1..=65536).contains(&value) {
                return Err(ResourceError::InvalidCount(name));
            }
        }
        Ok(())
    }
}

/// Validated admission state shared by runners and retained work.
///
/// Clones share permits. Dropping a runner does not release a provider/task lease
/// held by surviving work. Reuse this handle when starting a replacement runner.
/// Constructing it performs no telemetry reads and does not configure Tokio.
#[derive(Clone, Debug)]
pub struct RuntimeResources {
    limits: RuntimeResourceLimits,
    tasks: Arc<Semaphore>,
    providers: Arc<Semaphore>,
    sinks: Arc<Semaphore>,
    services: Arc<Semaphore>,
}

impl RuntimeResources {
    /// Validate all counts before allocating admission state.
    pub fn new(limits: RuntimeResourceLimits) -> Result<Self, ResourceError> {
        limits.validate()?;
        Ok(Self {
            limits,
            tasks: Arc::new(Semaphore::new(limits.scheduled_tasks)),
            providers: Arc::new(Semaphore::new(limits.provider_workers)),
            sinks: Arc::new(Semaphore::new(limits.sinks)),
            services: Arc::new(Semaphore::new(limits.services)),
        })
    }

    /// Inspect the validated count policy; values are caller settings, not OS samples.
    pub fn limits(&self) -> &RuntimeResourceLimits {
        &self.limits
    }

    /// Number of currently admitted application-scheduled loops.
    pub fn scheduled_tasks(&self) -> usize {
        self.limits.scheduled_tasks - self.tasks.available_permits()
    }

    /// Number of admitted provider jobs, including jobs surviving cancellation.
    pub fn provider_workers(&self) -> usize {
        self.limits.provider_workers - self.providers.available_permits()
    }

    pub(crate) fn try_task(&self) -> Result<OwnedSemaphorePermit, ResourceError> {
        self.tasks.clone().try_acquire_owned().map_err(|_| ResourceError::Exhausted("scheduled_tasks"))
    }

    pub(crate) async fn task(&self) -> Result<OwnedSemaphorePermit, ResourceError> {
        self.tasks
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| ResourceError::Exhausted("scheduled_tasks"))
    }

    pub(crate) async fn provider(&self) -> Result<OwnedSemaphorePermit, ResourceError> {
        self.providers
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| ResourceError::Exhausted("provider_workers"))
    }

    pub(crate) fn try_provider(&self) -> Result<OwnedSemaphorePermit, ResourceError> {
        self.providers
            .clone()
            .try_acquire_owned()
            .map_err(|_| ResourceError::Exhausted("provider_workers"))
    }

    pub(crate) fn pipeline(&self, sinks: usize, services: usize, events: usize, commands: usize) -> Result<PipelineAdmission, ResourceError> {
        if events > self.limits.observability_records {
            return Err(ResourceError::Exhausted("observability_records"));
        }
        if commands > self.limits.command_records {
            return Err(ResourceError::Exhausted("command_records"));
        }
        let sinks = u32::try_from(sinks).map_err(|_| ResourceError::Exhausted("sinks"))?;
        let services = u32::try_from(services).map_err(|_| ResourceError::Exhausted("services"))?;
        let sink_lease = self
            .sinks
            .clone()
            .try_acquire_many_owned(sinks)
            .map_err(|_| ResourceError::Exhausted("sinks"))?;
        let service_lease = self
            .services
            .clone()
            .try_acquire_many_owned(services)
            .map_err(|_| ResourceError::Exhausted("services"))?;
        Ok(PipelineAdmission {
            _sinks: sink_lease,
            _services: service_lease,
        })
    }
}

pub(crate) struct PipelineAdmission {
    _sinks: OwnedSemaphorePermit,
    _services: OwnedSemaphorePermit,
}

/// Invalid caller configuration or admission refused before starting more work.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ResourceError {
    /// A named count must be within 1..=65536.
    #[error("resource count {0} must be 1..=65536")]
    InvalidCount(&'static str),
    /// A named count allowance is exhausted or incompatible with the pipeline.
    #[error("resource allowance exhausted: {0}")]
    Exhausted(&'static str),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn invalid_limits_fail_before_admission_and_pipeline_failures_roll_back_leases() -> Result<(), ResourceError> {
        assert!(matches!(
            RuntimeResources::new(RuntimeResourceLimits {
                provider_workers: 0,
                ..Default::default()
            }),
            Err(ResourceError::InvalidCount("provider_workers"))
        ));
        assert!(matches!(
            RuntimeResources::new(RuntimeResourceLimits {
                app_event_records: usize::MAX,
                ..Default::default()
            }),
            Err(ResourceError::InvalidCount("app_event_records"))
        ));
        let resources = RuntimeResources::new(RuntimeResourceLimits {
            sinks: 1,
            services: 1,
            ..Default::default()
        })?;
        let held_service = resources.pipeline(0, 1, 64, 32)?;
        assert!(matches!(resources.clone().pipeline(1, 1, 64, 32), Err(ResourceError::Exhausted("services"))));
        let held_sink = resources.pipeline(1, 0, 64, 32)?;
        assert!(matches!(resources.pipeline(1, 0, 64, 32), Err(ResourceError::Exhausted("sinks"))));
        drop(held_sink);
        drop(held_service);
        assert!(resources.pipeline(1, 1, 64, 32).is_ok());
        assert!(matches!(
            resources.pipeline(1, 0, 65, 32),
            Err(ResourceError::Exhausted("observability_records"))
        ));
        assert!(matches!(resources.pipeline(1, 0, 64, 33), Err(ResourceError::Exhausted("command_records"))));
        Ok(())
    }

    #[tokio::test]
    async fn detached_blocking_work_keeps_its_lease_until_the_worker_exits() -> Result<(), Box<dyn std::error::Error>> {
        let resources = RuntimeResources::new(RuntimeResourceLimits::default())?;
        let admission = resources.provider().await?;
        let (release, blocked) = std::sync::mpsc::channel();
        let (started, waiting) = tokio::sync::oneshot::channel();
        let worker = tokio::task::spawn_blocking(move || {
            let _admission = admission;
            let _ = started.send(());
            let _ = blocked.recv_timeout(Duration::from_secs(2));
        });
        waiting.await?;
        drop(worker);
        assert_eq!(resources.clone().provider_workers(), 1);
        assert!(matches!(resources.try_provider(), Err(ResourceError::Exhausted("provider_workers"))));
        release.send(())?;
        let next = tokio::time::timeout(Duration::from_secs(1), resources.provider()).await??;
        assert_eq!(resources.provider_workers(), 1);
        drop(next);
        assert_eq!(resources.provider_workers(), 0);
        Ok(())
    }

    #[derive(Clone)]
    struct Environment;

    impl crate::o11y::RuntimeEnvironment for Environment {
        fn initial_host(&self) -> Result<crate::o11y::Host, crate::o11y::O11yError> {
            Ok(crate::o11y::Host::from_parts(Default::default(), Default::default()))
        }
        fn initial_process_stats(&self) -> Option<crate::o11y::ProcessStats> {
            None
        }
        fn refresh_host_stats(&self) -> Result<Option<crate::o11y::HostStats>, crate::o11y::O11yError> {
            Ok(None)
        }
        fn refresh_process_stats(&self) -> Option<crate::o11y::ProcessStats> {
            None
        }
    }

    #[tokio::test]
    async fn scheduled_loops_refuse_or_wait_and_release_admission_after_abort() -> Result<(), Box<dyn std::error::Error>> {
        let dirs = crate::dirs::Dirs {
            root_dir: Default::default(),
            log_root_dir: Default::default(),
            metadata_root_dir: Default::default(),
            data_root_dir: Default::default(),
            runtime_root_dir: Default::default(),
            cache_root_dir: Default::default(),
            secrets_root_dir: Default::default(),
        };
        let (sender, _receiver) = tokio::sync::mpsc::channel(2);
        let options = crate::o11y::O11yProcessorOptionsBuilder::<()>::default()
            .sender(sender)
            .flush_interval(Duration::from_secs(60))
            .status_interval(Duration::from_secs(60))
            .host_stats_interval(Duration::from_secs(60))
            .build()?;
        let resources = RuntimeResources::new(RuntimeResourceLimits {
            scheduled_tasks: 1,
            ..Default::default()
        })?;
        let mut wora: crate::Wora<(), ()> = crate::Wora::new_with_runtime_environment(&dirs, "resource-test".into(), 2, options, Environment)?;
        wora.resources = Some(resources.clone());
        let first = wora.try_schedule_task(Duration::from_secs(3600), |_| async { crate::TaskOp::Abort })?;
        assert_eq!(resources.scheduled_tasks(), 1);
        assert!(matches!(
            wora.try_schedule_task(Duration::from_secs(3600), |_| async { crate::TaskOp::Abort }),
            Err(ResourceError::Exhausted("scheduled_tasks"))
        ));
        let next = wora.schedule_task(Duration::from_secs(3600), |_| async { crate::TaskOp::Abort });
        tokio::pin!(next);
        assert!(tokio::time::timeout(Duration::from_millis(10), &mut next).await.is_err());
        assert_eq!(resources.scheduled_tasks(), 1);
        first.abort();
        assert!(first.await.is_err());
        let second = tokio::time::timeout(Duration::from_secs(1), next).await?;
        assert_eq!(resources.scheduled_tasks(), 1);
        second.abort();
        assert!(second.await.is_err());
        assert_eq!(resources.scheduled_tasks(), 0);
        Ok(())
    }

    #[test]
    fn type_changing_runner_builders_preserve_shared_resource_admission() -> Result<(), ResourceError> {
        let resources = RuntimeResources::new(RuntimeResourceLimits {
            scheduled_tasks: 1,
            ..Default::default()
        })?;
        let _task = resources.try_task()?;
        let options = crate::RunnerOptions::<()>::new()
            .with_resources(resources)
            .with_lock_backend(crate::lock::InMemoryLockBackend::default())
            .with_runtime_environment(Environment);
        assert_eq!(options.resources.as_ref().map(RuntimeResources::scheduled_tasks), Some(1));
        assert_eq!(options.clone().resources.as_ref().map(RuntimeResources::scheduled_tasks), Some(1));
        Ok(())
    }
}
