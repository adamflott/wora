use serde_derive::{Deserialize, Serialize};
use std::time::Duration;

/// The policy to use when the workload returns.
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub enum WorkloadRestartPolicy {
    /// Use an exponential backoff algorithm up to the retry times.
    ExponentialBackoff,
    /// Use the workload result as the exit code.
    ExitWithWorkloadReturn,
    /// Retry without any pause between invocations.
    RetryInstantly,
    /// Use a constant pause (in seconds) between invocations. Set via `workload_restart_policy_pause_duration`.
    #[default]
    RetryPause,
}

/// Runtime settings for applying a workload restart policy.
#[derive(Clone, Debug)]
pub struct RestartPolicyOptions {
    /// Policy used when `App::main` returns `MainRetryAction::UseRestartPolicy`.
    pub policy: WorkloadRestartPolicy,
    /// Base pause used by `RetryPause` and `ExponentialBackoff`.
    pub pause: Duration,
    /// Maximum number of retries after the first failed run. `None` retries forever.
    pub max_retries: Option<u32>,
    /// Maximum pause for exponential backoff. `None` leaves the backoff uncapped.
    pub max_backoff: Option<Duration>,
    /// Runtime supervision behavior for health and shutdown handling.
    pub supervision: SupervisionOptions,
}

impl Default for RestartPolicyOptions {
    fn default() -> Self {
        Self {
            policy: WorkloadRestartPolicy::default(),
            pause: Duration::from_secs(1),
            max_retries: None,
            max_backoff: None,
            supervision: SupervisionOptions::default(),
        }
    }
}

impl RestartPolicyOptions {
    /// Create options for `policy` with default timing and retry settings.
    pub fn new(policy: WorkloadRestartPolicy) -> Self {
        Self { policy, ..Self::default() }
    }

    /// Return whether another retry may be attempted after `completed_retries`.
    pub fn can_retry(&self, completed_retries: u32) -> bool {
        self.max_retries.is_none_or(|max_retries| completed_retries < max_retries)
    }

    /// Return the pause to use before retry number `retry_number`.
    pub fn pause_for_retry(&self, retry_number: u32) -> Duration {
        let pause = match self.policy {
            WorkloadRestartPolicy::ExponentialBackoff => {
                let multiplier = 2u32.saturating_pow(retry_number.saturating_sub(1).min(10));
                self.pause.saturating_mul(multiplier)
            }
            WorkloadRestartPolicy::RetryPause => self.pause,
            WorkloadRestartPolicy::RetryInstantly | WorkloadRestartPolicy::ExitWithWorkloadReturn => Duration::ZERO,
        };

        match self.max_backoff {
            Some(max_backoff) => pause.min(max_backoff),
            None => pause,
        }
    }
}

/// Action to take when the runtime is told the app is unhealthy.
#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub enum UnhealthyAction {
    /// Ignore unhealthy state transitions.
    Ignore,
    /// Request graceful shutdown and fail if the app does not stop in time.
    #[default]
    RequestShutdown,
    /// Request graceful shutdown and then apply the restart policy if the app does not stop in time.
    UseRestartPolicy,
}

/// Runtime supervision behavior.
#[derive(Clone, Debug)]
pub struct SupervisionOptions {
    /// Time to remain in `ReadinessState::Draining` before transitioning to
    /// `Stopping` and delivering the shutdown control event.
    pub drain_grace_period: Duration,
    /// Maximum time to wait for graceful shutdown after a shutdown request.
    pub shutdown_grace_period: Duration,
    /// Exit code used when shutdown times out.
    pub forced_shutdown_exit_code: i8,
    /// Action to take when the app reports `HealthState::Failed`.
    pub unhealthy_action: UnhealthyAction,
}

impl Default for SupervisionOptions {
    fn default() -> Self {
        Self {
            drain_grace_period: Duration::ZERO,
            shutdown_grace_period: Duration::from_secs(30),
            forced_shutdown_exit_code: 124,
            unhealthy_action: UnhealthyAction::default(),
        }
    }
}

/// Action returned by `App::main` to tell the runner what to do next.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum MainRetryAction {
    /// Terminate the runner with the given exit code.
    UseExitCode(i8),
    /// Ask the runner to apply its restart policy.
    UseRestartPolicy,
    /// Complete successfully.
    Success,
}
