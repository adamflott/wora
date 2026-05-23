use serde_derive::{Deserialize, Serialize};

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
