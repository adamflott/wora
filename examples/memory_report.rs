//! Print complete memory snapshots as JSON lines; Linux includes all procfs sources.
//! One Tokio worker keeps the reporting runtime's per-thread overhead small.

use wora::profiles::MemoryReporter;

#[tokio::main(worker_threads = 1)]
async fn main() -> Result<(), wora::errors::MainEarlyReturn> {
    MemoryReporter::new().run().await
}
