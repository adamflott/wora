//! Print complete memory snapshots as JSON lines; Linux includes all procfs sources.

use wora::profiles::MemoryReporter;

#[tokio::main]
async fn main() -> Result<(), wora::errors::MainEarlyReturn> {
    MemoryReporter::new().run().await
}
