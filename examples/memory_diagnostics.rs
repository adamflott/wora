//! Opt-in Linux memory diagnostics with independent report/diagnostic intervals.
//! One Tokio worker keeps the reporting runtime's per-thread overhead small.

#[cfg(target_os = "linux")]
mod linux {
    use clap::Parser;
    use std::path::PathBuf;
    use std::time::Duration;
    use wora::o11y::linux_mm::PageRange;
    use wora::o11y::{LinuxMemoryOptions, LinuxMemoryTraceOptions};
    use wora::profiles::MemoryReporter;

    #[derive(Parser)]
    #[command(about = "Print Linux memory snapshots and explicitly enabled diagnostics as JSON lines")]
    struct Args {
        /// Routine snapshot/report interval in seconds.
        #[arg(long, default_value_t = 5)]
        interval: u64,
        /// Minimum interval between expensive diagnostic batches, in seconds.
        #[arg(long, default_value_t = 60)]
        diagnostic_interval: u64,
        /// Optional first physical PFN; no physical scan occurs unless supplied.
        #[arg(long)]
        pfn_start: Option<u64>,
        /// Optional first virtual page index in the collecting process's pagemap.
        #[arg(long)]
        virtual_page_start: Option<u64>,
        /// Number of entries in each requested page range (1..=65536).
        #[arg(long, default_value_t = 4096)]
        page_count: u32,
        /// Maximum bytes per page-owner/kmemleak dump (1..=64 MiB).
        #[arg(long, default_value_t = 4 * 1024 * 1024)]
        dump_limit_bytes: u64,
        /// Enable OOM/reclaim/compaction/migration events in a private tracefs instance.
        #[arg(long)]
        trace: bool,
        /// Existing tracefs mount; used only with --trace.
        #[arg(long, default_value = "/sys/kernel/tracing")]
        tracefs_root: PathBuf,
    }

    pub async fn run() -> Result<(), wora::errors::MainEarlyReturn> {
        let args = Args::parse();
        let mut options = LinuxMemoryOptions::all_snapshots().with_interval(Duration::from_secs(args.diagnostic_interval));
        options.provenance_byte_limit = args.dump_limit_bytes;
        if let Some(start) = args.pfn_start {
            options = options.with_physical_page_range(PageRange { start, count: args.page_count });
        }
        if let Some(start) = args.virtual_page_start {
            options = options.with_virtual_page_range(PageRange { start, count: args.page_count });
        }
        let mut reporter = MemoryReporter::new()
            .with_name("wora-memory-diagnostics")
            .with_interval(Duration::from_secs(args.interval))
            .with_linux_memory_diagnostics(options);
        if args.trace {
            reporter = reporter.with_linux_memory_trace(LinuxMemoryTraceOptions {
                tracefs_root: args.tracefs_root,
                ..Default::default()
            });
        }
        reporter.run().await
    }
}

#[cfg(target_os = "linux")]
#[tokio::main(worker_threads = 1)]
async fn main() -> Result<(), wora::errors::MainEarlyReturn> {
    linux::run().await
}

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("The memory_diagnostics example requires Linux.");
}
