# wora

Write Once Run Anywhere (WORA): A Rust framework for building applications (daemons, etc.) that run in different environments (Linux, Kubernetes, etc.). Just like Java's claim, it really doesn't run everywhere. no_std or embedded environments are not supported.

## Status

This crate is an early-stage async framework. The main public API is usable, with Unix-like platforms as the primary target and environment-specific executors for `systemd`, `launchd`, and container-style deployments.

- restart policies, shutdown supervision, and signal mapping can be applied with `RunnerOptions` through `exec_async_runner_with_options`
- initial typed config and secret loading is built into the runner
- event-driven apps can use `Wora::run_event_loop` to auto-apply typed reloads
- `RunnerOptions::with_signal_mapper(...)` lets apps map Unix-like signals to control or app-defined events
- observability can be routed through sinks via `O11yProcessor`

## Feature Tour

- abstracts over common boilerplate with an API
- execute the same code in different executors (with or without recompiling)
- translates platform-specific control inputs into runtime control or app-defined events
- watches existing metadata and secrets directories and emits typed reload events
- supervises readiness, health, and graceful shutdown
- exposes host, process, runtime, and tracing observability events
- ships observability sinks and a processor for fan-out

Memory samples arrive as
`Event::SystemResource(SystemResourceStat::MemorySnapshot(snapshot))`. Read
`wora.memory_stats()` at any time for the latest stored sample, including during
app setup, or use `wora.subscribe_memory_stats()` for a cloneable Tokio watch
receiver. These APIs work on Linux and macOS, with portable memory and swap
totals in bytes. `stats_from_start()` continues to return the startup snapshot.

Set the sampling interval with `O11yPipeline::builder().host_stats_interval(...)`
(30 seconds by default). Change it while running through
`control.apply(O11ySettingsPatch::new().host_stats_interval(...)).await`.
The interval controls memory events, the stored sample, and host/process
observability metrics. Each successful host refresh updates the cache before
notifying the application. A full application event queue skips that periodic
notification; the cache and watch listeners still receive the latest sample.
Failed host refreshes retain the previous sample, and watch listeners may
coalesce updates when they read slowly.

The default `SystemRuntimeEnvironment` retains a host CPU/memory sampler and a
sampler for the daemon's own CPU, memory, and disk statistics. Process sampling
disables task enumeration and skips command lines and environments. Clones share
the sampling history needed for CPU percentages and disk deltas. `Host::update()`
reuses its host sampler; `Host::from_parts(...)` uses the supplied snapshot without
collecting telemetry. Standalone `HostStats::update()` performs a fresh host-only
collection; use `Host::update()` for CPU measurements across an interval.

On Linux, `snapshot.linux_mm` and `HostStats::linux_mm` include `Meminfo`,
`VmStat`, `MemoryPressure`, `SharedMemorySegments`, `Iomem`, `BuddyInfo`,
`ZoneInfo`, `Swaps`, `SlabInfo`, and `PageTypeInfo`, including their nested data.
`linux_mm.sysctls` additionally captures memory-related sysctl values: all
regular VM settings, kernel shared-memory and NUMA-balancing settings, the
perf-event locked-memory limit, and selected filesystem cache and descriptor
inventories. Read them through `snapshot.linux_mm.sysctls.parsed`, keyed by
dotted names such as `vm.swappiness` and `kernel.shmall`. Values use exact
integers, arrays and named records, retaining native units.
`unavailable` records per-key read/parse failures such as `permission_denied`, and
`skipped` identifies excluded action controls. Settings are refreshed at the
same interval as the other memory data.

The next memory views are included in the same snapshots and profile output:

| Field under `linux_mm` | Scope and contents |
| --- | --- |
| `cgroup` | The collecting process's cgroup v2: usage, limits, named statistics, local/hierarchical events, NUMA usage, swap/zswap, and pressure. Visible ancestors include usage and limit settings, nearest parent first. |
| `hugepages` | THP policies, khugepaged activity, per-size counters, and global/per-node HugeTLB pools from sysfs. |
| `process` | The collecting PID and `smaps_rollup`, including RSS, proportional set size (PSS), private/shared memory, dirty memory, swap and locked memory. |

Cgroup discovery uses `/proc/self/cgroup` and `/proc/self/mountinfo`, including
mount roots and escaped paths. It preserves `max` as an unlimited value and
keeps `memory.events` separate from `memory.events.local`. Ancestor settings
are not a computed effective limit: ancestors outside the visible mount remain
unknown. Cgroup v1 and inconsistent namespace layouts produce explicit
discovery failures. No cgroup memberships or limits are changed.

Hugepage numbers retain each file's units and size labels; policies expose
`selected` and `choices`. `memory.reclaim`, HugeTLB `demote`,
and write-only controls are excluded from reads. Process rollup values retain
their units (`kB` means KiB); `SmapsRollupValue::bytes()` provides checked
conversion. All three views record individual read/parse failures without
discarding other memory data. Process page-table walks can be expensive for
large workloads; choose the reporting interval accordingly.

For captured inputs, use `LinuxMemoryStats::from_roots(proc_root, sys_root)`;
`from_proc_root` collects proc-backed sources and leaves `hugepages` as `None`.
Cgroup mount points in captured mountinfo are used as written, so point them at
captured cgroup directories. The standalone readers are `CgroupMemoryStats`,
`HugePageStats`, and `ProcessMemoryStats` in `wora::o11y::linux_mm`.

The ten procfs snapshot sources are optional so unavailable or restricted files
do not interrupt other memory sampling. Values retain their source units and
page-type count bounds. Slab and page-type files commonly require root; page-type reads can be
expensive, so choose the interval accordingly. All types are re-exported through
`wora::o11y::linux_mm`, and the Linux dependency and fields are gated with
`#[cfg(target_os = "linux")]`.

The following sysctl read restrictions were observed on `inanna` as a normal
user (Linux 6.18.55, 2026-10-16):

| Sysctl | Access | Collection behavior |
| --- | --- | --- |
| `vm.mmap_rnd_bits` | Root-only read/write, mode `0600` | Value if readable; otherwise `unavailable: permission_denied` |
| `vm.mmap_rnd_compat_bits` | Root-only read/write, mode `0600` | Value if readable; otherwise `unavailable: permission_denied` |
| `vm.stat_refresh` | Root-only read/write, mode `0600` | Always skipped: reading changes VM statistics |

`vm.compact_memory` and `vm.drop_caches` have mode `0200`: they are write-only
action controls with no readable value, even for root. They are always skipped.
The collector does not invoke `sysctl`, write settings, or change privileges.
The kernel documents the read side effect of
[`stat_refresh`](https://docs.kernel.org/admin-guide/sysctl/vm.html#stat-refresh).
Available names and access permissions vary with the kernel and host policy.

Inside an app lifecycle hook, use the common API and guard Linux details:

```rust
let snapshot = wora.memory_stats();
println!("{} bytes used", snapshot.memory.used);

#[cfg(target_os = "linux")]
if let Some(meminfo) = &snapshot.linux_mm.meminfo {
    println!("available: {:?} bytes", meminfo.mem_available);
}

let mut updates = wora.subscribe_memory_stats();
tokio::spawn(async move {
    while updates.changed().await.is_ok() {
        let snapshot = updates.borrow_and_update().clone();
        println!("{} bytes used", snapshot.memory.used);
    }
});
```

## Supported Environments

- async-based apps
- Unix-like environments
  - Linux-specific host details through `procfs`
  - macOS host details through `sysinfo`
- service-manager and orchestration integrations
  - `SystemdExecutor`
  - `LaunchdExecutor`
  - `ContainerExecutor`

## Architecture

WORA has six main pieces:

- `App`: the workload lifecycle trait. Applications implement `setup`, `main`, `reload_config`, `reload_secrets`, and `end`.
- `AsyncExecutor`: the environment adapter. Executors provide directories, setup, readiness, and teardown behavior.
- `Wora`: the runtime context passed to apps. It carries directories, host data, event channels, runtime status, and observability options. Health and readiness are reported through `Wora::report_health(...)`, `Wora::report_readiness(...)`, or the cloned `RuntimeStatusHandle`.
- `WFS`: the virtual filesystem abstraction. `PhysicalVFS` is the host filesystem implementation.
- `LockBackend`: the single-instance coordination abstraction. `ProcLockBackend` uses host lock files and `InMemoryLockBackend` is useful for tests and fully virtual runner flows.
- `O11yProcessor`: the observability pipeline for fan-out into sinks.

`exec_async_runner` wires those pieces together by initializing observability, acquiring the runtime lock, building the `Wora` context, running executor setup, loading initial config and secrets, running app setup, watching the metadata and secrets directories, invoking `App::main`, supervising shutdown/readiness/health, and then running teardown. Runner-owned background work shares a cancellation token: teardown requests cooperative cancellation, waits for a bounded grace period, and aborts tasks that do not stop.

The runner always installs recursive watchers on both `metadata_root_dir` and `secrets_root_dir` after executor and app setup. Executors must create both watch roots even when an app uses `NoConfig` or `NoSecrets`; initial loading skips roots that are absent, but watcher installation still requires them. The default filesystem lock backend creates the parent of its lock path before acquiring the lock, allowing executor setup to create the remaining roots afterward.

Typed config and secret reloads are driven by filesystem watcher events (`Event::ConfigChanged` and `Event::SecretChanged`). `ControlEvent::ReloadConfiguration`, `ControlEvent::Suspend`, and `ControlEvent::LogRotation` are app-level control notifications: the runner delivers them to the app but does not automatically reload files, suspend work, or rotate sinks for the app. Use `RunnerOptions::with_signal_mapper(...)` when the default Unix-like signal mapping should produce app-defined events instead.

When a lock backend reports contention, the runner returns `MainEarlyReturn::AlreadyRunning(...)` instead of collapsing that case into a generic exit code.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Prerequisites

* Rust 1.95 or newer. The crate declares `rust-version = "1.95"` and the checked-in `rust-toolchain.toml` pins `1.95.0` for repository builds.

## Usage

Run the memory-reporting profile:

```sh
cargo run --example memory_report
```

Profiles combine a workload's default configuration with the existing WORA
runtime so small programs need very little setup. The first profile prints one
complete memory snapshot as a JSON line at startup and after every successful
host sample, flushing each line immediately. Stop it with Ctrl-C or SIGTERM.

```rust,no_run
use wora::profiles::MemoryReporter;

#[tokio::main(worker_threads = 1)]
async fn main() -> Result<(), wora::errors::MainEarlyReturn> {
    MemoryReporter::new().run().await
}
```

`MemoryReporter` defaults to a five-second interval, stdout, the application
name `wora-memory-report`, and a per-user Unix executor. Customize it with
`.with_interval(Duration::from_secs(30))`, `.with_name("my-memory-report")`, or
`.with_writer(writer)` for a Tokio async writer. Use `.run_with(exec, fs, options)`
to supply an existing executor and `RunnerOptions`. The profile uses `NoConfig`
and `NoSecrets`, and does not install a global tracing subscriber.

The `memory_report` and `memory_diagnostics` examples use one Tokio worker to
reduce per-thread memory overhead. Periodic synchronous collection runs on a
blocking worker. `MemoryReporter` runs on the caller's runtime, so applications
choose their worker count when creating that runtime.

On Linux, expensive diagnostics are explicitly selected and sampled separately
(60 seconds by default). Cached diagnostics carry `sampled_at`, so their age is
visible in each report. For example:

```rust,no_run
use wora::o11y::{LinuxMemoryCollector, LinuxMemoryOptions};
use wora::profiles::MemoryReporter;

# async fn example() -> Result<(), wora::errors::MainEarlyReturn> {
MemoryReporter::new()
    .with_linux_memory_diagnostics(
        LinuxMemoryOptions::new().enable(LinuxMemoryCollector::ProcessMappings),
    )
    .run().await
# }
```

Use `LinuxMemoryEnvironment::new(provider, options)` with runner options to add
the same diagnostics to another application or custom telemetry provider.
`.with_interval(...)` on `LinuxMemoryOptions` sets the diagnostic cadence;
the profile's interval controls routine reports. Neither grants privileges.

`LinuxMemoryStats::diagnostics` is an `Option<Arc<LinuxMemoryDiagnostics>>`.
Host samples, memory events, and cloned snapshots share each diagnostic batch;
refreshing diagnostics creates a new batch while retained snapshots keep their
original data. Construct this field with `Arc::new(...)` (or convert an existing
box with `Arc::from(...)`). Use `Arc::make_mut(...)` when a private writable copy
is needed. Existing JSON data fields retain the same format. Memory reporting
serializes borrowed fields directly, avoiding another owned snapshot copy.

Routine tables are also shared, through `LinuxMemoryStats::data`, an
`Arc<LinuxMemoryData>`. Existing field reads such as `linux_mm.vmstat` continue to
work through `Deref`; mutation makes an independent copy when needed. For struct
literals, construct `LinuxMemoryData { .. }` and convert it with `.into()`.
Borrow fields with `.as_ref()` when reading through the shared wrapper; moving a
field out requires owning the data, for example via `Arc::unwrap_or_clone`.
Routine JSON fields remain at their existing paths; collected routine and
diagnostic data now also contain `collection` accounting, whose
`limit_exceeded` field marks incomplete data.
`LinuxMemoryOptions` struct literals also need `limits` (or a default struct
update), and `LinuxMemoryDiagnostics` literals need `collection` accounting.

Default Linux collection limits are shared across each routine or diagnostic
batch: 16 MiB of admitted source bytes, 4 MiB per file, 64 KiB per line, 65,536
source attempts (including failures), 65,536 directory entries (at most 4,096 per
directory), 4,096-byte paths, and 64 cgroup ancestors. Oversized sources
fail rather than parsing a truncated prefix. EOF checks may read one additional
byte per source without retaining it. Cgroup mount discovery keeps only the best
matching mount, with deterministic ties, and reports incomplete ancestor visibility.

`LinuxMemoryLimits` additionally defaults to four live generations, 8 MiB of
encoded JSON per batch, and 32 MiB across retained batches. Routine and diagnostic
providers have separate allowances. Admission reserves room for a replacement
before collection. When readers retain all available generations or exhaust the
retained byte allowance, routine sampling skips the refresh and diagnostics reuse
their previous batch. Existing timestamps expose sample age. Inspect
`SystemRuntimeEnvironment::memory_sampling_status()` or
`LinuxMemoryEnvironment::diagnostic_sampling_status()` for live generations,
retained encoded bytes, and skipped refresh counts.

Set these limits with `SystemRuntimeEnvironment::with_linux_memory_limits(...)`,
`LinuxMemoryOptions::with_memory_limits(...)`, or the profile's
`.with_linux_memory_limits(...)`. With `.run_with(...)`, configure the supplied
routine provider separately. The profile reuses a single JSON buffer capped at
8 MiB per record, including the newline; `.with_output_byte_limit(...)` changes
that cap. Oversized reports fail without writing a partial record. The output
writer can have additional buffering.

Source and encoded payload byte limits describe different resources from Rust
heap capacity or RSS. Container/allocator overhead, CPU/disk/network inventories,
custom providers, caller-created copies, application queues and other sinks still
need their own budgets for a whole-daemon memory guarantee.

Routine Linux reports also include NUMA nodes/tiers, zram/zswap accounting, KSM,
process status/limits/OOM preferences, and HugeTLB/cpuset cgroup context.
`LinuxMemoryOptions::all_snapshots()` explicitly selects the full diagnostic set:
process mappings, SLUB, kernel allocation attribution, fragmentation, MGLRU,
page-owner, kernel leak suspects, and memory blocks/EDAC/CMA. Physical and virtual
page scans require explicit bounded `PageRange` values. Sources unavailable due
to privileges or kernel configuration retain independent failures.

```sh
cargo run --example memory_diagnostics -- --diagnostic-interval 60
# Also request a bounded physical-page range:
cargo run --example memory_diagnostics -- --pfn-start 0 --page-count 1024
# Explicitly enable a private tracefs instance (requires tracefs permissions):
cargo run --example memory_diagnostics -- --trace
```

The diagnostic example opts into potentially costly reads, including kmemleak.
`MemoryReporter::with_linux_memory_trace(LinuxMemoryTraceOptions::new())` adds
OOM victim, reclaim timing, compaction and migration records. These flush as
additional `{"timestamp":...,"memory_trace":...}` lines, including startup,
kernel-loss and unfamiliar records. The service owns only its private tracefs
instance and cleans it up on normal shutdown/cancellation. An abrupt process
death may leave the instance identified in the startup record. Snapshot and
event collectors do not attempt to obtain additional privileges.

Each JSON line contains `timestamp`, `memory`, `swap`, and, on Linux, `linux_mm`.
Linux file snapshots and sysctls expose `parsed` maps with structured
counters, booleans, numeric arrays, named filesystem vectors, units, node/CPU
ranges, and policy choices. For example,
`linux_mm.ksm.process.parsed.ksm_stat.ksm_merge_any` is a boolean, and
`linux_mm.sysctls.parsed["fs.dentry-state"].nr_dentry` is an integer. Raw source
text is not stored. Malformed or unsupported layouts report their source name
in `unavailable`.
All Linux sources listed above are included with every available nested
value, including zone per-CPU statistics, allocator counts, sysctls, cgroup memory,
hugepages and the process rollup. Unavailable
procfs snapshots appear as `null`; sysctls retain per-key errors and exclusions.
Source units, integer precision, and page-type lower
bounds are preserved. Slab and page-type files commonly require root; run with
suitable read permissions to include those sources. Output errors stop the
profile and are returned to the caller. On macOS, the same profile prints the
portable memory and swap totals.

Run the basic example:

```sh
cargo run --example basic
```

Run the daemon-style event loop example:

```sh
cargo run --example async_daemon -- --run-mode user
```

Run the Linux `systemd` daemon example:

```sh
cargo run --example systemd_daemon
```

Sample unit file:

```text
examples/systemd_daemon.service
```

Run the onboarding example:

```sh
cargo run --example onboarding
```

Run checks:

```sh
cargo fmt --check
cargo test
```

## AI Disclosure

OpenAI Codex is used to write documentation/tests and find issues. Everything is reviewed by me and I vouch for these AI-generated changes.

## Versioning

We use [SemVer](http://semver.org/) for versioning when x.y.z when x is > 0, otherwise versioning is arbitrary. For the versions available, see the [tags on this repository](https://github.com/adamflott/wora/tags). 

## Authors

* **Adam Flott** - *Initial work* - [adamflott](https://github.com/adamflott)

See also the list of [contributors](https://github.com/adamflott/wora/contributors) who participated in this project.

## License

This project is dual-licensed under either the [MIT License](LICENSE-MIT) or the [Apache License 2.0](LICENSE-APACHE), at your option.

## Acknowledgments
