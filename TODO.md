# Capability Roadmap

This document tracks capabilities that would make WORA safer and more complete
for long-running daemon workloads. Each section describes the intended behavior
and a possible implementation path rather than committing the crate to a final
public API.

## Bounded-memory runtime

Make it possible to configure a ceiling on WORA-owned memory across startup,
sampling, overload, reloads, and restarts, independent of uptime and event volume.
The ceiling must include transient allocation peaks as well as retained state.
RSS can fluctuate within a measured envelope; allocation-free execution would be
a separate, stricter goal. A whole-daemon guarantee also requires application
code, custom providers, sinks, executors, and the caller-owned Tokio runtime to
follow the same resource limits.

Current gaps identified by code inspection:

- `wora-observability-linux-mm/src/cgroup.rs` reads whole files and retains
  variable-sized mount candidates, ancestor snapshots, and counter/NUMA maps.
- `SystemRuntimeEnvironment` and `Host` in `src/o11y.rs` use
  `System::new_all()` and broad refreshes, collecting system-wide process data.
  `HostInfo` also collects variable-sized network tables.
- `MemorySnapshot::from_host_stats`, host-stat observability events, and
  `LinuxMemoryEnvironment` deep-clone detailed memory data. Old and replacement
  snapshots, diagnostic caches, and serialization buffers can overlap.
- Application and observability queues default to 1,024 and 64 items respectively,
  but item counts do not bound payload bytes or memory held by pending senders.
- `O11yMemorySink` appends formatted events indefinitely. JSON sinks and memory
  reporting construct complete output buffers before writing them.
- `Wora::schedule_task` creates independently owned periodic tasks without an
  admission limit. Config/secret reads, directory enumeration, and reload batches
  also lack memory budgets.

Implementation order:

### 1. Define the contract and measure a baseline

- [ ] Add validated memory-limit settings through existing runner and
  observability abstractions. Preserve the trait-oriented API; public struct
  field additions and snapshot ownership changes need an explicit compatibility
  decision.
- [ ] Budget collector scratch space, retained snapshots, queue payloads, pending
  producers, task state, thread stacks, sink/exporter buffers, and safety margin.
  Count allocation capacity and container overhead, not just serialized sizes.
- [ ] Account for old/new state overlap during sampling, config reloads, and
  serialization. Reserve budget before allocating or constructing large payloads.
- [ ] Measure a minimal release daemon, then sampling, reporting, and diagnostics
  separately. Use those measurements to choose realistic default/minimum budgets.
- [ ] Document the requirements for custom implementations and caller-owned
  resources, including bounded worker/blocking threads, and expose which
  configurations satisfy the bounded-memory contract.

### 2. Make cgroup collection the first bounded collector

- [ ] Extend the bounded-reader helpers in
  `wora-observability-linux-mm/src/files.rs` with shared collection budgets.
  Detect oversized input explicitly, including a valid-looking prefix, rather
  than treating a truncated read as complete.
- [ ] Bound file and line bytes, path lengths, ancestor depth, selected files,
  counter keys, NUMA entries, and aggregate retained bytes. Error messages and
  unavailable/skipped-entry maps must fit the same budget.
- [ ] Stream membership and mount discovery with bounded buffers. Keep only the
  best matching mount instead of collecting and sorting every candidate; preserve
  closest-root selection and deterministic tie-breaking.
- [ ] Preserve unknown kernel fields within the configured limits. Report explicit
  incomplete/unavailable results when limits are exceeded, including incomplete
  ancestor visibility; never present a partial table as complete accounting.
- [ ] Keep cgroup collection read-only, preserve literal `max` values and scope
  distinctions, and continue excluding action controls such as `memory.reclaim`.

### 3. Bound sampling and snapshot retention

- [ ] Replace broad `sysinfo` collection with the host fields and current-PID data
  actually needed. Apply limits before inventory collection allocates; truncating
  a fully collected table does not bound its peak memory.
- [ ] Provide a small routine telemetry profile through existing collection
  options. Put detailed process/socket, slab, zone/per-CPU, NUMA, hardware, and
  other inventories under explicit selection and aggregate diagnostic budgets.
- [ ] Apply shared input/output limits to all procfs/sysfs readers. Existing
  per-file, page-range, traversal, and trace-parser limits should participate in
  the aggregate budget rather than each consuming an independent allowance.
- [ ] Bound snapshot size and simultaneous copies first, then reduce deep cloning,
  especially diagnostic-cache copies. Consider shared immutable snapshots using
  `Arc`, with any public payload changes staged for an appropriate release.
- [ ] Cap live snapshot generations, including samples retained by listeners.
  When capacity is exhausted, skip sampling and expose sample age and skipped
  counts without allocating an unbounded history.

### 4. Define bounded overload and output behavior

- [ ] Coalesce periodic statistics into the latest sample. Give application
  events bounded producer concurrency plus backpressure or explicit rejection;
  preserve capacity for shutdown and control operations.
- [ ] Enforce both item and byte limits, including memory held outside queues by
  pending sends. Account for direct public sender access when defining the
  bounded-memory API contract.
- [ ] Add a memory sink with a ring buffer bounded by both total bytes and record
  count, with an explicit overflow policy. Exclude the existing unbounded capture
  behavior from configurations claiming a memory bound.
- [ ] Bound record formatting and serialization scratch space before growth.
  Reuse capped buffers or stream output, and avoid the extra full snapshot clone
  in `MemoryReportSink`.
- [ ] Audit forwarding sinks and exporter buffering/cardinality. Define behavior
  for stalled or failed outputs, with bounded counters for dropped/coalesced
  records and rate-limited diagnostics.

### 5. Cover task lifecycle and reload paths

- [ ] Add admission limits and runtime ownership for scheduled tasks. Cancel and
  reap tasks on the appropriate lifecycle boundary so retries/restarts cannot
  accumulate background work.
- [ ] Bound collector concurrency across shutdown and restart. Started
  `spawn_blocking` work cannot be aborted; a timeout must not allow replacement
  workers to accumulate while old work continues. See
  [Tokio's blocking-task lifecycle](https://docs.rs/tokio/latest/tokio/task/fn.spawn_blocking.html).
- [ ] Extend `WFS` with bounded read and directory-enumeration capabilities for
  config and secrets. Enforce file count/bytes and total reload-batch limits before
  allocation, including old/new parsed configuration overlap.
- [ ] Bound watcher registrations, pending paths, and debounce/coalescing state.
  On overflow, use a bounded dirty/rescan mechanism so missed notifications do not
  silently leave configuration stale.
- [ ] Apply the same limits to the reload-coalescing and atomic-snapshot roadmap
  items below, including snapshot acquisition and retries.

### 6. Verify bounds and document deployment containment

- [ ] Add adversarial fixtures for oversized files, long lines, deep cgroup
  hierarchies, large inventories, and large error sets. Assert allocation peaks
  as well as explicit limit-exceeded results.
- [ ] Exercise stalled sinks, saturated queues, retained snapshots, task-admission
  failures, blocking collectors, repeated reloads, and restarts. Verify bounded
  live allocations/task counts and responsive shutdown under overload.
- [ ] Run release daemons in dedicated Linux cgroups with an external measurement
  harness. Track RSS/PSS and cgroup usage/peaks separately through startup,
  warmup, bursts, and long-running operation. Add short regression checks to CI
  and longer soak tests separately.
- [ ] Document cgroup deployment limits as containment: `memory.high` applies
  reclaim/throttling pressure, while `memory.max` can invoke OOM killing and can
  be temporarily exceeded. Cgroup usage includes descendants and resources
  beyond process RSS. Visible limits and current usage cannot promise allocation
  headroom. See the [kernel memory-controller documentation](https://docs.kernel.org/admin-guide/cgroup-v2.html#memory).

First milestone: a bounded cgroup collector, targeted system sampling, and a small
telemetry profile with measured peak usage and regression coverage. Subsequent
stages should use that budget to establish bounds for the remaining runtime paths.

### Measured baseline: inanna, 2026-10-17

Inspected PID `131240`, running the release `memory_diagnostics` example as the
normal user on Linux 6.18.55, with 16 available CPUs and glibc 2.42. The inspected
runtime, profile, event, diagnostic-provider, and example source files matched the
local checkout by SHA-256. The remote toolchain setting was 1.99.1. Inspection
did not change, restart, or attach a debugger to the original process.

At approximately 18:36 EDT, after 34 minutes of uptime:

| Measurement | Observed value |
| --- | --- |
| Resident memory (`smaps_rollup` RSS) | 68.9 MiB |
| Proportional resident memory (PSS) | 66.6 MiB |
| Anonymous resident memory | 64.2 MiB |
| Virtual address space | 1.25 GiB |
| Threads | 20: main, 17 Tokio-named, two notify threads |
| Cached process/task `stat` descriptors | Approximately 1,900, mostly three copies per target |

The mappings contained 19 pairs consistent with glibc's 64 MiB non-main arenas.
Most reserved address space was nonresident. The main heap and those arena-like
mappings held 62.0 MiB of RSS together; mapping accounting cannot distinguish live
allocations from allocator-retained free memory or identify Rust allocation sites.
The process had no swapped or locked pages. A 130-second, one-second sampling
window observed RSS between 63.2 and 69.0 MiB, with 20 threads throughout; a later
sample during the probe workload reached 70.5 MiB. These observations do not
establish a long-term memory bound or rule out gradual growth.

The process's session cgroup was shared with login processes and reported about
7.25 GiB of usage, including 6.73 GiB of file cache. That total cannot be attributed
to this PID; use process accounting and a dedicated cgroup for future comparisons.

Isolated probes linked against the existing release libraries measured:

- Three `System::new_all()` instances added about 3.9 MiB of anonymous memory and
  retained 1,932 process/task entries and matching cached descriptors. Targeted
  host/current-PID sampling added about 80 KiB and retained one process entry.
  Use `ProcessesToUpdate::Some`, explicit CPU/memory/disk refresh flags, and
  `.without_tasks()`; `ProcessRefreshKind::nothing()` still enables tasks by default.
- Routine initial host collection used about 7.7 MiB RSS; enabling all snapshot
  diagnostics used about 12.2 MiB. Three additional `MemorySnapshot` clones raised
  the all-diagnostics case to about 20.6 MiB. Three SLUB-only snapshot clones added
  about 6.0 MiB. These are probe footprints, not precise attribution of the running
  daemon's allocations.

Fresh copies of the same executable were then run concurrently for 76 seconds,
with isolated XDG directories and output to `/dev/null`. Each retained the default
five-second report and 60-second diagnostic intervals. Final samples at 75 seconds:

| Configuration | RSS | Virtual size | Threads |
| --- | --- | --- | --- |
| Default | 60.7 MiB | 1,283.1 MiB | 20 |
| `TOKIO_WORKER_THREADS=1` | 39.3 MiB | 287.2 MiB | 5 |
| One worker plus `MALLOC_ARENA_MAX=2` | 32.1 MiB | 95.7 MiB | 5 |

All temporary comparison processes exited and their remote scratch directories
were removed. The original process remained running. These single short runs
support further controlled experiments; they do not establish production defaults,
long-term savings, or throughput equivalence.

Additional priorities supported by these measurements:

- [x] Use one Tokio worker in `memory_report` and `memory_diagnostics`, matching
  the measured configuration that lowered final comparison RSS by about 35%.
  The README and profile Rustdoc examples use the same runtime configuration;
  `MemoryReporter` continues to use the caller's runtime.
- [ ] Run an extended soak of the one-worker reporting examples, including slow
  outputs and diagnostic refreshes, to establish longer-term RSS and throughput.
- [ ] Compare a small glibc arena limit after reducing snapshot churn. The combined
  configuration lowered final comparison RSS by about 47%; test allocator
  contention and portability before recommending deployment settings.
- [x] Remove redundant system tables in `SystemRuntimeEnvironment` and
  `Host::from_parts`. Reuse the host sampler during `Host::update` and limit
  standalone `HostStats::update` to host CPU/memory collection. Process sampling
  retains only the daemon PID and preserves CPU/disk counter history.
- [x] Share diagnostic batches across host samples and memory snapshots, including
  SLUB data, and serialize memory/trace reports from borrowed fields to avoid
  report-only copies. See the snapshot-sharing implementation notes below.
- [ ] Profile remaining allocations to distinguish live objects, transient copy
  peaks, and allocator retention before assigning exact savings to individual
  code paths.

### Single-worker reporting examples: 2026-10-18

`memory_report` and `memory_diagnostics` now use
`#[tokio::main(worker_threads = 1)]`. The README and profile Rustdoc show the same
configuration. Periodic synchronous collection continues on the blocking pool,
and `MemoryReporter` uses the runtime supplied by its caller.

Native optimized Linux builds of the changed examples were linked against the
existing release dependency artifacts on inanna and run for 187 seconds. The
comparison used separate temporary XDG directories and JSON output files, with
the default five-second report and 60-second diagnostic intervals and no allocator
arena override. Final memory samples at 185 seconds:

| Example/runtime | RSS | Threads | Reports |
| --- | --- | --- | --- |
| Diagnostic example, original default-worker build | 71.8 MiB | 20 | 37 |
| Diagnostic example, one worker | 41.1 MiB | 5 | 37 |
| Routine report example, one worker | 12.6 MiB | 5 | 38 |

Both diagnostic runs produced four distinct diagnostic batches and averaged
5.155 seconds between reports, with maximum gaps below 5.281 seconds. The routine
report averaged 5.135 seconds, with a maximum gap below 5.143 seconds. All captured
lines parsed as JSON with the expected memory fields, and all three processes
exited with status zero after SIGTERM. The diagnostic example's final RSS was
about 43% lower with one worker in this comparison. This short run verifies the
configured reporting cadence; extended RSS and throughput testing remains open.

Local macOS validation passed `cargo fmt --check`, `cargo test`,
`cargo clippy --all-targets --all-features`, and `cargo doc --no-deps`. Temporary
Linux processes and scratch directories were cleaned up; PID `131240` remained
running with its original start time.

### Shared diagnostic batches and borrowed reports: 2026-10-18

The diagnostic provider caches `Arc<LinuxMemoryDiagnostics>`, and
`LinuxMemoryStats::diagnostics` is now an
`Option<Arc<LinuxMemoryDiagnostics>>`. Host samples, memory events, and cloned
snapshots share the same batch until diagnostics refresh. Refresh replaces the
cached handle while retained snapshots keep their original data. Callers that
construct the public diagnostic field must use `Arc::new` or convert an existing
box with `Arc::from`; `Arc::make_mut` provides an independent writable copy.

`MemoryReportSink` serializes borrowed memory fields with the event timestamp,
avoiding an additional owned `MemorySnapshot`. Trace reports also borrow their
records instead of constructing an intermediate JSON value. Both retain the
existing JSON field structure.

Regression coverage now checks sharing through cached provider clones, host/event
and memory-snapshot clones, retained data across refreshes, independent mutation,
release of old batches, and report serialization parity with public snapshots.
Local macOS formatting, tests, Clippy, and documentation checks passed.
Linux-specific regression execution and updated memory measurements remain to be
completed.

### Targeted system sampling: 2026-10-18

Default host collection refreshes only CPU and memory information. The process
sampler refreshes the daemon PID with CPU, memory, and disk-usage flags, explicitly
disabling task enumeration and omitting command/environment inventories. It seeds
the initial counters and retains their history across refreshes and environment
clones. `ProcessStats::current()` uses the same targeted collection for one-off
samples.

`Host::from_parts` keeps an empty sampler and uses the supplied data without
collecting another system-wide snapshot. `Host::update` refreshes its retained
sampler once and builds host statistics from that same CPU/memory sample.
Standalone `HostStats::update` retains its existing API and performs a fresh
host-only collection; callers needing interval CPU measurements should retain a
`Host` and use `Host::update`.

Regression tests verify empty host process tables, a single retained process
without command/environment data (and no tasks on Linux), CPU activity and disk
counter deltas across cloned environments, and host CPU values matching the
retained sampler. Local macOS formatting, tests, Clippy, and documentation checks
passed. Native Linux verification and updated RSS/descriptor measurements remain
pending; the earlier isolated sampler measurements are the baseline, not measured
savings from this implementation.

## Deletion-aware reload payloads

Config and secret removal events currently result in an empty reload because the
changed file can no longer be read. Applications can inspect the original event,
but `App::reload_config` and `App::reload_secrets` do not receive explicit removal
information. This makes it difficult to remove supplemental configuration or
invalidate a deleted secret reliably.

Implementation outline:

- Extend `ConfigReload<T>` with a `removed: Vec<PathBuf>` field.
- Extend `SecretReload<T>` with a deletion representation that includes both the
  removed path and the filename-derived key. A small `RemovedSecret` struct would
  avoid forcing applications to repeat key derivation.
- Pass `ChangeKind` into the config and secret reload loaders. For
  `ChangeKind::Removed`, record paths without attempting to read them.
- Handle mixed watcher events defensively: attempt to read paths that still
  exist and classify `NotFound` paths as removals.
- Keep the existing `main` and `files` fields so applications that ignore the new
  fields continue to work. Adding public struct fields is still source-breaking
  for callers using struct literals, so introduce constructors/builders or stage
  this change for an appropriate release.
- Add tests for main-config deletion, supplemental-config deletion, secret
  deletion, rename sequences, and events containing both present and absent
  paths.

## Reload coalescing and debouncing

Editors and projected-volume implementations often emit several create, rename,
and modify notifications for one logical update. Processing every notification
immediately can parse partially written files and invoke application reload hooks
multiple times for the same change.

Implementation outline:

- Add reload settings to `RunnerOptions`, including a debounce duration and a
  maximum coalescing window. Keep the default small enough for responsive reloads
  while allowing callers to disable debouncing when exact event delivery matters.
- Replace the two direct watcher-forwarding loops with per-root aggregation
  tasks. Each task should collect paths and change kinds until the debounce timer
  expires, then emit one `ConfigChanged` or `SecretChanged` event.
- Deduplicate paths while preserving deterministic ordering. When several kinds
  affect one path, collapse them according to final observable state: an absent
  path is removed, an existing path is created or modified.
- Ensure continuous write traffic cannot postpone reload forever by enforcing the
  maximum coalescing window.
- Keep filesystem I/O outside watcher callbacks. Callbacks should remain
  non-blocking and only enqueue raw notifications.
- Add paused-time Tokio tests for burst coalescing, maximum-window behavior,
  rename/save patterns, and independent config and secret streams.

## Atomic initial snapshots

Initial config and secret loading currently reads directory entries one at a
time. If a mounted configuration set changes during startup, an application can
receive values from different generations.

Implementation outline:

- Introduce an optional snapshot capability on `WFS`, either as a separate
  `SnapshotWFS` extension trait or as a high-level method returning an immutable
  directory snapshot. Avoid requiring low-level file-descriptor behavior from
  every virtual filesystem implementation.
- Represent a snapshot as ordered entries containing path, bytes, and optional
  metadata or generation information. Parse configuration and secrets only after
  the complete snapshot has been acquired.
- For `InMemoryVFS`, clone the relevant nodes while holding one read lock. This
  provides a genuinely atomic snapshot and a deterministic reference
  implementation.
- For `PhysicalVFS`, support a consistency strategy such as reading directory
  metadata before and after collection and retrying when it changes. On platforms
  that expose stronger primitives, an implementation may provide stronger
  guarantees.
- Add bounded retry settings and return a targeted error when a stable snapshot
  cannot be obtained; startup must not retry forever under continuous updates.
- Load config and secrets as separate snapshots initially. A future grouped
  snapshot API could provide cross-directory consistency when an environment can
  expose a shared generation identifier.
- Add tests that mutate files during snapshot acquisition, exercise retry limits,
  and verify that parsers never receive a mixed in-memory generation.

## Cross-platform API gating

Unix executors and signal APIs are currently exposed without a clear
platform-level boundary. This makes the crate's portable surface less obvious and
can cause non-Unix builds to compile modules that depend on Unix-only Tokio and
`nix` APIs.

Implementation outline:

- Gate Unix-specific modules and exports with `#[cfg(unix)]` or narrower target
  conditions where required. This includes `exec_unix`, Unix signal constants,
  signal registration, UID/GID helpers, resource limits, and memory locking.
- Separate portable executor lifecycle behavior from Unix process-hardening
  helpers. Keep `AsyncExecutor`, `RunnerOptions`, runtime events, VFS, locking
  abstractions, and observability available on every supported platform.
- Decide whether `RuntimeSignal` is a portable conceptual enum or a Unix API. If
  retained portably, move OS signal conversion behind Unix-only adapters and let
  other executors inject equivalent lifecycle inputs without importing Unix
  constants.
- Add platform-specific preludes or conditional re-exports so downstream users
  do not need to mirror the crate's internal cfg expressions.
- Document the supported target matrix and identify which built-in executors are
  available on Linux, macOS, other Unix systems, and non-Unix systems.
- Add CI compile checks for at least one Windows target in addition to existing
  Unix builds. Use `cargo check --target ...` where executing target tests is not
  practical.
- Add compile-time tests or small target-gated examples to ensure the portable
  prelude remains usable without Unix-only symbols.
