# Memory collection and resource determinism

WORA aims for deterministic memory and CPU usage: configurable resource budgets,
predictable sampling work, bounded retained state, and explicit overload behavior.
This is a design goal. The current collection and output limits cover parts of
memory reporting; a whole-runtime heap/RSS ceiling and CPU execution-time bound
remain open work. Application code, providers, sinks and the caller's Tokio runtime
also affect the daemon's resource use.

This guide is the home for memory usage instructions, source provenance, design
notes, experiments and unfinished resource-budget work. Start with the usage and
source sections; dated observations are preserved separately from current behavior.

- [Reading memory samples](#reading-memory-samples)
- [Memory value sources](#memory-value-sources)
- [Memory reporter](#memory-reporter)
- [Snapshot ownership and collection limits](#snapshot-ownership-and-collection-limits)
- [Diagnostic examples and output](#diagnostic-examples-and-output)
- [Bounded-memory roadmap](#bounded-memory-roadmap)
- [Research and measurements](#research-and-measurements)

## Reading memory samples

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

### Linux snapshot behavior

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

Additional memory views are included in the same snapshots and profile output:

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

### Reading snapshots in an app

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

## Memory value sources

The paths below describe the built-in collectors. `RuntimeEnvironment` can supply
custom samples, `Host::from_parts` accepts explicit data, and `from_*_root` readers
can use captured trees instead of the live host. File reads are best effort and
not atomic across sources. Host, cgroup, process, NUMA and allocator views overlap;
do not add them together or treat visible cgroup limits as allocation headroom.
The Rustdoc on each type and collector field records its source and units.

### Portable totals and process values

These mappings were checked against the locally resolved `sysinfo` 0.39.6 source.
All values in `MemStats`, `SwapStats` and the two memory fields of `ProcessStats`
are bytes. `MemorySnapshot` copies portable totals from `HostStats` and shares its
Linux tables; constructing a snapshot does not read the OS again.

| Type / field | Built-in source and interpretation |
| --- | --- |
| `MemStats::total` | `sysinfo::System::total_memory()`: Linux `/proc/meminfo` `MemTotal`; macOS `sysctl` `hw.memsize`. |
| `MemStats::free` | `System::free_memory()`: Linux `MemFree`; macOS Mach `host_statistics64(HOST_VM_INFO64)`, free pages minus speculative pages, multiplied by page size. This excludes reclaimable cache. |
| `MemStats::used` | `System::used_memory()`: Linux `MemTotal - MemAvailable`; macOS internal pages minus purgeable pages plus wired and compressor pages, multiplied by page size. It need not equal `total - free`. |
| Linux availability fallback | When `MemAvailable` is absent, sysinfo estimates it from `MemFree + Buffers + Cached + SReclaimable - Shmem` using saturating arithmetic. WORA does not expose availability as a separate portable field. |
| `SwapStats::{total,free,used}` | `System::{total_swap,free_swap,used_swap}()`: Linux `SwapTotal`, `SwapFree`, and their difference; macOS `sysctl` `vm.swapusage` `xsu_total`, `xsu_avail`, and their difference. |
| `ProcessStats::{memory,virtual_memory}` | `sysinfo::Process::{memory,virtual_memory}()`: Linux `/proc/<pid>/statm` resident and size fields multiplied by base page size, falling back to `/proc/<pid>/stat` RSS pages and `vsize` bytes; macOS `proc_pidinfo` task-info resident/virtual sizes. These describe the sampled PID, not the host. |

Linux `/proc/meminfo` labels sizes `kB`; sysinfo and procfs `Meminfo` convert those
KiB values to bytes. Other Linux types preserve source units unless explicitly
stated. Base page size is supplied by the consumer when converting page counts.

### Routine Linux sources

Fields are under `LinuxMemoryData` (accessible through `LinuxMemoryStats::Deref`).
The default procfs root is `/proc`; the sysfs root is `/sys`. A proc-only capture
omits sysfs-backed views. The process root is normally `/proc/self`.

| Field / types | Files read |
| --- | --- |
| `meminfo`: `Meminfo` | `/proc/meminfo`; sizes converted to bytes by procfs, with count fields retaining their documented meaning. |
| `vmstat`: `VmStat` | `/proc/vmstat`; native named VM gauges and cumulative counters, with units depending on the key. |
| `pressure`: `MemoryPressure`, `PressureRecord` | `/proc/pressure/memory`; `some`/`full` stall averages and total stall microseconds. |
| `shared_memory`: `SharedMemorySegments`, `Shm` | `/proc/sysvipc/shm`; System V segment sizes/resident/swapped bytes and native IDs/timestamps/counts. |
| `iomem`: `Iomem`, `PhysicalMemoryMap` | `/proc/iomem`; inclusive physical address ranges and labels; addresses may be redacted. |
| `buddyinfo`: `BuddyInfo`, `BuddyZone` | `/proc/buddyinfo`; free blocks by node/zone/order, each order `n` representing `2^n` base pages. |
| `zoneinfo`: `ZoneInfo`, `ZoneInfoZone`, `ZoneWatermarks`, `ZonePageSet` | `/proc/zoneinfo`; page inventory/watermarks plus native node/zone/per-CPU statistics. |
| `swaps`: `Swaps`, `SwapEntry`, `SwapType` | `/proc/swaps`; backing paths/types, KiB size/usage and signed priorities. |
| `slabinfo`: `SlabInfo`, `SlabCache`, `SlabGlobalStats`, `SlabCpuStats` | `/proc/slabinfo`; object/slab counts, object bytes, base-page geometry and optional allocator counters. |
| `pagetypeinfo`: `PageTypeInfo`, `PageTypeNode`, `PageTypeFreeList`, `PageTypeBlockCounts`, `FreeBlockCount` | `/proc/pagetypeinfo`; native block/pageblock gauges and capped free-list observations. |
| `sysctls`: `MemorySysctls` | `/proc/sys/vm/*`, `/proc/sys/kernel/{shm*,numa_balancing*,perf_event_mlock_kb}` and `/proc/sys/fs/{file-max,file-nr,nr_open,inode-nr,inode-state,dentry-state}`; dotted keys preserve native units. |
| `cgroup`: `CgroupMemoryStats`, `CgroupMemory`, `CgroupMemoryValue` | Discovery from `/proc/self/{cgroup,mountinfo}`, then `memory.*`, `hugetlb.*`, `cpuset.mems` and `cpuset.mems.effective` in the selected cgroup v2 directory; visible parents include usage/limits and node constraints. |
| `hugepages`: `HugePageStats`, `HugePageFiles` | `/sys/kernel/mm/transparent_hugepage/**`, `/sys/kernel/mm/hugepages/**` and `/sys/devices/system/node/node*/hugepages/**`; native policies, sizes and counters. |
| `numa`: `NumaStats` | `/sys/devices/system/node/{online,possible,has_*}`, `node*/{meminfo,numastat,vmstat,cpulist,cpumap,distance}` and `/sys/devices/virtual/memory_tiering/memory_tier*/nodelist`. |
| `compressed`: `CompressedMemoryStats`, `ZramDevice`, `ZramMemoryStat` | `/sys/module/zswap/parameters/*`, selected `/sys/kernel/debug/zswap/*` counters and selected files under `/sys/block/zram*`, including `mm_stat`, `io_stat`, `bd_stat` and `stat`. |
| `ksm`: `KsmStats` | `/sys/kernel/mm/ksm/*` and `/proc/self/{ksm_stat,ksm_merging_pages}`; native pages, bytes, times and flags. |
| `process`: `ProcessMemoryStats`, `SmapsRollup`, `SmapsRollupValue` | `/proc/self/{status,limits,stat,oom_score,oom_score_adj,smaps_rollup}`; rollup `kB` values retain KiB units and offer checked conversion via `bytes()`. |

### Opt-in diagnostic and tracing sources

Fields are under `LinuxMemoryDiagnostics` and have their own `sampled_at` time.
Paths use the configured procfs/sysfs roots; debugfs is expected at
`<sys_root>/kernel/debug`. No debugfs or tracefs mount is created.

| Field / types | Files read |
| --- | --- |
| `process_mappings`: `ProcessMemoryDetails`, `MemoryMapping` | `/proc/self/{maps,numa_maps,smaps}`; virtual byte intervals and native per-mapping quantities, flags and node counts. |
| `slub`: `SlubStats` | Selected state/counter files under `/sys/kernel/slab/<cache>/`; aliases resolve to canonical caches once. |
| `kernel_allocations`: `KernelAllocationStats`, `AllocationSites`, `AllocationSite` | `/proc/vmallocinfo` and `/proc/allocinfo`; native vmalloc records and byte/allocation attribution per call site. |
| `fragmentation`: `FragmentationStats`, `FragmentationZone` | `/sys/kernel/debug/extfrag/{unusable_index,extfrag_index}`; exact thousandths by node/zone/order, including the `-1.000` sentinel. |
| `lru_generations`: `LruStats`, `LruHistogram`, `LruGeneration` | `/sys/kernel/mm/lru_gen/{enabled,min_ttl_ms}` and `/sys/kernel/debug/{lru_gen,lru_gen_full}`; basic histograms use generation sequence, age milliseconds and page counts. |
| `physical_pages`: `PhysicalPageStats`, `PageRange` | Explicit PFN range in `/proc/{kpageflags,kpagecount,kpagecgroup}`; native-endian 64-bit flag/count/cgroup-inode entries. |
| `page_map`: `ProcessPageMap`, `PageMapEntry`, `PageRange` | Explicit virtual-page range in `/proc/self/pagemap`; decoded 64-bit entries; PFNs may be redacted. |
| `page_owner`: `PageOwnerStats` | `/sys/kernel/debug/page_owner` and `page_owner_stacks/{show_stacks,show_handles,show_stacks_handles,count_threshold}`; numeric settings are parsed, unsupported dumps produce errors. |
| `kernel_leaks`: `KernelLeakStats` | `/sys/kernel/debug/kmemleak`; unsupported finding dumps produce errors without retaining text; reads may incur scanning work. |
| `hardware`: `MemoryHardwareStats` | `/sys/devices/system/memory/*`, `/sys/devices/system/edac/mc/**`, `/sys/devices/system/node/node*/memory_failure/*` and `/sys/kernel/debug/cma/**`; selected block, EDAC, poison-recovery and CMA inventory/counters. |
| Separate trace stream: `MemoryTraceRecord`, `MemoryTraceEvent`, `MemoryTracepoint` | `<tracefs_root>/instances/<owned-instance>/trace_pipe`; `oom/mark_victim`, direct reclaim begin/end, compaction begin/end and `migrate/mm_migrate_pages`. WORA configures only that instance. |

`MemoryFiles::parsed` keys identify paths relative to the containing collector's
root; `ParsedMemoryValue` preserves named fields, exact numbers and native units.
`MemorySysctls::parsed` uses dotted names instead. `CollectionReport` and
`MemorySamplingStatus` are WORA's own accounting of admitted reads, retained
encoded bytes/generations and skipped refreshes; they are not kernel memory totals.
Parser-only APIs consume caller-supplied input without accessing these paths.
For parser formats, validation rules and standalone examples, see the
[Linux memory reader reference](../wora-observability-linux-mm/README.md).

## Memory reporter

Run the memory-reporting profile:

```sh
cargo run --example memory_report
```

Profiles combine a workload's default configuration with the existing WORA
runtime so small programs need very little setup. `MemoryReporter` prints one
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

### Runtime and diagnostic cadence

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

## Snapshot ownership and collection limits

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

## Diagnostic examples and output

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

## Bounded-memory roadmap

Make it possible to configure a ceiling on WORA-owned memory across startup,
sampling, overload, reloads, and restarts, independent of uptime and event volume.
The ceiling must include transient allocation peaks as well as retained state.
RSS can fluctuate within a measured envelope; allocation-free execution would be
a separate, stricter goal. A whole-daemon guarantee also requires application
code, custom providers, sinks, executors, and the caller-owned Tokio runtime to
follow the same resource limits.

Current gaps identified by code inspection:

- Cgroup files now use bounded readers and shared collection budgets, with one
  retained mount candidate and capped ancestors. Streaming discovery and exact
  allocation/container-overhead accounting remain open.
- System sampling now targets host metrics and the daemon PID, but `HostInfo`
  still collects variable-sized network tables outside the Linux memory budget.
- Routine Linux tables and diagnostic batches are shared with `Arc`, with limits
  on live generations and encoded payload bytes. Input/payload byte accounting
  does not yet include exact Rust container capacities or allocator overhead.
- Application and observability queues default to 1,024 and 64 items respectively,
  but item counts do not bound payload bytes or memory held by pending senders.
- `O11yMemorySink` appends formatted events indefinitely. Memory reporting reuses
  a capped JSON buffer; general JSON sinks still need bounded output buffers.
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

- [x] Extend the bounded-reader helpers in
  `wora-observability-linux-mm/src/files.rs` with shared collection budgets.
  Detect oversized input explicitly, including a valid-looking prefix, rather
  than treating a truncated read as complete.
- [ ] Bound file and line bytes, path lengths, ancestor depth, selected files,
  counter keys, NUMA entries, and aggregate retained bytes. Error messages and
  unavailable/skipped-entry maps must fit the same budget.
- [x] Keep only the best matching cgroup mount instead of collecting and sorting
  every candidate, preserving closest-root selection and deterministic ties.
- [ ] Stream membership and mount discovery instead of retaining complete bounded
  text buffers. Current shared file/line/input limits already cap those buffers.
- [ ] Preserve unknown kernel fields within the configured limits. Report explicit
  incomplete/unavailable results when limits are exceeded, including incomplete
  ancestor visibility; never present a partial table as complete accounting.
- [ ] Keep cgroup collection read-only, preserve literal `max` values and scope
  distinctions, and continue excluding action controls such as `memory.reclaim`.

### 3. Bound sampling and snapshot retention

- [x] Replace broad `sysinfo` collection with the host fields and current-PID data
  actually needed. Task enumeration and command/environment inventories are
  disabled. Remaining inventories still need limits applied before allocation;
  truncating a fully collected table does not bound its peak memory.
- [ ] Provide a small routine telemetry profile through existing collection
  options. Put detailed process/socket, slab, zone/per-CPU, NUMA, hardware, and
  other inventories under explicit selection and aggregate diagnostic budgets.
- [x] Apply shared input/work limits to routine and selected diagnostic
  procfs/sysfs readers, including per-file, page-range and traversal reads.
  Encoded snapshot and report limits are separate; event tracing retains its own
  bounded parser and per-CPU ring-buffer settings.
- [x] Share immutable routine tables and diagnostic batches using `Arc`, eliminate
  the periodic host-event clone, and enforce per-batch/retained encoded payload
  limits. Public struct-literal migration is documented; exact heap accounting
  remains part of the contract work above.
- [x] Cap live snapshot generations, including samples retained by listeners.
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
  Memory reporting now reuses a capped buffer and borrows snapshot data; apply
  equivalent limits to the remaining sinks and record formatters.
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
- [ ] Apply the same limits to the [reload-coalescing](../TODO.md#reload-coalescing-and-debouncing)
  and [atomic-snapshot](../TODO.md#atomic-initial-snapshots) roadmap items, including
  snapshot acquisition and retries.

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

## Research and measurements

These dated notes describe the checkout and host observed at the time. They are
not fresh measurements or guarantees for the current build. Later entries record
subsequent implementation work; earlier unchecked items may be superseded.

- [Original release-process baseline, 2026-10-17](#measured-baseline-inanna-2026-10-17)
- [Single-worker comparison, 2026-10-18](#single-worker-reporting-examples-2026-10-18)
- [Shared diagnostic batches and borrowed reports](#shared-diagnostic-batches-and-borrowed-reports-2026-10-18)
- [Targeted system sampling](#targeted-system-sampling-2026-10-18)
- [Rebuilt release-process measurements](#rebuilt-diagnostics-process-inanna-2026-10-18)
- [Routine sharing, budgets and local allocation profiling](#shared-routine-tables-and-collectionoutput-budgets-2026-10-18)

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
  The memory guide and profile Rustdoc examples use the same runtime configuration;
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
- [x] Profile remaining allocations locally with heaptrack; see the routine-table
  and budget notes below. Exact production RSS attribution and allocator-retained
  space remain unmeasured; no arena recommendation is based on this profile.

### Single-worker reporting examples: 2026-10-18

`memory_report` and `memory_diagnostics` now use
`#[tokio::main(worker_threads = 1)]`. The memory guide and profile Rustdoc show the same
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
Local macOS formatting, tests, Clippy, and documentation checks passed. The
Linux-specific regressions subsequently passed in the local Linux container
during the routine-sharing/budget work below. Measurements of the rebuilt Linux
process are recorded below.

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
passed. Linux regressions subsequently passed in the local Linux container during
the routine-sharing/budget work below. Updated RSS/descriptor measurements of the
rebuilt process are recorded below; they cover the combined
changes rather than attributing savings to this sampler alone.

### Rebuilt diagnostics process: inanna, 2026-10-18

Inspected the user-started PID `145249`, running the release `memory_diagnostics`
example from commit `8702dbe`. The five inspected source files covering system
sampling, diagnostic ownership, reporting, and the example matched the local
checkout by SHA-256. The binary was rebuilt at 13:30:23 EDT and the process started
at 13:30:27 EDT. It used the default five-second reporting and 60-second diagnostic
intervals, with stdout directed to `/dev/null` and no `MALLOC_ARENA_MAX` or
`TOKIO_WORKER_THREADS` environment override.

Initial inspection at 81 seconds observed 14.4 MiB RSS. A subsequent 181-second
window, from 13:32:20 to 13:35:21 EDT, sampled procfs 37 times at approximately
five-second intervals. RSS rose around successive diagnostic refreshes from
14.4 to 16.9, 17.6, and 17.6 MiB; the last of these increases was 40 KiB. The final
snapshot at about five minutes of process uptime showed:

| Measurement | Rebuilt PID `145249` | Original PID `131240` baseline |
| --- | --- | --- |
| Resident memory (RSS) | 17.6 MiB | 68.9 MiB |
| Proportional resident memory (PSS) | 15.3 MiB | 66.6 MiB |
| Anonymous resident memory | 12.9 MiB | 64.2 MiB |
| Virtual address space | 275.3 MiB | 1.25 GiB |
| Threads | 5 | 20 |
| Cached process/task `stat` descriptors | 1, for this PID | Approximately 1,900 |

The rebuilt process retained 17 descriptors throughout the sampling window and
had no swapped or locked pages. Its final RSS high-water mark was also 17.6 MiB.
The five threads were the main thread, two Tokio-named threads, and two notify
threads. Procfs I/O counters continued advancing, but JSON content and exact
reporting cadence were not verified because output was discarded. The process
retained its original start identity and was not modified, restarted, or attached
to a debugger.

Final RSS was about 74% below the original baseline and about 57% below the
earlier one-worker-only diagnostic run's 41.1 MiB. These runs differ in age and
host conditions; they show the combined improvement without isolating the savings
from sharing, borrowed serialization, and targeted sampling individually. The
single cached `stat` descriptor confirms the earlier system-wide descriptor
retention is absent in this process.

The main heap held about 4.0 MiB RSS and anonymous mappings about 8.8 MiB. Those
regions can contain both live allocations and allocator-retained free memory;
procfs alone cannot attribute their contents. Much of the virtual address space
remains nonresident, consistent with allocator reservations. The process still
shares a login-session cgroup, so its cgroup usage is not a per-process measure.

Next measurement priorities:

- [ ] Extend the soak beyond startup and several diagnostic refreshes; this short
  window does not establish constant memory or exclude gradual growth.
- [x] Profile remaining diagnostic allocations, including SLUB tables, routine
  snapshots, batch overlap and JSON output. The local heaptrack capture below
  identifies live allocation sites; unresolved frames, allocator retention and
  exact production RSS attribution still need follow-up.
- [ ] Implement shared collector/snapshot budgets, starting with the cgroup
  collector above. A smaller observed footprint does not provide a configured
  memory ceiling.

This inspection completed the updated RSS/descriptor measurement. The Linux
regressions subsequently passed during the routine-sharing/budget work below.

### Shared routine tables and collection/output budgets: 2026-10-18

- [x] Move periodic `HostStats` into observability events with
  `o11y_new_ev_hoststats_owned`, eliminating the event-only copy. The existing
  borrowed constructor and public lifecycle/provider traits remain available.
- [x] Share routine Linux tables through `LinuxMemoryStats::data`, an
  `Arc<LinuxMemoryData>`. Host samples, cached memory snapshots, and observability
  events share the same tables. Field mutation uses copy-on-write so retained
  snapshots preserve their data. Diagnostic ownership remains independently shared.
- [x] Reuse one memory-report JSON buffer with a configurable record byte limit
  (8 MiB by default, including the newline). Growth is checked before reserving;
  oversized records fail without writing a partial line. Trace records use the
  same buffer and limit.
- [x] Apply one input/work budget to each routine or diagnostic batch, including
  cgroup discovery/files, sysctls, hugepages, rollups, structured diagnostics and
  binary page ranges. Default limits admit 16 MiB across sources, 4 MiB per file,
  64 KiB per line, 65,536 directory entries, 65,536 source attempts (including
  failures), 4,096-byte paths and 64 cgroup ancestors.
  EOF probes may read one additional byte per source without retaining it. An
  exhausted limit is reported in `collection.limit_exceeded`; oversized sources
  are not parsed from valid-looking prefixes.
- [x] Limit routine and diagnostic providers independently to four live
  generations, 8 MiB encoded JSON per batch and 32 MiB across live batches.
  Reserve the maximum replacement payload allowance before collection; byte or
  generation exhaustion skips routine collection or reuses the previous
  diagnostic batch. Provider status APIs expose live generations, retained
  encoded bytes and saturating skipped-refresh counters. Original sample
  timestamps expose age. Releasing retained data permits collection to resume.

Configure these through `LinuxMemoryLimits`,
`SystemRuntimeEnvironment::with_linux_memory_limits`,
`LinuxMemoryOptions::with_memory_limits`, and
`MemoryReporter::with_linux_memory_limits`/`with_output_byte_limit`. With
`MemoryReporter::run_with`, configure the supplied routine provider separately.

Public struct-literal migration: construct routine fields using
`LinuxMemoryData { .. }.into()` rather than `LinuxMemoryStats { .. }`. Normal field
reads continue through `Deref`; borrow fields instead of moving them out of shared
data. Collected routine and diagnostic JSON adds `collection` accounting while
keeping existing data at its original paths. This guide and CHANGELOG describe this
source/API and output change.
`LinuxMemoryOptions` literals also require `limits` or a default struct update;
`LinuxMemoryDiagnostics` literals require the new `collection` accounting field.

Regression coverage verifies routine/diagnostic sharing, copy-on-write isolation,
live-generation and retained-byte admission/recovery, invalid/oversized limits,
aggregate reads, incomplete ancestor visibility, JSON escaping, exact
record/newline boundaries, buffer reuse, and unchanged existing snapshot values.
Failed explicit source lookups are admitted before metadata access and share a
file-attempt limit, preventing zero-byte failures from growing error maps without
consuming budget. Oversized source roots and names are rejected before reading.

Validation passed on macOS with repository Rust 1.95.0 (`cargo fmt --check`,
`cargo test`, `cargo clippy --all-targets --all-features`, `cargo doc --no-deps`).
Local aarch64 Linux/Rust 1.99.0 passed workspace tests, Clippy across all targets
and features, and workspace documentation. Nine opt-in live-kernel tests stayed
ignored. Linux Clippy reports existing warnings in the page/structured helpers,
raw sysinfo example and unused socket-test helper; the new code has no lint warnings.

Local allocation profiling used heaptrack 1.5.0 in a disposable aarch64 Linux
container with Rust 1.99.0 development builds. The changed diagnostic example ran
for about 33 seconds with one-second report and five-second diagnostic intervals,
produced 16 valid JSON reports spanning four diagnostic batches, and exited zero
after SIGTERM directed at the example process. Neither routine nor diagnostic
collection exhausted its source/work limits. Heaptrack reported 751,165 allocation
calls, a 21.76 MB peak tracked heap, and 39.96 MB peak RSS including profiler
overhead. This instrumented root/container workload differs from the normal-user
release process on inanna and does not update that process's RSS baseline.
This capture preceded the final file-attempt accounting and root-path guards.

Resolved peak allocation stacks include SLUB parsed-map nodes, vmalloc record
maps, per-mapping structured quantities and the Tokio stdout buffer. More than
half the peak attribution was unresolved, so these are observed sites rather than
a complete ownership breakdown. The baseline instrumented process failed under
the profiler; incomplete captures were excluded and no before/after savings
percentage is claimed. Stable baseline profiling and a release-process soak
remain follow-up measurements. Profiling containers were removed; captures remain
under the ignored `target/memory-profile-20261018-145249` directory.

Input and encoded-payload limits are distinct from an exact heap/RSS budget.
Allocator/container overhead, CPU/disk/network inventories, custom providers,
caller-created copies, general sinks, queues, scheduled tasks and reload paths
remain outside these limits. The broader bounded-memory contract and extended
production soak above remain open.
