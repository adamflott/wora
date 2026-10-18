# Memory reporting

Current APIs, sources and limits. Remaining implementation work is in
[the memory TODO](../wora-observability-linux-mm/TODO.md).

- [Usage](#usage)
- [Linux mm subsystems](#linux-mm-subsystems)
- [Supported Linux kernels](#supported-linux-kernels)
- [Collection and retention limits](#collection-and-retention-limits)
- [Recorded measurements](#recorded-measurements)

## Usage

Run the JSON-lines reporter:

```sh
cargo run --example memory_report
```

It reports at startup and every five seconds, flushes each record, and stops on
Ctrl-C/SIGTERM. Defaults: stdout, `wora-memory-report`, per-user Unix executor,
`NoConfig`/`NoSecrets`, no global tracing subscriber.

```rust,no_run
use wora::profiles::MemoryReporter;

#[tokio::main(worker_threads = 1)]
async fn main() -> Result<(), wora::errors::MainEarlyReturn> {
    MemoryReporter::new().run().await
}
```

Configure with `with_interval`, `with_name`, `with_writer`,
`with_linux_memory_limits` or `with_output_byte_limit`. `run_with` accepts an
executor, filesystem and runner options; configure its supplied routine provider's
limits separately. The profile uses the caller's Tokio runtime.

Within an app lifecycle hook:

```rust
let snapshot = wora.memory_stats();
println!("{} bytes used", snapshot.memory.used);
let updates = wora.subscribe_memory_stats();
```

The cache exists during setup; `stats_from_start()` retains the startup sample.
Apps also receive `Event::SystemResource(SystemResourceStat::MemorySnapshot(...))`.
A full app queue skips the periodic notification; the cache/watch still updates.
Watch receivers may coalesce samples. Failed/skipped host refreshes keep the previous
sample and timestamp.

The pipeline's `host_stats_interval` defaults to 30 seconds. Change it live with
`O11ySettingsPatch::new().host_stats_interval(...)` through the control handle.
This interval covers memory events and host/process metrics; the reporter overrides
it to five seconds. Periodic synchronous collection runs on a blocking worker.

### Linux diagnostics and tracing

Use `MemoryReporter::with_linux_memory_diagnostics(LinuxMemoryOptions::new()
.enable(LinuxMemoryCollector::ProcessMappings))` for selected diagnostics.
They default to a 60-second cadence; cached batches retain their own `sampled_at`.
`LinuxMemoryEnvironment::new(provider, options)` wraps another telemetry provider.

```sh
cargo run --example memory_diagnostics -- --diagnostic-interval 60
cargo run --example memory_diagnostics -- --pfn-start 0 --page-count 1024
cargo run --example memory_diagnostics -- --trace
```

The diagnostic example selects all snapshot diagnostics, including costly
kmemleak reads. Page scans require explicit `PageRange` values.
`with_linux_memory_trace(LinuxMemoryTraceOptions::new())` adds trace records.
Tracing requires access to existing tracefs, creates a private instance and removes
it on normal shutdown. Crashes/SIGKILL may leave the instance named in startup output.

Each report has `timestamp`, `memory`, `swap` and Linux-only `linux_mm`.
Trace lines instead contain `timestamp` and `memory_trace`. Output errors stop the
profile. Snapshot tables use structured values; trace payloads retain native text.

### Portable values and ownership

| Values | Built-in sources; all amounts below are bytes |
| --- | --- |
| `MemStats::{total,free,used}` on Linux | sysinfo reads `/proc/meminfo`: `MemTotal`, `MemFree`, `MemTotal - MemAvailable`. Availability has a cache/buffer/reclaimable-slab fallback on old kernels. |
| `MemStats` on macOS | sysinfo uses `hw.memsize` and Mach VM statistics. Free excludes speculative pages; used is internal minus purgeable plus wired/compressor pages. |
| `SwapStats::{total,free,used}` | Linux: `SwapTotal`, `SwapFree`, their difference. macOS: `vm.swapusage` total/available and their difference. |
| `ProcessStats::{memory,virtual_memory}` | Linux: `/proc/<pid>/statm` resident/size pages, falling back to `stat` RSS/vsize. macOS: `proc_pidinfo` resident/virtual sizes. |

These mappings describe sysinfo 0.39.6. `used` need not equal `total - free`.
Custom `RuntimeEnvironment` providers and captured roots can supply different data.
Host, process, cgroup and node views overlap; do not sum them.

`MemorySnapshot` copies portable totals and shares Linux data without another OS
read. Routine tables use `Arc<LinuxMemoryData>`; diagnostics use a separate
`Arc<LinuxMemoryDiagnostics>`. Mutation is copy-on-write. For struct literals,
construct `LinuxMemoryData { .. }.into()`; borrow fields when reading shared data.
JSON routine fields stay directly under `linux_mm`, with `collection` accounting.

## Linux mm subsystems

All fields below are under `linux_mm`. Paths use default `/proc` and `/sys` roots.
Rustdoc on the [reader types](../wora-observability-linux-mm/src/lib.rs) documents
nested values and conversions.

### Routine sources

| Field / subsystem | Reported data | Source |
| --- | --- | --- |
| `meminfo`: host inventory/cache | RAM categories, dirty/writeback, huge-page totals; sizes converted to bytes. | `/proc/meminfo` |
| `iomem`: physical address map | Labeled physical ranges; addresses may be redacted. | `/proc/iomem` |
| `vmstat`: reclaim/paging/writeback | Native VM gauges and cumulative counters; units vary by key. | `/proc/vmstat` |
| `buddyinfo`: buddy allocator | Free blocks by node/zone/order; order `n` contains `2^n` base pages. | `/proc/buddyinfo` |
| `zoneinfo`: zones/page caches | Inventory, watermarks, reserves and node/zone/CPU pagesets. | `/proc/zoneinfo` |
| `pagetypeinfo`: migration/pageblocks | Free blocks, pageblocks and optional mixed blocks; capped values retain lower bounds. | `/proc/pagetypeinfo` |
| `pressure`: memory PSI | `some`/`full` stall averages and total microseconds. | `/proc/pressure/memory` |
| `shared_memory`: System V IPC | Segment IDs, sizes, attachments and resident/swapped bytes. | `/proc/sysvipc/shm` |
| `swaps`: swap backing | Device/file inventory, KiB size/usage and signed priorities. | `/proc/swaps` |
| `slabinfo`: SLAB/SLUB objects | Object/slab counts, byte sizes, page geometry and optional legacy counters. | `/proc/slabinfo` format 2.1 |
| `sysctls`: VM/kernel policy | VM settings; shared-memory, NUMA and perf settings; selected descriptor/inode/dentry counters. | `/proc/sys/vm/*`, selected `/proc/sys/{kernel,fs}` files |
| `cgroup`: memcg/DMEM/cpuset/HugeTLB | v2 usage, limits, stats, pressure, events, swap/zswap, device-region byte usage/capacity/limits, HugeTLB and allowed memory nodes; selected ancestor usage/limits. | Membership/mount discovery in `/proc/self/{cgroup,mountinfo}`, then `memory.*`, selected `dmem.*`, `hugetlb.*`, `cpuset.mems{,.effective}` |
| `hugepages`: THP/mTHP/HugeTLB | Policies, khugepaged/per-size counters and global/node pools. Counts remain size-specific. | `/sys/kernel/mm/{transparent_hugepage,hugepages}`, `/sys/devices/system/node/node*/hugepages` |
| `numa`: locality/balancing/tiers/policy | Node memory/VM/locality tables, CPU lists/maps, distances, rated access-class latency (ns)/bandwidth (MiB/s), tier membership and global weighted-interleave weights/auto mode. | `/sys/devices/system/node` (including `node*/access*/initiators`), `/sys/devices/virtual/memory_tiering`, `/sys/kernel/mm/mempolicy/weighted_interleave` |
| `compressed`: zswap/zram | Zswap policy, failure counters, stored/incompressible pages and zram device memory/I/O/backing-device statistics. | `/sys/module/zswap/parameters`, `/sys/kernel/debug/zswap`, `/sys/block/zram*` |
| `ksm`: identical-page sharing | Global/process counters, scanner policy, signed profit and flags. | `/sys/kernel/mm/ksm/*`, `/proc/self/{ksm_stat,ksm_merging_pages}` |
| `process`: process VM/accounting | Status/peaks, limits, faults, OOM preferences and RSS/PSS/private/shared/swap rollup. | `/proc/self/{status,limits,stat,oom_score,oom_score_adj,smaps_rollup}` |

### Opt-in diagnostic sources

| Field / subsystem | Reported data | Source |
| --- | --- | --- |
| `diagnostics.process_mappings`: mappings/NUMA | Address ranges, backing metadata, native quantities and VM flags. | `/proc/self/{maps,numa_maps,smaps}` |
| `diagnostics.damon`: DAMON_STAT | Existing monitor state, estimated bytes/s, signed idle-time percentiles in ms, aggregation interval in us and worker PID. Disabled monitoring can leave stale results. | `/sys/module/damon_stat/parameters/*`; select `LinuxMemoryCollector::Damon` |
| `diagnostics.slub`: object caches | Per-cache/node occupancy, policy/debug counters and canonical aliases. | Selected `/sys/kernel/slab/<cache>/*` |
| `diagnostics.kernel_allocations`: vmalloc/call sites | Area metadata and allocinfo bytes/counts/source locations/accuracy markers. | `/proc/{vmallocinfo,allocinfo}` |
| `diagnostics.fragmentation`: contiguous allocation | Per-node/zone/order indices in thousandths, including the `-1.000` sentinel. | `/sys/kernel/debug/extfrag/{unusable_index,extfrag_index}` |
| `diagnostics.lru_generations`: Multi-Gen LRU | Policy, basic histograms and full cgroup/node/generation/tier statistics. Preserve page units, expiry/average/unavailable tags and optional MM counts; reviewed 6.12/7.2 layouts. | `/sys/kernel/mm/lru_gen`, `/sys/kernel/debug/{lru_gen,lru_gen_full}` |
| `diagnostics.physical_pages`: frame metadata | Bounded flags, mapping counts and charged cgroup inode numbers. | `/proc/{kpageflags,kpagecount,kpagecgroup}` |
| `diagnostics.page_map`: process page tables | Bounded raw/decoded flags and permitted PFN/swap identities. | `/proc/self/pagemap` |
| `diagnostics.page_owner`: allocation provenance | Allocation/PFN/pageblock/stack records, migration/charge labels, aggregate handles/base-page counts and threshold. Reviewed 6.12/7.2 layouts; 65536 records, 256 frames/stack, 4 KiB/line. | `/sys/kernel/debug/page_owner`, `page_owner_stacks/*` |
| `diagnostics.kernel_leaks`: kmemleak | Uncertain unreferenced-object suspects: address/scope, byte size, PID/comm, allocation jiffies, checksum and stack. Embedded object bytes are discarded; same record/frame/line caps as page owner. | `/sys/kernel/debug/kmemleak` |
| `diagnostics.hardware`: hotplug/EDAC/poison/CMA | Block state/size/zones, controller/DIMM errors, node recovery counters, CMA PFNs/counts/bitmaps. | `/sys/devices/system/{memory,edac/mc,node/node*/memory_failure}`, `/sys/kernel/debug/cma` |

Tracing reports `oom/mark_victim`, direct-reclaim and compaction begin/end, and
`migrate/mm_migrate_pages` through `MemoryTraceRecord`. Matched durations use
trace-clock nanoseconds; loss/unknown records remain visible. Durations are not a
complete latency census. `MemoryTraceParser` itself performs no privileged I/O.

### Reading rules

- Source reads are independent and non-atomic. Optional proc tables can be `null`;
  other collectors retain per-file `unavailable`/`skipped` entries. Zero remains distinct.
- Preserve native units: Linux `kB` means KiB; rollups offer checked `bytes()`
  conversion. Base-page counts require the actual page size. Do not sum overlapping scopes.
- NUMA access-class quantities describe rated topology for linked initiators;
  class 1 is CPU-only. Discovery caps at 64 classes per node, with 4 KiB per
  attribute, plus shared entry/file/byte limits. [Kernel ABI](https://docs.kernel.org/admin-guide/mm/numaperf.html).
- Slab/page-type/debugfs/PFN reads may require privileges. smaps/rollups/page-type
  reads can be expensive; page-owner mixed-table reads can drain CPU page lists,
  and kmemleak reads may incur scans.
- Snapshot collectors do not change settings/membership or open process memory files.
  Kmemleak's embedded object-byte dump is discarded from retained results.
  `memory.reclaim`, HugeTLB `demote`, write-only actions and `vm.stat_refresh` are excluded.
- `LinuxMemoryStats::from_roots(proc_root, sys_root)` reads captured trees;
  `from_proc_root` omits sysfs views. Captured cgroup mount paths are used as written.
- `MemoryFiles::parsed` uses relative paths; sysctl maps use dotted names.
  Structured values retain exact integers, units, policies and ranges. Unknown
  supported numeric keys survive; typed `Meminfo` and filename allowlists have limits.

Kernel references: [proc](https://docs.kernel.org/filesystems/proc.html),
[cgroups](https://docs.kernel.org/admin-guide/cgroup-v2.html#memory),
[PSI](https://docs.kernel.org/accounting/psi.html),
[THP](https://docs.kernel.org/admin-guide/mm/transhuge.html),
[HugeTLB](https://docs.kernel.org/admin-guide/mm/hugetlbpage.html),
[pagemap](https://docs.kernel.org/admin-guide/mm/pagemap.html).

## Supported Linux kernels

Support follows available interfaces/formats. No universal minimum/maximum kernel
has been validated; kernel configuration, mounts, permissions and backports matter.

| Kernel / format | Recorded evidence |
| --- | --- |
| Debian `6.12.95+deb13-cloud-arm64` / `6.12.95+deb13-amd64` | Live fixtures for buddyinfo, zoneinfo, swaps, slabinfo and pagetypeinfo; 4 KiB base pages. |
| Linux 6.18.55, `inanna` | Historical normal-user collection and release-process measurements below. |
| 4.19 zoneinfo / 6.1 legacy SLAB | Format review/synthetic coverage, not whole-crate live validation. |
| Other releases, including 7.x | Best-effort supported layouts; source review does not establish live-kernel support. |

[Fixture provenance](../wora-observability-linux-mm/tests/fixtures/README.md).
CI follows runner images, not a fixed booted-kernel matrix. Containers use the host/VM kernel.

| Interface | Upstream availability / current parser limit |
| --- | --- |
| cgroup v2 memory / cpuset / HugeTLB | 4.5 / 5.0 / 5.6; enabled controllers required; no v1 fallback. [manual][cgroups] |
| DMEM cgroups | 6.14; controller/device support required. Root capacity and child usage stay separate; limits preserve `max`. `dmem.peak` is presence-gated against the 7.3-rc7 development ABI. [ABI][dmem] |
| Weighted interleave | 6.9 node weights (1..=255); 6.16 `auto` mode. Read-only global policy observation; absent mode stays unknown. [ABI][weights] |
| `smaps_rollup` | 4.14; no implicit full-smaps fallback. [source][rollup] |
| Memory PSI | 4.20; PSI must be enabled. [source][psi] |
| MGLRU | 6.1; requires `CONFIG_LRU_GEN`, with optional debug statistics. [source][mglru] |
| Anonymous mTHP | 6.8; per-size files are optional/discovered. [source][mthp] |
| `allocinfo` | 6.10; accepts text formats 1.0/2.0. [source][allocinfo] |
| DAMON_STAT | 6.17; signed active-time percentiles/aggregation interval since 6.19, worker PID since 7.2. Requires `CONFIG_DAMON_STAT`; synthetic layout tests, no monitor enablement. [ABI][damon] |
| Pagemap flags | Modern 4.2+ layout; userfaultfd bit 57 since 5.13, guard bit 58 since 6.15. [ABI][pagemap] |

The reviewed 7.3-rc7 ABI reuses bit 57 for WP or RWP depending on the VMA.
`PageMapEntry::userfaultfd_protection(mapping.fields.get("VmFlags"))` interprets
matching smaps context; missing/conflicting modes stay unknown. The existing
`uffd_write_protected` field retains raw-bit semantics. Callers must match the
process, virtual page and capture time; independent reads can race.
Other supported layouts still depend on configuration; recent kernels do not
make page-owner, kmemleak, EDAC or CMA universally available.

[cgroups]: https://man7.org/linux/man-pages/man7/cgroups.7.html
[dmem]: https://github.com/torvalds/linux/blob/v7.3-rc7/Documentation/admin-guide/cgroup-v2.rst
[weights]: https://github.com/torvalds/linux/blob/v6.16/Documentation/ABI/testing/sysfs-kernel-mm-mempolicy-weighted-interleave
[rollup]: https://github.com/torvalds/linux/blob/v4.14/fs/proc/task_mmu.c
[psi]: https://github.com/torvalds/linux/blob/v4.20/Documentation/accounting/psi.txt
[mglru]: https://github.com/torvalds/linux/blob/v6.1/Documentation/admin-guide/mm/multigen_lru.rst
[mthp]: https://github.com/torvalds/linux/blob/v6.8/Documentation/admin-guide/mm/transhuge.rst
[allocinfo]: https://github.com/torvalds/linux/blob/v6.10/Documentation/mm/allocation-profiling.rst
[damon]: https://github.com/torvalds/linux/blob/v7.2/Documentation/admin-guide/mm/damon/stat.rst
[pagemap]: https://github.com/torvalds/linux/blob/v7.3-rc7/Documentation/admin-guide/mm/pagemap.rst

## Collection and retention limits

| Default | Limit |
| --- | --- |
| Input per routine/diagnostic batch | 16 MiB |
| File / text line | 4 MiB / 64 KiB |
| Source attempts / directory entries | 65,536 each; at most 4,096 entries per directory |
| Path / visible cgroup ancestors | 4,096 bytes / 64 |
| Live generations | 4 per routine/diagnostic provider |
| Encoded batch / retained encoded batches | 8 MiB / 32 MiB per provider |
| Reporter JSON record, including newline | 8 MiB |

`CollectionReport::limit_exceeded` marks incomplete collection. Oversized sources
fail instead of parsing a truncated prefix; EOF probes may read one extra byte.
Generation admission reserves room for a replacement; exhaustion skips routine
refreshes or reuses cached diagnostics. Check `memory_sampling_status()` or
`diagnostic_sampling_status()` for generations, retained encoded bytes and skips.

Configure with `LinuxMemoryLimits`,
`SystemRuntimeEnvironment::with_linux_memory_limits`,
`LinuxMemoryOptions::with_memory_limits` and the reporter's limit setters.
Standalone readers need `collect_with_limits` plus `read_source` to share a budget;
calling procfs parsers/`Current` directly does not acquire that budget.

Input and encoded bytes are not heap capacity or RSS. Other inventories, queues,
sinks, tasks, reloads, custom providers and writer buffers need separate budgets.
Visible cgroup limits are not allocation headroom: `memory.high` applies pressure;
`memory.max` can trigger OOM and can be exceeded temporarily.

## Recorded measurements

Historical observations, not current-build benchmarks or memory ceilings.
`inanna` ran Linux 6.18.55, glibc 2.42, with 16 available CPUs. Reports/diagnostics
used five-/60-second intervals unless noted.

| Observation | RSS | Threads | Context |
| --- | --- | --- | --- |
| Original diagnostics, 2026-10-17 | 68.9 MiB | 20 | PID 131240 at about 34 minutes; PSS 66.6 MiB; about 1,900 cached process/task stat descriptors. |
| Fresh default-worker comparison | 60.7 MiB | 20 | 75-second sample, stdout discarded. |
| Same comparison, one worker | 39.3 MiB | 5 | Same executable/intervals. |
| One worker plus `MALLOC_ARENA_MAX=2` | 32.1 MiB | 5 | Same short comparison; no throughput equivalence established. |
| Original-worker diagnostics, 2026-10-18 | 71.8 MiB | 20 | 185-second sample; 37 reports. |
| One-worker diagnostics | 41.1 MiB | 5 | Same comparison; 37 reports. |
| One-worker routine reporter | 12.6 MiB | 5 | Same comparison; 38 reports. |
| Rebuilt diagnostics, commit `8702dbe` | 17.6 MiB | 5 | PID 145249 at about five minutes; PSS 15.3 MiB; one cached stat descriptor. |

The rebuilt process rose from 14.4 to 17.6 MiB during a 181-second window; its
stdout was discarded, so JSON/cadence was not checked. Runs differ in age and host
conditions. Session cgroups were shared with login processes; their totals cannot
be attributed to these PIDs. No individual optimization's savings are isolated.

Local heaptrack 1.5.0 profiling used aarch64 Linux/Rust 1.99.0 development builds:
33 seconds, one-second reports/five-second diagnostics, 16 valid reports/four batches.
It recorded 751,165 allocations, 21.76 MB peak tracked heap and 39.96 MB peak RSS
including profiler overhead. More than half the peak attribution was unresolved;
the baseline profile failed, so this is not a before/after comparison. Captures
precede the final file-attempt/path guards and are in ignored
`target/memory-profile-20261018-145249`.
