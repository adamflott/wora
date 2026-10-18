# wora-observability-linux-mm

Typed Linux memory readers extending `procfs` 0.18. Linux-only; requires Rust
1.95+. No async runtime, exporter or WORA dependency.

[Memory guide](https://github.com/adamflott/wora/blob/master/docs/memory.md) ·
[Implementation TODO](https://github.com/adamflott/wora/blob/master/wora-observability-linux-mm/TODO.md)

## Coverage

| Area | Reported subsystems |
| --- | --- |
| Host allocation/reclaim | RAM/address maps, VM/cache/writeback counters, buddy blocks, zones/CPU pagesets, migration-type pageblocks, fragmentation, PSI and sysctls. |
| Accounting/placement | Memory/DMEM cgroups, OOM events, cpuset nodes, NUMA locality/tiers/weighted-interleave policy, THP/mTHP/khugepaged and HugeTLB pools. |
| Backing/sharing | Swap, zswap, zram, KSM and System V shared memory. |
| Process/pages | Status/limits/faults/OOM preferences, RSS/PSS rollups, mappings/NUMA policy, physical-frame metadata and pagemap flags. |
| Kernel/hardware diagnostics | SLAB/SLUB, shrinkers, vmalloc/allocation sites, DAMON_STAT, MGLRU, page owner, kmemleak, hotplug blocks, EDAC, poisoned-page recovery and CMA. |
| Trace parsing | OOM victims, direct reclaim, compaction and migration; WORA supplies the optional live tracefs service. |

The [subsystem/source tables](https://github.com/adamflott/wora/blob/master/docs/memory.md#linux-mm-subsystems)
and each type's Rustdoc describe paths, scope and units.
Zswap includes compression/decompression failure counts and stored-incompressible
page counts; counters absent on older kernels remain per-file errors.

## Kernel compatibility

Support is per interface/format; no universal kernel minimum/maximum is validated.
Recorded live fixtures cover Debian 6.12.95 on ARM64/x86_64, with historical
reporter observations on 6.18.55. The 4.19 zoneinfo and 6.1 legacy-SLAB formats
have review/synthetic coverage, not whole-crate live validation.

Features such as PSI, MGLRU and allocinfo require kernel/configuration support.
For the 7.3-rc7 pagemap ABI, `PageMapEntry::userfaultfd_protection` uses matching
smaps `VmFlags` to distinguish WP/RWP; missing context stays unknown. The existing
`uffd_write_protected` field remains a compatibility name for raw bit 57.
See [kernel compatibility](https://github.com/adamflott/wora/blob/master/docs/memory.md#supported-linux-kernels)
for feature versions and evidence. Other kernels use best-effort supported layouts.

## Parsing

Single-file snapshots implement procfs parsing traits. Parse captured text:

```rust
use wora_observability_linux_mm::{BuddyInfo, FromRead};

let info = BuddyInfo::from_read(b"Node 0, zone Normal 10 4 0\n".as_slice())?;
assert_eq!(info.zones[0].free_blocks, vec![10, 4, 0]);
# Ok::<(), wora_observability_linux_mm::ProcError>(())
```

`from_file(path)` accepts captures/alternate files; Linux `Current::current()`
reads live single-file sources. Multi-file readers expose explicit proc/sys/process/
debug-root constructors. Process roots are `/proc/<pid>`, not the proc mount.
Captured cgroup mount paths are used as written.

| Format | Preserved semantics |
| --- | --- |
| BuddyInfo | Free block counts by order; byte conversion needs the actual base page size. |
| ZoneInfo | Inventory/watermarks/reserves in pages; node/zone/CPU counters keep native units. |
| Swaps | KiB sizes/usage, signed priorities and decoded paths; backing files are not opened. |
| SlabInfo | Format 2.1, optional legacy stats; byte object sizes and page/slab geometry. |
| PageTypeInfo | Block/pageblock counts; `>N` is `FreeBlockCount::AtLeast(N)`, an inclusive bound. |
| SmapsRollup / mappings | Native numeric units; `kB` means KiB. Rollup `bytes()` checks overflow. |
| AllocationSites | Versions 1.0/2.0; byte/count attribution and accuracy markers. |
| DamonStats | Existing monitor state, estimated bytes/s, signed idle-time percentiles in ms, aggregation interval in us and worker PID; never enables monitoring. |
| CgroupMemory | DMEM region tables retain byte usage/capacity/limits and literal `max`; optional `dmem.peak` follows the 7.3-rc7 development ABI. |
| NumaStats | Access-class topology quantities retain nanoseconds and MiB/s under per-node source paths; they describe rated performance. |
| PageOwnerStats | Bounded allocation/page/stack records, optional migration/charge labels, aggregate signed handles and base-page counts; reviewed 6.12/7.2 layouts. |
| KernelLeakStats | Bounded unreferenced-object suspects: addresses, byte sizes, PID/comm, jiffies, checksum and stack. Embedded byte dumps are discarded; findings do not prove leaks. |
| LruStats | Basic histograms and full per-generation/tier statistics; preserve cgroup/node scope, page units, expiry, averages and unavailable markers. |
| ShrinkerStats | Object counts by kernel cgroup ID and possible-node column order; default eight count requests, no `scan` access. Each read can invoke many kernel callbacks. |
| MemoryFiles / sysctls | Structured integers, flags, units, ranges and policies; relative path/dotted keys. |

Unknown supported numeric keys are retained. Fixed typed fields, filename
allowlists and unsupported layouts limit forward compatibility.
Enable `serde1` for serialization/deserialization; deserialization/public struct
construction does not apply text-parser validation.

## Bounded collection

Use the shared budget explicitly for standalone readers:

```rust,no_run
use wora_observability_linux_mm::{BuddyInfo, CollectionLimits, collect_with_limits, read_source};

let (snapshot, accounting) = collect_with_limits(CollectionLimits::default(), || {
    read_source::<BuddyInfo>("/proc/buddyinfo")
})?;
let info = snapshot?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

The scope covers synchronous collectors on its current thread; nested scopes are
rejected. Direct procfs `Current`/`FromRead` calls do not acquire the I/O budget.
Defaults include 16 MiB input per batch, 4 MiB per file and 64 KiB per line;
[all limits](https://github.com/adamflott/wora/blob/master/docs/memory.md#collection-and-retention-limits)
include attempts, entries, paths and ancestor depth. These are not heap/RSS limits.

Oversized/malformed sources fail without a successful truncated prefix. Multi-file
snapshots retain independent errors; `CollectionReport::limit_exceeded` marks
incomplete collection. Reads are non-atomic, and missing/denied data differs from zero.

Snapshot collectors never write settings, change membership or open process memory files.
They exclude `memory.reclaim`, HugeTLB `demote`, `vm.stat_refresh` and write-only
controls. Reads may still cost kernel work: rollups/smaps walk page tables,
page-owner mixed pageblocks can drain CPU page lists, and kmemleak may scan.
Privileges and mounted/configured interfaces determine availability.

## Validation

Run on Linux:

```sh
cargo test --locked -p wora-observability-linux-mm --all-features
cargo doc --locked -p wora-observability-linux-mm --all-features --no-deps
```

Live-kernel tests are opt-in: `cargo test -p wora-observability-linux-mm --test live -- --ignored`.
[Fixture provenance](https://github.com/adamflott/wora/blob/master/wora-observability-linux-mm/tests/fixtures/README.md).

Read-only mapping-query evaluation (6.11+ headers; exit 77 means unsupported kernel):

```sh
cc -std=c11 -Wall -Wextra -Werror tools/procmap-query.c -o /tmp/procmap-query
/tmp/procmap-query
```

The probe caps calls/names and requests no build IDs. It is not a production
backend; maps/smaps remain the supported mapping/accounting readers.
