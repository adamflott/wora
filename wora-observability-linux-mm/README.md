# wora-observability-linux-mm

Typed, read-only Linux memory-management readers extending `procfs` 0.18.
Supported sources are `/proc/buddyinfo`, `/proc/zoneinfo`, `/proc/swaps`,
`/proc/slabinfo` and `/proc/pagetypeinfo`.
This is an extension crate;
the published procfs dependency supplies the parsing traits and error types.

The crate also re-exports procfs's `Meminfo`, `VmStat`, `MemoryPressure`,
`SharedMemorySegments`, and `Iomem` snapshots and their nested types. WORA
includes these alongside the extension readers in its memory events and cache.

The data model has no metric names, exporters, async runtime or WORA dependency.
It can be consumed by WORA host statistics or any other application. The crate
and its dependencies are enabled only on Linux. Parsing uses
`procfs-core`, and live readers implement procfs's `Current` trait. Reads are
synchronous. Rust 1.95 or newer is required.

```rust
use wora_observability_linux_mm::{BuddyInfo, FromRead};

let info = BuddyInfo::from_read("Node 0, zone Normal 10 4 0\n".as_bytes())?;
assert_eq!(info.zones[0].node, 0);
assert_eq!(info.zones[0].zone, "Normal");
assert_eq!(info.zones[0].free_blocks, vec![10, 4, 0]);
# Ok::<(), wora_observability_linux_mm::ProcError>(())
```

`BuddyInfo::from_file(path)` reads a captured file or an alternate proc mount.
Both `FromRead` and `FromBufRead` are re-exported for convenience. On Linux:

```rust,no_run
# #[cfg(target_os = "linux")]
# fn main() -> wora_observability_linux_mm::ProcResult<()> {
use wora_observability_linux_mm::{BuddyInfo, Current};

let info = BuddyInfo::current()?;
for zone in info.zones {
    for (order, blocks) in zone.free_blocks.iter().enumerate() {
        println!("node={} zone={} order={order} blocks={blocks}", zone.node, zone.zone);
    }
}
# Ok(())
# }
# #[cfg(not(target_os = "linux"))]
# fn main() {}
```

Counts remain `u64` gauges. Index zero is allocation order zero; a block at
order `n` contains `2^n` base pages. Page size is deliberately supplied by the
consumer when converting to bytes. The reader neither assumes a 4 KiB page nor
fixes the number of allocation orders or the set of zone names.
See [the buddyinfo interface](https://man7.org/linux/man-pages/man5/proc_buddyinfo.5.html).

Blank lines are ignored. Empty input, malformed rows, duplicate node/zone pairs,
inconsistent order counts within one file, and values outside their integer
range return `ProcError`. An I/O error or malformed later row fails the entire
read instead of returning earlier rows as a successful partial result. A
syntactically valid prefix cannot be identified as truncated without external
topology information. Different files and kernels can have different numbers
of orders.

`ZoneInfo` provides the same `from_read`, `from_file` and Linux `current` APIs:

```rust,no_run
use wora_observability_linux_mm::{FromRead, ZoneInfo};

let info = ZoneInfo::from_file("captured-proc/zoneinfo")?;
for zone in info.zones {
    println!("node={} zone={} free_pages={} high_pages={}",
        zone.node, zone.zone, zone.free_pages, zone.watermarks.high_pages);
}
# Ok::<(), wora_observability_linux_mm::ProcError>(())
```

Zone inventory, watermarks and protection reserves stay in base pages, without
assuming a page size. `boost`, `promo` and `cma` are optional. Watermarks already
include the kernel's boost. Protection entries retain their kernel zone class
indices; these are not positions in the list of populated zones. Empty zones
are retained and do not require VM statistics or pagesets.

`ZoneInfo.nodes` holds each per-node statistics block once, independent of the
zone under which the kernel printed it. `ZoneInfoZone.stats` and node maps keep
native numeric values, including new fields. Their units and semantics vary:
for example, `nr_kernel_stack` is KiB, while `nr_dirtied` is cumulative. Per-CPU
fields are isolated in `ZoneInfoZone.pagesets`; a CPU's `high` never overwrites
a zone watermark. The reporter currently exports no per-CPU series.

The parser handles the Linux 4.19 layout and newer optional fields, including
`high_min`/`high_max` pageset bounds and high-atomic reserves. See the kernel's
[4.19 formatter](https://github.com/torvalds/linux/blob/v4.19/mm/vmstat.c) and
[6.12 formatter](https://github.com/torvalds/linux/blob/v6.12/mm/vmstat.c).
Malformed numbers, missing inventory/watermark/protection fields, duplicate
zones, node blocks, fields or CPUs, and read failures fail the entire read.
Core watermarks and inventory are required, but kernel-dependent statistics
are not. A syntactically complete prefix cannot prove the file was untruncated.

`Swaps` provides the same parsing and Linux `Current` APIs:

```rust
use wora_observability_linux_mm::{FromRead, Swaps, SwapType};

let swaps = Swaps::from_read(
    b"Filename Type Size Used Priority\n/swapfile file 1048576 128 -2\n".as_slice(),
)?;
let swap = &swaps.entries[0];
assert_eq!(swap.swap_type, SwapType::File);
assert_eq!(swap.size_kib, 1048576);
assert_eq!(swap.used_kib, 128);
assert_eq!(swap.priority, -2);
# Ok::<(), wora_observability_linux_mm::ProcError>(())
```

Swap sizes and usage retain the source's KiB units, independent of base page
size. Consumers converting to bytes should use checked multiplication by 1024.
Priorities are signed `i32` values. `SwapType::Other` preserves unfamiliar
types. Paths decode three-digit octal escapes once, including escaped spaces,
tabs, newlines and backslashes; backing files are never opened or canonicalized.
The current text model requires UTF-8 paths and rejects NUL bytes.

A header-only table is a valid empty snapshot. Missing headers, malformed rows,
duplicate decoded paths, malformed escapes, out-of-range integers and I/O
errors fail the entire read. The format and KiB conversion are defined by the
kernel's [swap-table formatter](https://github.com/torvalds/linux/blob/v6.12/mm/swapfile.c).

`SlabInfo` supports version 2.1 tables from SLUB and legacy SLAB, including
the optional `(statistics)` format with `globalstat` and `cpustat` sections:

```rust,no_run
use wora_observability_linux_mm::{FromRead, SlabInfo};

let info = SlabInfo::from_file("captured-proc/slabinfo")?;
for cache in info.caches {
    println!("cache={} active_objects={} object_size_bytes={}",
        cache.name, cache.active_objects, cache.object_size_bytes);
}
# Ok::<(), wora_observability_linux_mm::ProcError>(())
```

`SlabInfo::current()` on Linux commonly requires root. The reader opens the
source read-only and returns permission errors through procfs's error type.
It does not change access rights or write allocator tunables. Cache names and
all integer values are retained, including zero-object caches. Object sizes are
reported bytes, potentially including padding/overhead. Slab geometry retains
base-page counts; a page size is not assumed. SLUB's active-object estimates and
mixed allocation orders mean geometry alone does not establish exact resident
memory totals. Optional CPU hit/miss statistics are aggregated per cache,
without individual CPU IDs. Global maxima are gauges; the other legacy
statistics are cumulative counters for the cache's lifetime.

The parser requires both version and column headers. Other format versions,
bad section names/counts, duplicate cache names, malformed numbers and read
errors fail the entire read. A complete header with no cache rows is valid.
The library always reads all caches; consumers can select which to export.
Plain SLUB captures from both Debian VMs and synthetic legacy statistics are
tested. See the [2.1 formatter](https://github.com/torvalds/linux/blob/v6.12/mm/slab_common.c)
and [legacy statistics](https://github.com/torvalds/linux/blob/v6.1/mm/slab.c).

`PageTypeInfo` supports free lists, pageblock counts and the optional page-owner
mixed-pageblock table. Each NUMA node has its own geometry, allocation-order
header and migration-type columns. Sparse node IDs, unfamiliar names, repeated
headers and different order counts are supported. Counts are native block or
pageblock gauges, not event counters or byte values.

```rust
use wora_observability_linux_mm::{FreeBlockCount, FromRead, PageTypeInfo};

let input = b"Page block order: 9\nPages per block: 512\n\
Free pages count per migrate type at order 0\n\
Node 2, zone Normal, type Movable >100000\n\
Number of blocks type Movable\nNode 2, zone Normal 8\n";
let info = PageTypeInfo::from_read(input.as_slice())?;
assert_eq!(info.nodes[0].free_blocks[0].counts[0], FreeBlockCount::AtLeast(100000));
# Ok::<(), wora_observability_linux_mm::ProcError>(())
```

The kernel prints `>N` when its free-list scan reaches a cap. Because it can
stop at N, `FreeBlockCount::AtLeast(N)` preserves an inclusive lower bound;
it is not an exact count or a promise of N + 1. The threshold is parsed rather
than fixed at 100000, including whitespace-padded forms. Pageblock and mixed
counts are exact observations; absent mixed tables remain `None`.

Empty input, missing sections, inconsistent geometry or widths, duplicate
nodes/rows/columns, invalid integers and mismatched zone/type inventories fail
the entire read. A complete-looking prefix ending after a whole node cannot
be distinguished from a truncated multi-node capture without outside topology.

Live `PageTypeInfo::current()` access commonly requires root. This interface
walks allocator lists and pageblocks and is expensive; consumers should opt in
and sample infrequently. With `CONFIG_PAGE_OWNER` and page-owner reporting
active, reading the mixed table calls `drain_all_pages`, changing per-CPU free
list accounting. The reader opens the file read-only and does not enable any
kernel feature. See the [formatter and cap condition](https://github.com/torvalds/linux/blob/v6.12/mm/vmstat.c)
and [mixed-pageblock accounting](https://github.com/torvalds/linux/blob/v6.12/mm/page_owner.c).

Enable `serde1` for `Serialize` and `Deserialize` on the data types, following
procfs's feature convention. WORA enables this feature and includes these
snapshots in the Linux-only
`HostStats::linux_mm` field of its existing `O11yEventKind::HostStats` events.
Apps also receive `Event::SystemResource(SystemResourceStat::MemorySnapshot)`;
`Wora::memory_stats()` returns the latest stored sample and
`Wora::subscribe_memory_stats()` subscribes to updates. The configurable
`host_stats_interval` controls both application memory events and observability.
Each source is optional: failed reads do not suppress other host statistics.
Deserialization and public field construction do not apply the text parser's
validation; the types are plain data transfer objects.

From this repository, run:

```sh
cargo test --locked -p wora-observability-linux-mm --all-features
# Explicitly opt into a live Linux source check:
cargo test --locked -p wora-observability-linux-mm --test live -- --ignored
```

The recorded fixtures came from the Debian ARM64 and x86_64 test VMs; provenance
is in `tests/fixtures/README.md`. New `/proc` readers belong here, with reporter
metric mapping in the application crate. Future upstream adoption can replace
an extension reader without introducing an observability dependency into it.
