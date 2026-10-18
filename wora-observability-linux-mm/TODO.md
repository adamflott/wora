# Memory TODO

Remaining work only. Runtime tasks name files outside this crate.
Current behavior and measurements: [memory guide](../docs/memory.md).

## Collector coverage

- [ ] **DMEM:** reuse `src/cgroup.rs` discovery for region-keyed `dmem.*` byte
  usage/capacity/limits; preserve `max`. Gate 7.3-rc7 `dmem.peak` separately. [ABI][dmem]
- [ ] **NUMA policy:** extend `src/numa.rs` with weighted-interleave node weights
  (6.9) and `auto` mode (6.16), without writing policy. [ABI][weights]
- [ ] **NUMA topology:** collect access-class latency/bandwidth attributes with
  node/entry limits; label these as topology, not measured throughput. [ABI][numaperf]
- [ ] **Pagemap RWP:** update `src/pages.rs`'s bit-57 model using VMA WP/RWP context;
  preserve unknown state and public API compatibility. Test the 7.3-rc7 ABI. [ABI][pagemap]
- [ ] **Page owner:** parse bounded allocation/page/stack records in `src/provenance.rs`.
- [ ] **Kmemleak:** parse bounded suspect records in `src/provenance.rs`; retain
  uncertainty and never issue scan/clear commands.
- [ ] **MGLRU:** parse `lru_gen_full` in `src/lru.rs`; preserve cgroup/node/generation scope.
- [ ] **Shrinkers:** add opt-in `shrinker/*/count` diagnostics; cap callbacks/entries
  and exclude `scan`. [ABI][shrinkers]
- [ ] **Mapping queries:** evaluate `PROCMAP_QUERY` (6.11) for bounded/filtered
  mapping metadata; retain smaps for accounting. [UAPI][queries]
- [ ] **Page queries:** evaluate read-only `PAGEMAP_SCAN` (6.7) with explicit ranges
  and result limits; exclude write-protection flags. [UAPI][queries]

## Collector and allocation limits

- [ ] Define a WORA-owned heap contract: scratch/retained allocations, task state,
  thread stacks and reserve; specify requirements for custom providers and runtimes.
- [ ] Expose validated runtime-wide queue/task/sink/provider budgets through
  existing runner options; plan compatibility for public API changes.
- [ ] Account for container capacities, key/error-map growth and allocator overhead;
  reserve old/new snapshot, reload and serialization overlap before allocation.
- [ ] Add a minimal selectable routine profile in `../src/o11y/linux_memory.rs`;
  make large slab/page-type/zone-CPU inventories optional within that profile.
- [ ] Bound CPU/disk/network inventories before collection in `../src/o11y.rs`.
- [ ] Stream cgroup membership/mount discovery in `src/cgroup.rs`; retain current
  bounded reads, deterministic mount selection and incomplete-ancestor reporting.

## Runtime, queues and outputs

- [ ] Coalesce queued periodic samples in `../src/lib.rs` / `../src/o11y.rs`;
  preserve shutdown/control capacity. The memory cache/watch already keeps the latest sample.
- [ ] Bound queue bytes and pending producers, including direct public senders;
  define rejection/backpressure behavior.
- [ ] Add a byte/record-limited ring mode to `O11yMemorySink` in `../src/o11y.rs`.
- [ ] Cap general sink formatting/JSON scratch buffers; reuse the memory reporter's
  approach in `../src/profiles.rs`.
- [ ] Bound exporter buffers/cardinality in `../wora-observability/`; define stalled
  output handling, drop/coalesce counters and rate-limited diagnostics.
- [ ] Add admission, cancellation and reaping for `Wora::schedule_task` in
  `../src/lib.rs`, including restart boundaries.
- [ ] Limit in-flight blocking collectors across shutdown/restart; timed-out
  `spawn_blocking` work must not accumulate replacement workers.
- [ ] Add bounded `WFS` reads/enumeration in `../src/vfs.rs`; cap config/secret file
  counts, bytes and parsed old/new reload overlap.
- [ ] Bound watcher registrations and pending paths; rescan from a bounded dirty
  marker after overflow. Apply budgets to reload coalescing and snapshot acquisition.

## Validation and deployment

- [ ] Add fixtures for already-readable newer values: 6.19 `sock_throttled`,
  7.1 `zswap_incomp`, and 7.2 per-size mTHP collapse/failure counters. [cgroups][dmem], [THP][thp]
- [ ] Add allocation-peak assertions for oversized sources, long lines, deep
  hierarchies, many keys/errors and large inventories; input-byte tests already exist.
- [ ] Stress slow sinks, full queues, retained snapshots, task limits, blocking
  collectors, repeated reloads/restarts; assert bounded allocations and responsive shutdown.
- [ ] Measure minimal release, sampling, reporting and each diagnostic separately;
  choose budget defaults from observed allocation peaks.
- [ ] Run release-process soaks in a dedicated cgroup; record build, RSS/PSS,
  cgroup current/peak, CPU, report cadence and throughput under slow outputs.
- [ ] Obtain a reproducible baseline heap profile; resolve unknown frames and
  separate live allocations from allocator-retained memory.
- [ ] Compare glibc arena limits after snapshot changes; measure contention,
  throughput and portability before recommending `MALLOC_ARENA_MAX`.
- [ ] Add booted-kernel validation for 6.1/6.6/7.2, non-4-KiB pages and restricted
  namespaces. Test new collectors' absence, permissions, limits and supported layouts.
- [ ] Add a deployment example using `memory.high`/`memory.max`; document pressure,
  OOM behavior and the difference between cgroup usage and process RSS.

## Upstream references

Version research checked 2026-10-18. 7.3-rc7 is a development kernel.

[dmem]: https://github.com/torvalds/linux/blob/v7.3-rc7/Documentation/admin-guide/cgroup-v2.rst
[weights]: https://github.com/torvalds/linux/blob/v6.16/Documentation/ABI/testing/sysfs-kernel-mm-mempolicy-weighted-interleave
[numaperf]: https://docs.kernel.org/admin-guide/mm/numaperf.html
[pagemap]: https://github.com/torvalds/linux/blob/v7.3-rc7/Documentation/admin-guide/mm/pagemap.rst
[shrinkers]: https://docs.kernel.org/admin-guide/mm/shrinker_debugfs.html
[queries]: https://github.com/torvalds/linux/blob/v6.11/include/uapi/linux/fs.h
[thp]: https://github.com/torvalds/linux/blob/v7.2/Documentation/admin-guide/mm/transhuge.rst
