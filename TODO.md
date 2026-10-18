# Runtime TODO

Linux reader/parser work: [reader TODO](wora-observability-linux-mm/TODO.md).
Current memory APIs and measurements: [memory guide](docs/memory.md).

## Memory and resource limits

- [ ] Complete runtime container/allocator accounting beyond budgeted Linux
  storage/headers; reserve reload and serialization overlap before allocation.
- [ ] Add a minimal selectable routine profile in `src/o11y/linux_memory.rs`;
  make large slab/page-type/zone-CPU inventories optional within that profile.
- [ ] Bound CPU/disk/network inventories before collection in `src/o11y.rs`.

## Queues, tasks and outputs

- [ ] Coalesce queued periodic samples in `src/lib.rs` / `src/o11y.rs`;
  preserve shutdown/control capacity. The memory cache/watch already keeps the latest sample.
- [ ] Bound queue bytes and pending producers, including direct public senders;
  define rejection/backpressure behavior.
- [ ] Add a byte/record-limited ring mode to `O11yMemorySink` in `src/o11y.rs`.
- [ ] Cap general sink formatting/JSON scratch buffers; reuse the memory reporter's
  approach in `src/profiles.rs`.
- [ ] Bound exporter buffers/cardinality in `wora-observability/`; define stalled
  output handling, drop/coalesce counters and rate-limited diagnostics.
- [ ] Complete cancellation and reaping for `Wora::schedule_task` in `src/lib.rs`
  across restart boundaries; count admission is already opt-in.
- [ ] Complete in-flight blocking-collector limits across shutdown/restart;
  reuse shared provider leases so timed-out workers cannot admit replacements.

## Memory validation and deployment

- [ ] Stress slow sinks, full queues, retained snapshots, task limits, blocking
  collectors and repeated reloads/restarts; assert bounded allocations and responsive shutdown.
- [ ] Measure minimal release, sampling, reporting and each diagnostic separately;
  choose budget defaults from observed allocation peaks.
- [ ] Run release-process soaks in a dedicated cgroup; record build, RSS/PSS,
  cgroup current/peak, CPU, report cadence and throughput under slow outputs.
- [ ] Obtain a reproducible baseline heap profile; resolve unknown frames and
  separate live allocations from allocator-retained memory.
- [ ] Compare glibc arena limits after snapshot changes; measure contention,
  throughput and portability before recommending `MALLOC_ARENA_MAX`.
- [ ] Add a deployment example using `memory.high`/`memory.max`; document pressure,
  OOM behavior and the difference between cgroup usage and process RSS.

## Deletion-aware reloads

Removed files are skipped today; reload hooks receive no explicit deletion payload.

- [ ] Add removed paths to `ConfigReload` and removed path/key pairs to `SecretReload`.
- [ ] Pass change kinds into loaders; treat missing paths in mixed events as removals.
- [ ] Preserve `ConfigReload::{main,supplemental}` and `SecretReload::files`;
  plan compatibility for added public fields.
- [ ] Test config/secret deletion, renames and mixed present/missing paths.

## Reload coalescing

Watcher events are currently forwarded individually.

- [ ] Add debounce and maximum-window settings to `RunnerOptions`.
- [ ] Bound watcher registrations and pending paths; aggregate/deduplicate paths
  per root using final change kinds, with a dirty-root rescan marker on overflow.
- [ ] Keep callbacks nonblocking; test bursts, rename/save patterns and continuous writes.

## Consistent initial snapshots

Startup loads files sequentially, so concurrent changes can mix generations.

- [ ] Add bounded `WFS` reads/enumeration in `src/vfs.rs`; cap config/secret file
  counts, bytes and parsed old/new reload overlap.
- [ ] Add an optional snapshot capability to `WFS`; parse only after bounded acquisition.
- [ ] Snapshot `InMemoryVFS` nodes under one read lock.
- [ ] For `PhysicalVFS`, use an explicit generation/consistency strategy where
  available; directory metadata checks alone do not guarantee atomic contents.
- [ ] Limit retries and return an error if consistency cannot be established;
  define config/secret consistency separately.
- [ ] Test concurrent mutation, retry limits and generation consistency.

## Platform boundaries

Unix modules, signals and process helpers remain exposed without complete cfg gates.

- [ ] Gate Unix modules, signal conversion, UID/GID, resource-limit and locking helpers.
- [ ] Define the portable executor/event API and conditionally export platform helpers.
- [ ] Document built-in executor availability by target.
- [ ] Add Windows compile checks and target-gated prelude/examples.
