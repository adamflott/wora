# Runtime TODO

Memory collectors, resource budgets and profiling work live in
[wora-observability-linux-mm/TODO.md](wora-observability-linux-mm/TODO.md).

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
- [ ] Aggregate/deduplicate paths per watch root; use final observable change kinds.
- [ ] Keep callbacks nonblocking; test bursts, rename/save patterns and continuous writes.

## Consistent initial snapshots

Startup loads files sequentially, so concurrent changes can mix generations.

- [ ] Add an optional snapshot capability to `WFS`; parse only after acquisition.
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
