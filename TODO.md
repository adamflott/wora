# Capability Roadmap

This document tracks capabilities that would make WORA safer and more complete
for long-running daemon workloads. Each section describes the intended behavior
and a possible implementation path rather than committing the crate to a final
public API.

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
