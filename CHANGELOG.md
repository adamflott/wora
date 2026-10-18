# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/)
and this project adheres to [Semantic Versioning](https://semver.org/).

<!-- next-header -->
## [Unreleased]

### Features

- Add structured `parsed` maps for process KSM counters and flags, sysctl numbers and vectors, NUMA tables, process memory information, zram statistics, SLUB node counts, cgroup node constraints, and hugepage policy selections.
- Name filesystem sysctl vector fields, preserve exact signed/unsigned integers and source units, and expose CPU/node ranges, bitmap words, and policy choices as structured values.

### Changes

- Limit default `sysinfo` collection to host CPU/memory and the current process's CPU, memory, and disk counters, with task enumeration disabled. Reuse samplers across refreshes and environment clones, and avoid collecting telemetry in `Host::from_parts`.
- Build refreshed host statistics from the retained CPU sampler so interval CPU values are preserved. Keep standalone `HostStats::update` as a host-only collection and document using `Host::update` for repeated CPU measurements.
- Share cached Linux diagnostic batches across host samples, memory events, and cloned snapshots using `Arc`. `LinuxMemoryStats::diagnostics` changes from `Option<Box<LinuxMemoryDiagnostics>>` to `Option<Arc<LinuxMemoryDiagnostics>>`; construct with `Arc::new` or convert with `Arc::from`, and use `Arc::make_mut` for an independent writable copy. JSON output retains its existing shape.
- Serialize memory-report and memory-trace JSON from borrowed fields, avoiding report-only snapshot clones and intermediate trace JSON values.
- Store only structured data in memory-file, sysctl, cgroup, and hugepage `parsed` maps; remove their former `values` maps and raw-value types. Parse specialized diagnostics during their bounded read without retaining source text.
- Structure smaps headers and accounting into mapping metadata, quantities, and flag lists. Report malformed or unsupported layouts under their source names in `unavailable`, without retaining raw contents or partial records. Raw-only serialized snapshots require migration.

### Tests

- Verify that host samplers retain no process tables, process sampling retains only the current PID without command/environment/task inventories, supplied host snapshots cause no telemetry collection, and CPU/disk counter history survives refreshes through cloned environments.
- Verify diagnostic sharing across provider clones, host samples and memory events, immutable retained snapshots across refreshes, release of old batches, and report JSON parity with public memory snapshots.
- Add coverage for mixed KSM counter/boolean records, filesystem sysctl layouts, integer limits, malformed data, native bases and units, captured process files, structured-only serialization, and propagation through memory events.

## [0.0.18] - 2026-10-16

### Features

- Extend Linux memory snapshots with NUMA nodes and tiers, zram/zswap accounting, KSM, process status and limits, and HugeTLB/cpuset cgroup context.
- Add opt-in process mapping, SLUB, allocation attribution, fragmentation, Multi-Gen LRU, bounded physical-page/pagemap, page-owner, kernel leak, and memory hardware/EDAC/CMA diagnostics.
- Add `LinuxMemoryOptions` and `LinuxMemoryEnvironment` for selecting diagnostics around an existing telemetry provider, plus `MemoryReporter::with_linux_memory_diagnostics(...)` and `LinuxMemoryOptions::all_snapshots()` for profile configuration. Physical-page and pagemap scans require explicit bounded ranges.
- Add managed `O11yEventKind::MemoryTrace` events for OOM victims, reclaim timing, compaction, and migration using an explicitly enabled private tracefs instance. Preserve emitting task and victim identities separately, report unsupported events and buffer loss, and flush trace records as additional JSON lines from the reporting profile.
- Add a configurable `memory_diagnostics` example with report and diagnostic intervals, optional physical and virtual page ranges, dump size limits, and opt-in tracing through a supplied tracefs mount.
- Add the Linux-only `wora-observability-linux-mm` workspace crate with typed readers for buddy allocator state, zone watermarks and statistics, swap devices, slab caches, and page-type free lists, alongside existing procfs memory, VM, pressure, shared-memory, and physical-map snapshots.
- Add timestamped `MemorySnapshot` application events, `Wora::memory_stats()` for cached reads without I/O, and `Wora::subscribe_memory_stats()` for watch subscriptions. Snapshots include portable memory and swap totals on Linux and macOS, with detailed Linux data under `linux_mm`.
- Add `profiles::MemoryReporter` and a minimal `memory_report` example that print the startup snapshot and subsequent samples as JSON lines, flushing each record immediately. Default to stdout, a per-user Unix executor, and a five-second interval, with configurable name, interval, writer, executor, filesystem, and runner options.
- Capture memory-related sysctls from `/proc/sys/vm` and selected kernel and filesystem settings, preserving native text values and reporting per-key read errors and exclusions. Skip `vm.stat_refresh` and write-only action controls.
- Capture the collecting process's cgroup v2 memory usage, limits, swap/zswap, counters, events, NUMA accounting, and pressure, plus visible ancestor usage and limits. Preserve unlimited values as `"max"` and report discovery failures explicitly.
- Capture transparent hugepage policies and per-size statistics, plus global and per-node HugeTLB pools, preserving native units and policy selections while excluding demotion actions and write-only controls.
- Add process `smaps_rollup` snapshots with the collecting PID, RSS, PSS, and kernel-dependent fields, retaining source units and providing checked byte conversion.

### Changes

- Seed the memory cache before application setup and refresh it at the configurable host-statistics interval, including live interval changes. Run periodic collection on a blocking worker and retain the previous sample after failed host refreshes.
- Keep memory cache and watch updates independent of application event queue backpressure; skip periodic application notifications when the queue is full.
- Sample selected diagnostics separately from routine host reports, defaulting to a 60-second interval, and retain their own `sampled_at` timestamp while cached data is reused. Expensive diagnostics remain disabled by default, including when running as root.
- Bound diagnostic file reads, directory enumeration, page ranges, and trace parser state. Report oversized files and short binary reads as failures instead of returning successful partial accounting; retain native units, unknown fields, and allocation accuracy markers.
- Supervise the memory trace service through the existing observability lifecycle and disable/remove its private instance on normal shutdown or cancellation, with setup and read failures reported as infrastructure failures.
- Update crate dependencies and simplify example event-loop error handling.

### Fixes

- Identify the operating system independently of the Linux distribution ID so supported distributions such as Debian can initialize host statistics.
- Preserve unprefixed hexadecimal memory-block sizes, physical indices, and CPU masks instead of treating digit-only forms as decimal scalars, and expose a checked byte value for memory-block size.
- Reject malformed fragmentation indices and clear pending trace duration matches after lost or unrecognized records. Keep zero or redacted pagemap PFNs distinct from usable physical-frame identities.

### Tests

- Add parser and fixture coverage for Linux memory sources, malformed input, numeric limits, sparse nodes, namespace and mount discovery, unavailable files, and excluded controls, plus opt-in live-reader tests.
- Add runtime coverage for memory caching, subscriptions, application events, interval updates, failed refreshes, full event queues, profile output and writer failures, graceful shutdown, and complete Linux snapshot serialization.
- Extend fixture coverage to diagnostic collectors, SLUB alias resolution, versioned allocation records, native hexadecimal formats, bounded binary page reads, and exclusion of hardware and allocator action controls.
- Verify diagnostic selection and independent caching/timestamps, serialization of all selected groups, trace victim identity and matched durations, loss notifications, nonblocking stream cancellation, setup cleanup, and immediate trace JSON output.

### Documentation

- Document the memory cache and event APIs, reporting profile, Linux reader types, source units, captured-file inputs, access requirements, and collection costs.
- Record normal-user sysctl access checks and distinguish privileged readable settings from excluded action controls.
- Document diagnostic selection and example options, explicit page ranges, dump bounds, overlapping accounting scopes, kernel configuration requirements, and independent sampling intervals.
- Describe private tracefs ownership, event loss, and cleanup limits after abrupt process termination, plus the collection cost of process mappings, page-owner data, and kernel leak findings.

## [0.0.17] - 2026-08-28

### Features

- Convert the repository into a Cargo workspace and add the `wora-observability` crate with optional Prometheus, Prometheus HTTP, OpenTelemetry, and OTLP support.
- Add `O11yPipeline` as the high-level observability API for constructing an owned event channel, named sinks, runtime settings, and managed exporter services.
- Add `O11yControlHandle` for atomically changing flush, status, and host-stat intervals at runtime; selecting sink failure policy; enabling or disabling named sinks; invoking flush, clear, and reconnect operations; and inspecting pipeline status.
- Add isolated and fail-fast sink failure policies, with sink and exporter-service lifecycle states exposed through pipeline status.
- Add a Prometheus daemon example with a managed `/metrics` service, stdout tracing, and configuration-driven runtime observability updates.

### Changes

- Track runner-owned background tasks in an internal lifecycle registry and use shared `CancellationToken` cancellation during teardown.
- Await cooperative task shutdown for a bounded grace period, abort unresponsive tasks, and report infrastructure task failures through `MainEarlyReturn::InfrastructureTask`.
- Change `exec_async_runner(...)` and `exec_async_runner_with_options(...)` to accept an owned `O11yPipeline`, allowing the runner to supervise the application, observability processor, and exporter services as one lifecycle.
- Route observability `Flush`, `Clear`, and `Reconnect` events through dedicated sink hooks, and flush sinks automatically when processing finishes.
- Update the built-in examples to use the owned pipeline API.

### Fixes

- Request graceful application shutdown when the observability processor or a managed exporter service fails, and return the failure as `MainEarlyReturn::InfrastructureTask` after teardown.
- Preserve failed sink state when using isolation mode and include backend error details in observability errors.
- Map readiness `Stopping` and `Draining` to distinct exported metric values.
- Remove the synthetic Prometheus sink-error counter; sink failures are now reported by the pipeline control plane.

### Tests

- Add task-lifecycle coverage for cooperative cancellation, post-shutdown emission prevention, forced abort fallback, and infrastructure failure reporting.
- Add runtime coverage for pipeline validation, atomic settings updates, named sink controls, sink isolation, lifecycle status, and managed exporter-service failures.
- Add behavioral tests for Prometheus metric updates and OpenTelemetry metric names and attributes.

### Documentation

- Document the exporter feature flags, managed Prometheus example, runtime configuration, control handle, metric names, and stable state mappings.
- Update the exporter design plan and existing examples for the final pipeline and builder APIs.

### CI / Tooling

- Remove PowerPC, PowerPC64, and s390x targets from the cross-build matrix.

## [0.0.16] - 2026-08-25

### Changes

- Scope system executor directories to the application name under `/var/log`, `/etc`, `/usr/local/data`, `/run`, and `/var/cache`.
- Give the bare Unix executor an app-scoped root with separate log, metadata, data, runtime, cache, and secrets directories to prevent filesystem watcher cross-talk.
- Make the built-in system and bare Unix executors create their configured directories and change to their root directory during setup.
- Report application setup failures through the new `MainEarlyReturn::ApplicationSetup` variant.
- Update crate dependencies and remove unused direct dependencies on `futures` and `opentelemetry`.

### Fixes

- Honor `UnhealthyAction::Ignore` without generating a shutdown request when application health changes to failed.
- Cancel pending readiness supervision after application main returns so teardown cannot hang while the application remains not ready, while preserving already-completed readiness errors.
- Ignore only `NotFound` errors while initially loading configuration and secrets; propagate all other virtual filesystem errors.
- Measure `RuntimeMetrics` event backlog capacity from the application event channel instead of the observability channel.
- Create missing parent directories before acquiring a filesystem-backed runtime lock.
- Generate collision-resistant lock names for concurrent executions using the process ID and an in-process nonce.
- Guarantee that runtime lock guards and lock artifacts are cleaned up on every runner exit path, including setup and supervision failures.

### Tests

- Add runtime coverage for application setup failures, readiness teardown, initial config and secret loading errors, unhealthy-action handling, app-scoped executor layouts, concurrent execution locks, and lock cleanup across failure paths.
- Add lock backend coverage for creating missing lock parent directories.

### Documentation

- Correct the README architecture count, runner setup order, executor directory requirements, and dual-license description.
- Add a project TODO document.

## [0.0.15] - 2026-07-01

### Features

- Add `RunnerOptions` as the consolidated async runner configuration API for boot-state directory, restart behavior, signal mapping, lock backend, and runtime environment injection.
- Add `exec_async_runner_with_options(...)` as the preferred runner entry point for customized runner behavior.
- Add `RuntimeSignal`, `SignalMapper`, and `default_signal_mapper(...)` for mapping Unix-like runtime signals into WORA events.
- Allow built-in Unix-like, `systemd`, `launchd`, and container executors to accept injected runtime signals for deterministic tests and custom signal mapping.

### Changes

- Simplify `exec_async_runner(...)` to run with default `RunnerOptions`.
- Replace the older specialized runner helper exports with `exec_async_runner_with_options(...)` and `RunnerOptions` in the prelude.
- Clarify that reload, suspend, and log-rotation control events are app-level notifications; typed config and secret reloads remain driven by filesystem watcher events.
- Update examples to use the simplified `exec_async_runner(...)` signature.
- Pin the repository toolchain to Rust `1.95.0`, matching the crate's declared `rust-version`.

### Fixes

- Make `WFS::dir_exists(...)` return `true` only for directories, not regular files.
- Remove blocking `futures::executor::block_on` usage from the physical filesystem watcher callback.
- Make watcher callbacks drop or skip events when the receiver channel is full or closed instead of blocking.
- Make runtime event source tests more deterministic with injected controls/signals and in-memory filesystem/lock backends.
- Harden the `systemd` notification test for Linux abstract sockets and permission-denied environments.

### Tests

- Re-enable coverage for executor runtime event sources driving control flow.
- Add coverage for mapping runtime signals to app-defined events through `RunnerOptions::with_signal_mapper(...)`.
- Update runner tests to cover the consolidated `RunnerOptions` API.

### Documentation

- Document runner signal mapping, watcher root expectations, and app-handled control events in README and Rustdoc.
- Update README prerequisites to describe the pinned Rust `1.95.0` repository toolchain.

### CI / Tooling

- Update `actions/checkout` from v6 to v7.

## [0.0.14] - 2026-06-14

### Features

- Move runtime control handling behind executor-owned event sources instead of hard-wired Unix signal handling in the runner.
- Add environment-specific executors for `systemd`, `launchd`, and container-style deployments.
- Add native `systemd` and `launchd` integration support through dedicated crates.
- Add runtime supervision for health, readiness, graceful shutdown, and draining before stopping.
- Add `drain_grace_period` along with explicit `Draining` and `Stopping` readiness phases.
- Add typed config and secret reload flows.
- Add `Wora::run_event_loop(...)` for reload-aware event-driven workloads.
- Add observability sinks plus runtime and process metrics support.
- Add a runtime environment abstraction for host and process metric collection.
- Add pluggable lock backends and in-memory locking support.
- Add a fully high-level virtual filesystem API with an in-memory implementation and watcher support.
- Add explicit boot-state tracking with file markers instead of directory markers.
- Add typed config and secret change events decoupled from raw `notify::Event`.

### Changes

- Clean up the public event model around `Event::Control(ControlEvent)`.
- Rename non-control event variants to `ConfigChanged`, `SecretChanged`, and `LeadershipChanged`.
- Remove `App::is_healthy()` and standardize supervision on `Wora` and `RuntimeStatusHandle`.
- Replace the `users` crate with `nix`-based user and group lookup plus privilege switching.

### Fixes

- Replace hard-coded lock-contention exit handling with structured `AlreadyRunning(...)` results.
- Deregister in-memory VFS watchers on drop.
- Flush buffered JSON-lines observability output on shutdown and channel close.
- Reuse host and process metric samplers instead of rebuilding them repeatedly.
- Fix timing-sensitive injected-control runtime tests.
- Fix parallel test collisions by giving injected-control test apps distinct runtime names.
- Replace the unsupported and security-problematic `users` dependency.

### Tests

- Expand runtime coverage for injected executor control events, in-memory VFS watcher delivery, in-memory lock backends, boot marker behavior, draining before stopping, container readiness withdrawal during draining, typed reload handling, runtime environment injection, and observability sink behavior.

### Documentation

- Add an onboarding example.
- Add a `systemd` daemon example.
- Add a sample `systemd` unit file.
- Add an exporter design document for Prometheus and OpenTelemetry integration.
- Update README and rustdocs to reflect the newer executor, supervision, reload, and observability model.

### CI / Tooling

- Add Dependabot configuration for Cargo and GitHub Actions.
- Update GitHub Actions dependencies.
- Add release profile tuning in `Cargo.toml`.

## [0.0.13] - 2026-05-23

### Fixes

- Add Cargo.lock
- Correct OpenAI product name and add a missing MIT license file
 
## [0.0.12] - 2026-05-23

### Features

- Add initial application configuration loading through `App::configure`.
- Add configurable restart handling with `RestartPolicyOptions`.
- Add retry limits and capped exponential backoff support for restart policies.
- Add disk collection to `HostStats`.
- Add `Host::update` and `HostStats::update`.

### Fixes

- Remove `unwrap()`/`expect()` usage from crate and examples.
- Use `create_dir_all` for virtual filesystem directory creation.
- Fix Linux build issues with updated `sysinfo` usage.
- Implement `Display` for `Event<T>` instead of directly implementing `ToString`.
- Clean up unnecessary clones and example code.

### Tests

- Add runtime integration tests for recursive directory creation, initial config loading, restart policy behavior, retry limits, capped backoff, and immediate exit behavior.

### CI / Tooling

- Add lints forbidding unsafe code and denying `unwrap`/`expect`.
- Expand CI with docs, rustfmt, macOS, and cross-target Linux builds.
- Remove unsupported Windows CI jobs.

### Documentation

- Add Rustdoc coverage across the public API.
- Expand README with architecture, usage, status, and AI disclosure.
- Add `AGENTS.md` with project guidance for future agents.
 
## [0.0.11] - 2026-05-23

### Features

- Started keeping a changelog
- Add `is_first_boot` option to `setup()`
 
### Fixes

- Drop group privileges before user privileges to avoid a security issue
- Use tokio fs operation for lock file removing in `exec_async_runner()`
- Don't identify MacOS as a Linux
- Bump and don't dep versions
- Remove unused nixshell
