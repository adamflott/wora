# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/)
and this project adheres to [Semantic Versioning](https://semver.org/).

<!-- next-header -->
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
