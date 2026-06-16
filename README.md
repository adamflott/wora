# wora

Write Once Run Anywhere (WORA): A Rust framework for building applications (daemons, etc.) that run in different environments (Linux, Kubernetes, etc.). Just like Java's claim, it really doesn't run everywhere. no_std or embedded environments are not supported.

## Status

This crate is an early-stage async framework. The main public API is usable, with Unix-like platforms as the primary target and environment-specific executors for `systemd`, `launchd`, and container-style deployments.

- restart policies, shutdown supervision, and signal mapping can be applied with `RunnerOptions` through `exec_async_runner_with_options`
- initial typed config and secret loading is built into the runner
- event-driven apps can use `Wora::run_event_loop` to auto-apply typed reloads
- `RunnerOptions::with_signal_mapper(...)` lets apps map Unix-like signals to control or app-defined events
- observability can be routed through sinks via `O11yProcessor`

## Feature Tour

- abstracts over common boilerplate with an API
- execute the same code in different executors (with or without recompiling)
- translates platform-specific control inputs into runtime control or app-defined events
- watches existing metadata and secrets directories and emits typed reload events
- supervises readiness, health, and graceful shutdown
- exposes host, process, runtime, and tracing observability events
- ships observability sinks and a processor for fan-out

## Supported Environments

- async-based apps
- Unix-like environments
  - Linux-specific host details through `procfs`
  - macOS host details through `sysinfo`
- service-manager and orchestration integrations
  - `SystemdExecutor`
  - `LaunchdExecutor`
  - `ContainerExecutor`

## Architecture

WORA has five main pieces:

- `App`: the workload lifecycle trait. Applications implement `setup`, `main`, `reload_config`, `reload_secrets`, and `end`.
- `AsyncExecutor`: the environment adapter. Executors provide directories, setup, readiness, and teardown behavior.
- `Wora`: the runtime context passed to apps. It carries directories, host data, event channels, runtime status, and observability options. Health and readiness are reported through `Wora::report_health(...)`, `Wora::report_readiness(...)`, or the cloned `RuntimeStatusHandle`.
- `WFS`: the virtual filesystem abstraction. `PhysicalVFS` is the host filesystem implementation.
- `LockBackend`: the single-instance coordination abstraction. `ProcLockBackend` uses host lock files and `InMemoryLockBackend` is useful for tests and fully virtual runner flows.
- `O11yProcessor`: the observability pipeline for fan-out into sinks.

`exec_async_runner` wires those pieces together by creating a lock file, initializing observability, building the `Wora` context, running executor and app setup, loading initial config and secrets, watching the metadata and secrets directories, invoking `App::main`, supervising shutdown/readiness/health, and then running teardown.

The runner always installs recursive watchers on both `metadata_root_dir` and `secrets_root_dir` after executor and app setup. Executors should create both watch roots even when an app uses `NoConfig` or `NoSecrets`; initial loading skips missing config/secrets directories, but watcher installation expects those directories to exist.

Typed config and secret reloads are driven by filesystem watcher events (`Event::ConfigChanged` and `Event::SecretChanged`). `ControlEvent::ReloadConfiguration`, `ControlEvent::Suspend`, and `ControlEvent::LogRotation` are app-level control notifications: the runner delivers them to the app but does not automatically reload files, suspend work, or rotate sinks for the app. Use `RunnerOptions::with_signal_mapper(...)` when the default Unix-like signal mapping should produce app-defined events instead.

When a lock backend reports contention, the runner returns `MainEarlyReturn::AlreadyRunning(...)` instead of collapsing that case into a generic exit code.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Prerequisites

* Rust 1.95 or newer. The crate declares `rust-version = "1.95"` and the checked-in `rust-toolchain.toml` pins `1.95.0` for repository builds.

## Usage

Run the basic example:

```sh
cargo run --example basic
```

Run the daemon-style event loop example:

```sh
cargo run --example async_daemon -- --run-mode user
```

Run the Linux `systemd` daemon example:

```sh
cargo run --example systemd_daemon
```

Sample unit file:

```text
examples/systemd_daemon.service
```

Run the onboarding example:

```sh
cargo run --example onboarding
```

Run checks:

```sh
cargo fmt --check
cargo test
```

## AI Disclosure

OpenAI Codex is used to write documentation/tests and find issues. Everything is reviewed by me and I vouch for these AI-generated changes.

## Versioning

We use [SemVer](http://semver.org/) for versioning when x.y.z when x is > 0, otherwise versioning is arbitrary. For the versions available, see the [tags on this repository](https://github.com/adamflott/wora/tags). 

## Authors

* **Adam Flott** - *Initial work* - [adamflott](https://github.com/adamflott)

See also the list of [contributors](https://github.com/adamflott/wora/contributors) who participated in this project.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details

## Acknowledgments
