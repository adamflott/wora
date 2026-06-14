# wora

Write Once Run Anywhere (WORA): A Rust framework for building applications (daemons, etc.) that run in different environments (Linux, Kubernetes, etc.). Just like Java's claim, it really doesn't run everywhere. no_std or embedded environments are not supported.

## Status

This crate is an early-stage async framework. The main public API is usable, with Unix-like platforms as the primary target and environment-specific executors for `systemd`, `launchd`, and container-style deployments.

- restart policies and shutdown supervision can be applied by `exec_async_runner_with_restart_options`
- initial typed config and secret loading is built into the runner
- event-driven apps can use `Wora::run_event_loop` to auto-apply typed reloads
- observability can be routed through sinks via `O11yProcessor`

## Feature Tour

- abstracts over common boilerplate with an API
- execute the same code in different executors (with or without recompiling)
- translates platform-specific control inputs into runtime control events
- watches metadata and secrets directories and emits typed reload events
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

- `App`: the workload lifecycle trait. Applications implement `setup`, `main`, `reload_config`, `reload_secrets`, `is_healthy`, and `end`.
- `AsyncExecutor`: the environment adapter. Executors provide directories, setup, readiness, and teardown behavior.
- `Wora`: the runtime context passed to apps. It carries directories, host data, event channels, runtime status, and observability options.
- `WFS`: the virtual filesystem abstraction. `PhysicalVFS` is the host filesystem implementation.
- `O11yProcessor`: the observability pipeline for fan-out into sinks.

`exec_async_runner` wires those pieces together by creating a lock file, initializing observability, building the `Wora` context, running executor and app setup, loading initial config and secrets, watching the metadata and secrets directories, invoking `App::main`, supervising shutdown/readiness/health, and then running teardown.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Prerequisites

* Rust >= 1.95

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
