# wora

Write Once Run Anywhere (WORA): A Rust framework for building applications (daemons, etc.) that run in different environments (Linux, Kubernetes, etc.). Just like Java's claim, it really doesn't run everywhere. no_std or embedded environments are not supported.

## Status

This crate is an early-stage async framework. The main public API is usable, with Unix-like platforms as the primary target.

- restart policies can be applied by `exec_async_runner_with_restart_options`
- initial metadata config can be loaded through `App::configure`
- integration tests cover basic runtime wiring

## Feature Tour

- abstracts over common boilerplate with an API
- execute the same code in different executors (with or without recompiling)
- forwards Unix signals into the application event channel
- watches metadata/config directories and emits configuration-change events
- exposes host information and basic observability events

## Supported Environments

- async-based apps
- Unix-like environments
  - Linux-specific host details through `procfs`
  - macOS host details through `sysinfo`

## Architecture

WORA has four main pieces:

- `App`: the workload lifecycle trait. Applications implement `setup`, `main`, `is_healthy`, and `end`.
- `AsyncExecutor`: the environment adapter. Executors provide directories, setup, readiness, and teardown behavior.
- `Wora`: the runtime context passed to apps. It carries directories, host data, event channels, leadership state, and observability options.
- `WFS`: the virtual filesystem abstraction. `PhysicalVFS` is the host filesystem implementation.

`exec_async_runner` wires those pieces together by creating a lock file, initializing observability, building the `Wora` context, running executor and app setup, watching the metadata directory, invoking `App::main`, and then running teardown.

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

Run checks:

```sh
cargo fmt --check
cargo test
```

## AI Disclosure

OpenAPI Codex is used to write documentation/tests and find issues. Everything is reviewed by me and I vouch for these AI-generated changes.

## Versioning

We use [SemVer](http://semver.org/) for versioning when x.y.z when x is > 0, otherwise versioning is arbitrary. For the versions available, see the [tags on this repository](https://github.com/adamflott/wora/tags). 

## Authors

* **Adam Flott** - *Initial work* - [adamflott](https://github.com/adamflott)

See also the list of [contributors](https://github.com/adamflott/wora/contributors) who participated in this project.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details

## Acknowledgments
