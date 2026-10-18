# wora

Write Once Run Anywhere: a Rust async framework for daemon applications running
under Unix processes, service managers and containers. Linux and macOS are the
primary targets; embedded and `no_std` environments are unsupported.

WORA aims for deterministic memory and CPU usage. Collection budgets, shared
snapshots and capped reports provide partial bounds; a whole-runtime guarantee
is still [planned](wora-observability-linux-mm/TODO.md).

## Features

- Application lifecycle, restart policy, readiness, health and graceful shutdown.
- Typed initial config/secrets and filesystem-driven reloads.
- Configurable Unix signal mapping and executor-provided runtime events.
- Host/process metrics, memory snapshots and tracing events routed to sinks.
- Physical or in-memory filesystems and lock backends.

The crate is early-stage. Built-in integrations include `SystemdExecutor`,
`LaunchdExecutor` and `ContainerExecutor`; availability depends on the target.

## Quick start

Requires Rust 1.95+. Repository builds pin 1.95.0 in `rust-toolchain.toml`.

```sh
cargo run --example basic
cargo run --example async_daemon -- --run-mode user
cargo run --example memory_report
```

`memory_report` writes JSON at startup and every five seconds. Stop it with
Ctrl-C or SIGTERM. See the [memory guide](docs/memory.md) for sampling, Linux
subsystems, kernel compatibility, limits and measurements.

Other examples: `onboarding`, Linux `systemd_daemon` and
[its unit file](examples/systemd_daemon.service).

## Runtime model

| API | Role |
| --- | --- |
| `App` | Workload setup, main, reload and teardown hooks. |
| `AsyncExecutor` | Environment directories, setup, readiness and teardown. |
| `Wora` | Runtime context, events, health/readiness and memory cache. |
| `WFS` | Filesystem and recursive watch abstraction. |
| `LockBackend` | Single-instance coordination; filesystem and in-memory implementations. |
| `O11yProcessor` | Observability sink fan-out. |

`exec_async_runner_with_options` adds `RunnerOptions` for restart/supervision,
signal mapping and injected providers. The runner acquires its lock, initializes
the executor/config/secrets/app, starts watchers, runs the workload and tears down.
Runner-owned background tasks receive cancellation and a bounded shutdown grace
period; remaining async tasks are aborted.

Executors must create both metadata and secrets watch roots, including for
`NoConfig`/`NoSecrets` apps. Initial loading tolerates missing roots, but recursive
watch installation requires them. Lock contention returns
`MainEarlyReturn::AlreadyRunning`.

`Wora::run_event_loop` can apply typed reloads from `ConfigChanged`/`SecretChanged`.
Reload, suspend and log-rotation control requests are delivered to the app for
handling. Use `RunnerOptions::with_signal_mapper` to customize signal events.

## Development

```sh
cargo fmt --all --check
cargo test --workspace --all-features
cargo clippy --workspace --all-targets --all-features
cargo doc --workspace --all-features --no-deps
```

[Runtime TODO](TODO.md) · [Memory TODO](wora-observability-linux-mm/TODO.md) ·
[Linux memory readers](wora-observability-linux-mm/README.md) ·
[Metric exporters](wora-observability/README.md)

## Project

Author: [Adam Flott](https://github.com/adamflott).
[Contributors](https://github.com/adamflott/wora/contributors).
Versions before 1.0 are arbitrary; later versions use SemVer.

OpenAI Codex is used for documentation, tests and issue discovery. Changes are
reviewed by the maintainer.

Dual-licensed under [MIT](LICENSE-MIT) or [Apache 2.0](LICENSE-APACHE).
