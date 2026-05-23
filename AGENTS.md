# AGENTS.md

## Project Overview

`wora` is a Rust async framework for writing daemon-style applications that can run under different environment executors. The crate exposes lifecycle traits, Unix-like executors, filesystem abstraction, runtime event handling, restart policy types, and observability helpers.

Key modules:

- `src/lib.rs`: core `Wora` runtime context, `App` lifecycle trait, `Config` trait, task scheduling, and `exec_async_runner`.
- `src/exec.rs`: `AsyncExecutor` trait and default process hardening helpers.
- `src/exec_unix.rs`: built-in Unix-like executors.
- `src/vfs.rs`: virtual filesystem trait and physical filesystem implementation.
- `src/o11y.rs`: host information, host stats, tracing layer, and observability event types.
- `src/events.rs`: application/runtime event model.
- `src/restart_policy.rs`: restart policy and app main return action types.

## Build and Test

Use the repository toolchain and keep commands simple:

```sh
cargo fmt --check
cargo test
cargo clippy --all-targets --all-features
cargo doc --no-deps
```

For examples:

```sh
cargo run --example basic
cargo run --example async_daemon -- --run-mode user
```

The integration tests in `tests/runtime.rs` exercise recursive directory creation, initial config, retry limits, exponential backoff caps, and restart-policy runner behavior.

## Coding Guidance

- Preserve the public trait-oriented API shape unless the task explicitly asks for a breaking change.
- Prefer extending existing abstractions (`App`, `AsyncExecutor`, `WFS`, `O11yEvent`) over adding parallel mechanisms.
- Use `tokio` async APIs in runtime paths.
- Keep Linux-only behavior behind `#[cfg(target_os = "linux")]`; macOS support exists for host info and user-mode examples.
- Keep public API additions documented with Rustdoc.
- Avoid panics and unwraps in library code; return one of the existing error enums or add a targeted error variant.
- Do not remove existing user changes in the working tree.

## Known Gaps

- Runtime config-change events are still delivered as raw `notify::Event` values.
- Host filesystem stats are collected, but deeper per-process resource refresh is still limited by the current `sysinfo` snapshot model.

## CI Notes

CI is defined in `.github/workflows/ci.yml`. It runs build, docs, and tests across Ubuntu stable/beta/nightly plus macOS and Windows stable jobs, runs cross builds/tests on several Linux targets, and checks rustfmt separately.
