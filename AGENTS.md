# AGENTS.md

## Project Overview

`wora` is a Rust async framework for writing daemon-style applications that can run under different environment executors. The crate exposes lifecycle traits, Unix-like executors, filesystem abstraction, runtime event handling, restart policy types, and observability helpers.

WORA aims for deterministic memory and CPU usage through configurable budgets,
predictable background work, and explicit overload behavior. Current Linux
collection/snapshot/output and opt-in runtime count limits are partial progress; do not claim a whole-runtime
heap/RSS ceiling or CPU execution-time guarantee without measurement and coverage.

Key modules and workspace crates:

- `src/lib.rs`: core `Wora` runtime context, `App` lifecycle trait, `Config` trait, task scheduling, and `exec_async_runner`.
- `src/exec.rs`: `AsyncExecutor` trait and default process hardening helpers.
- `src/exec_unix.rs`: built-in Unix-like executors.
- `src/vfs.rs`: virtual filesystem trait and physical filesystem implementation.
- `src/o11y.rs`: host information, host stats, tracing layer, and observability event types.
- `src/o11y/linux_memory.rs`: routine Linux memory sources, shared tables and limits.
- `src/o11y/linux_diagnostics.rs`: opt-in diagnostics, cadence and cached batches.
- `src/o11y/generations.rs`: live-generation admission, encoded-payload accounting and header leases.
- `src/o11y/linux_trace.rs`: managed private tracefs instance and memory events.
- `src/profiles.rs`: memory reporter profile and bounded JSON output.
- `src/events.rs`: application/runtime event model.
- `src/restart_policy.rs`: restart policy and app main return action types.
- `src/resources.rs`: validated runtime count budgets and shared admission leases.
- `wora-observability/`: Prometheus and OpenTelemetry adapters.
- External `linux-observability-mm` crate: Linux memory readers/parsers, re-exported
  as `wora::o11y::linux_mm`. WORA integration fixtures live in `tests/fixtures/linux-mm/`.

## Documentation

- Keep `README.md` focused on the project aim, architecture and quick starts.
- Keep current memory APIs, sources, limits and concise measurement summaries in
  `docs/memory.md`. Put runtime budgets, reporting, reloads and profiling work in
  `TODO.md`; keep Linux reader/parser work in the external
  [reader TODO](https://github.com/adamflott/linux-observability-mm/blob/master/TODO.md).
  Write short implementation tasks.
  Remove completed/stale tasks; link instead of duplicating roadmaps or prose.
- Document every public memory type's source in Rustdoc: the upstream API and OS
  file/API, scope (host/cgroup/process/node/cache), units, conversions and derived
  formulas. Nested tables should identify their containing source. Caller-supplied
  values and captured-root readers must not be described as fresh live samples.
- Keep parser/API details in the external `linux-observability-mm` README and
  Rustdoc. Keep WORA test inputs local; do not include files from a dependency's
  source tree.

## Build and Test

Use the repository toolchain (`rust-toolchain.toml`, currently Rust 1.95.0) and
keep commands simple. Include workspace members and optional adapters:

```sh
cargo fmt --all --check
cargo test --workspace --all-features
cargo clippy --workspace --all-targets --all-features
cargo doc --workspace --all-features --no-deps
```

For examples:

```sh
cargo run --example basic
cargo run --example async_daemon -- --run-mode user
cargo run --example memory_report
```

The integration tests in `tests/runtime.rs` exercise recursive directory creation, initial config, retry limits, exponential backoff caps, and restart-policy runner behavior.
`tests/linux_mm.rs` covers Linux integration; the Linux memory reader crate has
fixture/parser/budget tests. Linux-only code is compiled out on macOS, so run its
tests and Rustdoc on Linux when changing it. Live-kernel checks are opt-in and
ignored by default; ordinary fixture tests should not require root.

## Coding Guidance

- Breaking API changes are allowed before 1.0.0; SemVer starts at 1.0.0.
- Prefer extending existing abstractions (`App`, `AsyncExecutor`, `WFS`, `O11yEvent`) over adding parallel mechanisms.
- Use `tokio` async APIs in runtime paths.
- Keep Linux-only behavior behind `#[cfg(target_os = "linux")]`; macOS support exists for host info and user-mode examples.
- Keep public API additions documented with Rustdoc.
- Preserve memory source units, exact integer precision, lower bounds and scope
  distinctions. Missing/denied/malformed data must remain distinguishable from zero.
- Keep routine snapshots shared through `Arc<LinuxMemoryData>` and diagnostics
  through `Arc<LinuxMemoryDiagnostics>`; account for old/new generation overlap.
- Keep snapshot owning Arcs private and published tables immutable. Use fallible
  budgeted builders/replacements; retain header credit while generation weak refs exist.
- Apply shared collection limits before allocation/read growth. Source bytes,
  encoded JSON bytes, heap capacity and RSS are different measurements.
- Keep expensive diagnostics explicitly selected. Snapshot collectors must not
  write action controls or obtain privileges; tracing configures only its owned
  instance. Document kernel read side effects and collection cost.
- Avoid panics and unwraps in library code; return one of the existing error enums or add a targeted error variant.
- Do not remove existing user changes in the working tree.

## Known Gaps

- Typed reload events exist; deletion-aware payloads, reload coalescing and atomic
  initial snapshots remain roadmap items in `TODO.md`.
- Heap admission covers structured storage, source/token buffers and snapshot
  headers; other typed tables, inventories, sinks, queue payloads, tasks and reloads
  remain incomplete. The allocator reserve still needs measurement. Extended
  release-process soak measurements and runtime-wide contract enforcement remain open
  in `TODO.md`; reader allocation limits remain in the external reader TODO.

## CI Notes

CI is defined in `.github/workflows/ci.yml`. It runs build, docs, and tests on
Ubuntu stable/beta/nightly and macOS stable, plus workspace/all-feature builds,
docs, and tests for i686 and aarch64 Linux on compatible hosts without QEMU or
Cross containers. It checks rustfmt separately; local workspace/all-feature
checks also cover optional adapters on macOS.
