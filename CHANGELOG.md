# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/)
and this project adheres to [Semantic Versioning](https://semver.org/).

<!-- next-header -->
## [Unreleased] - ReleaseDate

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
