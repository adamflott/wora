# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/)
and this project adheres to [Semantic Versioning](https://semver.org/).

<!-- next-header -->
## [Unreleased] - ReleaseDate

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
