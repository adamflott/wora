# wora

Write Once Run Anywhere (WORA): A Rust framework for building applications (daemons, etc) that run in different environments (Linux, Kubernetes, etc). Just like Java's claim, it really doesn't run everywhere. no_std or embedded environments are not supported.

Feature Tour:

- abstracts over common boilerplate with an API
- execute the same code in different executors (with or without recompiling)

Supports:

- async based apps
- Unix-like environments

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Prerequisites

* Rust >= 1.67

## Usage

See `examples/basic.rs`

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/adamflott/wora/tags). 

## Authors

* **Adam Flott** - *Initial work* - [adamflott](https://github.com/adamflott)

See also the list of [contributors](https://github.com/adamflott/wora/contributors) who participated in this project.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details

## Acknowledgments