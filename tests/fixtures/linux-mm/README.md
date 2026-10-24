# Linux memory integration fixtures

These five inputs are retained from the former bundled Linux reader crate for
WORA's host-statistics, memory-event and JSON-reporting tests. Both the unit and
integration tests use these local files; they do not require the external
`linux-observability-mm` source tree or its test fixtures.

- `arm64-buddyinfo` was captured on 2026-10-15 from `/proc/buddyinfo` as an
  unprivileged user in the dedicated Debian 13 Lima VM `mm-debian13`, running
  `6.12.95+deb13-cloud-arm64` with 4096-byte base pages. Trailing spaces were
  removed; counts and row order were preserved. It is a historical capture of
  changing gauges.
- `synthetic-zoneinfo` covers sparse nodes, unknown zone names, optional fields,
  pagesets and exact large integers.
- `synthetic-swaps` covers devices, escaped file names, unknown types, large KiB
  values and signed priorities.
- `synthetic-slabinfo` covers empty caches, large object counts and unknown names.
- `synthetic-pagetypeinfo` covers varying order widths, sparse nodes, capped
  lower bounds, large integers and mixed-block tables.

Reader/parser tests and the full fixture collection are maintained in
[`linux-observability-mm`](https://github.com/adamflott/linux-observability-mm).
