# Linux memory reader TODO

Runtime budgets, reporting and profiling: [runtime TODO](../TODO.md).
Current behavior and measurements: [memory guide](../docs/memory.md).

## Collector and allocation limits

- [ ] Extend heap accounting to remaining typed/third-party tables, directory/path
  buffers, temporary sets/formatting and other collectors' error maps; validate allocator reserve.
- [ ] Stream cgroup membership/mount discovery in `src/cgroup.rs`; retain current
  bounded reads, deterministic mount selection and incomplete-ancestor reporting.

## Validation

- [ ] Add fixtures for already-readable newer values: 6.19 `sock_throttled`,
  7.1 `zswap_incomp`, and 7.2 per-size mTHP collapse/failure counters. [cgroups][dmem], [THP][thp]
- [ ] Add allocation-peak assertions for oversized sources, long lines, deep
  hierarchies, many keys/errors and large inventories; input-byte tests already exist.
- [ ] Add booted-kernel validation for 6.1/6.6/7.2, non-4-KiB pages and restricted
  namespaces. Test new collectors' absence, permissions, limits and supported layouts.

## Upstream references

Version research checked 2026-10-18. 7.3-rc7 is a development kernel.

[dmem]: https://github.com/torvalds/linux/blob/v7.3-rc7/Documentation/admin-guide/cgroup-v2.rst
[thp]: https://github.com/torvalds/linux/blob/v7.2/Documentation/admin-guide/mm/transhuge.rst
