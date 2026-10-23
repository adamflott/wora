Recorded on 2026-10-15 from the dedicated, booted Debian 13 Lima VMs:

| File | Guest | Kernel | Base page size |
| --- | --- | --- | --- |
| `arm64-buddyinfo` | `mm-debian13` | `6.12.95+deb13-cloud-arm64` | 4096 bytes |
| `x86_64-buddyinfo` | `mm-debian13-x86_64` | `6.12.95+deb13-amd64` | 4096 bytes |
| `arm64-zoneinfo` | `mm-debian13` | `6.12.95+deb13-cloud-arm64` | 4096 bytes |
| `x86_64-zoneinfo` | `mm-debian13-x86_64` | `6.12.95+deb13-amd64` | 4096 bytes |
| `arm64-swaps-empty` | `mm-debian13` | `6.12.95+deb13-cloud-arm64` | 4096 bytes |
| `x86_64-swaps-empty` | `mm-debian13-x86_64` | `6.12.95+deb13-amd64` | 4096 bytes |
| `arm64-slabinfo` | `mm-debian13` | `6.12.95+deb13-cloud-arm64` | 4096 bytes |
| `x86_64-slabinfo` | `mm-debian13-x86_64` | `6.12.95+deb13-amd64` | 4096 bytes |
| `arm64-pagetypeinfo` | `mm-debian13` | `6.12.95+deb13-cloud-arm64` | 4096 bytes |
| `x86_64-pagetypeinfo` | `mm-debian13-x86_64` | `6.12.95+deb13-amd64` | 4096 bytes |

Source: `cat /proc/buddyinfo`, `cat /proc/zoneinfo` and `cat /proc/swaps` as the unprivileged guest user. Trailing spaces
were removed; counts and row order were preserved. These are single captures
of changing gauges, not expected values for future live reads. Varied order
counts, sparse NUMA nodes and integer edge cases use synthetic inputs in tests.

`synthetic-zoneinfo` also exercises multiple sparse NUMA nodes, unknown zone
names, optional fields, newer pageset bounds and high-atomic fields. The root
reporter fixture contains the same synthetic input.

The recorded swap captures have only headers: neither VM had active swap.
`synthetic-swaps` exercises devices, escaped file names, unfamiliar types and
signed priorities. VM lifecycle validation saves additional populated captures
under `target/vm-validation*/swaps-active-proc/swaps` after enabling temporary
guest-only swap files, then verifies cleanup. The root reporter fixture contains
the same synthetic swap input.

Slabinfo captures used `sudo cat /proc/slabinfo` because both VMs restrict reads
to root. Each is a complete SLUB version 2.1 table; counts change over time.
`synthetic-slabinfo` covers zero-object caches, large integers and unknown names.
`slabinfo-statistics` is a synthetic legacy SLAB statistics table based on the
Linux 6.1 formatter, not a live capture. Copies of both synthetic inputs support
the root reporter tests.

Pagetypeinfo captures used `sudo cat /proc/pagetypeinfo`; trailing spaces were
removed. The ARM64 kernel includes CMA migration columns, while the x86_64
kernel does not. Neither capture contains capped values or mixed blocks.
`synthetic-pagetypeinfo` covers sparse nodes, varying order widths, large
integers, unknown names, reordered columns, capped counts and optional mixed
tables. The root reporter uses a copy of this synthetic input.

The `process-information-{status,stat,limits,numa_maps}` files are separate native
file values extracted from the normal-user memory diagnostics smoke capture on
the local Linux Docker VM on 2026-10-16. Source whitespace, units, and counters
are retained. They test parser behavior rather than expected values for a live
process; process memory can change between individual file reads.
