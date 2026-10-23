use std::collections::BTreeSet;
use std::io::BufRead;

use procfs_core::{FromBufRead, ProcError, ProcResult};

/// A snapshot of the buddy allocator's free-block inventory in `/proc/buddyinfo`.
///
/// Implements procfs's parsing traits on all platforms and its `Current` trait
/// on Linux. Every zone in a parsed file has the same nonzero number of orders;
/// the number itself is discovered from the input rather than fixed here.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct BuddyInfo {
    /// Node/zone rows in the order they appeared in the source.
    pub zones: Vec<BuddyZone>,
}

/// Free blocks for one NUMA node and memory zone.
///
/// One row of `/proc/buddyinfo`, parsed within [`BuddyInfo`]. All free-block
/// counts retain this node/zone scope; byte conversion requires base page size.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct BuddyZone {
    /// Kernel NUMA node identifier; nodes need not be contiguous.
    pub node: u32,
    /// Kernel zone name, preserved verbatim, including previously unknown names.
    pub zone: String,
    /// Current free-block counts, indexed by allocation order starting at zero.
    ///
    /// These are gauges, not cumulative events. A block at order `n` contains
    /// `2^n` base pages. The source contains no page-size information.
    pub free_blocks: Vec<u64>,
}

fn invalid(line: usize, reason: impl std::fmt::Display) -> ProcError {
    ProcError::Other(format!("buddyinfo line {line}: {reason}"))
}

fn unsigned(raw: &str, line: usize, field: &str) -> ProcResult<u64> {
    if raw.is_empty() || !raw.bytes().all(|b| b.is_ascii_digit()) {
        return Err(invalid(line, format!("invalid unsigned {field}: {raw:?}")));
    }
    raw.parse().map_err(|_| invalid(line, format!("{field} exceeds u64: {raw:?}")))
}

impl FromBufRead for BuddyInfo {
    fn from_buf_read<R: BufRead>(reader: R) -> ProcResult<Self> {
        let mut zones = Vec::new();
        let mut identities = BTreeSet::new();
        let mut order_count = None;
        for (offset, line) in reader.lines().enumerate() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let line_number = offset + 1;
            let mut fields = line.split_ascii_whitespace();
            if fields.next() != Some("Node") {
                return Err(invalid(line_number, "expected Node"));
            }
            let node_text = fields
                .next()
                .and_then(|raw| raw.strip_suffix(','))
                .ok_or_else(|| invalid(line_number, "expected a node ID followed by a comma"))?;
            let node = u32::try_from(unsigned(node_text, line_number, "node ID")?).map_err(|_| invalid(line_number, "node ID exceeds u32"))?;
            if fields.next() != Some("zone") {
                return Err(invalid(line_number, "expected zone"));
            }
            let zone = fields.next().ok_or_else(|| invalid(line_number, "missing zone name"))?.to_owned();
            let free_blocks = fields
                .enumerate()
                .map(|(order, raw)| unsigned(raw, line_number, &format!("order {order} count")))
                .collect::<ProcResult<Vec<_>>>()?;
            if free_blocks.is_empty() {
                return Err(invalid(line_number, "missing free-block counts"));
            }
            if let Some(expected) = order_count
                && expected != free_blocks.len()
            {
                return Err(invalid(line_number, format!("expected {expected} orders, got {}", free_blocks.len())));
            }
            order_count = Some(free_blocks.len());
            if !identities.insert((node, zone.clone())) {
                return Err(invalid(line_number, "duplicate node/zone row"));
            }
            zones.push(BuddyZone { node, zone, free_blocks });
        }
        if zones.is_empty() {
            return Err(ProcError::Other("buddyinfo: empty table".into()));
        }
        Ok(Self { zones })
    }
}

#[cfg(target_os = "linux")]
impl procfs::Current for BuddyInfo {
    const PATH: &'static str = "/proc/buddyinfo";
}
