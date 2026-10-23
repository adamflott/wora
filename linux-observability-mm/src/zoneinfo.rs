use std::collections::{BTreeMap, BTreeSet};
use std::io::BufRead;

use procfs_core::{FromBufRead, ProcError, ProcResult};

/// A snapshot of `/proc/zoneinfo`, including empty zones.
///
/// Node statistics are stored once per NUMA node, independently of the zone
/// underneath which the kernel prints them. Parsing supports the Linux 4.19
/// and later layouts, including optional fields added by newer kernels.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct ZoneInfo {
    /// Node statistics keyed by kernel NUMA node ID. A node without a populated
    /// zone may have no statistics block and therefore no entry here.
    pub nodes: BTreeMap<u32, BTreeMap<String, u64>>,
    /// Zones in source order; names and node IDs need not be contiguous or known.
    pub zones: Vec<ZoneInfoZone>,
}

/// Memory inventory, allocation thresholds and statistics for one zone.
///
/// Parsed from one node/zone section of `/proc/zoneinfo` by [`ZoneInfo`].
/// All inventory and protection fields retain that section's base-page scope.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct ZoneInfoZone {
    /// Kernel NUMA node ID.
    pub node: u32,
    /// Kernel zone name, preserved verbatim.
    pub zone: String,
    /// Free base pages at the time this line was read.
    pub free_pages: u64,
    /// Minimum, low and high allocation watermarks, in base pages.
    pub watermarks: ZoneWatermarks,
    /// Pages in the physical address range, including holes.
    pub spanned_pages: u64,
    /// Physically present pages.
    pub present_pages: u64,
    /// Pages managed by the allocator.
    pub managed_pages: u64,
    /// CMA pages, when reported by the kernel.
    pub cma_pages: Option<u64>,
    /// Low-memory protection reserves, in base pages, indexed by kernel zone
    /// class index. Indices must not be inferred from the list of zone names.
    pub protection: Vec<u64>,
    /// Remaining numeric zone fields, including VM/NUMA statistics and trailing
    /// fields such as `start_pfn` and `node_unreclaimable`.
    ///
    /// Values retain their kernel units and semantics: some are gauges, others
    /// cumulative counters. They must not all be interpreted as page counts.
    pub stats: BTreeMap<String, u64>,
    /// Per-CPU pagesets, kept separate from zone watermarks and statistics.
    pub pagesets: Vec<ZonePageSet>,
}

/// Allocation watermarks in base pages. These already include any boost as
/// printed by the kernel; consumers should not add `boost_pages` again.
///
/// Parsed from `/proc/zoneinfo` zone fields `min`, `low`, `high` and optional
/// `boost`/`promo` by [`ZoneInfo`]. All values are base-page counts.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct ZoneWatermarks {
    /// Minimum watermark.
    pub min_pages: u64,
    /// Low watermark.
    pub low_pages: u64,
    /// High watermark.
    pub high_pages: u64,
    /// Watermark boost, absent on older kernels.
    pub boost_pages: Option<u64>,
    /// Promotion watermark, absent on older kernels.
    pub promo_pages: Option<u64>,
}

/// Raw per-CPU pageset fields. These are parsed for library consumers but the
/// reporter does not export per-CPU series by default.
///
/// Parsed from a zone's `pagesets` / `cpu:` section in `/proc/zoneinfo`.
/// These numeric values are retained in kernel units; they are not the zone
/// watermarks or an independently sampled per-process view.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct ZonePageSet {
    /// Kernel CPU ID; CPU IDs may be sparse.
    pub cpu: u32,
    /// Numeric fields including `count`, `high`, `batch`, and (on SMP kernels)
    /// `vm stats threshold`. New fields such as `high_min` are preserved.
    pub stats: BTreeMap<String, u64>,
}

fn invalid(line: usize, reason: impl std::fmt::Display) -> ProcError {
    ProcError::Other(format!("zoneinfo line {line}: {reason}"))
}

fn unsigned(raw: &str, line: usize) -> ProcResult<u64> {
    if raw.is_empty() || !raw.bytes().all(|b| b.is_ascii_digit()) {
        return Err(invalid(line, format!("invalid unsigned value: {raw:?}")));
    }
    raw.parse().map_err(|_| invalid(line, "value exceeds u64"))
}

fn insert(map: &mut BTreeMap<String, u64>, key: &str, value: u64, line: usize) -> ProcResult<()> {
    if map.insert(key.into(), value).is_some() {
        return Err(invalid(line, format!("duplicate field {key:?}")));
    }
    Ok(())
}

struct ZoneBuilder {
    node: u32,
    zone: String,
    line: usize,
    fields: BTreeMap<String, u64>,
    protection: Option<Vec<u64>>,
    pagesets: Vec<ZonePageSet>,
}

impl ZoneBuilder {
    fn finish(mut self) -> ProcResult<ZoneInfoZone> {
        let mut required = |key: &str| self.fields.remove(key).ok_or_else(|| invalid(self.line, format!("missing {key:?}")));
        let free_pages = required("free")?;
        let min_pages = required("min")?;
        let low_pages = required("low")?;
        let high_pages = required("high")?;
        let spanned_pages = required("spanned")?;
        let present_pages = required("present")?;
        let managed_pages = required("managed")?;
        for pageset in &self.pagesets {
            for key in ["count", "high", "batch"] {
                if !pageset.stats.contains_key(key) {
                    return Err(invalid(self.line, format!("CPU {} missing {key}", pageset.cpu)));
                }
            }
        }
        Ok(ZoneInfoZone {
            node: self.node,
            zone: self.zone,
            free_pages,
            watermarks: ZoneWatermarks {
                min_pages,
                low_pages,
                high_pages,
                boost_pages: self.fields.remove("boost"),
                promo_pages: self.fields.remove("promo"),
            },
            spanned_pages,
            present_pages,
            managed_pages,
            cma_pages: self.fields.remove("cma"),
            protection: self.protection.ok_or_else(|| invalid(self.line, "missing protection"))?,
            stats: self.fields,
            pagesets: self.pagesets,
        })
    }
}

impl FromBufRead for ZoneInfo {
    fn from_buf_read<R: BufRead>(reader: R) -> ProcResult<Self> {
        let mut info = Self {
            nodes: BTreeMap::new(),
            zones: Vec::new(),
        };
        let mut current: Option<ZoneBuilder> = None;
        let mut identities = BTreeSet::new();
        let mut node_stats = false;
        let mut pagesets = false;
        for (offset, raw) in reader.lines().enumerate() {
            let raw = raw?;
            let text = raw.trim();
            let line = offset + 1;
            if text.is_empty() {
                continue;
            }
            if text.starts_with("Node ") || text.starts_with("Node\t") {
                if let Some(zone) = current.take() {
                    info.zones.push(zone.finish()?);
                }
                let fields: Vec<_> = text.split_ascii_whitespace().collect();
                if fields.len() != 4 || fields[0] != "Node" || fields[2] != "zone" {
                    return Err(invalid(line, "expected Node N, zone NAME"));
                }
                let node = fields[1].strip_suffix(',').ok_or_else(|| invalid(line, "missing node comma"))?;
                let node = u32::try_from(unsigned(node, line)?).map_err(|_| invalid(line, "node ID exceeds u32"))?;
                if !identities.insert((node, fields[3].to_owned())) {
                    return Err(invalid(line, "duplicate node/zone"));
                }
                current = Some(ZoneBuilder {
                    node,
                    zone: fields[3].into(),
                    line,
                    fields: BTreeMap::new(),
                    protection: None,
                    pagesets: Vec::new(),
                });
                node_stats = false;
                pagesets = false;
                continue;
            }
            let zone = current.as_mut().ok_or_else(|| invalid(line, "field before zone header"))?;
            if text == "per-node stats" {
                if info.nodes.insert(zone.node, BTreeMap::new()).is_some() {
                    return Err(invalid(line, "duplicate per-node statistics block"));
                }
                node_stats = true;
                pagesets = false;
            } else if text == "pagesets" {
                if pagesets || !zone.pagesets.is_empty() {
                    return Err(invalid(line, "duplicate pagesets block"));
                }
                pagesets = true;
                node_stats = false;
            } else if let Some(raw_values) = text.strip_prefix("protection:") {
                let values = raw_values
                    .trim()
                    .strip_prefix('(')
                    .and_then(|s| s.strip_suffix(')'))
                    .ok_or_else(|| invalid(line, "expected parenthesized protection reserves"))?;
                let values = values.split(',').map(|s| unsigned(s.trim(), line)).collect::<ProcResult<Vec<_>>>()?;
                if zone.protection.replace(values).is_some() {
                    return Err(invalid(line, "duplicate protection"));
                }
                node_stats = false;
            } else if let Some(cpu) = text.strip_prefix("cpu:") {
                if !pagesets {
                    return Err(invalid(line, "CPU outside pagesets"));
                }
                let cpu = u32::try_from(unsigned(cpu.trim(), line)?).map_err(|_| invalid(line, "CPU ID exceeds u32"))?;
                if zone.pagesets.iter().any(|p| p.cpu == cpu) {
                    return Err(invalid(line, "duplicate CPU"));
                }
                zone.pagesets.push(ZonePageSet { cpu, stats: BTreeMap::new() });
            } else if let Some((key, value)) = text.split_once(':') {
                // The threshold is printed at zone indentation despite belonging
                // to the preceding CPU. Other pageset fields are more indented.
                let cpu_field = key == "vm stats threshold" || raw.len() - raw.trim_start().len() > 4;
                let value = unsigned(value.trim(), line)?;
                if pagesets && cpu_field {
                    let cpu = zone.pagesets.last_mut().ok_or_else(|| invalid(line, "pageset field before CPU"))?;
                    insert(&mut cpu.stats, key, value, line)?;
                } else {
                    pagesets = false;
                    node_stats = false;
                    insert(&mut zone.fields, key, value, line)?;
                }
            } else {
                let fields: Vec<_> = text.split_ascii_whitespace().collect();
                if fields.len() == 3 && fields[..2] == ["pages", "free"] {
                    node_stats = false;
                    pagesets = false;
                    insert(&mut zone.fields, "free", unsigned(fields[2], line)?, line)?;
                } else if fields.len() == 2 && fields[0].bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
                    let value = unsigned(fields[1], line)?;
                    if node_stats {
                        let stats = info.nodes.get_mut(&zone.node).ok_or_else(|| invalid(line, "missing node block"))?;
                        insert(stats, fields[0], value, line)?;
                    } else if pagesets {
                        return Err(invalid(line, "expected a pageset field or zone trailer"));
                    } else {
                        insert(&mut zone.fields, fields[0], value, line)?;
                    }
                } else {
                    return Err(invalid(line, "expected a numeric zoneinfo field"));
                }
            }
        }
        if let Some(zone) = current {
            info.zones.push(zone.finish()?);
        }
        if info.zones.is_empty() || info.nodes.values().any(BTreeMap::is_empty) {
            return Err(ProcError::Other("zoneinfo: empty table or node statistics block".into()));
        }
        Ok(info)
    }
}

#[cfg(target_os = "linux")]
impl procfs::Current for ZoneInfo {
    const PATH: &'static str = "/proc/zoneinfo";
}
