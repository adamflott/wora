use std::collections::{BTreeMap, BTreeSet};
use std::io::BufRead;

use procfs_core::{FromBufRead, ProcError, ProcResult};

/// A `/proc/pagetypeinfo` snapshot. The kernel repeats geometry and table
/// headers for each memory-bearing NUMA node; memoryless nodes are omitted.
///
/// Parsing is portable. Linux `Current` access commonly requires root and can
/// be expensive. With page-owner reporting active, reading the optional mixed
/// table also drains per-CPU page lists; this reader never writes the interface.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct PageTypeInfo {
    /// Per-node sections in source order.
    pub nodes: Vec<PageTypeNode>,
}

/// Free-list and pageblock inventories for one NUMA node.
///
/// Parsed from the geometry and tables for this node in `/proc/pagetypeinfo`
/// by [`PageTypeInfo`]. Geometry is in base pages; free-list and pageblock
/// counts remain separate units.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct PageTypeNode {
    /// Kernel NUMA node ID; IDs need not be contiguous.
    pub node: u32,
    /// Base-2 order of a pageblock.
    pub page_block_order: u32,
    /// Base pages per pageblock, equal to `2^page_block_order`.
    pub pages_per_block: u64,
    /// Allocation orders from the free-list header, starting at zero.
    pub orders: Vec<u32>,
    /// Migration types in pageblock-header order, including unfamiliar names.
    pub migrate_types: Vec<String>,
    /// Free-block inventory, with capped observations retained as bounds.
    pub free_blocks: Vec<PageTypeFreeList>,
    /// Pageblock inventory by zone and migration type.
    pub page_blocks: Vec<PageTypeBlockCounts>,
    /// Optional mixed-pageblock inventory. Absence means the kernel did not
    /// report this section, rather than that every mixed count was zero.
    pub mixed_blocks: Option<Vec<PageTypeBlockCounts>>,
}

/// Free buddy blocks for one zone/migration type. Despite the kernel header's
/// wording, these counts are blocks at each allocation order, not base pages.
///
/// One `/proc/pagetypeinfo` free-list row within [`PageTypeNode`]. Each
/// column counts blocks of `2^order` base pages, retaining kernel scan caps.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct PageTypeFreeList {
    /// Kernel zone name, preserved verbatim.
    pub zone: String,
    /// Kernel migration-type name, preserved verbatim.
    pub migrate_type: String,
    /// Counts aligned with the containing node's `orders`.
    pub counts: Vec<FreeBlockCount>,
}

/// An exact free-block count or an observation limited by the kernel's scan cap.
///
/// Parsed from `/proc/pagetypeinfo` free-list columns by [`PageTypeInfo`].
/// The kernel's `>N` spelling is retained as an inclusive lower bound.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde1", serde(rename_all = "snake_case"))]
pub enum FreeBlockCount {
    /// An uncapped observation.
    Exact(u64),
    /// A capped observation printed as `>N`. The kernel can stop at N, so N is
    /// an inclusive lower bound, not an exact count or a guarantee of N + 1.
    AtLeast(u64),
}

/// Exact pageblock counts for one zone. Mixed and ordinary pageblock tables
/// use the same shape; values retain their migration-type header names.
///
/// One ordinary or mixed pageblock row of `/proc/pagetypeinfo`, associated
/// with the geometry and migration-type header in [`PageTypeNode`].
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct PageTypeBlockCounts {
    /// Kernel zone name.
    pub zone: String,
    /// Current pageblock counts keyed by migration type.
    pub counts: BTreeMap<String, u64>,
}

fn invalid(line: usize, reason: impl std::fmt::Display) -> ProcError {
    ProcError::Other(format!("pagetypeinfo line {line}: {reason}"))
}

fn unsigned(raw: &str, line: usize) -> ProcResult<u64> {
    if raw.is_empty() || !raw.bytes().all(|b| b.is_ascii_digit()) {
        return Err(invalid(line, format!("invalid unsigned value: {raw:?}")));
    }
    raw.parse().map_err(|_| invalid(line, "value exceeds u64"))
}

fn identifier(raw: &str, line: usize) -> ProcResult<u32> {
    u32::try_from(unsigned(raw, line)?).map_err(|_| invalid(line, "identifier/order exceeds u32"))
}

fn types(fields: &[&str], line: usize) -> ProcResult<Vec<String>> {
    let unique: BTreeSet<_> = fields.iter().copied().collect();
    if fields.is_empty() || unique.len() != fields.len() {
        return Err(invalid(line, "missing or duplicate migration-type columns"));
    }
    Ok(fields.iter().map(|s| (*s).into()).collect())
}

enum Section {
    PagesPerBlock,
    FreeHeader,
    Free,
    Blocks,
    Mixed(Vec<String>),
}

struct NodeBuilder {
    line: usize,
    node: Option<u32>,
    page_block_order: u32,
    pages_per_block: Option<u64>,
    orders: Vec<u32>,
    migrate_types: Vec<String>,
    free: Vec<PageTypeFreeList>,
    blocks: Vec<PageTypeBlockCounts>,
    mixed: Option<Vec<PageTypeBlockCounts>>,
    section: Section,
}

impl NodeBuilder {
    fn finish(self) -> ProcResult<PageTypeNode> {
        let node = self.node.ok_or_else(|| invalid(self.line, "missing node rows"))?;
        let pages_per_block = self.pages_per_block.ok_or_else(|| invalid(self.line, "missing pages per block"))?;
        let expected_types: BTreeSet<_> = self.migrate_types.iter().map(String::as_str).collect();
        if self.orders.is_empty() || expected_types.is_empty() || self.blocks.is_empty() {
            return Err(invalid(self.line, "incomplete free-list/pageblock tables"));
        }
        let mut free_zones: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
        for row in &self.free {
            if !free_zones.entry(&row.zone).or_default().insert(&row.migrate_type) {
                return Err(invalid(self.line, "duplicate free-list row"));
            }
        }
        if free_zones.values().any(|t| *t != expected_types) {
            return Err(invalid(self.line, "incomplete or mismatched migration types in free-list rows"));
        }
        let zones: BTreeSet<_> = free_zones.keys().copied().collect();
        for rows in std::iter::once(&self.blocks).chain(self.mixed.iter()) {
            let row_zones: BTreeSet<_> = rows.iter().map(|r| r.zone.as_str()).collect();
            if row_zones != zones || rows.len() != row_zones.len() {
                return Err(invalid(self.line, "missing, duplicate or mismatched pageblock zones"));
            }
        }
        Ok(PageTypeNode {
            node,
            page_block_order: self.page_block_order,
            pages_per_block,
            orders: self.orders,
            migrate_types: self.migrate_types,
            free_blocks: self.free,
            page_blocks: self.blocks,
            mixed_blocks: self.mixed,
        })
    }

    fn row(&mut self, fields: &[&str], line: usize) -> ProcResult<()> {
        if fields.len() < 4 || fields[0] != "Node" || fields[2] != "zone" {
            return Err(invalid(line, "expected a Node N, zone row"));
        }
        let raw_node = fields[1].strip_suffix(',').ok_or_else(|| invalid(line, "missing node comma"))?;
        let node = identifier(raw_node, line)?;
        if self.node.is_some_and(|expected| expected != node) {
            return Err(invalid(line, "node changed without repeated geometry headers"));
        }
        self.node = Some(node);
        match &self.section {
            Section::Free => {
                if fields.len() < 7 || fields[4] != "type" {
                    return Err(invalid(line, "expected zone NAME, type NAME and counts"));
                }
                let zone = fields[3].strip_suffix(',').ok_or_else(|| invalid(line, "missing zone comma"))?;
                if zone.is_empty() {
                    return Err(invalid(line, "empty zone name"));
                }
                let mut values = fields[6..].iter().copied();
                let mut counts = Vec::new();
                while let Some(raw) = values.next() {
                    let count = if raw == ">" {
                        let value = values.next().ok_or_else(|| invalid(line, "missing capped count"))?;
                        FreeBlockCount::AtLeast(unsigned(value, line)?)
                    } else if let Some(value) = raw.strip_prefix('>') {
                        FreeBlockCount::AtLeast(unsigned(value, line)?)
                    } else {
                        FreeBlockCount::Exact(unsigned(raw, line)?)
                    };
                    counts.push(count);
                }
                if counts.len() != self.orders.len() {
                    return Err(invalid(line, "free counts do not match allocation orders"));
                }
                self.free.push(PageTypeFreeList {
                    zone: zone.into(),
                    migrate_type: fields[5].into(),
                    counts,
                });
            }
            Section::Blocks | Section::Mixed(_) => {
                let names = match &self.section {
                    Section::Mixed(names) => names,
                    _ => &self.migrate_types,
                };
                if fields.len() != 4 + names.len() {
                    return Err(invalid(line, "pageblock counts do not match type columns"));
                }
                let counts = names
                    .iter()
                    .cloned()
                    .zip(fields[4..].iter().map(|raw| unsigned(raw, line)))
                    .map(|(k, v)| v.map(|v| (k, v)))
                    .collect::<ProcResult<_>>()?;
                let row = PageTypeBlockCounts {
                    zone: fields[3].into(),
                    counts,
                };
                match &mut self.section {
                    Section::Mixed(_) => self.mixed.as_mut().ok_or_else(|| invalid(line, "missing mixed section"))?.push(row),
                    _ => self.blocks.push(row),
                }
            }
            _ => return Err(invalid(line, "row before required geometry/table headers")),
        }
        Ok(())
    }
}

fn append(info: &mut PageTypeInfo, builder: NodeBuilder, ids: &mut BTreeSet<u32>) -> ProcResult<()> {
    let line = builder.line;
    let node = builder.finish()?;
    if !ids.insert(node.node) {
        return Err(invalid(line, "duplicate node section"));
    }
    info.nodes.push(node);
    Ok(())
}

impl FromBufRead for PageTypeInfo {
    fn from_buf_read<R: BufRead>(reader: R) -> ProcResult<Self> {
        let mut info = Self { nodes: Vec::new() };
        let mut current: Option<NodeBuilder> = None;
        let mut ids = BTreeSet::new();
        for (offset, line) in reader.lines().enumerate() {
            let line = line?;
            let line_number = offset + 1;
            let fields: Vec<_> = line.split_ascii_whitespace().collect();
            if fields.is_empty() {
                continue;
            }
            if fields.starts_with(&["Page", "block", "order:"]) && fields.len() == 4 {
                if let Some(node) = current.take() {
                    append(&mut info, node, &mut ids)?;
                }
                current = Some(NodeBuilder {
                    line: line_number,
                    node: None,
                    page_block_order: identifier(fields[3], line_number)?,
                    pages_per_block: None,
                    orders: Vec::new(),
                    migrate_types: Vec::new(),
                    free: Vec::new(),
                    blocks: Vec::new(),
                    mixed: None,
                    section: Section::PagesPerBlock,
                });
                continue;
            }
            let node = current.as_mut().ok_or_else(|| invalid(line_number, "expected Page block order header"))?;
            if fields.starts_with(&["Pages", "per", "block:"]) && fields.len() == 4 && matches!(node.section, Section::PagesPerBlock) {
                let pages = unsigned(fields[3], line_number)?;
                if 1_u64.checked_shl(node.page_block_order) != Some(pages) {
                    return Err(invalid(line_number, "pageblock geometry is inconsistent or exceeds u64"));
                }
                node.pages_per_block = Some(pages);
                node.section = Section::FreeHeader;
            } else if fields.starts_with(&["Free", "pages", "count", "per", "migrate", "type", "at", "order"]) && matches!(node.section, Section::FreeHeader) {
                node.orders = fields[8..].iter().map(|s| identifier(s, line_number)).collect::<ProcResult<_>>()?;
                if node.orders.is_empty() || node.orders.iter().enumerate().any(|(i, v)| u32::try_from(i).ok() != Some(*v)) {
                    return Err(invalid(line_number, "orders must be contiguous starting at zero"));
                }
                node.section = Section::Free;
            } else if fields.starts_with(&["Number", "of", "blocks", "type"]) && matches!(node.section, Section::Free) {
                node.migrate_types = types(&fields[4..], line_number)?;
                node.section = Section::Blocks;
            } else if fields.starts_with(&["Number", "of", "mixed", "blocks"]) && matches!(node.section, Section::Blocks) {
                let names = types(&fields[4..], line_number)?;
                if names.iter().collect::<BTreeSet<_>>() != node.migrate_types.iter().collect::<BTreeSet<_>>() {
                    return Err(invalid(line_number, "mixed type columns differ from pageblock columns"));
                }
                node.mixed = Some(Vec::new());
                node.section = Section::Mixed(names);
            } else {
                node.row(&fields, line_number)?;
            }
        }
        if let Some(node) = current {
            append(&mut info, node, &mut ids)?;
        }
        if info.nodes.is_empty() {
            return Err(ProcError::Other("pagetypeinfo: empty table".into()));
        }
        Ok(info)
    }
}

#[cfg(target_os = "linux")]
impl procfs::Current for PageTypeInfo {
    const PATH: &'static str = "/proc/pagetypeinfo";
}
