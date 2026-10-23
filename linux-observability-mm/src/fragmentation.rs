use crate::{MemoryFiles, MemoryReadError};
use std::collections::BTreeSet;
use std::path::Path;

/// Explicit per-zone/per-order fragmentation indices from existing debugfs.
///
/// Reads `/sys/kernel/debug/extfrag/unusable_index` and `extfrag_index`,
/// or the corresponding files beneath an explicit debugfs root. These are
/// host allocator indices, not process or cgroup memory amounts.
#[derive(Clone, Default, Debug)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct FragmentationStats {
    /// Per-file failures for `unusable_index` and `extfrag_index`.
    pub files: MemoryFiles,
    /// Fraction of free memory unusable for an allocation order, in thousandths.
    pub unusable: Option<Vec<FragmentationZone>>,
    /// External-fragmentation index, including the kernel's `-1.000` sentinel.
    pub external: Option<Vec<FragmentationZone>>,
}

/// One node/zone's indices. No fixed zone set, page size or order count is assumed.
///
/// One row of `extfrag/{unusable_index,extfrag_index}` under debugfs, parsed
/// by [`FragmentationStats`]. Each column is scaled to exact integer thousandths.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct FragmentationZone {
    /// NUMA node ID.
    pub node: u32,
    /// Native zone name.
    pub zone: String,
    /// Allocation order -> exact thousandths, avoiding floating-point rounding.
    /// `-1000` in extfrag means the allocation would succeed, not zero pressure.
    pub orders: Vec<i32>,
}

impl FragmentationStats {
    /// Read debugfs if already mounted; never mounts it or triggers compaction.
    pub fn current() -> Self {
        Self::from_debug_root("/sys/kernel/debug")
    }
    /// Read a supplied debugfs mount or captured directory.
    pub fn from_debug_root(root: impl AsRef<Path>) -> Self {
        let root = root.as_ref().join("extfrag");
        let mut files = MemoryFiles::default();
        let unusable = files.read_with(&root, "unusable_index", 4 * 1024 * 1024, FragmentationZone::parse_table);
        let external = files.read_with(&root, "extfrag_index", 4 * 1024 * 1024, FragmentationZone::parse_table);
        Self { files, unusable, external }
    }
}

impl FragmentationZone {
    /// Parse a native fragmentation table, rejecting malformed or duplicate rows.
    pub fn parse_table(text: &str) -> Result<Vec<Self>, MemoryReadError> {
        let mut rows = Vec::new();
        let mut seen = BTreeSet::new();
        for line in text.lines().filter(|v| !v.trim().is_empty()) {
            let fields: Vec<_> = line.split_ascii_whitespace().collect();
            if fields.len() < 5 || fields[0] != "Node" || fields[2] != "zone" {
                return Err(invalid());
            }
            let node = fields[1].strip_suffix(',').ok_or_else(invalid)?.parse::<u32>().map_err(|_| invalid())?;
            if !seen.insert((node, fields[3])) {
                return Err(invalid());
            }
            let orders = fields[4..]
                .iter()
                .map(|value| {
                    let (whole, fraction) = value.split_once('.').ok_or_else(invalid)?;
                    if fraction.len() != 3 || !fraction.bytes().all(|b| b.is_ascii_digit()) {
                        return Err(invalid());
                    }
                    let negative = whole.starts_with('-');
                    let digits = whole.strip_prefix('-').unwrap_or(whole);
                    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
                        return Err(invalid());
                    }
                    let whole = digits.parse::<i32>().map_err(|_| invalid())?;
                    let value = whole
                        .checked_mul(1000)
                        .and_then(|n| n.checked_add(fraction.parse().ok()?))
                        .ok_or_else(invalid)?;
                    let value = if negative { -value } else { value };
                    if !((0..=1000).contains(&value) || value == -1000) {
                        return Err(invalid());
                    }
                    Ok(value)
                })
                .collect::<Result<_, _>>()?;
            rows.push(Self {
                node,
                zone: fields[3].into(),
                orders,
            });
        }
        Ok(rows)
    }
}

fn invalid() -> MemoryReadError {
    MemoryReadError::read_failed("malformed fragmentation table")
}
