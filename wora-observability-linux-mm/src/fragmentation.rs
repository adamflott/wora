use crate::{MemoryFiles, MemoryReadError, MemoryValue};
use std::collections::BTreeSet;
use std::path::Path;

/// Explicit per-zone/per-order fragmentation indices from existing debugfs.
#[derive(Clone, Default, Debug)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct FragmentationStats {
    /// Original `unusable_index` and `extfrag_index` files with per-file failures.
    pub files: MemoryFiles,
    /// Fraction of free memory unusable for an allocation order, in thousandths.
    pub unusable: Option<Vec<FragmentationZone>>,
    /// External-fragmentation index, including the kernel's `-1.000` sentinel.
    pub external: Option<Vec<FragmentationZone>>,
}

/// One node/zone's indices. No fixed zone set, page size or order count is assumed.
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
        let mut files = MemoryFiles::from_names(root.as_ref().join("extfrag"), &["unusable_index", "extfrag_index"]);
        let mut parse = |name: &str| match files.values.get(name) {
            Some(MemoryValue::Text(text)) => match FragmentationZone::parse_table(text) {
                Ok(value) => Some(value),
                Err(error) => {
                    files.unavailable.insert(format!("{name}.parsed"), error);
                    None
                }
            },
            Some(_) => {
                files.unavailable.insert(format!("{name}.parsed"), invalid());
                None
            }
            None => None,
        };
        let unusable = parse("unusable_index");
        let external = parse("extfrag_index");
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
    MemoryReadError::ReadFailed("malformed fragmentation table".into())
}
