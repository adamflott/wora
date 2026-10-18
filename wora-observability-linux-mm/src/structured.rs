use std::collections::BTreeSet;
use std::path::Path;

use crate::MemoryReadError;
use crate::storage::CollectMemory;

/// Structured kernel data without floating-point conversion or assumed units.
///
/// Quantities are maps with `value` and `unit`. CPU/node lists are lists of
/// inclusive `{start, end}` ranges; bitmap words retain most-significant-first
/// order. Native strings and reserved/future fields remain representable.
///
/// The containing [`crate::MemoryFiles::parsed`],
/// [`crate::MemorySysctls::parsed`] or other collector map identifies the source
/// file/key; this value alone does not retain an absolute path or imply bytes.
/// Parsing caller-supplied text performs no I/O.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde1", serde(untagged))]
pub enum ParsedMemoryValue {
    /// Exact nonnegative integer, including values above i64::MAX.
    Unsigned(u64),
    /// Exact negative integer.
    Signed(i64),
    /// A documented textual boolean, such as KSM's yes/no flags.
    Bool(bool),
    /// Ordered values, ranges, flags, reserved fields or records.
    List(crate::MemoryList<Self>),
    /// Named fields, counters, quantities or node-specific values.
    Map(crate::MemoryMap<Self>),
    /// Textual enum, unit, filename, or unfamiliar value with no numeric meaning.
    Text(crate::MemoryText),
}

impl ParsedMemoryValue {
    /// Copy caller/source text after allocation admission in the active budget.
    pub fn try_text(text: impl AsRef<str>) -> Result<Self, MemoryReadError> {
        Ok(Self::Text(crate::MemoryText::try_new(text.as_ref())?))
    }

    /// Collect values with fallible capacity growth in the active budget.
    pub fn try_list(values: impl IntoIterator<Item = Self>) -> Result<Self, MemoryReadError> {
        Ok(Self::List(crate::MemoryList::try_from_iter(values)?))
    }

    /// Collect named fields with fallible key/entry admission in the active budget.
    pub fn try_map<K: AsRef<str>>(fields: impl IntoIterator<Item = (K, Self)>) -> Result<Self, MemoryReadError> {
        Ok(Self::Map(crate::MemoryMap::try_from_iter(fields)?))
    }
    /// Look up a named field in a structured record.
    pub fn get(&self, name: &str) -> Option<&Self> {
        match self {
            Self::Map(fields) => fields.get(name),
            _ => None,
        }
    }
    /// Read an unsigned integer without casting negative values.
    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Self::Unsigned(value) => Some(*value),
            _ => None,
        }
    }
    /// Read a signed integer only when it fits in i64.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Signed(value) => Some(*value),
            Self::Unsigned(value) => i64::try_from(*value).ok(),
            _ => None,
        }
    }
    /// Read a textual boolean without treating numeric bitmasks as flags.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(value) => Some(*value),
            _ => None,
        }
    }
}

/// Parse a memory sysctl without reading it. Known filesystem vectors have named
/// fields; other numeric vectors remain ordered arrays, including one-element
/// `vm.lowmem_reserve_ratio` vectors. Unknown textual settings remain strings.
/// Malformed known vectors fail atomically; no missing counter is invented.
pub fn parse_sysctl_value(name: &str, text: &str) -> Result<ParsedMemoryValue, MemoryReadError> {
    crate::heap::with_default_budget(|| parse_sysctl_inner(name, text))
}

fn parse_sysctl_inner(name: &str, text: &str) -> Result<ParsedMemoryValue, MemoryReadError> {
    let fields = words(text)?;
    match name {
        "fs.dentry-state" => named_vector(&fields, &["nr_dentry", "nr_unused", "age_limit", "want_pages", "nr_negative"], 4, 6),
        "fs.file-nr" => named_vector(&fields, &["nr_files", "nr_free_files", "max_files"], 3, 3),
        "fs.inode-nr" => named_vector(&fields, &["nr_inodes", "nr_unused"], 2, 2),
        "fs.inode-state" => named_vector(&fields, &["nr_inodes", "nr_unused"], 7, 7),
        "vm.lowmem_reserve_ratio" => {
            if fields.is_empty() {
                return Err(invalid("empty lowmem reserve ratio vector"));
            }
            Ok(ParsedMemoryValue::List(fields.iter().map(|field| number(field)).collect_memory()?))
        }
        _ => atom(text),
    }
}

/// Parse a known memory-information layout without I/O. The path provides format
/// context (for example process `stat` versus zram's block `stat`). Unknown
/// multiline diagnostic dumps return `None`; collectors report them unavailable
/// rather than retaining unparsed contents.
pub fn parse_memory_file(path: impl AsRef<Path>, text: &str) -> Result<Option<ParsedMemoryValue>, MemoryReadError> {
    crate::heap::with_default_budget(|| parse_file_inner(path.as_ref(), text))
}

fn parse_file_inner(path: &Path, text: &str) -> Result<Option<ParsedMemoryValue>, MemoryReadError> {
    let name = path.file_name().and_then(|v| v.to_str()).unwrap_or_default();
    let parent = path.parent().and_then(Path::file_name).and_then(|v| v.to_str()).unwrap_or_default();
    let value = match name {
        "read_latency" | "write_latency" | "read_bandwidth" | "write_bandwidth"
            if parent == "initiators"
                && path
                    .parent()
                    .and_then(Path::parent)
                    .and_then(Path::file_name)
                    .and_then(|name| name.to_str())
                    .and_then(|name| crate::files::numbered(name, "access"))
                    .is_some() =>
        {
            let text = text.trim();
            if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(invalid("invalid NUMA topology attribute"));
            }
            map([
                ("value", number(text)?),
                ("unit", ParsedMemoryValue::try_text(if name.ends_with("latency") { "ns" } else { "MiB/s" })?),
            ])?
        }
        "auto" if parent == "weighted_interleave" => match text.trim() {
            "true" => ParsedMemoryValue::Bool(true),
            "false" => ParsedMemoryValue::Bool(false),
            _ => return Err(invalid("invalid weighted-interleave mode")),
        },
        name if parent == "weighted_interleave" && crate::files::numbered(name, "node").is_some() => {
            let text = text.trim();
            let weight = text.parse::<u8>().map_err(|_| invalid("invalid weighted-interleave weight"))?;
            if weight == 0 || !text.bytes().all(|byte| byte.is_ascii_digit()) {
                return Err(invalid("weighted-interleave weight must be 1..=255"));
            }
            ParsedMemoryValue::Unsigned(u64::from(weight))
        }
        "ksm_stat" => counter_table(text, true)?,
        "numastat" | "vmstat" => counter_table(text, false)?,
        "meminfo" => node_meminfo(text)?,
        "status" => crate::process_values::status(text)?,
        "limits" => crate::process_values::limits(text)?,
        "stat" if text.trim_start().split_once('(').is_some() => crate::process_values::stat(text)?,
        "stat" if parent.starts_with("zram") => {
            let fields = words(text)?;
            named_vector(
                &fields,
                &[
                    "reads_completed",
                    "reads_merged",
                    "sectors_read",
                    "read_ms",
                    "writes_completed",
                    "writes_merged",
                    "sectors_written",
                    "write_ms",
                    "io_in_progress",
                    "io_ms",
                    "weighted_io_ms",
                    "discards_completed",
                    "discards_merged",
                    "sectors_discarded",
                    "discard_ms",
                    "flushes_completed",
                    "flush_ms",
                ],
                11,
                17,
            )?
        }
        "mm_stat" => {
            let parsed = crate::ZramMemoryStat::parse(text)?;
            let mut fields = crate::MemoryMap::try_from_iter(parsed.fields.into_iter().map(|(name, value)| (name, ParsedMemoryValue::Unsigned(value))))?;
            if !parsed.extra.is_empty() {
                fields.try_insert("extra", ParsedMemoryValue::try_list(parsed.extra.into_iter().map(ParsedMemoryValue::Unsigned))?)?;
            }
            ParsedMemoryValue::Map(fields)
        }
        "io_stat" => named_vector(&words(text)?, &["failed_reads", "failed_writes", "invalid_io", "notify_free"], 4, 4)?,
        "bd_stat" => named_vector(&words(text)?, &["bd_count", "bd_reads", "bd_writes"], 3, 3)?,
        "numa_maps" => crate::process_values::numa_maps(text)?,
        "maps" | "smaps" => crate::process_values::mappings(text)?,
        "vmallocinfo" => crate::process_values::vmalloc(text)?,
        "cpulist" | "nodelist" | "cpuset.mems" | "cpuset.mems.effective" | "possible" | "has_cpu" | "has_memory" | "has_normal_memory" | "has_high_memory" => {
            ranges(text)?
        }
        "online" if parent == "node" => ranges(text)?,
        "cpumap" | "bitmap" => bitmap(text)?,
        "block_size_bytes" | "phys_index" => hexadecimal(text)?,
        "enabled" | "defrag" | "shmem_enabled" | "advisor_mode" | "comp_algorithm" | "recomp_algorithm" if text.contains(['[', ']']) => policy(&words(text)?)?,
        "distance" => ParsedMemoryValue::List(words(text)?.iter().map(|field| number(field)).collect_memory()?),
        "objects" | "objects_partial" | "total_objects" | "slabs" | "partial" | "slabs_cpu_partial" if text.contains("N") => node_counts(text)?,
        name if name.starts_with("hugetlb.") && name.ends_with(".numa_stat") => assignments(text)?,
        "page_owner" => crate::provenance::page_owner(text)?,
        "show_stacks" | "show_handles" | "show_stacks_handles" => crate::provenance::owner_stacks(name, text)?,
        "kmemleak" => crate::provenance::kernel_leaks(text)?,
        "lru_gen_full" => crate::lru::full_histograms(text)?,
        "count"
            if path
                .parent()
                .and_then(Path::parent)
                .and_then(Path::file_name)
                .is_some_and(|name| name == "shrinker") =>
        {
            crate::shrinker::counts(text)?
        }
        _ if text.lines().count() > 1 => return Ok(None),
        _ => atom(text)?,
    };
    Ok(Some(value))
}

pub(crate) fn invalid(message: &str) -> MemoryReadError {
    MemoryReadError::read_failed(message)
}

pub(crate) fn words(text: &str) -> Result<crate::MemoryList<&str>, MemoryReadError> {
    let mut fields = crate::MemoryList::scratch(crate::HeapBudget::current());
    for field in text.split_ascii_whitespace().take(65537) {
        fields.try_push(field)?;
    }
    if fields.len() > 65536 {
        return Err(invalid("structured value exceeds 65536 items"));
    }
    Ok(fields)
}

pub(crate) fn number(text: &str) -> Result<ParsedMemoryValue, MemoryReadError> {
    let digits = text.strip_prefix('-').or_else(|| text.strip_prefix('+')).unwrap_or(text);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(invalid("expected an integer"));
    }
    if text.starts_with('-') {
        text.parse::<i64>()
            .map(|value| {
                if value < 0 {
                    ParsedMemoryValue::Signed(value)
                } else {
                    ParsedMemoryValue::Unsigned(0)
                }
            })
            .map_err(|_| invalid("signed integer overflow"))
    } else {
        digits
            .parse::<u64>()
            .map(ParsedMemoryValue::Unsigned)
            .map_err(|_| invalid("unsigned integer overflow"))
    }
}

fn numeric_token(text: &str) -> bool {
    let digits = text.strip_prefix('-').or_else(|| text.strip_prefix('+')).unwrap_or(text);
    !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit())
}

pub(crate) fn atom(text: &str) -> Result<ParsedMemoryValue, MemoryReadError> {
    let text = text.trim();
    if numeric_token(text) {
        return number(text);
    }
    if text.starts_with("0x") {
        return hexadecimal(text);
    }
    match text {
        "yes" | "Y" | "true" => return Ok(ParsedMemoryValue::Bool(true)),
        "no" | "N" | "false" => return Ok(ParsedMemoryValue::Bool(false)),
        _ => {}
    }
    let fields = words(text)?;
    if fields.len() > 1 && fields.iter().all(|field| numeric_token(field)) {
        return Ok(ParsedMemoryValue::List(fields.iter().map(|field| number(field)).collect_memory()?));
    }
    let quantity = if fields.len() == 2 && numeric_token(fields[0]) && is_unit(fields[1]) {
        Some((fields[0], fields[1]))
    } else if fields.len() == 1 {
        let split = text.find(|c: char| !(c.is_ascii_digit() || c == '-' || c == '+'));
        split.and_then(|split| {
            let (value, unit) = text.split_at(split);
            (numeric_token(value) && is_unit(unit)).then_some((value, unit))
        })
    } else {
        None
    };
    if let Some((value, unit)) = quantity {
        return map([("value", number(value)?), ("unit", ParsedMemoryValue::try_text(unit)?)]);
    }
    ParsedMemoryValue::try_text(text)
}

fn is_unit(unit: &str) -> bool {
    !unit.is_empty() && unit.bytes().all(|b| b.is_ascii_alphabetic() || matches!(b, b'%' | b'/'))
}

pub(crate) fn map<const N: usize>(fields: [(&str, ParsedMemoryValue); N]) -> Result<ParsedMemoryValue, MemoryReadError> {
    ParsedMemoryValue::try_map(fields)
}

pub(crate) fn put(fields: &mut crate::MemoryMap<ParsedMemoryValue>, name: &str, value: ParsedMemoryValue) -> Result<(), MemoryReadError> {
    if name.is_empty() || fields.try_insert(name, value)?.is_some() {
        return Err(invalid("empty or duplicate structured field"));
    }
    Ok(())
}

fn named_vector(fields: &[&str], names: &[&str], minimum: usize, reserved_end: usize) -> Result<ParsedMemoryValue, MemoryReadError> {
    if fields.len() < minimum {
        return Err(invalid("incomplete kernel counter vector"));
    }
    let mut result = crate::MemoryMap::default();
    for (name, value) in names.iter().zip(fields) {
        result.try_insert(*name, number(value)?)?;
    }
    let reserved: crate::MemoryList<_> = fields
        .iter()
        .skip(names.len())
        .take(reserved_end.saturating_sub(names.len()))
        .map(|v| number(v))
        .collect_memory()?;
    if !reserved.is_empty() {
        result.try_insert("reserved", ParsedMemoryValue::List(reserved))?;
    }
    let extra: crate::MemoryList<_> = fields.iter().skip(reserved_end.max(names.len())).map(|v| number(v)).collect_memory()?;
    if !extra.is_empty() {
        result.try_insert("extra", ParsedMemoryValue::List(extra))?;
    }
    Ok(ParsedMemoryValue::Map(result))
}

fn counter_table(text: &str, ksm: bool) -> Result<ParsedMemoryValue, MemoryReadError> {
    let mut result = crate::MemoryMap::default();
    for (index, line) in text.lines().filter(|v| !v.trim().is_empty()).enumerate() {
        if index >= 65536 {
            return Err(invalid("counter table exceeds 65536 records"));
        }
        let line = line.trim();
        let split = line
            .find(char::is_whitespace)
            .or_else(|| line.find(':'))
            .ok_or_else(|| invalid("missing counter value"))?;
        let name = line[..split].trim_end_matches(':');
        let value = atom(line[split..].trim_start_matches(':').trim())?;
        if ksm && matches!(name, "ksm_merge_any" | "ksm_mergeable") {
            if !matches!(value, ParsedMemoryValue::Bool(_)) {
                return Err(invalid("expected KSM yes/no flag"));
            }
        } else if !matches!(value, ParsedMemoryValue::Unsigned(_) | ParsedMemoryValue::Signed(_)) {
            return Err(invalid("expected numeric memory counter"));
        }
        put(&mut result, name, value)?;
    }
    Ok(ParsedMemoryValue::Map(result))
}

fn node_meminfo(text: &str) -> Result<ParsedMemoryValue, MemoryReadError> {
    let mut result = crate::MemoryMap::default();
    let mut node = None;
    for line in text.lines().filter(|v| !v.trim().is_empty()) {
        let fields = words(line)?;
        if fields.len() < 4 || fields[0] != "Node" {
            return Err(invalid("expected node meminfo record"));
        }
        let id = fields[1].parse::<u32>().map_err(|_| invalid("invalid meminfo node ID"))?;
        if node.is_some_and(|n| n != id) {
            return Err(invalid("mixed node IDs in node meminfo"));
        }
        node = Some(id);
        let (_, value) = line.split_once(':').ok_or_else(|| invalid("missing node meminfo colon"))?;
        let value = atom(value)?;
        if matches!(value, ParsedMemoryValue::Text(_)) {
            return Err(invalid("invalid node memory amount"));
        }
        put(&mut result, fields[2].trim_end_matches(':'), value)?;
    }
    Ok(ParsedMemoryValue::Map(result))
}

pub(crate) fn ranges(text: &str) -> Result<ParsedMemoryValue, MemoryReadError> {
    let mut result = crate::MemoryList::default();
    let mut previous = None;
    if text.trim().is_empty() {
        return Ok(ParsedMemoryValue::List(result));
    }
    for (index, range) in text.trim().split(',').enumerate() {
        if index >= 65536 {
            return Err(invalid("ID list exceeds 65536 ranges"));
        }
        let (start, end) = range.split_once('-').map_or((range, range), |v| v);
        if start.is_empty() || end.is_empty() || !start.bytes().chain(end.bytes()).all(|b| b.is_ascii_digit()) {
            return Err(invalid("invalid ID range"));
        }
        let start = start.parse::<u32>().map_err(|_| invalid("invalid ID range"))?;
        let end = end.parse::<u32>().map_err(|_| invalid("invalid ID range"))?;
        if start > end || previous.is_some_and(|p| start <= p) {
            return Err(invalid("reversed, duplicate or overlapping ID range"));
        }
        previous = Some(end);
        result.try_push(map([
            ("start", ParsedMemoryValue::Unsigned(u64::from(start))),
            ("end", ParsedMemoryValue::Unsigned(u64::from(end))),
        ])?)?;
    }
    Ok(ParsedMemoryValue::List(result))
}

pub(crate) fn hexadecimal(text: &str) -> Result<ParsedMemoryValue, MemoryReadError> {
    let text = text.trim().strip_prefix("0x").unwrap_or(text.trim());
    if text.is_empty() || !text.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(invalid("invalid hexadecimal value"));
    }
    u64::from_str_radix(text, 16)
        .map(ParsedMemoryValue::Unsigned)
        .map_err(|_| invalid("hexadecimal value overflow"))
}

pub(crate) fn bitmap(text: &str) -> Result<ParsedMemoryValue, MemoryReadError> {
    let mut result = crate::MemoryList::default();
    for (index, word) in text.trim().split(',').enumerate() {
        if index >= 65536 {
            return Err(invalid("bitmap exceeds 65536 words"));
        }
        let value = u32::from_str_radix(word, 16).map_err(|_| invalid("invalid 32-bit bitmap word"))?;
        result.try_push(ParsedMemoryValue::Unsigned(u64::from(value)))?;
    }
    map([("words", ParsedMemoryValue::List(result))])
}

fn policy(fields: &[&str]) -> Result<ParsedMemoryValue, MemoryReadError> {
    let mut selected = None;
    let mut choices = crate::MemoryList::default();
    let mut seen = BTreeSet::new();
    for field in fields {
        let name = if let Some(name) = field.strip_prefix('[').and_then(|v| v.strip_suffix(']')) {
            if selected.replace(ParsedMemoryValue::try_text(name)?).is_some() {
                return Err(invalid("multiple selected policy choices"));
            }
            name
        } else {
            *field
        };
        if name.is_empty() || name.contains(['[', ']']) || !seen.insert(name) {
            return Err(invalid("malformed policy choice"));
        }
        choices.try_push(ParsedMemoryValue::try_text(name)?)?;
    }
    map([
        ("selected", selected.ok_or_else(|| invalid("missing selected policy choice"))?),
        ("choices", ParsedMemoryValue::List(choices)),
    ])
}

fn node_counts(text: &str) -> Result<ParsedMemoryValue, MemoryReadError> {
    let fields = words(text)?;
    let mut nodes = crate::MemoryMap::default();
    for field in fields.iter().skip(1) {
        let (node, value) = field
            .strip_prefix('N')
            .and_then(|v| v.split_once('='))
            .ok_or_else(|| invalid("invalid per-node allocator count"))?;
        let node = node.parse::<u32>().map_err(|_| invalid("invalid allocator node ID"))?;
        put(&mut nodes, &node.to_string(), number(value)?)?;
    }
    map([
        ("total", number(fields.first().ok_or_else(|| invalid("missing allocator total"))?)?),
        ("nodes", ParsedMemoryValue::Map(nodes)),
    ])
}

fn assignments(text: &str) -> Result<ParsedMemoryValue, MemoryReadError> {
    let mut result = crate::MemoryMap::default();
    for field in words(text)? {
        let (name, value) = field.split_once('=').ok_or_else(|| invalid("invalid named counter"))?;
        put(&mut result, name, number(value)?)?;
    }
    Ok(ParsedMemoryValue::Map(result))
}
