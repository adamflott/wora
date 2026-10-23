use crate::structured::{invalid, map};
use crate::{MemoryFiles, MemoryReadError, ParsedMemoryValue};
use std::path::Path;

/// Opt-in page allocation provenance from debugfs. Requires page-owner support
/// and normally `page_owner=on` at boot, plus privileged debugfs access.
///
/// Reads `/sys/kernel/debug/page_owner` and
/// `page_owner_stacks/{show_stacks,show_handles,show_stacks_handles,count_threshold}`,
/// or those names below an explicit debugfs root. Allocation/page/stack records
/// follow the reviewed 6.12/7.2 layouts. Unknown layouts fail atomically.
#[derive(Clone, Default, Debug)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct PageOwnerStats {
    /// Parsed records/settings keyed by their relative debugfs source path.
    /// `page_owner` lists allocation metadata, PFNs/pageblocks, stack symbols,
    /// optional migration reason and cgroup charge labels. Timestamps retain ns;
    /// order/block/PFN values are not bytes. Cgroup labels are not full paths.
    /// Stack tables retain signed handles and base-page counts without summing
    /// them into host memory. Reads cap at 65536 records, 256 frames per stack
    /// and 4096 bytes per line, in addition to the source/shared I/O limits.
    pub files: MemoryFiles,
}

pub(crate) fn page_owner(text: &str) -> Result<ParsedMemoryValue, MemoryReadError> {
    provenance_records(text, None, |lines| {
        let header = lines
            .first()
            .and_then(|line| line.strip_prefix("Page allocated via order "))
            .ok_or_else(bad_owner)?;
        let (order, header) = header.split_once(", mask ").ok_or_else(bad_owner)?;
        let (mask, header) = header.split_once(", pid ").ok_or_else(bad_owner)?;
        let (pid, header) = header.split_once(", tgid ").ok_or_else(bad_owner)?;
        let (tgid, header) = header.split_once(" (").ok_or_else(bad_owner)?;
        let (comm, timestamp) = header.rsplit_once("), ts ").ok_or_else(bad_owner)?;
        let timestamp = timestamp.strip_suffix(" ns").ok_or_else(bad_owner)?;
        let page = lines.get(1).ok_or_else(bad_owner)?;
        let (fields, flags) = crate::process_values::prefix(page, 9)?;
        if fields[0] != "PFN" || fields[2] != "type" || fields[4] != "Block" || fields[6] != "type" || fields[8] != "Flags" {
            return Err(bad_owner());
        }
        let mut result = crate::MemoryMap::try_from_iter([
            (
                "allocation",
                map([
                    ("order", unsigned(order)?),
                    ("mask", bit_names(mask)?),
                    ("pid", signed_id(pid)?),
                    ("tgid", signed_id(tgid)?),
                    ("comm", ParsedMemoryValue::try_text(comm)?),
                    ("timestamp_ns", unsigned(timestamp)?),
                ])?,
            ),
            (
                "page",
                map([
                    ("pfn", hex(fields[1])?),
                    ("migratetype", ParsedMemoryValue::try_text(fields[3])?),
                    ("block", unsigned(fields[5])?),
                    ("block_migratetype", ParsedMemoryValue::try_text(fields[7])?),
                    ("flags", bit_names(flags)?),
                ])?,
            ),
        ])?;
        let mut stack = crate::MemoryList::default();
        for line in &lines[2..] {
            if let Some(reason) = line.strip_prefix("Page has been migrated, last migrate reason: ") {
                if reason.is_empty() || result.try_insert("migration_reason", ParsedMemoryValue::try_text(reason)?)?.is_some() {
                    return Err(bad_owner());
                }
            } else if *line == "Slab cache page" {
                if result.try_insert("slab_cache_page", ParsedMemoryValue::Bool(true))?.is_some() {
                    return Err(bad_owner());
                }
            } else if let Some(charge) = line.strip_prefix("Charged ") {
                let (objcg, charge) = charge.strip_prefix("(via objcg) ").map_or((false, charge), |rest| (true, rest));
                let charge = charge.strip_prefix("to ").ok_or_else(bad_owner)?;
                let (offline, charge) = charge.strip_prefix("offline ").map_or((false, charge), |rest| (true, rest));
                let label = charge.strip_prefix("memcg ").filter(|label| !label.is_empty()).ok_or_else(bad_owner)?;
                let value = map([
                    ("label", ParsedMemoryValue::try_text(label)?),
                    ("via_objcg", ParsedMemoryValue::Bool(objcg)),
                    ("offline", ParsedMemoryValue::Bool(offline)),
                ])?;
                if result.try_insert("charge", value)?.is_some() {
                    return Err(bad_owner());
                }
            } else {
                push_frame(&mut stack, line)?;
            }
        }
        result.try_insert("stack", ParsedMemoryValue::List(stack))?;
        Ok(ParsedMemoryValue::Map(result))
    })
}

pub(crate) fn owner_stacks(name: &str, text: &str) -> Result<ParsedMemoryValue, MemoryReadError> {
    let has_stack = name != "show_handles";
    let has_handle = name != "show_stacks";
    let has_pages = name != "show_stacks_handles";
    provenance_records(text, None, |lines| {
        let trailers = usize::from(has_handle) + usize::from(has_pages);
        let stack_end = lines.len().checked_sub(trailers).ok_or_else(bad_owner)?;
        if !has_stack && stack_end != 0 {
            return Err(bad_owner());
        }
        let mut record = crate::MemoryMap::default();
        if has_stack {
            let mut stack = crate::MemoryList::default();
            for line in &lines[..stack_end] {
                push_frame(&mut stack, line)?;
            }
            record.try_insert("stack", ParsedMemoryValue::List(stack))?;
        }
        if has_handle {
            let handle = lines[stack_end].strip_prefix("handle: ").ok_or_else(bad_owner)?;
            record.try_insert("handle", signed_id(handle)?)?;
        }
        if has_pages {
            let pages = lines[lines.len() - 1].strip_prefix("nr_base_pages: ").ok_or_else(bad_owner)?;
            record.try_insert("nr_base_pages", unsigned(pages)?)?;
        }
        Ok(ParsedMemoryValue::Map(record))
    })
}

fn provenance_records(
    text: &str,
    header: Option<&str>,
    parse: impl Fn(&[&str]) -> Result<ParsedMemoryValue, MemoryReadError>,
) -> Result<ParsedMemoryValue, MemoryReadError> {
    let mut records = crate::MemoryList::default();
    let mut lines = crate::MemoryList::scratch(crate::HeapBudget::current());
    for line in text.lines().chain(std::iter::once("")) {
        if line.len() > 4096 {
            return Err(invalid("provenance line exceeds 4096 bytes"));
        }
        if !lines.is_empty() && (line.trim().is_empty() || header.is_some_and(|header| line.starts_with(header))) {
            if records.len() == 65536 {
                return Err(invalid("provenance dump exceeds 65536 records"));
            }
            records.try_push(parse(&lines)?)?;
            lines.clear();
        }
        if !line.trim().is_empty() {
            if lines.len() == 512 {
                return Err(invalid("provenance record exceeds 512 lines"));
            }
            lines.try_push(line)?;
        }
    }
    Ok(ParsedMemoryValue::List(records))
}

fn push_frame(stack: &mut crate::MemoryList<ParsedMemoryValue>, line: &str) -> Result<(), MemoryReadError> {
    if !line.starts_with([' ', '\t']) || line.trim().is_empty() || stack.len() == 256 {
        return Err(invalid("invalid provenance stack or more than 256 frames"));
    }
    stack.try_push(ParsedMemoryValue::try_text(line.trim())?)?;
    Ok(())
}

fn unsigned(text: &str) -> Result<ParsedMemoryValue, MemoryReadError> {
    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(invalid("invalid provenance unsigned integer"));
    }
    crate::structured::number(text)
}

fn signed_id(text: &str) -> Result<ParsedMemoryValue, MemoryReadError> {
    text.parse::<i32>().map_err(|_| invalid("invalid provenance signed identifier"))?;
    crate::structured::number(text)
}

fn hex(text: &str) -> Result<ParsedMemoryValue, MemoryReadError> {
    let digits = text.strip_prefix("0x").unwrap_or(text);
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(invalid("invalid provenance hexadecimal value"));
    }
    u64::from_str_radix(digits, 16)
        .map(ParsedMemoryValue::Unsigned)
        .map_err(|_| invalid("provenance hexadecimal value exceeds u64"))
}

fn bit_names(text: &str) -> Result<ParsedMemoryValue, MemoryReadError> {
    let (bits, names) = match text.split_once('(') {
        Some((bits, names)) => (bits, names.strip_suffix(')').ok_or_else(bad_owner)?),
        None => (text, ""),
    };
    map([("bits", hex(bits)?), ("names", ParsedMemoryValue::try_text(names)?)])
}

fn bad_owner() -> MemoryReadError {
    invalid("malformed page-owner record")
}

impl PageOwnerStats {
    /// Read existing page-owner tracking without enabling it or changing filters.
    pub fn current() -> Self {
        Self::from_debug_root("/sys/kernel/debug")
    }
    /// Read a supplied debugfs root with a 4 MiB per-file bound.
    pub fn from_debug_root(root: impl AsRef<Path>) -> Self {
        Self::from_debug_root_with_limit(root, 4 * 1024 * 1024)
    }
    /// Read with an explicit byte limit (1..=64 MiB). Oversized sources fail
    /// independently; no truncated stack dump is presented as complete.
    pub fn from_debug_root_with_limit(root: impl AsRef<Path>, limit: u64) -> Self {
        Self {
            files: MemoryFiles::from_names_with_limit(
                root,
                &[
                    "page_owner",
                    "page_owner_stacks/show_stacks",
                    "page_owner_stacks/show_handles",
                    "page_owner_stacks/show_stacks_handles",
                    "page_owner_stacks/count_threshold",
                ],
                limit,
            ),
        }
    }
}

/// Opt-in kernel leak suspects, requiring `CONFIG_DEBUG_KMEMLEAK` and enablement.
/// These are diagnostic findings, not proof of a leak or a complete leak census.
///
/// Reads `/sys/kernel/debug/kmemleak`, or `kmemleak` below an explicit
/// debugfs root, using reviewed 6.12/7.2 suspect layouts. Embedded object-byte
/// dumps are validated then discarded; no scan/clear command is written.
#[derive(Clone, Default, Debug)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct KernelLeakStats {
    /// Suspect records keyed by `kmemleak`, with independent read/parse errors.
    /// Addresses, byte sizes, PID/comm, allocation jiffies, optional checksum and
    /// stack symbols retain their native meaning. Addresses may be redacted;
    /// jiffies are not wall-clock time. Virtual/physical/per-CPU scope stays
    /// separate. Limits: 65536 records, 256 frames/stack and 4096 bytes/line.
    pub files: MemoryFiles,
}

pub(crate) fn kernel_leaks(text: &str) -> Result<ParsedMemoryValue, MemoryReadError> {
    provenance_records(text, Some("unreferenced object"), |lines| {
        let header = lines.first().and_then(|line| line.strip_prefix("unreferenced object")).ok_or_else(bad_leak)?;
        let (kind, header) = if let Some(header) = header.strip_prefix(" (phys) ") {
            ("physical", header)
        } else if let Some(header) = header.strip_prefix(" (percpu) ") {
            ("percpu", header)
        } else {
            ("virtual", header.strip_prefix(' ').ok_or_else(bad_leak)?)
        };
        let (address, size) = header.split_once(" (size ").ok_or_else(bad_leak)?;
        let size = size.strip_suffix("):").ok_or_else(bad_leak)?;
        let identity = lines.get(1).and_then(|line| line.strip_prefix("  comm \"")).ok_or_else(bad_leak)?;
        let (comm, identity) = identity.rsplit_once("\", pid ").ok_or_else(bad_leak)?;
        let (pid, jiffies) = identity.split_once(", jiffies ").ok_or_else(bad_leak)?;
        let mut result = crate::MemoryMap::try_from_iter([
            ("address", hex(address)?),
            ("address_space", ParsedMemoryValue::try_text(kind)?),
            ("size_bytes", unsigned(size)?),
            ("comm", ParsedMemoryValue::try_text(comm)?),
            ("pid", signed_id(pid)?),
            ("allocation_jiffies", unsigned(jiffies)?),
        ])?;
        let mut index = 2;
        if let Some(header) = lines.get(index).and_then(|line| line.strip_prefix("  hex dump (first ")) {
            let header = header.strip_suffix("):").ok_or_else(bad_leak)?;
            let (count, cpu) = if let Some((count, cpu)) = header.split_once(" bytes on cpu ") {
                (count, Some(cpu))
            } else {
                (header.strip_suffix(" bytes").ok_or_else(bad_leak)?, None)
            };
            let count = unsigned(count)?.as_u64().ok_or_else(bad_leak)?;
            let size = result.get("size_bytes").and_then(ParsedMemoryValue::as_u64).ok_or_else(bad_leak)?;
            if count > 32 || count > size {
                return Err(bad_leak());
            }
            result.try_insert("discarded_dump_bytes", ParsedMemoryValue::Unsigned(count))?;
            if let Some(cpu) = cpu {
                result.try_insert("dump_cpu", unsigned(cpu)?)?;
            }
            index += 1;
            let mut remaining = count as usize;
            while remaining != 0 {
                let bytes = remaining.min(16);
                let line = lines.get(index).and_then(|line| line.strip_prefix("    ")).ok_or_else(bad_leak)?;
                let hex = line.get(..bytes * 3 - 1).ok_or_else(bad_leak)?;
                let mut fields = hex.split_ascii_whitespace();
                for _ in 0..bytes {
                    let byte = fields.next().ok_or_else(bad_leak)?;
                    if byte.len() != 2 || !byte.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                        return Err(bad_leak());
                    }
                }
                if fields.next().is_some() || !line.as_bytes().get(bytes * 3 - 1).is_some_and(u8::is_ascii_whitespace) {
                    return Err(bad_leak());
                }
                remaining -= bytes;
                index += 1;
            }
        }
        let backtrace = lines.get(index).ok_or_else(bad_leak)?;
        if let Some(checksum) = backtrace.strip_prefix("  backtrace (crc ").and_then(|value| value.strip_suffix("):")) {
            result.try_insert("checksum", hex(checksum)?)?;
        } else if *backtrace != "  backtrace:" {
            return Err(bad_leak());
        }
        let mut stack = crate::MemoryList::default();
        for line in &lines[index + 1..] {
            push_frame(&mut stack, line)?;
        }
        result.try_insert("stack", ParsedMemoryValue::List(stack))?;
        Ok(ParsedMemoryValue::Map(result))
    })
}

fn bad_leak() -> MemoryReadError {
    invalid("malformed kmemleak suspect")
}

impl KernelLeakStats {
    /// Read existing kmemleak findings. Reading can incur scanning work depending
    /// on the kernel; this must not be included in routine sampling implicitly.
    pub fn current() -> Self {
        Self::from_debug_root("/sys/kernel/debug")
    }
    /// Read a supplied debugfs root with a 4 MiB limit; never writes scan/clear/off.
    pub fn from_debug_root(root: impl AsRef<Path>) -> Self {
        Self::from_debug_root_with_limit(root, 4 * 1024 * 1024)
    }
    /// Read with an explicit byte limit (1..=64 MiB), failing rather than truncating.
    pub fn from_debug_root_with_limit(root: impl AsRef<Path>, limit: u64) -> Self {
        Self {
            files: MemoryFiles::from_names_with_limit(root, &["kmemleak"], limit),
        }
    }
}
