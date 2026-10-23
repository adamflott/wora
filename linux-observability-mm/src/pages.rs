use crate::MemoryReadError;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;

/// Explicit bounded range of PFNs or virtual page indices (not byte addresses).
///
/// This is caller-supplied selection, not a sampled memory total. Physical
/// readers seek to `start * 8` in `/proc/kpage*`; process pagemap readers seek
/// to that offset in `/proc/<pid>/pagemap`. Byte addresses must first be
/// converted to page indices using the relevant base page size.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct PageRange {
    /// First PFN for kpage files, or virtual page index for pagemap.
    pub start: u64,
    /// Number of entries, from 1 through 65536. No whole-machine scan is inferred.
    pub count: u32,
}

impl PageRange {
    /// Validate bounds and checked eight-byte file offsets without performing I/O.
    pub fn validate(self) -> io::Result<()> {
        if self.count == 0 || self.count > 65536 || self.start.checked_add(u64::from(self.count)).and_then(|end| end.checked_mul(8)).is_none() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "page range requires 1..=65536 entries and non-overflowing offsets",
            ));
        }
        Ok(())
    }
}

/// Physical-page accounting for exactly the requested PFN range.
///
/// Reads `/proc/kpageflags`, `/proc/kpagecount` and `/proc/kpagecgroup` over
/// the supplied range, or the same files under an explicit proc root. Each
/// entry is a native-endian `u64`; index `i` describes PFN `range.start + i`.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct PhysicalPageStats {
    /// PFN range used for all three arrays; index zero corresponds to `start`.
    pub range: PageRange,
    /// Raw kpageflags bits, preserving flags introduced by newer kernels.
    pub flags: Option<Vec<u64>>,
    /// Kernel mapping counts; may be averaged across large allocations.
    pub mapping_counts: Option<Vec<u64>>,
    /// Charged memory-cgroup inode numbers (not paths or cgroup IDs).
    pub cgroup_inodes: Option<Vec<u64>>,
    /// Independent read failures keyed by kpage filename.
    pub unavailable: BTreeMap<String, MemoryReadError>,
}

impl PhysicalPageStats {
    /// Read a bounded host PFN range. Commonly requires root.
    pub fn current(range: PageRange) -> Self {
        Self::from_proc_root("/proc", range)
    }
    /// Read an explicit procfs root. Binary captures must use native endianness.
    /// Reads are not atomic across files; no page states are changed.
    pub fn from_proc_root(root: impl AsRef<Path>, range: PageRange) -> Self {
        let mut unavailable = BTreeMap::new();
        let mut read = |name: &str| match read_page_entries(root.as_ref().join(name), range) {
            Ok(value) => Some(value),
            Err(error) => {
                unavailable.insert(name.into(), error.into());
                None
            }
        };
        let flags = read("kpageflags");
        let mapping_counts = read("kpagecount");
        let cgroup_inodes = read("kpagecgroup");
        Self {
            range,
            flags,
            mapping_counts,
            cgroup_inodes,
            unavailable,
        }
    }
}

/// Bounded process pagemap snapshot, separate from physical-page arrays.
///
/// Reads `/proc/self/pagemap` by default, or `pagemap` below an explicit
/// process root. Each native-endian 64-bit entry describes one virtual page
/// in `range`; [`PageMapEntry`] decodes flags and PFN/swap information.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct ProcessPageMap {
    /// Explicit process identity.
    pub pid: u32,
    /// Virtual page indices, not PFNs. The caller supplies the base page size.
    pub range: PageRange,
    /// Decoded entries, or `None` on any read failure (including a short read).
    pub entries: Option<Vec<PageMapEntry>>,
    /// Read failure for `pagemap`.
    pub unavailable: BTreeMap<String, MemoryReadError>,
}

impl ProcessPageMap {
    /// Read this process's requested virtual pages; PFNs may be redacted.
    pub fn current(range: PageRange) -> Self {
        Self::from_process_root(std::process::id(), "/proc/self", range)
    }
    /// Read explicitly identified process files or native-endian binary captures.
    pub fn from_process_root(pid: u32, root: impl AsRef<Path>, range: PageRange) -> Self {
        let mut unavailable = BTreeMap::new();
        let entries = match read_page_entries(root.as_ref().join("pagemap"), range) {
            Ok(entries) => Some(entries.into_iter().map(PageMapEntry::decode).collect()),
            Err(error) => {
                unavailable.insert("pagemap".into(), error.into());
                None
            }
        };
        Self {
            pid,
            range,
            entries,
            unavailable,
        }
    }
}

/// Modern pagemap entry, retaining unknown bits in `raw`.
///
/// Decoded from one native-endian 64-bit `/proc/<pid>/pagemap` entry, normally
/// by [`ProcessPageMap`]. Flags and payload fields all come from `raw`;
/// [`Self::decode`] itself performs no I/O or privilege checks.
/// The decoder targets the modern (4.2+) flag layout. Optional userfaultfd and
/// guard bits depend on kernel support; preserving `raw` does not establish
/// semantic support for every newer kernel extension.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
pub struct PageMapEntry {
    /// Original 64-bit entry.
    pub raw: u64,
    /// Page is resident according to the entry.
    pub present: bool,
    /// Entry encodes swap information.
    pub swapped: bool,
    /// Soft-dirty bit; never cleared by this reader.
    pub soft_dirty: bool,
    /// Kernel exclusive-mapping flag; large allocations have special semantics.
    pub exclusive: bool,
    /// Compatibility name for raw pagemap bit 57 (userfaultfd tracking).
    /// Introduced for WP in 5.13; the 7.3-rc7 ABI also uses it for RWP.
    /// `true` alone does not establish write-only protection. Use
    /// [`Self::userfaultfd_protection`] with the matching VMA's `VmFlags`.
    /// See the [upstream ABI](https://github.com/torvalds/linux/blob/v7.3-rc7/Documentation/admin-guide/mm/pagemap.rst).
    pub uffd_write_protected: bool,
    /// Guard-region flag (bit 58), available since Linux 6.15.
    pub guard: bool,
    /// File page or shared anonymous page.
    pub file_or_shared_anonymous: bool,
    /// Nonzero PFN for resident, non-swapped pages. Zero is ambiguous/redacted,
    /// so it is never presented as a usable physical-frame identity.
    pub pfn: Option<u64>,
    /// Swap type when swapped.
    pub swap_type: Option<u8>,
    /// Swap offset when swapped.
    pub swap_offset: Option<u64>,
}

impl PageMapEntry {
    /// Interpret bit 57 using the matching VMA's smaps `VmFlags` value.
    ///
    /// Supply `mapping.fields.get("VmFlags")` from [`crate::MemoryMapping`].
    /// `uw` means `VM_UFFD_WP`; `ur` means `VM_UFFD_RWP` in the 7.3-rc7 ABI.
    /// A set bit with missing, malformed, absent or conflicting mode flags is
    /// unknown. A clear bit means unprotected regardless of registration mode.
    ///
    /// This method performs no reads and does not verify process/address/time
    /// identity. The caller must associate the entry with its VMA; independently
    /// collected smaps/pagemap data can race with mapping or registration changes.
    pub fn userfaultfd_protection(&self, vm_flags: Option<&crate::ParsedMemoryValue>) -> UserfaultfdProtection {
        if self.raw & (1 << 57) == 0 {
            return UserfaultfdProtection::Unprotected;
        }
        let Some(crate::ParsedMemoryValue::List(flags)) = vm_flags else {
            return UserfaultfdProtection::Unknown;
        };
        let mut wp = false;
        let mut rwp = false;
        for flag in flags {
            let crate::ParsedMemoryValue::Text(flag) = flag else {
                return UserfaultfdProtection::Unknown;
            };
            match flag.as_str() {
                "uw" => wp = true,
                "ur" => rwp = true,
                _ => {}
            }
        }
        match (wp, rwp) {
            (true, false) => UserfaultfdProtection::WriteProtected,
            (false, true) => UserfaultfdProtection::ReadWriteProtected,
            _ => UserfaultfdProtection::Unknown,
        }
    }

    /// Decode the documented modern Linux bits. CAP_SYS_ADMIN controls PFN access.
    pub fn decode(raw: u64) -> Self {
        let bit = |n: u32| raw & (1u64 << n) != 0u64;
        let payload = raw & ((1u64 << 55) - 1);
        Self {
            raw,
            present: bit(63),
            swapped: bit(62),
            soft_dirty: bit(55),
            exclusive: bit(56),
            uffd_write_protected: bit(57),
            guard: bit(58),
            file_or_shared_anonymous: bit(61),
            pfn: (bit(63) && !bit(62) && payload != 0).then_some(payload),
            swap_type: bit(62).then_some((payload & 31) as u8),
            swap_offset: bit(62).then_some(payload >> 5),
        }
    }
}

/// Userfaultfd protection derived from pagemap bit 57 and matching smaps VMA flags.
///
/// Produced by [`PageMapEntry::userfaultfd_protection`] from caller-supplied
/// `/proc/<pid>/pagemap` and `smaps` data. This is per-virtual-page state, not a
/// memory quantity or a new live sample. The RWP interpretation follows 7.3-rc7.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde1", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde1", serde(rename_all = "snake_case"))]
#[non_exhaustive]
pub enum UserfaultfdProtection {
    /// Bit 57 is clear; the page is not marked WP/RWP by pagemap.
    Unprotected,
    /// Bit 57 is set in a `VM_UFFD_WP` (`uw`) VMA.
    WriteProtected,
    /// Bit 57 is set in a `VM_UFFD_RWP` (`ur`) VMA.
    ReadWriteProtected,
    /// Bit 57 is set, but the VMA mode is missing, invalid or inconsistent.
    Unknown,
}

/// Read exactly a bounded range of native-endian 64-bit kernel page entries.
/// Alignment/overflow/size checks run before opening the file. A short read fails
/// the entire range; it is never padded with zeroes or returned as partial data.
pub fn read_page_entries(path: impl AsRef<Path>, range: PageRange) -> io::Result<Vec<u64>> {
    range.validate()?;
    let path = path.as_ref();
    crate::budget::charge_file()?;
    crate::budget::check_path(path)?;
    let byte_count = range.count as usize * 8;
    if byte_count > crate::budget::file_limit(byte_count) {
        return Err(crate::budget::exceeded(crate::CollectionLimit::FileBytes));
    }
    crate::budget::charge_bytes(byte_count)?;
    if !std::fs::symlink_metadata(path)?.is_file() {
        return Err(io::Error::other("expected a regular page information file"));
    }
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(range.start * 8))?;
    let mut bytes = vec![0; byte_count];
    file.read_exact(&mut bytes)?;
    Ok(bytes
        .chunks_exact(8)
        .map(|b| u64::from_ne_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]))
        .collect())
}
