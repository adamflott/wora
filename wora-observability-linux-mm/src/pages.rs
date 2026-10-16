use crate::MemoryReadError;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;

/// Explicit bounded range of PFNs or virtual page indices (not byte addresses).
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
    /// Userfaultfd write-protection flag.
    pub uffd_write_protected: bool,
    /// Guard-region flag on kernels supporting it.
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

/// Read exactly a bounded range of native-endian 64-bit kernel page entries.
/// Alignment/overflow/size checks run before opening the file. A short read fails
/// the entire range; it is never padded with zeroes or returned as partial data.
pub fn read_page_entries(path: impl AsRef<Path>, range: PageRange) -> io::Result<Vec<u64>> {
    range.validate()?;
    let path = path.as_ref();
    if !std::fs::symlink_metadata(path)?.is_file() {
        return Err(io::Error::other("expected a regular page information file"));
    }
    let mut file = File::open(path)?;
    file.seek(SeekFrom::Start(range.start * 8))?;
    let mut bytes = vec![0; range.count as usize * 8];
    file.read_exact(&mut bytes)?;
    Ok(bytes
        .chunks_exact(8)
        .map(|b| u64::from_ne_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]))
        .collect())
}
