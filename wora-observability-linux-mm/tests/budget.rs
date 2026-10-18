#![cfg(target_os = "linux")]

use std::fs;
use wora_observability_linux_mm::{CgroupMemory, CgroupMemoryStats, CollectionLimit, CollectionLimits, MemoryFiles, collect_with_limits};

#[test]
fn one_budget_covers_multiple_sources_and_never_parses_an_oversized_prefix() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    fs::write(root.path().join("one"), "1234\n")?;
    fs::write(root.path().join("two"), "5678\n")?;
    let limits = CollectionLimits {
        input_bytes: 8,
        file_bytes: 8,
        line_bytes: 8,
        ..Default::default()
    };
    let ((first, second), report) = collect_with_limits(limits, || {
        (MemoryFiles::from_names(root.path(), &["one"]), MemoryFiles::from_names(root.path(), &["two"]))
    })?;
    assert!(first.parsed.contains_key("one"));
    assert!(!second.parsed.contains_key("two"));
    assert!(second.unavailable.contains_key("two"));
    assert!(report.input_bytes <= limits.input_bytes);
    assert_eq!(report.limit_exceeded, Some(CollectionLimit::InputBytes));

    fs::write(root.path().join("memory.current"), "12\n3456789\n")?;
    let (snapshot, report) = collect_with_limits(limits, || CgroupMemory::from_directory(root.path()))?;
    assert!(!snapshot.parsed.contains_key("memory.current"));
    assert!(snapshot.unavailable.contains_key("memory.current"));
    assert_eq!(report.limit_exceeded, Some(CollectionLimit::FileBytes));
    Ok(())
}

#[test]
fn line_and_entry_limits_report_incomplete_collection() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    fs::write(root.path().join("value"), "123456789\n")?;
    let limits = CollectionLimits {
        line_bytes: 8,
        ..Default::default()
    };
    let (files, report) = collect_with_limits(limits, || MemoryFiles::from_names(root.path(), &["value"]))?;
    assert!(files.parsed.is_empty());
    assert_eq!(report.limit_exceeded, Some(CollectionLimit::LineBytes));
    for index in 0..10 {
        fs::write(root.path().join(format!("memory.future{index}")), "1\n")?;
    }
    let limits = CollectionLimits {
        entries: 3,
        ..Default::default()
    };
    let (files, report) = collect_with_limits(limits, || CgroupMemory::from_directory(root.path()))?;
    assert!(files.unavailable.contains_key("."));
    assert_eq!(report.entries, 3);
    assert_eq!(report.limit_exceeded, Some(CollectionLimit::Entries));
    Ok(())
}

#[test]
fn ancestor_limit_exposes_incomplete_visibility() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let proc = root.path().join("proc");
    let mount = root.path().join("cgroup");
    fs::create_dir_all(proc.join("self"))?;
    fs::create_dir_all(mount.join("a/b"))?;
    fs::write(proc.join("self/cgroup"), "0::/a/b\n")?;
    fs::write(proc.join("self/mountinfo"), format!("10 1 0:20 / {} rw - cgroup2 cgroup rw\n", mount.display()))?;
    let limits = CollectionLimits {
        ancestors: 1,
        ..Default::default()
    };
    let (snapshot, report) = collect_with_limits(limits, || CgroupMemoryStats::from_proc_root(&proc))?;
    assert!(snapshot.current.is_some());
    assert_eq!(snapshot.ancestors.len(), 1);
    assert!(snapshot.unavailable.contains_key("cgroup2"));
    assert_eq!(report.limit_exceeded, Some(CollectionLimit::Ancestors));
    Ok(())
}

#[test]
fn budgets_validate_before_collection_and_restore_after_unwind() -> Result<(), Box<dyn std::error::Error>> {
    let mut invoked = false;
    assert!(
        collect_with_limits(
            CollectionLimits {
                input_bytes: 0,
                ..Default::default()
            },
            || {
                invoked = true;
            }
        )
        .is_err()
    );
    assert!(!invoked);
    let (nested, _) = collect_with_limits(CollectionLimits::default(), || collect_with_limits(CollectionLimits::default(), || ()))?;
    assert!(nested.is_err());
    let unwind = std::panic::catch_unwind(|| {
        let _ = collect_with_limits(CollectionLimits::default(), || panic!("simulated collector failure"));
    });
    assert!(unwind.is_err());
    let (_, report) = collect_with_limits(CollectionLimits::default(), || ())?;
    assert_eq!(report.input_bytes, 0);
    assert_eq!(report.limit_exceeded, None);
    Ok(())
}

#[test]
fn binary_ranges_share_the_budget_and_paths_are_checked_before_reading() -> Result<(), Box<dyn std::error::Error>> {
    use wora_observability_linux_mm::{PageRange, read_page_entries};
    let root = tempfile::tempdir()?;
    let path = root.path().join("pagemap");
    fs::write(&path, [1u64.to_ne_bytes(), 2u64.to_ne_bytes()].concat())?;
    let limits = CollectionLimits {
        input_bytes: 24,
        file_bytes: 16,
        line_bytes: 16,
        ..Default::default()
    };
    let ((first, second), report) = collect_with_limits(limits, || {
        (
            read_page_entries(&path, PageRange { start: 0, count: 2 }),
            read_page_entries(&path, PageRange { start: 0, count: 2 }),
        )
    })?;
    assert_eq!(first?, [1, 2]);
    assert!(second.is_err());
    assert!(report.input_bytes <= limits.input_bytes);
    assert_eq!(report.limit_exceeded, Some(CollectionLimit::InputBytes));

    let limits = CollectionLimits {
        path_bytes: root.path().as_os_str().len() + 1,
        ..Default::default()
    };
    let (files, report) = collect_with_limits(limits, || MemoryFiles::from_names(root.path(), &["pagemap"]))?;
    assert!(files.parsed.is_empty());
    assert!(!files.unavailable.is_empty());
    assert_eq!(report.limit_exceeded, Some(CollectionLimit::PathBytes));
    Ok(())
}

#[test]
fn failed_explicit_sources_cannot_bypass_the_budget_with_zero_input_bytes() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let names = (0..1000).map(|index| format!("missing{index}")).collect::<Vec<_>>();
    let refs = names.iter().map(String::as_str).collect::<Vec<_>>();
    let limits = CollectionLimits {
        files: 2,
        ..Default::default()
    };
    let (files, report) = collect_with_limits(limits, || MemoryFiles::from_names(root.path(), &refs))?;
    assert!(files.parsed.is_empty());
    assert_eq!(files.unavailable.len(), 3); // Two source errors and one fixed limit error.
    assert_eq!(report.input_bytes, 0);
    assert_eq!(report.files, 2);
    assert_eq!(report.limit_exceeded, Some(CollectionLimit::Files));
    Ok(())
}

#[test]
fn shared_storage_keeps_credit_and_refuses_copy_before_mutation() -> Result<(), Box<dyn std::error::Error>> {
    use wora_observability_linux_mm::{HeapBudget, HeapError, HeapKind, HeapLimits, MemoryList};
    let probe = HeapBudget::default();
    let mut values = MemoryList::new(probe.clone());
    values.try_push(1u64)?;
    let charged = probe.usage().retained_bytes;
    let budget = HeapBudget::new(HeapLimits {
        retained_bytes: charged,
        scratch_bytes: 1024,
        allocation_overhead: probe.limits().allocation_overhead,
    })?;
    let mut values = MemoryList::new(budget.clone());
    values.try_push(1u64)?;
    let retained = values.clone();
    assert_eq!(budget.usage().retained_bytes, charged);
    assert_eq!(values.try_push(2), Err(HeapError::Exhausted(HeapKind::Retained)));
    assert_eq!(&*values, &[1]);
    assert_eq!(&*retained, &[1]);
    drop(values);
    assert_eq!(budget.usage().retained_bytes, charged);
    drop(retained);
    assert_eq!(budget.usage().retained_bytes, 0);
    assert!(budget.usage().retained_peak_bytes <= charged);
    Ok(())
}

#[test]
fn source_scratch_and_error_maps_fail_without_successful_empty_samples() -> Result<(), Box<dyn std::error::Error>> {
    use wora_observability_linux_mm::{HeapBudget, HeapError, HeapKind, HeapLimits, MemoryReadError, with_heap_budget};
    let root = tempfile::tempdir()?;
    fs::write(root.path().join("value"), "12345678\n")?;
    let scratch = HeapBudget::new(HeapLimits {
        scratch_bytes: 8,
        allocation_overhead: 0,
        ..Default::default()
    })?;
    let (files, report) = with_heap_budget(&scratch, || {
        collect_with_limits(CollectionLimits::default(), || MemoryFiles::from_names(root.path(), &["value"]))
    })?;
    assert!(files.parsed.is_empty());
    assert_eq!(files.allocation_error, Some(HeapError::Exhausted(HeapKind::Scratch)));
    assert_eq!(
        files.unavailable.get("value"),
        Some(&MemoryReadError::Allocation(HeapError::Exhausted(HeapKind::Scratch)))
    );
    assert_eq!(report.limit_exceeded, Some(CollectionLimit::ScratchHeap));
    assert_eq!(scratch.usage().scratch_bytes, 0);
    drop(files);
    assert_eq!(scratch.usage().retained_bytes, 0);

    let retained = HeapBudget::new(HeapLimits {
        retained_bytes: 1,
        ..Default::default()
    })?;
    let (files, report) = with_heap_budget(&retained, || {
        collect_with_limits(CollectionLimits::default(), || MemoryFiles::from_names(root.path(), &["missing", "other"]))
    })?;
    assert!(files.parsed.is_empty());
    assert!(files.unavailable.is_empty());
    assert_eq!(files.allocation_error, Some(HeapError::Exhausted(HeapKind::Retained)));
    assert_eq!(report.limit_exceeded, Some(CollectionLimit::RetainedHeap));
    assert_eq!(retained.usage().retained_bytes, 0);
    assert!(MemoryFiles::try_from_names(root.path(), &["value"], 100, &retained).is_err());
    Ok(())
}

#[test]
fn parsed_storage_shares_credit_and_refusal_releases_partial_tables() -> Result<(), Box<dyn std::error::Error>> {
    use wora_observability_linux_mm::{HeapBudget, HeapLimits, parse_memory_file, with_heap_budget};
    let budget = HeapBudget::default();
    let value = with_heap_budget(&budget, || parse_memory_file("/proc/self/status", "Name:\ttest\nVmRSS:\t12 kB\n"))?.ok_or("missing parsed value")?;
    let bytes = budget.usage().retained_bytes;
    assert!(bytes > 0);
    let cloned = value.clone();
    assert_eq!(budget.usage().retained_bytes, bytes);
    drop(value);
    assert_eq!(budget.usage().retained_bytes, bytes);
    drop(cloned);
    assert_eq!(budget.usage().retained_bytes, 0);
    assert_eq!(budget.usage().scratch_bytes, 0);

    let small = HeapBudget::new(HeapLimits {
        retained_bytes: 1024,
        ..Default::default()
    })?;
    let text = (0..100).map(|key| format!("key{key} {key}\n")).collect::<String>();
    assert!(with_heap_budget(&small, || parse_memory_file("/proc/vmstat", &text)).is_err());
    assert_eq!(small.usage().retained_bytes, 0);
    assert_eq!(small.usage().scratch_bytes, 0);
    assert!(small.usage().retained_peak_bytes <= 1024);
    Ok(())
}
