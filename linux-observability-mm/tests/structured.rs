#![cfg(target_os = "linux")]

use std::{collections::BTreeMap, fs};
use wora_observability_linux_mm::{
    CgroupMemory, CgroupMemoryValue, HugePageFiles, KsmStats, MemoryFiles, MemorySysctls, ParsedMemoryValue as Value, parse_memory_file, parse_sysctl_value,
};

fn field<'a>(value: &'a Value, key: &str) -> Result<&'a Value, Box<dyn std::error::Error>> {
    value.get(key).ok_or_else(|| format!("missing structured field {key}").into())
}

#[test]
fn user_ksm_example_is_a_named_record_with_booleans_and_exact_counts() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let text = "ksm_rmap_items 0\nksm_zero_pages 0\nksm_merging_pages 0\nksm_process_profit -9223372036854775808\nksm_merge_any: no\nksm_mergeable: yes\nfuture_pages 18446744073709551615\n";
    fs::write(root.path().join("ksm_stat"), text)?;
    let snapshot = KsmStats::from_roots(root.path(), root.path(), 42);
    let value = &snapshot.process.parsed["ksm_stat"];
    assert_eq!(field(value, "ksm_rmap_items")?.as_u64(), Some(0));
    assert_eq!(field(value, "ksm_merge_any")?.as_bool(), Some(false));
    assert_eq!(field(value, "ksm_mergeable")?.as_bool(), Some(true));
    assert_eq!(field(value, "ksm_process_profit")?.as_i64(), Some(i64::MIN));
    assert_eq!(field(value, "future_pages")?.as_u64(), Some(u64::MAX));
    assert_eq!(field(value, "future_pages")?.as_i64(), None);
    #[cfg(feature = "serde1")]
    {
        let json = serde_json::to_value(snapshot)?;
        assert_eq!(json["process"]["parsed"]["ksm_stat"]["ksm_merge_any"], false);
        assert_eq!(json["process"]["parsed"]["ksm_stat"]["future_pages"], u64::MAX);
        assert!(json["process"].get("values").is_none());
    }
    Ok(())
}

#[test]
fn sysctls_have_named_vector_fields_reserved_slots_signed_scalars_and_arrays() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    for (name, value) in [
        ("fs/dentry-state", "317916\t300300\t45\t0\t34063\t0\t123"),
        ("fs/file-nr", "40 0 18446744073709551615"),
        ("fs/inode-nr", "100 10"),
        ("fs/inode-state", "100 10 0 0 0 0 0"),
        ("kernel/shmall", "18446744073692774399"),
        ("kernel/shm_next_id", "-1"),
        ("vm/lowmem_reserve_ratio", "256 256 32 0"),
        ("vm/future_vector", "-1 0 18446744073709551615"),
        ("vm/numa_zonelist_order", "Node"),
        ("vm/swappiness", "0"),
    ] {
        let path = root.path().join("sys").join(name);
        fs::create_dir_all(path.parent().ok_or("no parent")?)?;
        fs::write(path, value)?;
    }
    let snapshot = MemorySysctls::from_proc_root(root.path());
    let dentry = &snapshot.parsed["fs.dentry-state"];
    assert_eq!(field(dentry, "nr_dentry")?.as_u64(), Some(317916));
    assert_eq!(field(dentry, "nr_unused")?.as_u64(), Some(300300));
    assert_eq!(field(dentry, "age_limit")?.as_u64(), Some(45));
    assert_eq!(field(dentry, "nr_negative")?.as_u64(), Some(34063));
    assert_eq!(field(dentry, "reserved")?, &Value::try_list(vec![Value::Unsigned(0)])?);
    assert_eq!(field(dentry, "extra")?, &Value::try_list(vec![Value::Unsigned(123)])?);
    assert_eq!(field(&snapshot.parsed["fs.file-nr"], "max_files")?.as_u64(), Some(u64::MAX));
    assert_eq!(snapshot.parsed["kernel.shm_next_id"].as_i64(), Some(-1));
    assert_eq!(snapshot.parsed["vm.swappiness"], Value::Unsigned(0));
    assert_eq!(snapshot.parsed["vm.numa_zonelist_order"], Value::try_text("Node")?);
    assert_eq!(
        parse_sysctl_value("vm.lowmem_reserve_ratio", "256")?,
        Value::try_list(vec![Value::Unsigned(256)])?
    );
    #[cfg(feature = "serde1")]
    {
        let json = serde_json::to_value(&snapshot)?;
        assert_eq!(json["parsed"]["vm.future_vector"], serde_json::json!([-1, 0, u64::MAX]));
        assert!(json.get("values").is_none());
        assert_eq!(serde_json::from_value::<MemorySysctls>(json)?, snapshot);
    }
    Ok(())
}

#[test]
fn malformed_known_formats_fail_atomically_without_retaining_raw_values() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    for text in [
        "ksm_rmap_items 3\nksm_rmap_items 4\n",
        "ksm_merge_any: maybe\n",
        "ksm_rmap_items 18446744073709551616\n",
        "ksm_process_profit -9223372036854775809\n",
    ] {
        fs::write(root.path().join("ksm_stat"), text)?;
        let snapshot = MemoryFiles::from_names(root.path(), &["ksm_stat"]);
        assert!(!snapshot.parsed.contains_key("ksm_stat"));
        assert!(snapshot.unavailable.contains_key("ksm_stat"));
    }
    for (name, text) in [
        ("fs.dentry-state", "1 2 3"),
        ("fs.file-nr", "1 broken 3"),
        ("fs.inode-state", "1 2 3"),
        ("vm.future", "18446744073709551616"),
        ("vm.lowmem_reserve_ratio", ""),
    ] {
        assert!(parse_sysctl_value(name, text).is_err(), "{name}: {text}");
    }
    for (name, text) in [
        ("cpulist", "0,,3"),
        ("cpulist", "3-1"),
        ("cpulist", "1-3,2-4"),
        ("cpulist", "+1"),
        ("cpumap", "100000000"),
        ("enabled", "[always] [never]"),
    ] {
        assert!(parse_memory_file(name, text).is_err(), "{name}: {text}");
    }
    Ok(())
}

#[test]
fn numa_policies_units_masks_and_node_counts_keep_their_meanings() -> Result<(), Box<dyn std::error::Error>> {
    let meminfo = parse_memory_file("meminfo", "Node 9 MemFree: 9007199254740993 kB\nNode 9 HugePages_Total: 3\n")?.ok_or("no meminfo")?;
    assert_eq!(field(field(&meminfo, "MemFree")?, "value")?.as_u64(), Some(9_007_199_254_740_993));
    assert_eq!(field(field(&meminfo, "MemFree")?, "unit")?, &Value::try_text("kB")?);
    let policy = parse_memory_file("enabled", "always [madvise] never")?.ok_or("no policy")?;
    assert_eq!(field(&policy, "selected")?, &Value::try_text("madvise")?);
    assert_eq!(parse_memory_file("block_size_bytes", "8000000")?, Some(Value::Unsigned(0x8000000)));
    assert_eq!(parse_memory_file("phys_index", "0000001a")?, Some(Value::Unsigned(26)));
    assert_eq!(
        parse_memory_file("cpumap", "00000001,80000000")?,
        Some(Value::try_map(BTreeMap::from([(
            "words",
            Value::try_list(vec![Value::Unsigned(1), Value::Unsigned(0x80000000)])?
        )]))?)
    );
    let nodes = parse_memory_file("objects", "100 N0=25 N9=75")?.ok_or("no counts")?;
    assert_eq!(field(field(&nodes, "nodes")?, "9")?.as_u64(), Some(75));
    let size = parse_memory_file("demote_size", "32768kB")?.ok_or("no size")?;
    assert_eq!(field(&size, "value")?.as_u64(), Some(32768));
    assert_eq!(parse_memory_file("shrinker_enabled", "N")?, Some(Value::Bool(false)));
    assert_eq!(parse_memory_file("dimm_label", "[DIMM_A1]")?, Some(Value::try_text("[DIMM_A1]")?));
    assert!(parse_memory_file("page_owner", "Page allocated via order 0\n allocation_stack+0x1\n").is_err());
    Ok(())
}

#[test]
fn recorded_process_files_parse_status_limits_faults_and_numa_placement() -> Result<(), Box<dyn std::error::Error>> {
    let status = parse_memory_file("status", include_str!("fixtures/process-information-status"))?.ok_or("no status")?;
    assert_eq!(field(field(&status, "Uid")?, "real")?.as_u64(), Some(1000));
    assert_eq!(field(field(&status, "VmRSS")?, "unit")?, &Value::try_text("kB")?);
    let limits = parse_memory_file("limits", include_str!("fixtures/process-information-limits"))?.ok_or("no limits")?;
    assert!(field(field(&limits, "Max locked memory")?, "soft")?.as_u64().is_some());
    let stat = parse_memory_file("stat", include_str!("fixtures/process-information-stat"))?.ok_or("no stat")?;
    assert_eq!(field(&stat, "pid")?.as_u64(), Some(7));
    assert!(field(&stat, "minflt")?.as_u64().is_some());
    assert!(field(&stat, "vsize")?.as_u64().is_some());
    let Value::List(mappings) = parse_memory_file("numa_maps", include_str!("fixtures/process-information-numa_maps"))?.ok_or("no mappings")? else {
        return Err("expected numa mappings".into());
    };
    assert!(!mappings.is_empty());
    assert!(field(&mappings[0], "nodes")?.get("0").is_some());
    let status = parse_memory_file(
        "status",
        "Name:\t123\nUmask:\t0022\nCapBnd:\tffffffffffffffff\nMems_allowed_list:\t0-4294967295\n",
    )?
    .ok_or("no status")?;
    assert_eq!(field(&status, "Name")?, &Value::try_text("123")?);
    assert_eq!(field(&status, "Umask")?.as_u64(), Some(0o22));
    assert_eq!(field(&status, "CapBnd")?.as_u64(), Some(u64::MAX));
    Ok(())
}

#[test]
fn per_mapping_accounting_block_vectors_and_vmalloc_areas_are_structured() -> Result<(), Box<dyn std::error::Error>> {
    let smaps = "1000-3000 rw-p 00001000 00:09 42 /path with  two spaces\nRss: 12 kB\nFuture: 9 pages\nVmFlags: rd wr zz\n";
    let Value::List(mappings) = parse_memory_file("smaps", smaps)?.ok_or("no mappings")? else {
        return Err("expected mappings".into());
    };
    assert_eq!(field(&mappings[0], "pathname")?, &Value::try_text("/path with  two spaces")?);
    assert_eq!(field(field(field(&mappings[0], "fields")?, "Rss")?, "value")?.as_u64(), Some(12));
    let io = parse_memory_file("io_stat", "1 2 3 4 5")?.ok_or("no io stats")?;
    assert_eq!(field(&io, "failed_reads")?.as_u64(), Some(1));
    assert_eq!(field(&io, "extra")?, &Value::try_list(vec![Value::Unsigned(5)])?);
    let Value::List(areas) = parse_memory_file("vmallocinfo", "0x00000000-0x00000000 8192 caller+0x4 pages=2 vmalloc N9=2\n")?.ok_or("no areas")? else {
        return Err("expected areas".into());
    };
    assert_eq!(field(&areas[0], "size_bytes")?.as_u64(), Some(8192));
    assert_eq!(field(field(&areas[0], "nodes")?, "9")?.as_u64(), Some(2));
    Ok(())
}

#[test]
fn cgroup_node_constraints_and_hugepage_policy_are_emitted_as_structures() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    fs::write(root.path().join("cpuset.mems.effective"), "0-1,9")?;
    fs::write(root.path().join("hugetlb.2MB.numa_stat"), "total=4096 N0=0 N9=4096")?;
    let group = CgroupMemory::from_directory(root.path());
    assert!(matches!(group.parsed["cpuset.mems.effective"], CgroupMemoryValue::Structured(Value::List(_))));
    let CgroupMemoryValue::Structured(numa) = &group.parsed["hugetlb.2MB.numa_stat"] else {
        return Err("no NUMA structure".into());
    };
    assert_eq!(field(numa, "N9")?.as_u64(), Some(4096));
    fs::write(root.path().join("enabled"), "always [madvise] never")?;
    let huge = HugePageFiles::from_directory(root.path());
    assert_eq!(field(&huge.parsed["enabled"], "selected")?, &Value::try_text("madvise")?);
    Ok(())
}

#[cfg(feature = "serde1")]
#[test]
fn raw_only_snapshots_are_rejected_and_parsed_values_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    let old = serde_json::json!({"values":{"vm.swappiness":"60"}, "unavailable":{}, "skipped":{}});
    assert!(serde_json::from_value::<MemorySysctls>(old.clone()).is_err());
    assert!(serde_json::from_value::<MemoryFiles>(old).is_err());
    let tree = parse_sysctl_value("vm.future", "-1 18446744073709551615 0")?;
    assert_eq!(serde_json::from_value::<Value>(serde_json::to_value(&tree)?)?, tree);
    assert_eq!(parse_sysctl_value("vm.future", "-0")?, Value::Unsigned(0));
    Ok(())
}
