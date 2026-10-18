#![cfg(target_os = "linux")]
use wora_observability_linux_mm::{KernelLeakStats, PageOwnerStats, ParsedMemoryValue};

// Synthetic records matching mm/page_owner.c in Linux 6.12 and 7.2.
const OWNER: &str = "Page allocated via order 2, mask 0x140cca(GFP_HIGHUSER_MOVABLE|__GFP_COMP), pid 12, tgid 9 (worker (pool)), ts 18446744073709551615 ns\nPFN 0x20000000000001 type Movable Block 77 type Unmovable Flags 0x17ffffc0000000(anon|swapbacked)\n alloc_pages+0x123/0x456\n caller+0x1/0x2 [test_module]\nPage has been migrated, last migrate reason: compaction\nSlab cache page\nCharged (via objcg) to offline memcg worker\n\n";

const LEAK: &str = "unreferenced object 0xffff000000000001 (size 9007199254740993):\n  comm \"worker\", pid 9, jiffies 18446744073709551615\n  hex dump (first 6 bytes):\n    53 45 43 52 45 54                                 SECRET\n  backtrace (crc ffffffff):\n    [<0000000000000000>] allocate+0x1/0x2 [module]\n";

#[test]
fn kmemleak_suspects_keep_uncertain_metadata_and_stacks_but_discard_object_bytes() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    // Native seq_file records need no blank separator. Newer variants add scope
    // and a per-CPU dump label; physical objects have no byte dump.
    let percpu = LEAK
        .replace("unreferenced object ", "unreferenced object (percpu) ")
        .replace("6 bytes):", "6 bytes on cpu 17):");
    let physical = "unreferenced object (phys) 0x0 (size 32):\n  comm \"init\", pid 0, jiffies 0\n  backtrace:\n    0x0000000000000000\n";
    std::fs::write(root.path().join("kmemleak"), format!("{LEAK}{percpu}{physical}"))?;
    let snapshot = KernelLeakStats::from_debug_root(root.path());
    let ParsedMemoryValue::List(records) = &snapshot.files.parsed["kmemleak"] else {
        return Err("no suspects".into());
    };
    assert_eq!(records.len(), 3);
    assert_eq!(records[0].get("address").and_then(ParsedMemoryValue::as_u64), Some(0xffff000000000001));
    assert_eq!(records[0].get("size_bytes").and_then(ParsedMemoryValue::as_u64), Some(9_007_199_254_740_993));
    assert_eq!(records[0].get("allocation_jiffies").and_then(ParsedMemoryValue::as_u64), Some(u64::MAX));
    assert_eq!(records[0].get("checksum").and_then(ParsedMemoryValue::as_u64), Some(u64::from(u32::MAX)));
    assert_eq!(records[1].get("address_space"), Some(&ParsedMemoryValue::try_text("percpu")?));
    assert_eq!(records[1].get("dump_cpu").and_then(ParsedMemoryValue::as_u64), Some(17));
    assert_eq!(records[2].get("address_space"), Some(&ParsedMemoryValue::try_text("physical")?));
    assert_eq!(records[2].get("address").and_then(ParsedMemoryValue::as_u64), Some(0));
    #[cfg(feature = "serde1")]
    {
        let json = serde_json::to_string(&snapshot)?;
        assert!(!json.contains("SECRET"));
        assert!(!json.contains("53 45 43"));
        let decoded: KernelLeakStats = serde_json::from_str(&json)?;
        assert_eq!(decoded.files.parsed["kmemleak"], snapshot.files.parsed["kmemleak"]);
    }
    assert_eq!(std::fs::read_to_string(root.path().join("kmemleak"))?, format!("{LEAK}{percpu}{physical}"));
    Ok(())
}

#[test]
fn kmemleak_rejects_truncated_malformed_or_oversized_dumps_without_partial_suspects() -> Result<(), Box<dyn std::error::Error>> {
    use wora_observability_linux_mm::parse_memory_file;
    for bad in [
        LEAK.replace(" (size 9007199254740993):", " (size -1):"),
        LEAK.replace("jiffies 18446744073709551615", "jiffies 18446744073709551616"),
        LEAK.replace("53 45 43 52 45 54", "zz 45 43 52 45 54"),
        LEAK.replace("first 6 bytes", "first 33 bytes"),
        LEAK.split("  backtrace").next().ok_or("no prefix")?.into(),
        LEAK.to_owned() + "unreferenced object 0xff (size 1):\n",
        LEAK.replace("    [<0000000000000000>] allocate+0x1/0x2 [module]\n", &"    allocate+0x1/0x2\n".repeat(257)),
    ] {
        assert!(parse_memory_file("kmemleak", &bad).is_err());
    }
    assert_eq!(parse_memory_file("kmemleak", "")?, Some(ParsedMemoryValue::try_list(Vec::new())?));
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("kmemleak"), LEAK)?;
    let limited = KernelLeakStats::from_debug_root_with_limit(root.path(), 8);
    assert!(!limited.files.parsed.contains_key("kmemleak"));
    assert!(limited.files.unavailable.contains_key("kmemleak"));
    std::fs::write(root.path().join("kmemleak"), LEAK.to_owned() + "bad record\n")?;
    let malformed = KernelLeakStats::from_debug_root(root.path());
    assert!(!malformed.files.parsed.contains_key("kmemleak"));
    Ok(())
}

#[test]
fn page_owner_records_preserve_allocation_page_stack_and_charge_scopes() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("page_owner"), OWNER.repeat(2))?;
    let snapshot = PageOwnerStats::from_debug_root(root.path());
    let ParsedMemoryValue::List(records) = &snapshot.files.parsed["page_owner"] else {
        return Err("no owner records".into());
    };
    assert_eq!(records.len(), 2);
    let allocation = records[0].get("allocation").ok_or("no allocation")?;
    assert_eq!(allocation.get("order").and_then(ParsedMemoryValue::as_u64), Some(2));
    assert_eq!(allocation.get("timestamp_ns").and_then(ParsedMemoryValue::as_u64), Some(u64::MAX));
    assert_eq!(allocation.get("comm"), Some(&ParsedMemoryValue::try_text("worker (pool)")?));
    let page = records[0].get("page").ok_or("no page")?;
    assert_eq!(page.get("pfn").and_then(ParsedMemoryValue::as_u64), Some(9_007_199_254_740_993));
    assert_eq!(page.get("block").and_then(ParsedMemoryValue::as_u64), Some(77));
    assert_eq!(records[0].get("migration_reason"), Some(&ParsedMemoryValue::try_text("compaction")?));
    let charge = records[0].get("charge").ok_or("no charge")?;
    assert_eq!(charge.get("offline").and_then(ParsedMemoryValue::as_bool), Some(true));
    assert_eq!(charge.get("via_objcg").and_then(ParsedMemoryValue::as_bool), Some(true));
    #[cfg(feature = "serde1")]
    {
        let json = serde_json::to_value(&snapshot)?;
        assert_eq!(json["files"]["parsed"]["page_owner"][0]["stack"][1], "caller+0x1/0x2 [test_module]");
        assert_eq!(json["files"]["parsed"]["page_owner"][0]["allocation"]["timestamp_ns"], u64::MAX);
    }
    assert_eq!(std::fs::read_to_string(root.path().join("page_owner"))?, OWNER.repeat(2));
    Ok(())
}

#[test]
fn page_owner_aggregate_layouts_keep_signed_handles_and_native_page_counts() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join("page_owner_stacks"))?;
    for (name, text) in [
        ("show_stacks", " alloc_pages+0x1/0x2\nnr_base_pages: 9007199254740993\n\n"),
        ("show_handles", "handle: -2147483648\nnr_base_pages: 13\n\nhandle: 5\nnr_base_pages: 2\n\n"),
        ("show_stacks_handles", " caller+0x1/0x2\nhandle: -1\n\n"),
    ] {
        std::fs::write(root.path().join("page_owner_stacks").join(name), text)?;
    }
    let snapshot = PageOwnerStats::from_debug_root(root.path());
    let ParsedMemoryValue::List(handles) = &snapshot.files.parsed["page_owner_stacks/show_handles"] else {
        return Err("no handles".into());
    };
    assert_eq!(handles.len(), 2);
    assert_eq!(handles[0].get("handle").and_then(ParsedMemoryValue::as_i64), Some(i64::from(i32::MIN)));
    assert_eq!(handles[0].get("nr_base_pages").and_then(ParsedMemoryValue::as_u64), Some(13));
    let ParsedMemoryValue::List(stacks) = &snapshot.files.parsed["page_owner_stacks/show_stacks"] else {
        return Err("no stacks".into());
    };
    assert_eq!(stacks[0].get("nr_base_pages").and_then(ParsedMemoryValue::as_u64), Some(9_007_199_254_740_993));
    let ParsedMemoryValue::List(joined) = &snapshot.files.parsed["page_owner_stacks/show_stacks_handles"] else {
        return Err("no joined stack".into());
    };
    assert_eq!(joined[0].get("handle").and_then(ParsedMemoryValue::as_i64), Some(-1));
    assert!(joined[0].get("nr_base_pages").is_none());
    Ok(())
}

#[test]
fn provenance_parsing_rejects_bad_or_partial_records_and_bounds_stack_growth() -> Result<(), Box<dyn std::error::Error>> {
    use wora_observability_linux_mm::parse_memory_file;
    for bad in [
        "Page allocated via order 0\nPFN 42 pid 9\n".to_owned(),
        OWNER.replace("0x140cca", "0xnot_hex"),
        OWNER.replace("pid 12", "pid 2147483648"),
        OWNER.replace("type Movable Block 77", "type Movable Block -1"),
        OWNER.to_owned() + "unknown new record\n",
        OWNER.replace(" alloc_pages+0x123/0x456\n", &" frame+0x1/0x2\n".repeat(257)),
    ] {
        assert!(parse_memory_file("page_owner", &bad).is_err());
    }
    for (name, bad) in [
        ("show_stacks", " frame+0x1/0x2\n"),
        ("show_handles", "handle: 1\nnr_base_pages: -1\n"),
        ("show_stacks_handles", " frame+0x1/0x2\nhandle: 1\nnr_base_pages: 1\n"),
        ("show_handles", "handle: 1\nnr_base_pages: 2\n\nhandle: bad\n"),
    ] {
        assert!(parse_memory_file(name, bad).is_err());
    }
    assert!(parse_memory_file("show_stacks", &(" ".to_owned() + &"x".repeat(4096))).is_err());
    assert_eq!(parse_memory_file("page_owner", "\n\n")?, Some(ParsedMemoryValue::try_list(Vec::new())?));
    let root = tempfile::tempdir()?;
    std::fs::write(root.path().join("page_owner"), OWNER.to_owned() + "unknown record\n")?;
    let snapshot = PageOwnerStats::from_debug_root(root.path());
    assert!(!snapshot.files.parsed.contains_key("page_owner"));
    assert!(snapshot.files.unavailable.contains_key("page_owner"));
    Ok(())
}

#[test]
fn provenance_is_bounded_and_does_not_change_tracking_or_filters() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    std::fs::create_dir(root.path().join("page_owner_stacks"))?;
    let dump = "Page allocated via order 0\nPFN 42 pid 9\n allocation_stack+0x1\n";
    std::fs::write(root.path().join("page_owner"), dump)?;
    std::fs::write(root.path().join("page_owner_stacks/count_threshold"), "100\n")?;
    std::fs::write(root.path().join("kmemleak"), "unreferenced object 0xffff0000 (size 32):\n backtrace\n")?;
    let owner = PageOwnerStats::from_debug_root_with_limit(root.path(), 8);
    assert!(!owner.files.parsed.contains_key("page_owner"));
    assert!(owner.files.unavailable.contains_key("page_owner"));
    assert_eq!(owner.files.parsed["page_owner_stacks/count_threshold"], ParsedMemoryValue::Unsigned(100));
    let leaks = KernelLeakStats::from_debug_root(root.path());
    assert!(!leaks.files.parsed.contains_key("kmemleak"));
    assert!(leaks.files.unavailable.contains_key("kmemleak"));
    assert_eq!(std::fs::read_to_string(root.path().join("page_owner"))?, dump);
    assert_eq!(std::fs::read_to_string(root.path().join("page_owner_stacks/count_threshold"))?, "100\n");
    assert!(PageOwnerStats::from_debug_root_with_limit(root.path(), 0).files.parsed.is_empty());
    Ok(())
}
