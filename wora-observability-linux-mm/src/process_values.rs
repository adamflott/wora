use crate::storage::CollectMemory;
use crate::structured::{atom, bitmap, hexadecimal, invalid, map, number, put, ranges, words};
use crate::{MemoryMapping, MemoryReadError, ParsedMemoryValue as Value};

pub(crate) fn status(text: &str) -> Result<Value, MemoryReadError> {
    let mut fields = crate::MemoryMap::default();
    for line in text.lines().filter(|v| !v.trim().is_empty()) {
        let (name, value) = line.split_once(':').ok_or_else(|| invalid("malformed process status"))?;
        let value = value.trim();
        let parsed = match name {
            "Name" => Value::try_text(value)?,
            "Umask" => Value::Unsigned(u64::from_str_radix(value, 8).map_err(|_| invalid("invalid process umask"))?),
            "Uid" | "Gid" => roles(value)?,
            "Groups" | "NStgid" | "NSpid" | "NSpgid" | "NSsid" => Value::List(words(value)?.iter().map(|v| number(v)).collect_memory()?),
            "Cpus_allowed" | "Mems_allowed" => bitmap(value)?,
            "Cpus_allowed_list" | "Mems_allowed_list" => ranges(value)?,
            "State" => {
                let (code, description) = value.split_once(char::is_whitespace).ok_or_else(|| invalid("invalid process state"))?;
                map([
                    ("code", Value::try_text(code)?),
                    (
                        "description",
                        Value::try_text(description.trim().trim_start_matches('(').trim_end_matches(')'))?,
                    ),
                ])?
            }
            "SigQ" => {
                let (queued, limit) = value.split_once('/').ok_or_else(|| invalid("invalid queued signal count"))?;
                map([("queued", number(queued)?), ("limit", number(limit)?)])?
            }
            name if name.starts_with("Cap") || matches!(name, "SigPnd" | "ShdPnd" | "SigBlk" | "SigIgn" | "SigCgt") => hexadecimal(value)?,
            _ => atom(value)?,
        };
        put(&mut fields, name, parsed)?;
    }
    Ok(Value::Map(fields))
}

fn roles(text: &str) -> Result<Value, MemoryReadError> {
    let fields = words(text)?;
    if fields.len() != 4 {
        return Err(invalid("expected four process identity values"));
    }
    let mut result = crate::MemoryMap::default();
    for (name, value) in ["real", "effective", "saved", "filesystem"].iter().zip(fields.iter()) {
        result.try_insert(*name, number(value)?)?;
    }
    Ok(Value::Map(result))
}

pub(crate) fn limits(text: &str) -> Result<Value, MemoryReadError> {
    let mut result = crate::MemoryMap::default();
    let mut lines = text.lines().filter(|v| !v.trim().is_empty());
    if !lines
        .next()
        .is_some_and(|line| line.starts_with("Limit") && line.contains("Soft Limit") && line.contains("Hard Limit"))
    {
        return Err(invalid("missing process limits header"));
    }
    for line in lines {
        let fields = words(line)?;
        let index = fields
            .iter()
            .position(|v| *v == "unlimited" || v.bytes().all(|b| b.is_ascii_digit()))
            .ok_or_else(|| invalid("missing process limit values"))?;
        if index == 0 || fields.len() < index + 2 {
            return Err(invalid("incomplete process limits row"));
        }
        let limit = |text: &str| if text == "unlimited" { Ok(Value::try_text(text)?) } else { number(text) };
        let mut values = crate::MemoryMap::try_from_iter([("soft", limit(fields[index])?), ("hard", limit(fields[index + 1])?)])?;
        if fields.len() > index + 2 {
            values.try_insert("unit", Value::try_text(fields[index + 2..].join(" "))?)?;
        }
        put(&mut result, &fields[..index].join(" "), Value::Map(values))?;
    }
    Ok(Value::Map(result))
}

pub(crate) fn stat(text: &str) -> Result<Value, MemoryReadError> {
    let open = text.find('(').ok_or_else(|| invalid("missing process command"))?;
    let close = text
        .rfind(')')
        .filter(|end| *end > open)
        .ok_or_else(|| invalid("missing process command delimiter"))?;
    let tail = words(&text[close + 1..])?;
    if tail.len() < 22 || tail[0].len() != 1 {
        return Err(invalid("incomplete process stat accounting"));
    }
    let mut result = crate::MemoryMap::try_from_iter([
        ("pid", number(text[..open].trim())?),
        ("comm", Value::try_text(&text[open + 1..close])?),
        ("state", Value::try_text(tail[0])?),
    ])?;
    const NAMES: &[&str] = &[
        "ppid",
        "pgrp",
        "session",
        "tty_nr",
        "tpgid",
        "flags",
        "minflt",
        "cminflt",
        "majflt",
        "cmajflt",
        "utime",
        "stime",
        "cutime",
        "cstime",
        "priority",
        "nice",
        "num_threads",
        "itrealvalue",
        "starttime",
        "vsize",
        "rss",
        "rsslim",
        "startcode",
        "endcode",
        "startstack",
        "kstkesp",
        "kstkeip",
        "signal",
        "blocked",
        "sigignore",
        "sigcatch",
        "wchan",
        "nswap",
        "cnswap",
        "exit_signal",
        "processor",
        "rt_priority",
        "policy",
        "delayacct_blkio_ticks",
        "guest_time",
        "cguest_time",
        "start_data",
        "end_data",
        "start_brk",
        "arg_start",
        "arg_end",
        "env_start",
        "env_end",
        "exit_code",
    ];
    for (name, value) in NAMES.iter().zip(tail.iter().skip(1)) {
        result.try_insert(*name, number(value)?)?;
    }
    if tail.len() > NAMES.len() + 1 {
        result.try_insert("extra", Value::List(tail.iter().skip(NAMES.len() + 1).map(|v| number(v)).collect_memory()?))?;
    }
    Ok(Value::Map(result))
}

pub(crate) fn numa_maps(text: &str) -> Result<Value, MemoryReadError> {
    let mut records = crate::MemoryList::default();
    for line in text.lines().filter(|v| !v.trim().is_empty()) {
        let fields = words(line)?;
        if fields.len() < 2 {
            return Err(invalid("incomplete numa_maps record"));
        }
        let mut values = crate::MemoryMap::default();
        let mut nodes = crate::MemoryMap::default();
        for field in fields.iter().skip(2) {
            if let Some((name, value)) = field.split_once('=') {
                if let Some(node) = name.strip_prefix('N').filter(|v| !v.is_empty() && v.bytes().all(|b| b.is_ascii_digit())) {
                    let node = node.parse::<u32>().map_err(|_| invalid("numa_maps node ID overflow"))?;
                    put(&mut nodes, &node.to_string(), number(value)?)?;
                } else {
                    put(&mut values, name, if name == "file" { Value::try_text(value)? } else { atom(value)? })?;
                }
            } else {
                put(&mut values, field, Value::Bool(true))?;
            }
        }
        records.try_push(map([
            ("address", hexadecimal(fields[0])?),
            ("policy", Value::try_text(fields[1])?),
            ("nodes", Value::Map(nodes)),
            ("fields", Value::Map(values)),
        ])?)?;
        if records.len() > 65536 {
            return Err(invalid("numa_maps exceeds 65536 records"));
        }
    }
    Ok(Value::List(records))
}

pub(crate) fn mappings(text: &str) -> Result<Value, MemoryReadError> {
    let mappings = MemoryMapping::parse_smaps(text)?;
    let records = mappings
        .into_iter()
        .map(|mapping| {
            map([
                ("start", Value::Unsigned(mapping.address_range.0)),
                ("end", Value::Unsigned(mapping.address_range.1)),
                ("permissions", Value::Text(mapping.permissions)),
                ("offset", Value::Unsigned(mapping.offset)),
                (
                    "device",
                    map([("major", Value::Unsigned(mapping.device.0)), ("minor", Value::Unsigned(mapping.device.1))])?,
                ),
                ("inode", Value::Unsigned(mapping.inode)),
                ("pathname", Value::Text(mapping.pathname)),
                ("fields", Value::Map(mapping.fields)),
            ])
        })
        .collect_memory()?;
    Ok(Value::List(records))
}

pub(crate) fn vmalloc(text: &str) -> Result<Value, MemoryReadError> {
    let mut records = crate::MemoryList::default();
    for line in text.lines().filter(|v| !v.trim().is_empty()) {
        let (fields, rest) = prefix(line, 2)?;
        let (start, end) = fields[0].split_once('-').ok_or_else(|| invalid("invalid vmalloc address range"))?;
        let mut record = crate::MemoryMap::try_from_iter([("start", hexadecimal(start)?), ("end", hexadecimal(end)?), ("size_bytes", number(fields[1])?)])?;
        let mut flags = crate::MemoryList::default();
        let mut attributes = crate::MemoryMap::default();
        let mut nodes = crate::MemoryMap::default();
        for (index, field) in words(rest)?.into_iter().enumerate() {
            if let Some((key, value)) = field.split_once('=') {
                if let Some(node) = key.strip_prefix('N').filter(|v| !v.is_empty() && v.bytes().all(|b| b.is_ascii_digit())) {
                    put(&mut nodes, node, number(value)?)?;
                } else {
                    put(&mut attributes, key, if key == "phys" { hexadecimal(value)? } else { atom(value)? })?;
                }
            } else if index == 0 && !matches!(field, "vmalloc" | "vmap" | "ioremap" | "user" | "vpages") {
                record.try_insert("caller", Value::try_text(field)?)?;
            } else {
                flags.try_push(Value::try_text(field)?)?;
            }
        }
        record.try_insert("flags", Value::List(flags))?;
        record.try_insert("attributes", Value::Map(attributes))?;
        record.try_insert("nodes", Value::Map(nodes))?;
        records.try_push(Value::Map(record))?;
        if records.len() > 65536 {
            return Err(invalid("vmalloc table exceeds 65536 records"));
        }
    }
    Ok(Value::List(records))
}

pub(crate) fn prefix(mut text: &str, count: usize) -> Result<(crate::MemoryList<&str>, &str), MemoryReadError> {
    let mut fields = crate::MemoryList::scratch(crate::HeapBudget::current());
    for _ in 0..count {
        text = text.trim_start();
        let end = text.find(char::is_whitespace).unwrap_or(text.len());
        if end == 0 {
            return Err(invalid("incomplete table row"));
        }
        fields.try_push(&text[..end])?;
        text = &text[end..];
    }
    Ok((fields, text.trim_start()))
}
