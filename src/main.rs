use byte_unit::Byte;
use chrono::prelude::*;
use std::collections::HashMap;
use std::str::FromStr;

use zfs_autosnap::zfs::SnapshotMetadata;
use zfs_autosnap::{zfs, AgeCheckResult, Result, RetentionPolicy, PROPERTY_SNAPKEEP};

const VERSION: &str = env!("CARGO_PKG_VERSION");

fn gc_find() -> Result<AgeCheckResult> {
    // List all snapshots we're interested in, group them by dataset, check them against
    // their parent dataset's retention policy, and aggregate them into the final result,
    // which can be presented to the user (do_status()) or the garbage collector (do_gc()).
    let snapshots = zfs::list_snapshots()?;
    let mut by_dataset = HashMap::<String, Vec<SnapshotMetadata>>::new();
    for snapshot in snapshots {
        if let Some(dataset_name) = snapshot.name.split('@').next() {
            let group = by_dataset
                .entry(dataset_name.to_string())
                .or_insert_with(Vec::new);
            group.push(snapshot);
        }
    }
    let mut keep = vec![];
    let mut delete = vec![];
    for (key, group) in by_dataset.iter_mut() {
        let policy = RetentionPolicy::from_str(&zfs::get_property(key, PROPERTY_SNAPKEEP)?)
            .map_err(|()| "unable to parse retention policy")?;
        let check = policy.check_age(group);
        keep.extend(check.keep);
        delete.extend(check.delete);
    }
    Ok(AgeCheckResult { keep, delete })
}

fn do_help() {
    println!("Usage:");
    println!("    zfs-autosnap <status | snap | gc | help | version>");
    println!("Tips:");
    println!("    use 'zfs set at.rollc.at:snapkeep=h24d30w8m6y1 some/dataset' to enable.");
    println!("    use 'zfs set at.rollc.at:snapkeep=- some/dataset@some-snap' to retain.");
    println!("    add 'zfs-autosnap snap' to cron.hourly.");
    println!("    add 'zfs-autosnap gc'   to cron.daily.");
    do_version();
}

fn do_version() {
    println!(
        "zfs-autosnap v{} <https://github.com/wezm/zfs-autosnap>",
        VERSION
    );
}

fn do_status() -> Result<()> {
    // Present a nice summary to the user.
    let check = gc_find()?;
    if !check.keep.is_empty() {
        println!(
            "keep: {}",
            Byte::from_bytes(check.keep.iter().map(|s| s.used.get_bytes()).sum::<u128>())
                .get_appropriate_unit(true)
        );
        for s in check.keep {
            println!(
                "keep: {}\t{}\t{}",
                s.name,
                s.created.to_rfc3339_opts(SecondsFormat::Secs, true),
                s.used.get_appropriate_unit(true)
            );
        }
    }
    if !check.delete.is_empty() {
        println!(
            "delete: {}",
            Byte::from_bytes(
                check
                    .delete
                    .iter()
                    .map(|s| s.used.get_bytes())
                    .sum::<u128>()
            )
            .get_appropriate_unit(true)
        );
        for s in check.delete {
            println!(
                "delete: {}\t{}\t{}",
                s.name,
                s.created.to_rfc3339_opts(SecondsFormat::Secs, true),
                s.used.get_appropriate_unit(true)
            );
        }
    }
    Ok(())
}

fn do_snap() -> Result<()> {
    // Perform a snapshot of each managed dataset.
    for dataset in &zfs::list_datasets_for_snapshot()? {
        let s = zfs::snapshot(dataset)?;
        println!("snapshot: {}", s.name);
    }
    Ok(())
}

fn do_gc() -> Result<()> {
    // Garbage collection. Find all snapshots to delete, and delete them without asking
    // twice. If you need to only check the status, use do_status.
    let check = gc_find()?;
    if !check.delete.is_empty() {
        println!(
            "delete: {}",
            Byte::from_bytes(
                check
                    .delete
                    .iter()
                    .map(|s| s.used.get_bytes())
                    .sum::<u128>()
            )
            .get_appropriate_unit(true)
        );
    }
    for s in check.delete {
        println!(
            "delete: {}\t{}\t{}",
            s.name,
            s.created.to_rfc3339_opts(SecondsFormat::Secs, true),
            s.used.get_appropriate_unit(true)
        );
        zfs::destroy_snapshot(s)?;
    }
    Ok(())
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let action = &args.get(1).map(|s| s.as_str());
    match action {
        None | Some("help" | "-h" | "--help") => {
            do_help();
            Ok(())
        }
        Some("version" | "-v" | "--version") => {
            do_version();
            Ok(())
        }
        Some("status") => do_status(),
        Some("snap") => do_snap(),
        Some("gc") => do_gc(),
        _ => {
            do_help();
            std::process::exit(111);
        }
    }
}
