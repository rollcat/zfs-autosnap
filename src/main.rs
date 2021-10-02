use byte_unit::Byte;
use chrono::prelude::*;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;

use zfs_autosnap::zfs::SnapshotMetadata;
use zfs_autosnap::{zfs, Result, RetentionPolicy, PROPERTY_SNAPKEEP};

const VERSION: &'static str = env!("CARGO_PKG_VERSION");

#[derive(Debug)]
struct AgeCheckResult {
    keep: Vec<SnapshotMetadata>,
    delete: Vec<SnapshotMetadata>,
}

fn check_age(snapshots: &mut [SnapshotMetadata], rp: RetentionPolicy) -> AgeCheckResult {
    let mut to_keep = HashSet::<&SnapshotMetadata>::new();
    // Sort newest snapshots first, so when we consider which ones to retain, the oldest
    // come last (and fall off the keep-set).
    snapshots.sort_unstable_by_key(|s| -s.created.timestamp());
    for (pattern, rule) in rp.rules() {
        // RetentionPolicy.rules() creates a set of date format patterns (see strftime(3)),
        // which are meant to be lossy/fuzzy (e.g. year-month-day; year-week, etc).
        let mut last = None;
        match rule {
            Some(0) => {}
            Some(number_to_keep) => {
                let mut kept = 0;
                for snapshot in snapshots.iter() {
                    // We use these date patterns to format each snapshot's creation date, to
                    // put it in an ad-hoc bucket (last / period); then keep track of how many
                    // snapshots (kept) we've retained so far for the current bucket.
                    let period = Some(snapshot.created.format(pattern).to_string());
                    if last != period {
                        last = period;
                        to_keep.insert(snapshot);
                        kept += 1;
                        if kept == number_to_keep {
                            // This is as many snapshots as we wanted to
                            // keep, let's visit the next retention rule.
                            break;
                        }
                    }
                }
            }
            None => {}
        }
    }

    let (keep, delete): (Vec<_>, Vec<_>) = snapshots
        .iter()
        .partition(|snapshot| to_keep.contains(snapshot));
    AgeCheckResult {
        keep: keep.into_iter().cloned().collect(),
        delete: delete.into_iter().cloned().collect(),
    }
}

fn gc_find() -> Result<AgeCheckResult> {
    // List all snapshots we're interested in, group them by dataset, check them against
    // their parent dataset's retention policy, and aggregate them into the final result,
    // which can be presented to the user (do_status()) or the garbage collector (do_gc()).
    let snapshots = zfs::list_snapshots()?;
    let mut by_dataset = HashMap::<String, Vec<SnapshotMetadata>>::new();
    for snapshot in snapshots {
        if let Some(dataset_name) = snapshot.name.split("@").next() {
            let group = by_dataset.entry(dataset_name.to_string()).or_insert(vec![]);
            group.push(snapshot);
        }
    }
    let mut keep = vec![];
    let mut delete = vec![];
    for (key, group) in by_dataset.iter_mut() {
        let check = check_age(
            group,
            RetentionPolicy::from_str(&zfs::get_property(key, PROPERTY_SNAPKEEP)?)
                .map_err(|()| "unable to parse retention policy")?,
        );
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
        "zfs-autosnap v{} <https://github.com/rollcat/zfs-autosnap>",
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
    let action = &args.get(1).and_then(|s| Some(s.as_str()));
    match action {
        None | Some("help" | "-h" | "--help") => {
            do_help();
            Ok(())
        }
        Some("version" | "-v" | "--version") => {
            do_version();
            Ok(())
        }
        Some("status") => {
            do_status()?;
            Ok(())
        }
        Some("snap") => {
            do_snap()?;
            Ok(())
        }
        Some("gc") => {
            do_gc()?;
            Ok(())
        }
        _ => {
            do_help();
            std::process::exit(111);
        }
    }
}
