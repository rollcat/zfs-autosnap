use anyhow::Result;
use byte_unit::Byte;
use chrono::prelude::*;
use lazy_static::lazy_static;
use regex::Regex;
use std::collections::{HashMap, HashSet};

const VERSION: &'static str = env!("CARGO_PKG_VERSION");

// We use this property to control the retention policy.  Check readme.md, but also
// check_age, ZFS::list_snapshots, and ZFS::list_datasets_for_snapshot.
const PROPERTY_SNAPKEEP: &str = "at.rollc.at:snapkeep";

// Describes the number of snapshots to keep for each period.
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
struct RetentionPolicy {
    yearly: Option<i32>,
    monthly: Option<u32>,
    weekly: Option<u32>,
    daily: Option<u32>,
    hourly: Option<u32>,
}

impl RetentionPolicy {
    fn parse(x: &str) -> Option<RetentionPolicy> {
        lazy_static! {
            static ref RE: Regex = Regex::new("[hdwmy][0-9]+").unwrap();
        }
        let mut rp = RetentionPolicy {
            yearly: None,
            monthly: None,
            weekly: None,
            daily: None,
            hourly: None,
        };
        for cap in RE.captures_iter(x) {
            match cap.get(0) {
                Some(m) => {
                    let s = &m.as_str();
                    let prefix = s.chars().nth(0).unwrap();
                    let value = s.get(1..).unwrap();
                    match prefix {
                        'y' => {
                            rp.yearly = Some(value.parse::<i32>().unwrap());
                        }
                        'm' => {
                            rp.monthly = Some(value.parse::<u32>().unwrap());
                        }
                        'w' => {
                            rp.weekly = Some(value.parse::<u32>().unwrap());
                        }
                        'd' => {
                            rp.daily = Some(value.parse::<u32>().unwrap());
                        }
                        'h' => {
                            rp.hourly = Some(value.parse::<u32>().unwrap());
                        }
                        _ => {
                            unreachable!();
                        }
                    }
                }
                _ => {}
            };
        }
        Some(rp)
    }

    fn rules(&self) -> [(&str, Option<u32>); 5] {
        [
            ("%Y-%m-%d %H", self.hourly),
            ("%Y-%m-%d", self.daily),
            ("%Y w%w", self.weekly),
            ("%Y-%m", self.monthly),
            (
                "%Y",
                // NOTE: chrono keeps years as i32 (signed); however there were no ZFS
                // deployments before ca (+)2006, so I guess it's safe to cast to u32.
                match self.yearly {
                    Some(y) => Some(y as u32),
                    None => None,
                },
            ),
        ]
    }
}

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
struct SnapshotMetadata {
    name: String,
    created: chrono::DateTime<Utc>,
    used: Byte,
}

#[derive(Debug)]
struct AgeCheckResult {
    keep: Vec<SnapshotMetadata>,
    delete: Vec<SnapshotMetadata>,
}

fn check_age(snapshots: Vec<SnapshotMetadata>, rp: RetentionPolicy) -> AgeCheckResult {
    let mut keep = HashSet::<SnapshotMetadata>::new();
    let mut snapshots: Vec<SnapshotMetadata> = snapshots.iter().map(|s| s.clone()).collect();
    // Sort newest snapshots first, so when we consider which ones to retain, the oldest
    // come last (and fall off the keep-set).
    snapshots.sort_unstable_by_key(|s| -s.created.timestamp());
    'next_rule: for (pattern, rule) in &rp.rules() {
        // RetentionPolicy.rules() creates a set of date format patterns (see strftime(3)),
        // which are meant to be lossy/fuzzy (e.g. year-month-day; year-week, etc).
        let mut last: Option<String> = None;
        if let Some(keepn) = rule {
            if *keepn == 0 {
                continue 'next_rule;
            }
            let mut kept = 0;
            for s in &snapshots {
                // We use these date patterns to format each snapshot's creation date, to
                // put it in an ad-hoc bucket (last / period); then keep track of how many
                // snapshots (kept) we've retained so far for the current bucket.
                let period: String = format!("{}", s.created.format(pattern));
                if last != Some(period) {
                    last = Some(format!("{}", s.created.format(pattern)));
                    keep.insert(s.clone());
                    kept += 1;
                    if kept == *keepn {
                        // This is as many snapshots as we wanted to
                        // keep, let's visit the next retention rule.
                        continue 'next_rule;
                    }
                }
            }
        }
    }
    AgeCheckResult {
        keep: snapshots
            .iter()
            .filter(|s| keep.contains(s))
            .map(|s| s.clone())
            .collect(),
        delete: snapshots
            .iter()
            .filter(|s| !keep.contains(s))
            .map(|s| s.clone())
            .collect(),
    }
}

struct ZFS;
impl ZFS {
    fn snapshot(dataset: &str) -> Result<SnapshotMetadata> {
        // Take a snapshot of the given dataset, with an auto-generated name.
        let now = Utc::now();
        let name = format!(
            "{}@{}-autosnap",
            dataset,
            now.to_rfc3339_opts(SecondsFormat::Secs, true)
        );
        ZFS::_call_do("snap", &[&name])?;
        Ok(SnapshotMetadata {
            name: name.clone(),
            created: now,
            used: ZFS::_parse_used(&ZFS::get_property(&name, "used")?).unwrap(),
        })
    }

    fn list_snapshots() -> Result<Vec<SnapshotMetadata>> {
        // List all snapshots under our control.
        // zfs list -H -t snapshot -o name,creation,used,at.rollc.at:snapkeep
        Ok(ZFS::_call_read(
            "list",
            &[
                "-t",
                "snapshot",
                "-o",
                format!("name,creation,used,{}", PROPERTY_SNAPKEEP).as_str(),
            ],
        )?
        .iter()
        .filter(|v| match &v[..] {
            // Skip snapshots that don't have the 'at.rollc.at:snapkeep' property.
            // This works both for datasets where a snapshot did not inherit the property
            // (which means the dataset should not be managed), and for explicitly marking a
            // snapshot to be retained / opted out.
            [_, _, _, snapkeep] => snapkeep != "-",
            _ => panic!("Parse error"),
        })
        .map(|v| {
            let sm = match &v[..] {
                [name, created, used, _] => SnapshotMetadata {
                    name: name.to_string(),
                    created: chrono::DateTime::from_utc(
                        chrono::NaiveDateTime::parse_from_str(&created, "%a %b %e %H:%M %Y")
                            .unwrap(),
                        chrono::Utc,
                    ),
                    used: ZFS::_parse_used(used).unwrap(),
                },
                _ => panic!("Parse error"),
            };
            sm
        })
        .collect())
    }

    fn get_property(dataset: &str, property: &str) -> Result<String> {
        // Get a single named property on given dataset.
        // zfs list -H -t snapshot -o name,creation,used,at.rollc.at:snapkeep
        Ok(ZFS::_call_read("get", &[property, "-o", "value", dataset])?
            .iter()
            .next()
            .unwrap()[0]
            .clone())
    }

    fn list_datasets_for_snapshot() -> Result<Vec<String>> {
        // Which datasets should get a snapshot?
        // zfs get -H -t filesystem,volume -o name,value at.rollc.at:snapkeep
        Ok(ZFS::_call_read(
            "get",
            &[
                "-t",
                "filesystem,volume",
                "-o",
                "name,value",
                PROPERTY_SNAPKEEP,
            ],
        )?
        .iter()
        .filter(|kv| kv[1] != "-")
        .map(|kv| kv[0].clone())
        .collect())
    }

    fn destroy_snapshot(snapshot: SnapshotMetadata) -> Result<()> {
        // This will destroy the named snapshot. Since ZFS has a single verb for destroying
        // anything, which could cause irreparable harm, we double check that the name we
        // got passed looks like a snapshot name, and hard-crash otherwise.
        if !snapshot.name.contains("@") {
            panic!("Tried to destroy something that is not a snapshot");
        }
        // zfs destroy -H ...@...
        ZFS::_call_do("destroy", &[&snapshot.name])
    }

    fn _call_read(action: &str, args: &[&str]) -> Result<Vec<Vec<String>>> {
        // Helper function to get/list datasets and their properties into a nice table.
        Ok(subprocess::Exec::cmd("zfs")
            .arg(action)
            .arg("-H")
            .args(args)
            .stdout(subprocess::Redirection::Pipe)
            .capture()?
            .stdout_str()
            .split("\n")
            .filter(|s| s != &"")
            .map(|s| s.split("\t").map(|ss| ss.to_string()).collect())
            .collect())
    }

    fn _call_do(action: &str, args: &[&str]) -> Result<()> {
        // Perform a side effect, like snapshot or destroy.
        if subprocess::Exec::cmd("zfs")
            .arg(action)
            .args(args)
            .join()?
            .success()
        {
            Ok(())
        } else {
            Err(anyhow::Error::msg("zfs command error"))
        }
    }

    fn _parse_used(x: &str) -> Result<Byte> {
        // The zfs(1) commandline tool says e.g. 1.2M but means 1.2MiB,
        // so we mash it to make byte_unit parsing happy.
        match x.chars().last() {
            Some('K' | 'M' | 'G' | 'T' | 'P' | 'E' | 'Z') => {
                Ok(Byte::from_str(x.to_owned() + "iB")?)
            }
            _ => Ok(Byte::from_str(x)?),
        }
    }
}

fn gc_find() -> Result<AgeCheckResult> {
    // List all snapshots we're interested in, group them by dataset, check them against
    // their parent dataset's retention policy, and aggreagate them into the final result,
    // which can be presented to the user (do_status()) or the garbage collector (do_gc()).
    let snapshots = ZFS::list_snapshots()?;
    let mut by_dataset = HashMap::<&str, Vec<SnapshotMetadata>>::new();
    for snapshot in &snapshots {
        if let Some(dataset_name) = snapshot.name.split("@").next() {
            let group = by_dataset.entry(dataset_name).or_insert(vec![]);
            group.push(snapshot.clone());
        }
    }
    let mut aggregate = AgeCheckResult {
        keep: vec![],
        delete: vec![],
    };
    for (key, group) in &by_dataset {
        let check = check_age(
            group.to_vec(),
            RetentionPolicy::parse(&ZFS::get_property(key, PROPERTY_SNAPKEEP)?).unwrap(),
        );
        aggregate.keep.extend_from_slice(&check.keep);
        aggregate.delete.extend_from_slice(&check.delete);
    }
    Ok(aggregate)
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
    for dataset in &ZFS::list_datasets_for_snapshot()? {
        let s = ZFS::snapshot(dataset)?;
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
        ZFS::destroy_snapshot(s)?;
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
