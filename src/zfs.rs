use byte_unit::Byte;
use chrono::prelude::*;

use crate::{Result, PROPERTY_SNAPKEEP};

#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct SnapshotMetadata {
    pub name: String,
    pub created: chrono::DateTime<Utc>,
    pub used: Byte,
}

pub fn snapshot(dataset: &str) -> Result<SnapshotMetadata> {
    // Take a snapshot of the given dataset, with an auto-generated name.
    let now = Utc::now();
    let name = format!(
        "{}@{}-autosnap",
        dataset,
        now.to_rfc3339_opts(SecondsFormat::Secs, true)
    );
    call_do("snap", &[&name])?;
    Ok(SnapshotMetadata {
        name: name.clone(),
        created: now,
        used: parse_used(&get_property(&name, "used")?)?,
    })
}

pub fn list_snapshots() -> Result<Vec<SnapshotMetadata>> {
    // List all snapshots under our control.
    // zfs list -H -t snapshot -o name,creation,used,at.rollc.at:snapkeep
    let lines = call_read(
        "list",
        &[
            "-t",
            "snapshot",
            "-o",
            &format!("name,creation,used,{}", PROPERTY_SNAPKEEP),
        ],
    )?;
    parse_snapshots(lines)
}

fn parse_snapshots(lines: Vec<Vec<String>>) -> Result<Vec<SnapshotMetadata>> {
    let mut snapshots = Vec::with_capacity(lines.len());
    for line in lines {
        // Skip snapshots that don't have the 'at.rollc.at:snapkeep' property.
        // This works both for datasets where a snapshot did not inherit the property
        // (which means the dataset should not be managed), and for explicitly marking a
        // snapshot to be retained / opted out.
        match line.as_slice() {
            [_, _, _, snapkeep] if snapkeep == "-" => continue,
            [name, created, used, _] => {
                let metadata = SnapshotMetadata {
                    name: name.to_string(),
                    created: chrono::DateTime::from_utc(
                        chrono::NaiveDateTime::parse_from_str(created, "%a %b %e %H:%M %Y")?,
                        chrono::Utc,
                    ),
                    used: parse_used(used)?,
                };
                snapshots.push(metadata)
            }
            _ => return Err("list snapshots parse error".into()),
        }
    }
    Ok(snapshots)
}

pub fn get_property(dataset: &str, property: &str) -> Result<String> {
    // Get a single named property on given dataset.
    // zfs get -H -o value $property $dataset
    Ok(call_read("get", &["-o", "value", property, dataset])?
        .get(0)
        .unwrap()[0]
        .clone())
}

pub fn list_datasets_for_snapshot() -> Result<Vec<String>> {
    // Which datasets should get a snapshot?
    // zfs get -H -t filesystem,volume -o name,value at.rollc.at:snapkeep
    Ok(call_read(
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

pub fn destroy_snapshot(snapshot: SnapshotMetadata) -> Result<()> {
    // This will destroy the named snapshot. Since ZFS has a single verb for destroying
    // anything, which could cause irreparable harm, we double check that the name we
    // got passed looks like a snapshot name, and return an error otherwise.
    if !snapshot.name.contains('@') {
        return Err("Tried to destroy something that is not a snapshot".into());
    }
    // zfs destroy -H ...@...
    call_do("destroy", &[&snapshot.name])
}

fn call_read(action: &str, args: &[&str]) -> Result<Vec<Vec<String>>> {
    // Helper function to get/list datasets and their properties into a nice table.
    Ok(subprocess::Exec::cmd("zfs")
        .arg(action)
        .arg("-H")
        .args(args)
        .stdout(subprocess::Redirection::Pipe)
        .capture()?
        .stdout_str()
        .lines()
        .filter(|&s| !s.is_empty())
        .map(|s| s.split('\t').map(|ss| ss.to_string()).collect())
        .collect())
}

fn call_do(action: &str, args: &[&str]) -> Result<()> {
    // Perform a side effect, like snapshot or destroy.
    if subprocess::Exec::cmd("zfs")
        .arg(action)
        .args(args)
        .join()?
        .success()
    {
        Ok(())
    } else {
        Err("zfs command error".into())
    }
}

fn parse_used(x: &str) -> Result<Byte> {
    // The zfs(1) commandline tool says e.g. 1.2M but means 1.2MiB,
    // so we mash it to make byte_unit parsing happy.
    match x.chars().last() {
        Some('K' | 'M' | 'G' | 'T' | 'P' | 'E' | 'Z') => Ok(Byte::from_str(x.to_owned() + "iB")?),
        _ => Ok(Byte::from_str(x)?),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_snapshots() {
        let lines = vec![
            // name, created, used, snapkeep
            vec![
                String::from("first"),
                String::from("Sat Oct 2 09:59 2021"),
                String::from("13G"),
                String::from("at.rollc.at:snapkeep=h24d30w8m6y1"),
            ],
            vec![
                String::from("skip"),
                String::from("Sat Oct 1 19:59 2021"),
                String::from("2G"),
                String::from("-"),
            ],
        ];
        let snapshots = parse_snapshots(lines).unwrap();
        assert_eq!(
            snapshots,
            vec![SnapshotMetadata {
                name: String::from("first"),
                created: chrono::DateTime::from_utc(
                    chrono::NaiveDateTime::parse_from_str(
                        "Sat Oct 2 09:59 2021",
                        "%a %b %e %H:%M %Y",
                    )
                    .unwrap(),
                    chrono::Utc,
                ),
                used: Byte::from(13u64 * 1024 * 1024 * 1024),
            }]
        );
    }

    #[test]
    fn test_parse_snapshots_empty() {
        let lines = vec![];
        let snapshots = parse_snapshots(lines).unwrap();
        assert_eq!(snapshots, vec![]);
    }

    #[test]
    fn test_parse_snapshots_invalid_row() {
        let lines = vec![vec![String::from("unexpected")]];
        let err = parse_snapshots(lines).unwrap_err();
        assert_eq!(err.to_string(), "list snapshots parse error");
    }

    #[test]
    fn test_parse_snapshots_invalid_date() {
        let lines = vec![vec![
            String::from("first"),
            String::from("2 Oct 2021 9:52AM"),
            String::from("3G"),
            String::from("at.rollc.at:snapkeep=h24d30w8m6y1"),
        ]];
        let err = parse_snapshots(lines).unwrap_err();
        assert_eq!(err.to_string(), "input contains invalid characters");
    }
}
