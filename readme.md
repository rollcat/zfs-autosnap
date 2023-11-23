# zfs-autosnap

Minimal viable ZFS snapshot utility.

Add `zfs-autosnap snap` to your cron.hourly, and `zfs-autosnap gc` to
cron.daily; then set `at.rollc.at:snapkeep=h24d30w8m6y1` (or whatever
is your retention policy) on datasets you want managed. Try
`zfs-autosnap status` to check what's going on.

Retenion policy is set via the property `at.rollc.at:snapkeep`, which
must be present on any datasets (filesystems or volumes) that you'd
like to be managed. The proposed default of `h24d30w8m6y1` means to
keep 24 hourly, 30 daily, 8 weekly, 6 monthly and 1 yearly snapshots.

The garbage collector looks at every snapshot under the managed
datasets, and considers its creation time to decide whether to keep
it. The snapshot name does not matter! If you'd like to retain a
particular snapshot (e.g. right before a risky upgrade), set its
`at.rollc.at:snapkeep` property to a literal minus (`-`).

As always, when in doubt, consider reading the source: it's mere
400ish lines of relatively clean Rust.

## Tested on

- FreeBSD 14.0
- NixOS 23.05
- Ubuntu 22.04

## Safety

It will try not to eat your data; the only destructive operation is
contained within a function that will refuse to work on things that
are not snapshots - but there's NO WARRANTY. Previous version (written
in Python), was in production use since ca 2015 and there were zero
incidents; this (Rust) version is basically a source port.

USE AT YOUR OWN RISK.
