<h1 align="center">
  📸<br>
  zfs-autosnap
</h1>

<div align="center">
  <strong>Automatic ZFS snapshot utility.</strong>
</div>

<br>

<div align="center">
  <a href="https://cirrus-ci.com/github/wezm/zfs-autosnap">
    <img src="https://api.cirrus-ci.com/github/wezm/zfs-autosnap.svg" alt="Build Status"></a>
  <a href="https://crates.io/crates/zfs-autosnap">
    <img src="https://img.shields.io/crates/v/zfs-autosnap.svg" alt="Version">
  </a>
  <img src="https://img.shields.io/crates/l/zfs-autosnap.svg" alt="License">
</div>

<br>

`zfs-autosnap` periodically snapshots one or more [ZFS] datasets and removes old
ones according to a retention policy.

Changes From Upstream
---------------------

In my fork I have:

* Reduced the number of dependencies
* Tidied and reorganised the code
* Added a small number of unit tests

Usage
-----

1. Add the retention policy to the dataset you want to snapshot. E.g.
   `zfs set at.rollc.at:snapkeep=h24d30w8m6y1 tank`
2. Run `zfs-autosnap snap` hourly via `cron.hourly` or systemd timer
3. Run `zfs-autosnap gc` daily via `cron.daily`

Try `zfs-autosnap status` to check what's going on.

### systemd

See the [systemd] directory for sample timer files. To use these you need to
update the dataset name, then enable and start the timers. E.g.

    systemctl enable --now zfs-autosnap-snap.timer
    systemctl enable --now zfs-autosnap-gc.timer

Installation
------------

### Pre-compiled Binaries

Pre-compiled binaries are available for x86\_64 Linux and FreeBSD, they have no
third-party runtime dependencies:

* [FreeBSD 13 amd64](https://releases.wezm.net/zfs-autosnap/0.3.0/zfs-autosnap-0.3.0-amd64-unknown-freebsd.tar.gz)
* [Linux x86\_64](https://releases.wezm.net/zfs-autosnap/0.3.0/zfs-autosnap-0.3.0-x86_64-unknown-linux-musl.tar.gz)

Example to download and extract a binary:

    curl https://releases.wezm.net/zfs-autosnap/0.3.0/zfs-autosnap-0.3.0-x86_64-unknown-linux-musl.tar.gz | tar zxf -

### Build from Source

**Minimum Supported Rust Version:** 1.53.0

`zfs-autosnap` is implemented in Rust. See the Rust website for [instructions
on installing the toolchain][rustup].

#### From Git Checkout or Release Tarball

Build the binary with `cargo build --release --locked`. The binary will be in
`target/release/zfs-autosnap`.

#### From crates.io

`cargo install zfs-autosnap`

#### Arch Linux PKGBUILD

I have a PKGBUILD for a pacman package in my personal package collection, which
can be used as the basis of your own:

<https://github.com/wezm/aur/tree/master/zfs-autosnap>

How It Works
------------

Retention policy is set via the property `at.rollc.at:snapkeep`, which must be
present on any datasets (filesystems or volumes) that you'd like to be managed.
The proposed default of `h24d30w8m6y1` means to keep 24 hourly, 30 daily, 8
weekly, 6 monthly and 1 yearly snapshots but you can select any value for each
time unit, including omitting it.

The garbage collector looks at every snapshot under the managed datasets, and
considers its creation time to decide whether to keep it. 

**Important:** The snapshot name does not matter, only the creation time.

If you'd like to ensure a particular snapshot is not removed, set its
`at.rollc.at:snapkeep` property to minus (`-`).

If in doubt, consider reading the source: it's only about 500ish lines of code.

Safety
------

`zfs-autosnap` will try not to eat your data; the only destructive operation is
contained within a function that will refuse to work on things that are not
snapshots - but there's NO WARRANTY. The previous version (written in Python), was
in production use since ca 2015 and there were zero incidents; this (Rust)
version is basically a source port (prior to wezm's changes).

USE AT YOUR OWN RISK.

[ZFS]: https://en.wikipedia.org/wiki/ZFS
[systemd]: systemd
