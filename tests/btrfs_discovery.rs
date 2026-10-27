#![cfg(target_os = "linux")]
use dedupit::{Granularity, Options, Scanner, Stats};
use std::{
    fs::{self, File, OpenOptions},
    os::unix::fs::FileExt,
    path::Path,
};

#[cfg(feature = "test-failpoints")]
#[path = "support/batch_crashes.rs"]
mod batch_crashes;
#[path = "support/btrfs_stress.rs"]
mod stress;

fn setup() -> (tempfile::TempDir, Options) {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    fs::create_dir(&data).unwrap();
    let options = Options {
        database: dir.path().join("index.sqlite"),
        roots: vec![data],
        chunk_size: 4096,
        granularity: Granularity::Both,
        verify: false,
        scanner: Scanner::Btrfs,
        commit_batch_size: 64,
    };
    (dir, options)
}
fn first(options: &Options) -> Option<Stats> {
    match dedupit::run(options, false) {
        Ok(stats) => {
            assert!(stats.errors.is_empty(), "{stats:?}");
            Some(stats)
        }
        Err(error) => {
            let message = format!("{error:#}");
            if std::env::var_os("DEDUPIT_REQUIRE_BTRFS_SCANNER").is_some() {
                panic!("native discovery required: {message}");
            }
            assert!(
                message.contains("Operation not permitted")
                    || message.contains("not on btrfs")
                    || message.contains("nested mount")
                    || message.contains("subvolume root is hidden"),
                "unexpected discovery error: {message}"
            );
            eprintln!("SKIP: btrfs generation discovery needs btrfs and native ioctl capabilities: {message}");
            None
        }
    }
}
fn scan(options: &Options) -> Stats {
    let stats = dedupit::run(options, false).unwrap();
    assert!(stats.errors.is_empty(), "{stats:?}");
    stats
}
fn data(size: usize) -> Vec<u8> {
    let mut state = 123456789u64;
    (0..size)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect()
}

#[test]
fn native_discovery_hidden_bind_mount_preserves_scope_and_incrementality() {
    hidden_bind_roundtrip(false);
    hidden_bind_roundtrip(true);
}

fn hidden_bind_roundtrip(cross_device: bool) {
    use std::{
        ffi::CString,
        os::{
            fd::AsRawFd,
            unix::{ffi::OsStrExt, fs::MetadataExt, process::CommandExt},
        },
        process::Command,
    };
    let (dir, options) = setup();
    if first(&options).is_none() {
        return;
    }
    let bytes = data(128 * 1024);
    fs::write(options.roots[0].join("a"), &bytes).unwrap();
    fs::write(options.roots[0].join("b"), &bytes).unwrap();
    fs::write(dir.path().join("outside"), &bytes).unwrap();
    fs::hard_link(options.roots[0].join("a"), dir.path().join("outside-link")).unwrap();
    let external = tempfile::tempdir_in("/tmp").unwrap();
    let visible = if cross_device {
        external.path()
    } else {
        dir.path()
    }
    .join("bound with space");
    fs::create_dir(&visible).unwrap();
    let database = dir.path().join("bound.sqlite");
    let run = |list: bool, source_path: &Path| {
        let source = CString::new(source_path.as_os_str().as_bytes()).unwrap();
        let target = CString::new(visible.as_os_str().as_bytes()).unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_dedupit"));
        command
            .args([
                "--scanner",
                "btrfs",
                "--chunk-size",
                "4096",
                "--apply",
                "--json",
                "--database",
            ])
            .arg(&database);
        if list {
            command.arg("--list");
        }
        command.arg(&visible);
        // SAFETY: only async-signal-safe syscalls run between fork and exec.
        // Mounts are private before the bind, including when the host uses
        // shared propagation; the test never changes the parent's namespace.
        unsafe {
            command.pre_exec(move || {
                if libc::unshare(libc::CLONE_NEWNS) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                if libc::mount(
                    std::ptr::null(),
                    c"/".as_ptr(),
                    std::ptr::null(),
                    libc::MS_REC | libc::MS_PRIVATE,
                    std::ptr::null(),
                ) != 0
                {
                    return Err(std::io::Error::last_os_error());
                }
                if libc::mount(
                    source.as_ptr(),
                    target.as_ptr(),
                    std::ptr::null(),
                    libc::MS_BIND,
                    std::ptr::null(),
                ) != 0
                {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        if list {
            let lines: Vec<serde_json::Value> = String::from_utf8(output.stdout)
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect();
            for record in lines.iter().filter(|r| r["type"] == "duplicate") {
                for key in ["source", "destination"] {
                    assert!(Path::new(record[key].as_str().unwrap()).starts_with(&visible));
                }
            }
            lines.last().unwrap()["stats"].clone()
        } else {
            serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap()
        }
    };
    // Same-filesystem binds are intentional: comparing only st_dev while
    // ascending would silently choose the wrong namespace-relative paths.
    if !cross_device {
        assert_eq!(
            File::open(&visible).unwrap().metadata().unwrap().dev(),
            File::open(&options.roots[0])
                .unwrap()
                .metadata()
                .unwrap()
                .dev()
        );
    }
    let initial = run(false, &options.roots[0]);
    assert_eq!(initial["files_hashed"], 2);
    assert_eq!(initial["duplicate_files"], 1);
    assert_eq!(initial["errors"], serde_json::json!([]));
    assert!(initial["bytes_deduped"].as_u64().unwrap() > 0);
    let idle = run(true, &options.roots[0]);
    assert_eq!(idle["files_seen"], 0, "{idle}");
    assert_eq!(idle["bytes_hashed"], 0);
    assert_eq!(idle["jobs_attempted"], 0);
    assert_eq!(
        fs::read_dir(&visible).unwrap().count(),
        0,
        "bind mount leaked into parent"
    );
    let a = OpenOptions::new()
        .write(true)
        .open(options.roots[0].join("a"))
        .unwrap();
    a.write_all_at(&[123], 0).unwrap();
    a.sync_all().unwrap();
    assert_eq!(fs::read(options.roots[0].join("b")).unwrap(), bytes);
    let changed = run(false, &options.roots[0]);
    assert_eq!(changed["files_hashed"], 1, "{changed}");
    assert_eq!(changed["duplicate_files"], 0);
    assert!(changed["bytes_hashed"].as_u64().unwrap() < bytes.len() as u64);
    // Nested subvolumes remain visible below the bind prefix, including old
    // data discovered independently of the parent's generation watermark.
    #[repr(C)]
    struct Args {
        fd: i64,
        name: [u8; 4088],
    }
    nix::ioctl_write_ptr!(create_subvolume, 0x94, 14, Args);
    let parent = File::open(&options.roots[0]).unwrap();
    let mut args = Args {
        fd: 0,
        name: [0; 4088],
    };
    args.name[..6].copy_from_slice(b"nested");
    // SAFETY: live parent fd and correctly sized, NUL-terminated input.
    unsafe { create_subvolume(parent.as_raw_fd(), &args) }.unwrap();
    fs::write(options.roots[0].join("nested/c"), &bytes).unwrap();
    let nested = run(false, &options.roots[0]);
    assert!(
        nested["btrfs_subvolumes"].as_u64().unwrap() >= 2,
        "{nested}"
    );
    assert_eq!(nested["duplicate_files"], 1, "{nested}");
    assert_eq!(run(true, &options.roots[0])["bytes_hashed"], 0);
    // Rebinding to another prefix in the same subvolume invalidates the old
    // crawl, even though filesystem/subvolume identities and visible root match.
    let replacement = dir.path().join("replacement");
    fs::create_dir(&replacement).unwrap();
    fs::write(replacement.join("only"), &bytes).unwrap();
    let rebound = run(true, &replacement);
    assert_eq!(rebound["files_hashed"], 1, "{rebound}");
    assert_eq!(rebound["duplicate_files"], 0, "{rebound}");
}

#[test]
fn native_apply_caches_mappings_without_refresh_and_keeps_writes_incremental() {
    let (_dir, mut options) = setup();
    options.chunk_size = 128 * 1024;
    let bytes = data(2 * 1024 * 1024);
    for name in ["a", "b"] {
        fs::write(options.roots[0].join(name), &bytes).unwrap();
    }
    for (name, edge) in [("c", 19), ("d", 21)] {
        let mut partial = vec![edge; 3 * options.chunk_size];
        partial[options.chunk_size..2 * options.chunk_size]
            .copy_from_slice(&bytes[..options.chunk_size]);
        fs::write(options.roots[0].join(name), partial).unwrap();
    }
    if first(&options).is_none() {
        return;
    }
    let applied = dedupit::run(&options, true).unwrap();
    assert!(applied.errors.is_empty(), "{applied:?}");
    assert!(
        applied.bytes_deduped > 0 && applied.jobs_completed >= 2,
        "{applied:?}"
    );
    assert_eq!(applied.btrfs_mapping_updates, applied.jobs_completed);
    let idle = scan(&options); // New Index/connection, no settling pass allowed.
    assert_eq!(idle.files_seen, 0, "{idle:?}");
    assert_eq!(idle.bytes_hashed, 0);
    assert!(applied.btrfs_events_reused > 0, "{applied:?}");
    for name in ["a", "b"] {
        assert_eq!(fs::read(options.roots[0].join(name)).unwrap(), bytes);
    }
    OpenOptions::new()
        .write(true)
        .open(options.roots[0].join("b"))
        .unwrap()
        .write_all_at(&[42; 4096], 1024 * 1024)
        .unwrap();
    let changed = scan(&options);
    assert_eq!(changed.files_hashed, 1, "{changed:?}");
    assert_eq!(
        changed.bytes_hashed, options.chunk_size as u64,
        "{changed:?}"
    );
    assert!(changed.chunks_reused > 0);
    let expected = stress::inventory(&options.database, &options.roots[0]);
    let mut fresh = options.clone();
    fresh.database.set_file_name("oracle.sqlite");
    fresh.scanner = Scanner::Portable;
    fresh.verify = true;
    scan(&fresh);
    assert_eq!(
        expected,
        stress::inventory(&fresh.database, &options.roots[0])
    );
    // Namespace changes still take precedence over cached inode observations.
    fs::rename(options.roots[0].join("d"), options.roots[0].join("renamed")).unwrap();
    scan(&options);
    dedupit::list_duplicates(&options, |d| {
        assert!(d.source.exists() && d.destination.exists());
        Ok(())
    })
    .unwrap();
}

#[test]
fn native_discovery_idle_run_opens_no_old_files_and_small_writes_reuse_hashes() {
    let (_dir, mut options) = setup();
    options.granularity = Granularity::File;
    let bytes = data(2 * 1024 * 1024);
    fs::write(options.roots[0].join("a"), &bytes).unwrap();
    fs::write(options.roots[0].join("b"), &bytes).unwrap();
    let Some(initial) = first(&options) else {
        return;
    };
    assert_eq!(initial.duplicate_files, 1);
    assert_eq!(initial.files_hashed, 2);
    let idle = scan(&options);
    assert_eq!(idle.files_seen, 0);
    assert_eq!(idle.bytes_hashed, 0);
    let file = OpenOptions::new()
        .write(true)
        .open(options.roots[0].join("b"))
        .unwrap();
    file.write_all_at(&vec![42; 4096], 1024 * 1024).unwrap();
    let changed = scan(&options);
    assert_eq!(changed.files_seen, 1);
    assert_eq!(changed.files_hashed, 1);
    assert!(
        changed.bytes_hashed > 0 && changed.bytes_hashed < bytes.len() as u64,
        "{changed:?}"
    );
    assert!(changed.chunks_reused > 0);
    assert_eq!(changed.duplicate_files, 0);
    fs::remove_file(options.roots[0].join("a")).unwrap();
    let deleted = scan(&options);
    assert_eq!(deleted.files_removed, 1);
}

#[test]
fn native_discovery_renames_and_deleted_directories_preserve_scope() {
    let (_dir, options) = setup();
    fs::create_dir_all(options.roots[0].join("old/child")).unwrap();
    let bytes = data(65536);
    fs::write(options.roots[0].join("old/child/a"), &bytes).unwrap();
    fs::write(options.roots[0].join("b"), &bytes).unwrap();
    if first(&options).is_none() {
        return;
    }
    fs::rename(options.roots[0].join("old"), options.roots[0].join("new")).unwrap();
    let renamed = scan(&options);
    assert_eq!(renamed.bytes_hashed, 0);
    assert_eq!(renamed.duplicate_files, 1);
    dedupit::list_duplicates(&options, |duplicate| {
        assert!(duplicate.source.exists());
        assert!(duplicate.destination.exists());
        Ok(())
    })
    .unwrap();
    fs::remove_dir_all(options.roots[0].join("new/child")).unwrap();
    let removed = scan(&options);
    assert_eq!(removed.files_removed, 1);
    assert_eq!(removed.duplicate_files, 0);
}

#[test]
fn native_discovery_truncation_and_append_agree_with_full_rehash() {
    let (_dir, options) = setup();
    fs::write(options.roots[0].join("a"), data(128 * 1024)).unwrap();
    if first(&options).is_none() {
        return;
    }
    let file = OpenOptions::new()
        .write(true)
        .open(options.roots[0].join("a"))
        .unwrap();
    file.set_len(5000).unwrap();
    scan(&options);
    file.set_len(1024 * 1024).unwrap();
    file.write_all_at(&vec![42; 4096], 900 * 1024).unwrap();
    scan(&options);
    let hash = |path: &Path| {
        rusqlite::Connection::open(path)
            .unwrap()
            .query_row("SELECT hash FROM ranges WHERE kind=1", [], |r| {
                r.get::<_, Vec<u8>>(0)
            })
            .unwrap()
    };
    let incremental = hash(&options.database);
    let mut fresh = options.clone();
    fresh.database.set_file_name("fresh.sqlite");
    fresh.scanner = Scanner::Portable;
    fresh.verify = true;
    scan(&fresh);
    assert_eq!(incremental, hash(&fresh.database));
}

#[test]
fn native_discovery_new_snapshot_indexes_old_generation_data() {
    use std::os::fd::AsRawFd;
    #[repr(C)]
    struct Args {
        fd: i64,
        name: [u8; 4088],
    }
    nix::ioctl_write_ptr!(create_subvolume, 0x94, 14, Args);
    nix::ioctl_write_ptr!(create_snapshot, 0x94, 1, Args);
    let (_dir, options) = setup();
    // Probe before creating subvolumes: local unprivileged runs skip safely.
    if first(&options).is_none() {
        return;
    }
    let parent = File::open(&options.roots[0]).unwrap();
    let mut args = Args {
        fd: 0,
        name: [0; 4088],
    };
    args.name[..4].copy_from_slice(b"base");
    // SAFETY: live parent fd and initialized, correctly sized UAPI input.
    unsafe { create_subvolume(parent.as_raw_fd(), &args) }.unwrap();
    fs::write(options.roots[0].join("base/a"), data(65536)).unwrap();
    scan(&options);
    let base = File::open(options.roots[0].join("base")).unwrap();
    let mut args = Args {
        fd: base.as_raw_fd().into(),
        name: [0; 4088],
    };
    args.name[..4].copy_from_slice(b"snap");
    // SAFETY: source and parent fds stay live; input name is terminated.
    unsafe { create_snapshot(parent.as_raw_fd(), &args) }.unwrap();
    let added = scan(&options);
    assert!(added.btrfs_subvolumes >= 3);
    assert_eq!(added.duplicate_files, 1);
    assert!(added.files_hashed >= 1);
}

#[test]
fn native_discovery_hardlink_outside_scope_does_not_keep_deleted_input() {
    let (dir, options) = setup();
    fs::write(options.roots[0].join("f"), data(65536)).unwrap();
    fs::hard_link(options.roots[0].join("f"), dir.path().join("outside-link")).unwrap();
    let Some(initial) = first(&options) else {
        return;
    };
    assert_eq!(initial.files_hashed, 1);
    fs::remove_file(options.roots[0].join("f")).unwrap();
    let removed = scan(&options);
    assert_eq!(removed.files_removed, 1);
    let count: u64 = rusqlite::Connection::open(&options.database)
        .unwrap()
        .query_row("SELECT COUNT(*) FROM files", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 0);
    assert!(dir.path().join("outside-link").exists());
}

#[test]
fn native_discovery_moved_in_old_subtree_tracks_later_nested_deletion() {
    let (dir, options) = setup();
    let outside = dir.path().join("outside");
    fs::create_dir_all(outside.join("child/grandchild")).unwrap();
    fs::write(outside.join("child/grandchild/a"), data(32768)).unwrap();
    if first(&options).is_none() {
        return;
    }
    let inside = options.roots[0].join("moved");
    fs::rename(&outside, &inside).unwrap();
    assert_eq!(scan(&options).files_hashed, 1);
    fs::remove_dir_all(inside.join("child")).unwrap();
    assert_eq!(scan(&options).files_removed, 1);
    let count: u64 = rusqlite::Connection::open(&options.database)
        .unwrap()
        .query_row("SELECT COUNT(*) FROM files", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 0);
}
