#![cfg(target_os = "linux")]
use dedupit::{Granularity, Options, Scanner, Stats};
use std::{
    fs::{self, File, OpenOptions},
    os::unix::fs::FileExt,
    path::Path,
};

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
