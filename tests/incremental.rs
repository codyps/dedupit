use dedupit::{
    run, run_with_backend, Backend, Granularity, NativeBackend, Options, Outcome, Range,
};
use std::{
    fs::{self, File, OpenOptions},
    io,
    os::unix::{
        ffi::OsStringExt,
        fs::{symlink, FileExt, MetadataExt},
    },
    path::Path,
};
use tempfile::TempDir;

fn setup(granularity: Granularity) -> (TempDir, Options) {
    let dir = tempfile::tempdir().unwrap();
    fs::create_dir(dir.path().join("data")).unwrap();
    let options = Options {
        database: dir.path().join("index.sqlite"),
        roots: vec![dir.path().join("data")],
        chunk_size: 4096,
        granularity,
        verify: false,
        scanner: dedupit::Scanner::Portable,
    };
    (dir, options)
}

fn put(options: &Options, name: &str, bytes: &[u8]) {
    fs::write(options.roots[0].join(name), bytes).unwrap();
}

#[derive(Default)]
struct ComparingBackend {
    calls: usize,
    max_progress: Option<u64>,
}

impl Backend for ComparingBackend {
    fn dedupe(
        &mut self,
        source: &File,
        dest: &File,
        range: Range,
        offset: u64,
    ) -> io::Result<Outcome> {
        self.calls += 1;
        let len = self.max_progress.unwrap_or(range.len).min(range.len);
        let mut a = vec![0; len as usize];
        let mut b = vec![0; len as usize];
        source.read_exact_at(&mut a, range.offset)?;
        dest.read_exact_at(&mut b, offset)?;
        Ok(if a == b {
            Outcome::Shared(len)
        } else {
            Outcome::Differs
        })
    }
}

#[test]
fn unchanged_runs_read_nothing_and_only_changed_files_are_hashed() {
    let (_dir, options) = setup(Granularity::Both);
    put(&options, "a", &vec![1; 8192]);
    put(&options, "b", &vec![1; 8192]);
    let first = run(&options, false).unwrap();
    assert_eq!(first.files_hashed, 2);
    assert_eq!(first.duplicate_files, 1);
    assert_eq!(first.duplicate_chunks, 3); // Includes repeated ranges within a file.
    let second = run(&options, false).unwrap();
    assert_eq!(second.files_unchanged, 2);
    assert_eq!(second.bytes_hashed, 0);
    put(&options, "b", &vec![2; 8192]);
    let third = run(&options, false).unwrap();
    assert_eq!(third.files_hashed, 1);
    assert_eq!(third.files_unchanged, 1);
    assert_eq!(third.duplicate_files, 0);
    fs::remove_file(options.roots[0].join("a")).unwrap();
    assert_eq!(run(&options, false).unwrap().files_removed, 1);
}

#[test]
fn successful_work_is_persistent_and_new_files_match_old_files() {
    let (_dir, options) = setup(Granularity::File);
    put(&options, "a", &vec![1; 8192]);
    put(&options, "b", &vec![1; 8192]);
    let mut backend = ComparingBackend::default();
    assert_eq!(
        run_with_backend(&options, true, &mut backend)
            .unwrap()
            .jobs_completed,
        1
    );
    let mut restarted = ComparingBackend::default();
    let second = run_with_backend(&options, true, &mut restarted).unwrap();
    assert_eq!(second.bytes_hashed, 0);
    assert_eq!(restarted.calls, 0);
    put(&options, "c", &vec![1; 8192]);
    let third = run_with_backend(&options, true, &mut restarted).unwrap();
    assert_eq!(third.files_hashed, 1);
    assert_eq!(third.jobs_completed, 1);
}

#[test]
fn deleted_representative_requeues_surviving_duplicates() {
    let (_dir, options) = setup(Granularity::File);
    for name in ["a", "b", "c"] {
        put(&options, name, &vec![1; 8192]);
    }
    let mut backend = ComparingBackend::default();
    assert_eq!(
        run_with_backend(&options, true, &mut backend)
            .unwrap()
            .jobs_completed,
        2
    );
    let conn = rusqlite::Connection::open(&options.database).unwrap();
    let source: Vec<u8> = conn
        .query_row("SELECT path FROM files ORDER BY id LIMIT 1", [], |r| {
            r.get(0)
        })
        .unwrap();
    fs::remove_file(std::ffi::OsString::from_vec(source)).unwrap();
    drop(conn);
    let result = run_with_backend(&options, true, &mut backend).unwrap();
    assert_eq!(result.files_removed, 1);
    assert_eq!(result.jobs_completed, 1);
}

#[test]
fn whole_files_take_precedence_and_partial_duplicates_still_share() {
    let (_dir, options) = setup(Granularity::Both);
    let mut data = vec![1; 4096];
    data.extend(vec![2; 4096]);
    put(&options, "a", &data);
    put(&options, "b", &data);
    let mut backend = ComparingBackend::default();
    let first = run_with_backend(&options, true, &mut backend).unwrap();
    assert_eq!(first.jobs_completed, 1);
    assert_eq!(backend.calls, 1);
    data[..4096].fill(3);
    put(&options, "c", &data);
    assert_eq!(
        run_with_backend(&options, true, &mut backend)
            .unwrap()
            .jobs_completed,
        1
    );
}

#[test]
fn partial_kernel_progress_is_continued_and_zero_progress_is_retryable() {
    let (_dir, options) = setup(Granularity::File);
    put(&options, "a", &vec![1; 12288]);
    put(&options, "b", &vec![1; 12288]);
    let mut zero = ComparingBackend {
        calls: 0,
        max_progress: Some(0),
    };
    let first = run_with_backend(&options, true, &mut zero).unwrap();
    assert_eq!(first.jobs_completed, 0);
    assert!(!first.errors.is_empty());
    let mut partial = ComparingBackend {
        calls: 0,
        max_progress: Some(4096),
    };
    assert_eq!(
        run_with_backend(&options, true, &mut partial)
            .unwrap()
            .jobs_completed,
        1
    );
    assert_eq!(partial.calls, 3);
}

#[test]
fn unsupported_backend_leaves_data_intact_and_work_retryable() {
    struct Unsupported;
    impl Backend for Unsupported {
        fn dedupe(&mut self, _: &File, _: &File, _: Range, _: u64) -> io::Result<Outcome> {
            Ok(Outcome::Unsupported)
        }
    }
    let (_dir, options) = setup(Granularity::Both);
    put(&options, "a", &vec![1; 4096]);
    put(&options, "b", &vec![1; 4096]);
    let result = run_with_backend(&options, true, &mut Unsupported).unwrap();
    assert!(result.unsupported > 0);
    assert_eq!(result.jobs_completed, 0);
    assert_eq!(fs::read(options.roots[0].join("b")).unwrap(), vec![1; 4096]);
    assert!(
        run_with_backend(&options, true, &mut ComparingBackend::default())
            .unwrap()
            .jobs_completed
            > 0
    );
}

#[test]
fn hardlinks_symlinks_non_utf8_and_index_files_are_handled() {
    let (dir, mut options) = setup(Granularity::Both);
    put(&options, "a", &vec![1; 4096]);
    fs::hard_link(
        options.roots[0].join("a"),
        options.roots[0].join("hardlink"),
    )
    .unwrap();
    symlink("a", options.roots[0].join("symlink")).unwrap();
    fs::write(
        options.roots[0].join(std::ffi::OsString::from_vec(vec![0xff])),
        vec![2; 4096],
    )
    .unwrap();
    // Index under the scan root must not recursively index its own writes.
    options.database = options.roots[0].join("index.sqlite");
    options.roots.push(dir.path().to_owned());
    let result = run(&options, false).unwrap();
    assert_eq!(result.files_hashed, 2);
    assert_eq!(result.hardlinks_skipped, 1);
    assert_eq!(result.duplicate_files, 0);
    assert_eq!(run(&options, false).unwrap().bytes_hashed, 0);
}

#[test]
fn configuration_and_lock_conflicts_are_rejected() {
    let (_dir, mut options) = setup(Granularity::Both);
    put(&options, "a", &vec![1; 4096]);
    run(&options, false).unwrap();
    options.chunk_size = 8192;
    assert!(run(&options, false)
        .unwrap_err()
        .to_string()
        .contains("differ"));
    options.chunk_size = 4096;
    let mut lock_name = options.database.as_os_str().to_owned();
    lock_name.push(".dedupit-lock");
    let lock = File::open(lock_name).unwrap();
    fs2::FileExt::lock_exclusive(&lock).unwrap();
    assert!(run(&options, false)
        .unwrap_err()
        .to_string()
        .contains("already in use"));
}

#[test]
fn database_hardlink_cannot_bypass_inode_lock() {
    let (dir, options) = setup(Granularity::File);
    put(&options, "a", &vec![1; 4096]);
    run(&options, false).unwrap();
    let alias = dir.path().join("alias.sqlite");
    fs::hard_link(&options.database, &alias).unwrap();
    let locked = OpenOptions::new()
        .read(true)
        .write(true)
        .open(alias)
        .unwrap();
    fs2::FileExt::lock_exclusive(&locked).unwrap();
    assert!(run(&options, false)
        .unwrap_err()
        .to_string()
        .contains("inode is already in use"));
}

#[test]
fn version_one_index_migrates_without_rehashing_portable_files() {
    let (_dir, options) = setup(Granularity::Both);
    put(&options, "a", &vec![1; 4096]);
    put(&options, "b", &vec![1; 4096]);
    run(&options, false).unwrap();
    let conn = rusqlite::Connection::open(&options.database).unwrap();
    // Reproduce the previous released schema, preserving files/ranges/jobs.
    conn.execute_batch("DROP TABLE btrfs_files; DROP TABLE btrfs_dirs; DROP TABLE discovery; DROP TABLE crawls;
      DROP TABLE file_extents; DROP TABLE extent_state; DROP TABLE chunk_cache; PRAGMA user_version=1;").unwrap();
    drop(conn);
    let result = run(&options, false).unwrap();
    assert_eq!(result.bytes_hashed, 0);
    assert_eq!(result.duplicate_files, 1);
    let conn = rusqlite::Connection::open(&options.database).unwrap();
    assert_eq!(
        conn.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        2
    );
}

#[test]
fn sparse_and_dense_files_match_without_reading_holes() {
    let (_dir, options) = setup(Granularity::File);
    let sparse = File::create(options.roots[0].join("sparse")).unwrap();
    sparse.set_len(1024 * 1024).unwrap();
    sparse.write_all_at(&[42], 512 * 1024).unwrap();
    sparse.sync_all().unwrap();
    let mut dense = vec![0; 1024 * 1024];
    dense[512 * 1024] = 42;
    put(&options, "dense", &dense);
    let result = run(&options, false).unwrap();
    assert_eq!(result.duplicate_files, 1);
    assert!(result.hole_bytes_skipped > 0);
    assert!(result.bytes_hashed < 2 * 1024 * 1024);
}

#[test]
fn verify_rehashes_unchanged_files() {
    let (_dir, mut options) = setup(Granularity::File);
    put(&options, "a", &vec![1; 4096]);
    run(&options, false).unwrap();
    options.verify = true;
    assert_eq!(run(&options, false).unwrap().files_hashed, 1);
}

#[test]
fn concurrent_writes_are_not_cached_as_success() {
    struct Writer;
    impl Backend for Writer {
        fn dedupe(&mut self, _: &File, dest: &File, r: Range, off: u64) -> io::Result<Outcome> {
            dest.write_all_at(&[99], off)?;
            Ok(Outcome::Shared(r.len))
        }
    }
    let (_dir, options) = setup(Granularity::File);
    put(&options, "a", &vec![1; 4096]);
    put(&options, "b", &vec![1; 4096]);
    let result = run_with_backend(&options, true, &mut Writer).unwrap();
    assert_eq!(result.jobs_completed, 0);
    assert!(result.stale > 0);
    let next = run(&options, false).unwrap();
    assert_eq!(next.files_hashed, 1);
    assert_eq!(next.duplicate_files, 0);
}

fn is_btrfs(path: &Path) -> bool {
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;
        let file = File::open(path).unwrap();
        let mut stat = std::mem::MaybeUninit::<libc::statfs>::zeroed();
        // SAFETY: writable statfs output, live fd; read only on success.
        unsafe {
            libc::fstatfs(file.as_raw_fd(), stat.as_mut_ptr()) == 0
                && stat.assume_init().f_type as libc::c_ulong == 0x9123683e
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = path;
        false
    }
}

#[test]
fn native_btrfs_partial_extents_preserve_contents_and_cow() {
    let (dir, mut options) = setup(Granularity::Chunk);
    if !is_btrfs(dir.path()) {
        eprintln!("SKIP: btrfs integration requires a btrfs temp directory");
        return;
    }
    options.chunk_size = 128 * 1024;
    let mut a = vec![1; 128 * 1024];
    a.extend(vec![2; 128 * 1024]);
    let mut b = vec![3; 128 * 1024];
    b.extend(vec![2; 128 * 1024]);
    put(&options, "a", &a);
    put(&options, "b", &b);
    let before = fs::metadata(options.roots[0].join("b")).unwrap();
    let result = run(&options, true).unwrap();
    assert!(result.errors.is_empty(), "{result:?}");
    assert_eq!(result.jobs_completed, 1, "{result:?}");
    let after = fs::metadata(options.roots[0].join("b")).unwrap();
    assert_eq!(before.ino(), after.ino());
    assert_eq!(before.mode(), after.mode());
    assert_eq!(fs::read(options.roots[0].join("a")).unwrap(), a);
    assert_eq!(fs::read(options.roots[0].join("b")).unwrap(), b);
    let second = run(&options, true).unwrap();
    assert_eq!(second.bytes_hashed, 0, "{second:?}");
    assert_eq!(second.jobs_attempted, 0);
    let writer = OpenOptions::new()
        .write(true)
        .open(options.roots[0].join("b"))
        .unwrap();
    writer.write_all_at(&[99], 128 * 1024).unwrap();
    assert_eq!(fs::read(options.roots[0].join("a")).unwrap(), a);
}

#[test]
fn native_btrfs_whole_file_handles_large_files_and_eof_tail() {
    let (dir, options) = setup(Granularity::File);
    if !is_btrfs(dir.path()) {
        eprintln!("SKIP: btrfs integration requires a btrfs temp directory");
        return;
    }
    let data = vec![7; 17 * 1024 * 1024 + 123];
    put(&options, "a", &data);
    put(&options, "b", &data);
    let result = run(&options, true).unwrap();
    assert!(result.errors.is_empty(), "{result:?}");
    assert_eq!(result.jobs_completed, 1, "{result:?}");
    assert_eq!(fs::read(options.roots[0].join("b")).unwrap(), data);
    assert_eq!(run(&options, true).unwrap().jobs_attempted, 0);
}

#[test]
fn native_kernel_rejects_false_hash_matches() {
    let (dir, options) = setup(Granularity::File);
    if !is_btrfs(dir.path()) {
        eprintln!("SKIP: native validation requires btrfs");
        return;
    }
    put(&options, "a", &vec![1; 128 * 1024]);
    put(&options, "b", &vec![2; 128 * 1024]);
    run(&options, false).unwrap();
    let conn = rusqlite::Connection::open(&options.database).unwrap();
    conn.execute("UPDATE ranges SET hash=zeroblob(32)", [])
        .unwrap();
    drop(conn);
    let result = run_with_backend(&options, true, &mut NativeBackend).unwrap();
    assert_eq!(result.jobs_completed, 0);
    assert!(result.stale > 0);
    assert_eq!(
        fs::read(options.roots[0].join("a")).unwrap(),
        vec![1; 128 * 1024]
    );
    assert_eq!(
        fs::read(options.roots[0].join("b")).unwrap(),
        vec![2; 128 * 1024]
    );
}

#[test]
fn duplicate_list_streams_representatives_and_offsets() {
    let (_dir, options) = setup(Granularity::Both);
    put(&options, "a", &vec![1; 4096]);
    put(&options, "b", &vec![1; 4096]);
    run(&options, false).unwrap();
    let mut files = 0;
    let mut chunks = 0;
    dedupit::list_duplicates(&options, |duplicate| {
        assert_ne!(duplicate.source, duplicate.destination);
        assert_eq!(duplicate.source_offset, 0);
        assert_eq!(duplicate.destination_offset, 0);
        assert_eq!(duplicate.length, 4096);
        if duplicate.whole_file {
            files += 1;
        } else {
            chunks += 1;
        }
        Ok(())
    })
    .unwrap();
    assert_eq!((files, chunks), (1, 1));
}

#[test]
fn unsupported_groups_do_not_attempt_every_pair() {
    struct Unsupported(usize);
    impl Backend for Unsupported {
        fn dedupe(&mut self, _: &File, _: &File, _: Range, _: u64) -> io::Result<Outcome> {
            self.0 += 1;
            Ok(Outcome::Unsupported)
        }
    }
    let (_dir, options) = setup(Granularity::File);
    for i in 0..50 {
        put(&options, &format!("{i}"), &vec![1; 4096]);
    }
    let mut backend = Unsupported(0);
    let result = run_with_backend(&options, true, &mut backend).unwrap();
    assert_eq!(result.jobs_completed, 0);
    assert_eq!(backend.0, 49);
}

#[test]
fn native_btrfs_crash_after_sharing_replays_safely() {
    struct Interrupted;
    impl Backend for Interrupted {
        fn dedupe(
            &mut self,
            source: &File,
            dest: &File,
            range: Range,
            off: u64,
        ) -> io::Result<Outcome> {
            let outcome = NativeBackend.dedupe(source, dest, range, off)?;
            assert!(matches!(
                outcome,
                Outcome::Shared(_) | Outcome::AlreadyShared(_)
            ));
            panic!("simulated interruption after sharing and before checkpoint");
        }
    }
    let (dir, options) = setup(Granularity::File);
    if !is_btrfs(dir.path()) {
        eprintln!("SKIP: crash replay requires btrfs");
        return;
    }
    let data = vec![1; 128 * 1024];
    put(&options, "a", &data);
    put(&options, "b", &data);
    let interrupted = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        run_with_backend(&options, true, &mut Interrupted).unwrap();
    }));
    assert!(interrupted.is_err());
    let replay = run(&options, true).unwrap();
    assert!(replay.errors.is_empty(), "{replay:?}");
    assert_eq!(replay.jobs_completed, 1);
    assert_eq!(fs::read(options.roots[0].join("a")).unwrap(), data);
    assert_eq!(fs::read(options.roots[0].join("b")).unwrap(), data);
    assert_eq!(run(&options, true).unwrap().jobs_attempted, 0);
}

#[cfg(target_os = "linux")]
fn extent_physical(file: &File, offset: u64) -> u64 {
    use std::os::fd::AsRawFd;
    #[repr(C)]
    #[derive(Default)]
    struct Header {
        start: u64,
        length: u64,
        flags: u32,
        mapped: u32,
        count: u32,
        reserved: u32,
    }
    #[repr(C)]
    #[derive(Default)]
    struct Extent {
        logical: u64,
        physical: u64,
        length: u64,
        reserved64: [u64; 2],
        flags: u32,
        reserved: [u32; 3],
    }
    #[repr(C)]
    #[derive(Default)]
    struct Request {
        header: Header,
        extent: Extent,
    }
    nix::ioctl_readwrite!(mapping, b'f', 11, Header);
    let mut request = Request {
        header: Header {
            start: offset,
            length: 4096,
            count: 1,
            ..Default::default()
        },
        ..Default::default()
    };
    file.sync_all().unwrap();
    // SAFETY: C-layout output buffer includes the single requested extent.
    let pointer = std::ptr::addr_of_mut!(request).cast::<Header>();
    unsafe { mapping(file.as_raw_fd(), pointer) }.unwrap();
    assert_eq!(request.header.mapped, 1);
    assert!(request.extent.logical <= offset);
    assert!(request.extent.logical + request.extent.length > offset);
    // This helper is used only for uncompressed, aligned data below.
    assert_eq!(
        request.extent.flags & (0x2 | 0x4 | 0x8 | 0x80 | 0x100 | 0x200 | 0x400 | 0x800),
        0
    );
    request.extent.physical + offset - request.extent.logical
}

#[cfg(target_os = "linux")]
#[test]
fn native_btrfs_physically_shares_ranges_and_recognizes_existing_sharing() {
    let (dir, mut options) = setup(Granularity::Chunk);
    if !is_btrfs(dir.path()) {
        eprintln!("SKIP: physical mapping test requires btrfs");
        return;
    }
    options.chunk_size = 128 * 1024;
    // Incompressible data exercises FIEMAP as well as the compressed test above.
    let mut state = 123456789u64;
    let a: Vec<u8> = (0..256 * 1024)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect();
    let mut b = a.clone();
    b[0] ^= 1;
    put(&options, "a", &a);
    put(&options, "b", &b);
    let source = File::open(options.roots[0].join("a")).unwrap();
    let target = OpenOptions::new()
        .read(true)
        .write(true)
        .open(options.roots[0].join("b"))
        .unwrap();
    assert_ne!(
        extent_physical(&source, 128 * 1024),
        extent_physical(&target, 128 * 1024)
    );
    let result = run(&options, true).unwrap();
    assert!(result.errors.is_empty(), "{result:?}");
    assert_eq!(result.jobs_completed, 1);
    assert_eq!(
        extent_physical(&source, 128 * 1024),
        extent_physical(&target, 128 * 1024)
    );
    assert_eq!(
        NativeBackend
            .dedupe(
                &source,
                &target,
                Range {
                    offset: 128 * 1024,
                    len: 128 * 1024
                },
                128 * 1024
            )
            .unwrap(),
        Outcome::AlreadyShared(128 * 1024)
    );
}
