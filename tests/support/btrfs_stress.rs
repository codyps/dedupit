//! Seeded, process-level differential test. Keep databases outside the scope:
//! the oracle must never index either database or influence the workload.
use super::{first, setup};
use rusqlite::{Connection, OpenFlags};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    ffi::OsString,
    fs::{self, File, OpenOptions},
    os::{
        fd::AsRawFd,
        unix::{
            ffi::OsStringExt, fs::symlink, fs::FileExt, fs::MetadataExt, process::ExitStatusExt,
        },
    },
    path::{Path, PathBuf},
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};
use walkdir::WalkDir;

// SplitMix64 is specified here so replay is independent of dependency versions.
struct Random(u64);
impl Random {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e3779b97f4a7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        z ^ (z >> 31)
    }
    fn below(&mut self, limit: usize) -> usize {
        (self.next() % limit as u64) as usize
    }
    fn bytes(&mut self, length: usize) -> Vec<u8> {
        // A small block pool deliberately creates duplicate chunks and files,
        // including zeros, compressible data and incompressible data.
        match self.below(4) {
            0 => vec![0; length],
            1 => vec![42; length],
            _ => {
                let mut block = Random(self.below(8) as u64 + 1);
                let block: Vec<_> = (0..4096).map(|_| block.next() as u8).collect();
                block.into_iter().cycle().take(length).collect()
            }
        }
    }
    fn size(&mut self, maximum: usize) -> usize {
        if self.below(3) == 0 {
            let boundaries = [0, 1, 4095, 4096, 4097, 65535, 65536, 65537];
            boundaries[self.below(boundaries.len())].min(maximum)
        } else {
            self.below(maximum + 1)
        }
    }
}

struct Workload {
    inside: PathBuf,
    outside: PathBuf,
    random: Random,
    serial: u64,
}
impl Workload {
    fn entries(&self, root: &Path, directory: bool) -> Vec<PathBuf> {
        let mut paths: Vec<_> = WalkDir::new(root)
            .follow_links(false)
            .into_iter()
            .map(Result::unwrap)
            .filter(|e| {
                e.path() != root
                    && if directory {
                        e.file_type().is_dir()
                    } else {
                        e.file_type().is_file()
                    }
            })
            .map(|e| e.into_path())
            .collect();
        paths.sort();
        paths
    }
    fn choose(&mut self, root: &Path, directory: bool) -> Option<PathBuf> {
        let paths = self.entries(root, directory);
        (!paths.is_empty()).then(|| paths[self.random.below(paths.len())].clone())
    }
    fn name(&mut self, root: &Path) -> PathBuf {
        self.serial += 1;
        // Include names that JSON/display conversions cannot round trip.
        let name = if self.serial % 7 == 0 {
            let mut name = format!("entry-{}-", self.serial).into_bytes();
            name.push(0xff);
            OsString::from_vec(name)
        } else {
            OsString::from(format!("entry-{}", self.serial))
        };
        root.join(name)
    }
    fn destination(&mut self, root: &Path) -> PathBuf {
        let mut parents = self.entries(root, true);
        parents.retain(|p| p.strip_prefix(root).unwrap().components().count() < 3);
        parents.push(root.to_owned());
        let parent = parents[self.random.below(parents.len())].clone();
        self.name(&parent)
    }
    fn create(&mut self, root: &Path) {
        let path = self.destination(root);
        let size = self.random.size(64 * 1024);
        fs::write(&path, self.random.bytes(size)).unwrap();
        eprintln!("  create {path:?}, size={size}");
    }
    fn mutate(&mut self, operation: usize) {
        let root = self.inside.clone();
        let outside = self.outside.clone();
        if self.entries(&root, false).is_empty() {
            self.create(&root);
        }
        let file = self.choose(&root, false).unwrap();
        match operation {
            0 => self.create(&root),
            1 => {
                let target = self.destination(&root);
                fs::copy(&file, &target).unwrap();
                eprintln!("  copy {file:?} -> {target:?}");
            }
            2 | 3 | 5 => {
                let handle = OpenOptions::new().write(true).open(&file).unwrap();
                let size = handle.metadata().unwrap().len();
                let offset = match operation {
                    2 => self.random.below(size.max(1) as usize) as u64,
                    3 => size,
                    _ => 128 * 1024 + self.random.below(64 * 1024) as u64,
                };
                let length = 1 + self.random.below(8192);
                handle
                    .write_all_at(&self.random.bytes(length), offset)
                    .unwrap();
                eprintln!("  write op={operation} {file:?} at {offset}, len={length}");
            }
            4 => {
                let size = self.random.size(128 * 1024) as u64;
                OpenOptions::new()
                    .write(true)
                    .open(&file)
                    .unwrap()
                    .set_len(size)
                    .unwrap();
                eprintln!("  truncate/extend {file:?}, size={size}");
            }
            6 => {
                let handle = OpenOptions::new().write(true).open(&file).unwrap();
                let offset = self.random.below(16) as i64 * 4096;
                // SAFETY: live fd, valid flags and nonnegative aligned bounds.
                let result = unsafe {
                    libc::fallocate(
                        handle.as_raw_fd(),
                        libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
                        offset,
                        8192,
                    )
                };
                assert_eq!(result, 0, "hole punch: {}", std::io::Error::last_os_error());
                eprintln!("  punch hole {file:?} at {offset}, len=8192");
            }
            7 | 9 => {
                let target = self.destination(&root);
                if operation == 7 {
                    fs::rename(&file, &target).unwrap();
                } else {
                    fs::hard_link(&file, &target).unwrap();
                }
                eprintln!("  rename/link op={operation} {file:?} -> {target:?}");
            }
            8 => {
                fs::remove_file(&file).unwrap();
                eprintln!("  unlink {file:?}");
            }
            10 | 11 => {
                if let Some(directory) = self.choose(&root, true) {
                    if operation == 10 {
                        let target = self.name(&root);
                        fs::rename(&directory, &target).unwrap();
                        eprintln!("  rename directory {directory:?} -> {target:?}");
                    } else {
                        fs::remove_dir_all(&directory).unwrap();
                        eprintln!("  remove directory tree {directory:?}");
                    }
                } else {
                    let directory = self.name(&root);
                    fs::create_dir(&directory).unwrap();
                    self.create(&directory);
                    eprintln!("  create populated directory {directory:?}");
                }
            }
            12 => {
                let (from, to) = if self.random.below(2) == 0 {
                    (outside, root)
                } else {
                    (root, outside)
                };
                if let Some(source) = self
                    .choose(&from, true)
                    .or_else(|| self.choose(&from, false))
                {
                    let target = self.destination(&to);
                    fs::rename(&source, &target).unwrap();
                    eprintln!("  move across scope {source:?} -> {target:?}");
                } else {
                    self.create(&from);
                }
            }
            13 => {
                let temporary = self.name(&outside);
                let size = self.random.size(64 * 1024);
                fs::write(&temporary, self.random.bytes(size)).unwrap();
                fs::rename(&temporary, &file).unwrap();
                eprintln!("  atomic replacement {file:?}, size={size}");
            }
            14 => {
                let directory = self.destination(&root);
                fs::create_dir(&directory).unwrap();
                self.create(&directory);
                eprintln!("  create populated directory {directory:?}");
            }
            15 => {
                let target = self.destination(&root);
                symlink(&file, &target).unwrap();
                eprintln!("  symlink {target:?} -> {file:?}");
            }
            _ => unreachable!(),
        }
    }
}

fn command(database: &Path, root: &Path, portable: bool) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_dedupit"));
    cmd.arg("--database")
        .arg(database)
        .args(["--json", "--chunk-size", "4096", "--scanner"])
        .arg(if portable { "portable" } else { "btrfs" });
    if portable {
        cmd.arg("--verify");
    }
    cmd.arg(root);
    cmd
}
fn run(database: &Path, root: &Path, portable: bool) -> Value {
    let output = command(database, root, portable).output().unwrap();
    assert!(
        output.status.success(),
        "dedupit {:?}: {}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stderr),
        String::from_utf8_lossy(&output.stdout)
    );
    let stats: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(stats["errors"], serde_json::json!([]));
    assert_eq!(stats["scanner_fallbacks"], serde_json::json!([]));
    if !portable {
        assert!(stats["btrfs_subvolumes"].as_u64().unwrap() > 0);
    }
    stats
}

fn interrupt(database: &Path, root: &Path, log: &Path) -> bool {
    let output = File::create(log).unwrap();
    let mut child = command(database, root, false)
        .stdout(Stdio::from(output.try_clone().unwrap()))
        .stderr(Stdio::from(output))
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(
                status.success(),
                "child exited {status}: {}",
                fs::read_to_string(log).unwrap()
            );
            eprintln!("  scan completed before interruption");
            return false;
        }
        let queued = Connection::open_with_flags(database, OpenFlags::SQLITE_OPEN_READ_ONLY)
            .and_then(|conn| {
                conn.query_row("SELECT COUNT(*) FROM discovery", [], |r| r.get::<_, u64>(0))
            })
            .unwrap_or(0);
        if queued > 0 {
            child.kill().unwrap();
            let status = child.wait().unwrap();
            if status.success() {
                eprintln!("  scan finished while delivering interruption");
                return false;
            }
            assert_eq!(
                status.signal(),
                Some(libc::SIGKILL),
                "child failed before interruption: {}",
                fs::read_to_string(log).unwrap()
            );
            eprintln!("  killed process with {queued} durable discovery jobs; restarting");
            return true;
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("scan did not make progress before interruption deadline");
        }
        thread::sleep(Duration::from_millis(1));
    }
}

#[derive(Debug, PartialEq, Eq)]
struct IndexedFile {
    fingerprint: Vec<u8>,
    ranges: Vec<(u32, u64, u64, Vec<u8>)>,
}
fn inventory(database: &Path, root: &Path) -> BTreeMap<(u64, u64), IndexedFile> {
    let conn = Connection::open_with_flags(database, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    assert_eq!(
        conn.query_row("PRAGMA integrity_check", [], |r| r.get::<_, String>(0))
            .unwrap(),
        "ok"
    );
    let files = conn
        .prepare("SELECT id,dev,ino,path,fingerprint FROM files")
        .unwrap()
        .query_map([], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, u64>(1)?,
                r.get::<_, u64>(2)?,
                r.get::<_, Vec<u8>>(3)?,
                r.get::<_, Vec<u8>>(4)?,
            ))
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    let mut result = BTreeMap::new();
    for (id, dev, ino, path, fingerprint) in files {
        let path = PathBuf::from(OsString::from_vec(path));
        assert!(
            path.starts_with(root),
            "out-of-scope file in index: {path:?}"
        );
        let metadata = fs::symlink_metadata(&path)
            .unwrap_or_else(|error| panic!("indexed path {path:?} is unavailable: {error}"));
        assert!(
            metadata.is_file(),
            "indexed a symlink or special file: {path:?}"
        );
        assert_eq!(
            (dev, ino),
            (metadata.dev(), metadata.ino()),
            "stale path: {path:?}"
        );
        let ranges = conn
            .prepare(
                "SELECT kind,offset,length,hash FROM ranges WHERE file_id=?1 ORDER BY kind,offset",
            )
            .unwrap()
            .query_map([id], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        result.insert(
            (dev, ino),
            IndexedFile {
                fingerprint,
                ranges,
            },
        );
    }
    result
}

#[test]
fn random_changes_and_process_restarts_match_fresh_full_scan() {
    let seeds = if let Ok(value) = std::env::var("DEDUPIT_STRESS_SEED") {
        vec![value
            .parse::<u64>()
            .expect("DEDUPIT_STRESS_SEED must be a decimal u64")]
    } else {
        vec![1, 0xdead_beef, 0x0123_4567_89ab_cdef]
    };
    let rounds = std::env::var("DEDUPIT_STRESS_ROUNDS")
        .map(|v| v.parse::<usize>().unwrap())
        .unwrap_or(16);
    assert!((1..=256).contains(&rounds));
    for seed in seeds {
        eprintln!("STRESS seed={seed}, rounds={rounds}");
        let (dir, options) = setup();
        if first(&options).is_none() {
            return;
        }
        let outside = dir.path().join("outside");
        fs::create_dir(&outside).unwrap();
        let mut workload = Workload {
            inside: options.roots[0].clone(),
            outside,
            random: Random(seed),
            serial: 0,
        };
        for _ in 0..12 {
            workload.create(&options.roots[0]);
        }
        let nested = workload.outside.join("old/child");
        fs::create_dir_all(&nested).unwrap();
        workload.create(&nested);
        run(&options.database, &options.roots[0], false);
        let mut interruptions = 0;
        for round in 0..rounds {
            eprintln!("STRESS seed={seed}, round={round}");
            let mut operations: Vec<_> = (0..16).collect();
            for i in (1..operations.len()).rev() {
                let other = workload.random.below(i + 1);
                operations.swap(i, other);
            }
            for operation in operations {
                workload.mutate(operation);
            }
            if round % 4 == 2 {
                let interrupted = interrupt(
                    &options.database,
                    &options.roots[0],
                    &dir.path().join("interrupted.log"),
                );
                interruptions += usize::from(interrupted);
                if interrupted {
                    eprintln!("  mutating while dedupit is stopped");
                    workload.mutate(2);
                    workload.mutate(0);
                }
            }
            let incremental = run(&options.database, &options.roots[0], false);
            // A different empty database each round is an independent oracle;
            // --verify and --scanner portable prohibit metadata/hash reuse.
            let oracle_dir = tempfile::tempdir_in(dir.path()).unwrap();
            let fresh = oracle_dir.path().join("full.sqlite");
            let full = run(&fresh, &options.roots[0], true);
            let actual = inventory(&options.database, &options.roots[0]);
            let expected = inventory(&fresh, &options.roots[0]);
            assert_eq!(
                actual.keys().collect::<Vec<_>>(),
                expected.keys().collect::<Vec<_>>(),
                "file inventory mismatch: seed={seed}, round={round}"
            );
            for (identity, expected) in expected {
                assert_eq!(
                    actual[&identity], expected,
                    "seed={seed}, round={round}, file={identity:?}"
                );
            }
            for field in [
                "duplicate_files",
                "duplicate_file_bytes",
                "duplicate_chunks",
                "duplicate_chunk_bytes",
            ] {
                assert_eq!(
                    incremental[field], full[field],
                    "seed={seed}, round={round}, field={field}"
                );
            }
            let idle = run(&options.database, &options.roots[0], false);
            assert_eq!(idle["bytes_hashed"], 0, "seed={seed}, round={round}");
            assert_eq!(idle["files_seen"], 0, "seed={seed}, round={round}");
            eprintln!("STRESS verified seed={seed}, round={round}");
        }
        if rounds >= 4 {
            assert!(
                interruptions > 0,
                "no queued scan was interrupted for seed={seed}"
            );
        }
        eprintln!("STRESS complete seed={seed}, interruptions={interruptions}");
    }
}
