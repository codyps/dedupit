//! Seeded, process-level differential test. Keep databases outside the scope:
//! the oracle must never index either database or influence the workload.
use super::{first, setup};
use rusqlite::{Connection, OpenFlags};
use serde_json::Value;
use std::{
    cell::RefCell,
    collections::BTreeMap,
    ffi::OsString,
    fs::{self, OpenOptions},
    io::Read,
    os::{
        fd::AsRawFd,
        unix::{ffi::OsStringExt, fs::symlink, fs::FileExt, fs::MetadataExt, fs::OpenOptionsExt},
    },
    path::{Path, PathBuf},
    process::Command,
    rc::Rc,
};
#[cfg(not(feature = "test-failpoints"))]
use std::{
    fs::File,
    os::unix::process::ExitStatusExt,
    process::Stdio,
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

#[derive(Clone)]
enum Expected {
    Directory,
    File(Rc<RefCell<ExpectedFile>>),
    Symlink(PathBuf),
}
struct ExpectedFile {
    identity: (u64, u64),
    bytes: Vec<u8>,
}

struct Workload {
    inside: PathBuf,
    outside: PathBuf,
    random: Random,
    serial: u64,
    // Independent of dedupit's hashes and of reads from the real files. Hardlink
    // names share one model object; independent copies never share that object.
    expected: BTreeMap<PathBuf, Expected>,
}
impl Workload {
    fn controls(&self) -> PathBuf {
        self.inside.join("sharing-controls")
    }
    fn entries(&self, root: &Path, directory: bool) -> Vec<PathBuf> {
        let mut paths: Vec<_> = WalkDir::new(root)
            .follow_links(false)
            .into_iter()
            .map(Result::unwrap)
            .filter(|e| {
                e.path() != root
                    && !e.path().starts_with(self.controls())
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
        let bytes = self.random.bytes(size);
        self.store_file(&path, bytes);
        eprintln!("  create {path:?}, size={size}");
    }
    fn store_file(&mut self, path: &Path, bytes: Vec<u8>) {
        fs::write(path, &bytes).unwrap();
        let meta = fs::symlink_metadata(path).unwrap();
        self.expected.insert(
            path.to_owned(),
            Expected::File(Rc::new(RefCell::new(ExpectedFile {
                identity: (meta.dev(), meta.ino()),
                bytes,
            }))),
        );
    }
    fn contents(&self, path: &Path) -> Rc<RefCell<ExpectedFile>> {
        match self.expected.get(path) {
            Some(Expected::File(file)) => Rc::clone(file),
            _ => panic!("file absent from content model: {path:?}"),
        }
    }
    fn directory(&mut self, path: &Path) {
        fs::create_dir(path).unwrap();
        self.expected.insert(path.to_owned(), Expected::Directory);
    }
    fn rename(&mut self, source: &Path, target: &Path) {
        fs::rename(source, target).unwrap();
        let moved: Vec<_> = self
            .expected
            .keys()
            .filter(|p| p.starts_with(source))
            .cloned()
            .collect();
        assert!(
            !moved.is_empty(),
            "rename source missing from model: {source:?}"
        );
        for path in moved {
            let entry = self.expected.remove(&path).unwrap();
            let suffix = path.strip_prefix(source).unwrap();
            let destination = if suffix.as_os_str().is_empty() {
                target.to_owned()
            } else {
                target.join(suffix)
            };
            self.expected.insert(destination, entry);
        }
    }
    fn forget(&mut self, path: &Path) {
        self.expected.retain(|p, _| !p.starts_with(path));
    }
    fn write_at(&mut self, path: &Path, bytes: &[u8], offset: u64) {
        OpenOptions::new()
            .write(true)
            .open(path)
            .unwrap()
            .write_all_at(bytes, offset)
            .unwrap();
        let file = self.contents(path);
        let mut file = file.borrow_mut();
        let end = offset as usize + bytes.len();
        if end > file.bytes.len() {
            file.bytes.resize(end, 0);
        }
        file.bytes[offset as usize..end].copy_from_slice(bytes);
    }
    fn verify_contents(&self, stage: &str) {
        let mut actual: Vec<_> = [&self.inside, &self.outside]
            .into_iter()
            .flat_map(|root| {
                WalkDir::new(root)
                    .follow_links(false)
                    .into_iter()
                    .map(|e| e.unwrap().into_path())
            })
            .collect();
        actual.sort();
        assert_eq!(
            actual,
            self.expected.keys().cloned().collect::<Vec<_>>(),
            "namespace mismatch {stage}"
        );
        for (path, expected) in &self.expected {
            let meta = fs::symlink_metadata(path).unwrap();
            match expected {
                Expected::Directory => {
                    assert!(meta.is_dir(), "directory changed: {path:?}, {stage}")
                }
                Expected::Symlink(target) => {
                    assert!(meta.is_symlink(), "symlink changed: {path:?}, {stage}");
                    assert_eq!(
                        fs::read_link(path).unwrap(),
                        *target,
                        "symlink target changed: {path:?}, {stage}"
                    );
                }
                Expected::File(file) => {
                    let file = file.borrow();
                    assert!(meta.is_file(), "file type changed: {path:?}, {stage}");
                    assert_eq!(
                        (meta.dev(), meta.ino()),
                        file.identity,
                        "file identity changed: {path:?}, {stage}"
                    );
                    // Do not generate atime changes that would disturb idle-run assertions.
                    let mut actual = Vec::new();
                    OpenOptions::new()
                        .read(true)
                        .custom_flags(libc::O_NOATIME | libc::O_NOFOLLOW)
                        .open(path)
                        .unwrap()
                        .read_to_end(&mut actual)
                        .unwrap();
                    assert_eq!(
                        actual.len(),
                        file.bytes.len(),
                        "file length changed: {path:?}, {stage}"
                    );
                    if let Some(offset) = actual.iter().zip(&file.bytes).position(|(a, b)| a != b) {
                        panic!("content mismatch {path:?}, offset={offset}, expected={}, actual={}, {stage}", file.bytes[offset], actual[offset]);
                    }
                }
            }
        }
    }
    fn prepare_sharing(&mut self, seed: u64, round: usize) {
        let controls = self.controls();
        if !self.expected.contains_key(&controls) {
            self.directory(&controls);
        }
        let mut random = Random(seed ^ (round as u64).wrapping_mul(0xd1342543de82ef95));
        // Incompressible, aligned controls must exercise real whole-file and
        // partial-range sharing, regardless of the random workload's duplicates.
        let whole: Vec<_> = (0..65536).map(|_| random.next() as u8).collect();
        self.store_file(&controls.join("whole-a"), whole.clone());
        self.store_file(&controls.join("whole-c"), whole.clone());
        self.store_file(&controls.join("whole-d"), whole.clone());
        self.store_file(&controls.join("whole-b"), whole);
        let first: Vec<_> = (0..12288).map(|_| random.next() as u8).collect();
        let mut second: Vec<_> = (0..12288).map(|_| random.next() as u8).collect();
        second[4096..8192].copy_from_slice(&first[4096..8192]);
        self.store_file(&controls.join("partial-a"), first);
        self.store_file(&controls.join("partial-b"), second);
    }
    fn check_sharing(&self, database: &Path) {
        let conn = Connection::open_with_flags(database, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        for (kind, names) in [(1, ["whole-a", "whole-b"]), (0, ["partial-a", "partial-b"])] {
            let paths = names.map(|name| self.controls().join(name).into_os_string().into_vec());
            let completed: u64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM completed c JOIN ranges r ON r.id=c.target
                 JOIN files f ON f.id=r.file_id WHERE r.kind=?1 AND f.path IN (?2,?3)",
                    rusqlite::params![kind, paths[0], paths[1]],
                    |r| r.get(0),
                )
                .unwrap();
            assert!(
                completed > 0,
                "--apply did not complete control sharing kind={kind}"
            );
        }
    }
    fn probe_cow(&mut self, round: usize) {
        for (name, offset) in [("whole-a", 4096), ("partial-a", 4096)] {
            let path = self.controls().join(name);
            let original = self.contents(&path).borrow().bytes[offset];
            self.write_at(&path, &[original ^ 0xff, round as u8], offset as u64);
        }
        // The independently modeled B files must retain their original bytes.
        self.verify_contents("after writes to shared whole-file and partial-range sources");
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
                let bytes = self.contents(&file).borrow().bytes.clone();
                let meta = fs::symlink_metadata(&target).unwrap();
                self.expected.insert(
                    target.clone(),
                    Expected::File(Rc::new(RefCell::new(ExpectedFile {
                        identity: (meta.dev(), meta.ino()),
                        bytes,
                    }))),
                );
                eprintln!("  copy {file:?} -> {target:?}");
            }
            2 | 3 | 5 => {
                let size = self.contents(&file).borrow().bytes.len() as u64;
                let offset = match operation {
                    2 => self.random.below(size.max(1) as usize) as u64,
                    3 => size,
                    _ => 128 * 1024 + self.random.below(64 * 1024) as u64,
                };
                let length = 1 + self.random.below(8192);
                let bytes = self.random.bytes(length);
                self.write_at(&file, &bytes, offset);
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
                self.contents(&file)
                    .borrow_mut()
                    .bytes
                    .resize(size as usize, 0);
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
                let model = self.contents(&file);
                let mut model = model.borrow_mut();
                let end = (offset as usize + 8192).min(model.bytes.len());
                if (offset as usize) < end {
                    model.bytes[offset as usize..end].fill(0);
                }
                eprintln!("  punch hole {file:?} at {offset}, len=8192");
            }
            7 | 9 => {
                let target = self.destination(&root);
                if operation == 7 {
                    self.rename(&file, &target);
                } else {
                    fs::hard_link(&file, &target).unwrap();
                    self.expected
                        .insert(target.clone(), Expected::File(self.contents(&file)));
                }
                eprintln!("  rename/link op={operation} {file:?} -> {target:?}");
            }
            8 => {
                fs::remove_file(&file).unwrap();
                self.forget(&file);
                eprintln!("  unlink {file:?}");
            }
            10 | 11 => {
                if let Some(directory) = self.choose(&root, true) {
                    if operation == 10 {
                        let target = self.name(&root);
                        self.rename(&directory, &target);
                        eprintln!("  rename directory {directory:?} -> {target:?}");
                    } else {
                        fs::remove_dir_all(&directory).unwrap();
                        self.forget(&directory);
                        eprintln!("  remove directory tree {directory:?}");
                    }
                } else {
                    let directory = self.name(&root);
                    self.directory(&directory);
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
                    self.rename(&source, &target);
                    eprintln!("  move across scope {source:?} -> {target:?}");
                } else {
                    self.create(&from);
                }
            }
            13 => {
                let temporary = self.name(&outside);
                let size = self.random.size(64 * 1024);
                let bytes = self.random.bytes(size);
                self.store_file(&temporary, bytes);
                self.rename(&temporary, &file);
                eprintln!("  atomic replacement {file:?}, size={size}");
            }
            14 => {
                let directory = self.destination(&root);
                self.directory(&directory);
                self.create(&directory);
                eprintln!("  create populated directory {directory:?}");
            }
            15 => {
                let target = self.destination(&root);
                symlink(&file, &target).unwrap();
                self.expected
                    .insert(target.clone(), Expected::Symlink(file.clone()));
                eprintln!("  symlink {target:?} -> {file:?}");
            }
            _ => unreachable!(),
        }
    }
}

fn command(database: &Path, root: &Path, portable: bool, apply: bool) -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_dedupit"));
    cmd.arg("--database")
        .arg(database)
        .args(["--commit-batch-size", "2"])
        .args(["--json", "--chunk-size", "4096", "--scanner"])
        .arg(if portable { "portable" } else { "btrfs" });
    if portable {
        cmd.arg("--verify");
    }
    if apply {
        cmd.arg("--apply");
    }
    cmd.arg(root);
    cmd
}
pub(super) fn run(database: &Path, root: &Path, portable: bool, apply: bool) -> Value {
    let output = command(database, root, portable, apply).output().unwrap();
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

#[cfg(feature = "test-failpoints")]
fn interrupt(database: &Path, root: &Path, _log: &Path, apply: bool) -> bool {
    let observation = super::batch_crashes::crash_at(&super::batch_crashes::Crash {
        database,
        root,
        portable: false,
        apply,
        batch_size: 2,
        hit: 3,
        event: if apply {
            "apply:staged"
        } else {
            "native-file:staged"
        },
    });
    assert_eq!(observation.staged["autocommit"], false);
    if apply {
        assert!(
            observation.staged["completed"].as_u64().unwrap()
                > observation.durable["completed"].as_u64().unwrap()
        );
    } else {
        assert!(
            observation.staged["discovery"].as_u64().unwrap()
                < observation.durable["discovery"].as_u64().unwrap()
        );
    }
    eprintln!(
        "  killed inside uncommitted {} batch after a previous batch committed",
        if apply { "apply" } else { "native-file" }
    );
    true
}

#[cfg(not(feature = "test-failpoints"))]
fn interrupt(database: &Path, root: &Path, log: &Path, apply: bool) -> bool {
    let observer = Connection::open_with_flags(database, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    observer.busy_timeout(Duration::from_millis(50)).unwrap();
    // A rescan can remove old completion rows, so their count is not monotonic.
    // Fresh sharing controls guarantee new AUTOINCREMENT range IDs each round;
    // a newly completed ID therefore identifies durable apply progress.
    let completed_before = if apply {
        observer
            .query_row("SELECT COALESCE(MAX(target),0) FROM completed", [], |r| {
                r.get::<_, u64>(0)
            })
            .unwrap()
    } else {
        0
    };
    let output = File::create(log).unwrap();
    let mut child = command(database, root, false, apply)
        .stdout(Stdio::from(output.try_clone().unwrap()))
        .stderr(Stdio::from(output))
        .spawn()
        .unwrap();
    // Emulated apply/fsync can take much longer than a scan. Short bounded read
    // waits and a persistent observer avoid reopening SQLite on every poll.
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut last_error = String::new();
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
        let observation = observer.query_row(if apply {
                    "SELECT (SELECT COALESCE(MAX(target),0) FROM completed), (SELECT COUNT(*) FROM ranges WHERE pending=1)"
                } else {
                    "SELECT 0, COUNT(*) FROM discovery"
                }, [], |r| Ok((r.get::<_, u64>(0)?, r.get::<_, u64>(1)?)));
        let (progress, pending) = match observation {
            Ok(observation) => {
                last_error.clear();
                observation
            }
            Err(error) => {
                last_error = error.to_string();
                (0, 0)
            }
        };
        if pending > 0 && (!apply || progress > completed_before) {
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
            if apply {
                eprintln!("  killed --apply after new completed target={progress}, with {pending} pending ranges; restarting");
            } else {
                eprintln!("  killed process with {pending} durable discovery jobs; restarting");
            }
            return true;
        }
        if Instant::now() >= deadline {
            // The child may have exited while the last observation was waiting
            // for SQLite. A missed interruption window is not a stalled child.
            if let Some(status) = child.try_wait().unwrap() {
                assert!(
                    status.success(),
                    "child exited {status}: {}",
                    fs::read_to_string(log).unwrap()
                );
                eprintln!("  scan completed before interruption deadline");
                return false;
            }
            let wait =
                fs::read_to_string(format!("/proc/{}/wchan", child.id())).unwrap_or_default();
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("interruption deadline: apply={apply}, prior_target={completed_before}, current_target={progress}, pending={pending}, sqlite={last_error:?}, wait={wait:?}, child_log={}", fs::read_to_string(log).unwrap());
        }
        thread::sleep(Duration::from_millis(5));
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct IndexedFile {
    fingerprint: Vec<u8>,
    ranges: Vec<(u32, u64, u64, Vec<u8>)>,
}
pub(super) fn inventory(database: &Path, root: &Path) -> BTreeMap<(u64, u64), IndexedFile> {
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
fn independent_content_oracle_tracks_aliases_and_rejects_matching_corruption() {
    let (dir, options) = setup();
    let outside = dir.path().join("outside");
    fs::create_dir(&outside).unwrap();
    let mut workload = Workload {
        inside: options.roots[0].clone(),
        outside: outside.clone(),
        random: Random(1),
        serial: 0,
        expected: BTreeMap::from([
            (options.roots[0].clone(), Expected::Directory),
            (outside.clone(), Expected::Directory),
        ]),
    };
    let a = options.roots[0].join("a");
    let b = options.roots[0].join("b");
    let alias = outside.join("alias");
    workload.store_file(&a, vec![42; 8]);
    workload.store_file(&b, vec![42; 8]);
    fs::hard_link(&a, &alias).unwrap();
    workload
        .expected
        .insert(alias, Expected::File(workload.contents(&a)));
    workload.write_at(&a, &[99], 2);
    workload.verify_contents("hardlink changes propagate; independent copy retains its bytes");
    let replacement = outside.join("replacement");
    workload.store_file(&replacement, vec![7; 8]);
    workload.rename(&replacement, &a);
    workload.verify_contents("atomic replacement leaves old hardlink aliases intact");
    // A fresh scan would faithfully hash these corrupted bytes. The independent
    // content model must reject them even when both files become identical.
    fs::write(&a, [0; 8]).unwrap();
    fs::write(&b, [0; 8]).unwrap();
    let error = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        workload.verify_contents("deliberate matching corruption");
    }))
    .unwrap_err();
    assert!(error
        .downcast_ref::<String>()
        .unwrap()
        .contains("content mismatch"));
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
            expected: BTreeMap::from([
                (options.roots[0].clone(), Expected::Directory),
                (dir.path().join("outside"), Expected::Directory),
            ]),
        };
        for _ in 0..12 {
            workload.create(&options.roots[0]);
        }
        let nested = workload.outside.join("old/child");
        workload.directory(&workload.outside.join("old"));
        workload.directory(&nested);
        workload.create(&nested);
        workload.prepare_sharing(seed, 0);
        workload.verify_contents("before initial --apply");
        let initial = run(&options.database, &options.roots[0], false, true);
        assert!(initial["jobs_completed"].as_u64().unwrap() > 0);
        assert!(initial["bytes_deduped"].as_u64().unwrap() > 0);
        workload.check_sharing(&options.database);
        workload.verify_contents("after initial --apply");
        let mut interruptions = 0;
        let mut apply_interruptions = 0;
        for round in 0..rounds {
            eprintln!("STRESS seed={seed}, round={round}");
            workload.prepare_sharing(seed, round + 1);
            let mut operations: Vec<_> = (0..16).collect();
            for i in (1..operations.len()).rev() {
                let other = workload.random.below(i + 1);
                operations.swap(i, other);
            }
            for operation in operations {
                workload.mutate(operation);
            }
            workload.verify_contents("after random writes and filesystem mutations");
            if round % 4 == 2 {
                let interrupted = interrupt(
                    &options.database,
                    &options.roots[0],
                    &dir.path().join("interrupted.log"),
                    false,
                );
                interruptions += usize::from(interrupted);
                if interrupted {
                    workload.verify_contents("after interrupted discovery");
                    eprintln!("  mutating while dedupit is stopped");
                    workload.mutate(2);
                    workload.mutate(0);
                }
            }
            run(&options.database, &options.roots[0], false, false);
            if round % 4 == 0 {
                let interrupted = interrupt(
                    &options.database,
                    &options.roots[0],
                    &dir.path().join("interrupted-apply.log"),
                    true,
                );
                apply_interruptions += usize::from(interrupted);
                if interrupted {
                    workload.verify_contents("after interrupted --apply");
                    eprintln!("  mutating while --apply is stopped");
                    workload.mutate(2);
                    workload.mutate(0);
                }
            }
            workload.verify_contents("before resumed --apply");
            run(&options.database, &options.roots[0], false, true);
            workload.check_sharing(&options.database);
            workload.verify_contents("after --apply and restart");
            workload.probe_cow(round);
            let incremental = run(&options.database, &options.roots[0], false, false);
            // A different empty database each round is an independent oracle;
            // --verify and --scanner portable prohibit metadata/hash reuse.
            let oracle_dir = tempfile::tempdir_in(dir.path()).unwrap();
            let fresh = oracle_dir.path().join("full.sqlite");
            let full = run(&fresh, &options.roots[0], true, false);
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
            let idle = run(&options.database, &options.roots[0], false, true);
            assert_eq!(idle["bytes_hashed"], 0, "seed={seed}, round={round}");
            assert_eq!(idle["files_seen"], 0, "seed={seed}, round={round}");
            workload.verify_contents("after idle --apply restart");
            eprintln!("STRESS verified seed={seed}, round={round}");
        }
        if rounds >= 4 {
            assert!(
                interruptions > 0,
                "no queued scan was interrupted for seed={seed}"
            );
            assert!(
                apply_interruptions > 0,
                "no active --apply was interrupted for seed={seed}"
            );
        }
        eprintln!("STRESS complete seed={seed}, interruptions={interruptions}, apply_interruptions={apply_interruptions}, contents verified");
    }
}
