//! Deterministic process crashes with an independent reader at each batch frontier.
use super::{data, first, setup, stress};
use dedupit::{Options, Scanner};
use rusqlite::{types::ValueRef, Connection, OpenFlags};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    os::unix::{fs::FileExt, process::ExitStatusExt},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

pub(super) struct Crash<'a> {
    pub database: &'a Path,
    pub root: &'a Path,
    pub portable: bool,
    pub apply: bool,
    pub event: &'a str,
    pub hit: usize,
    pub batch_size: usize,
}
pub(super) struct Observation {
    pub staged: Value,
    pub durable: Value,
    pub uncommitted_frames: usize,
}
struct ChildGuard(Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn reader(database: &Path) -> Connection {
    let conn = Connection::open_with_flags(database, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
    conn.busy_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(
        conn.query_row("PRAGMA integrity_check", [], |r| r.get::<_, String>(0))
            .unwrap(),
        "ok"
    );
    assert!(!conn
        .prepare("PRAGMA foreign_key_check")
        .unwrap()
        .exists([])
        .unwrap());
    conn
}
fn durable_state(database: &Path) -> (Value, blake3::Hash) {
    let conn = reader(database);
    let count = |table: &str| -> u64 {
        conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))
            .unwrap()
    };
    let counts = serde_json::json!({"files":count("files"), "completed":count("completed"),
        "ranges":count("ranges"), "discovery":count("discovery")});
    let mut hash = blake3::Hasher::new();
    for table in [
        "files",
        "ranges",
        "chunk_cache",
        "file_extents",
        "extent_state",
        "completed",
        "crawls",
        "discovery",
        "btrfs_files",
        "btrfs_dirs",
        "config",
        "sqlite_sequence",
    ] {
        hash.update(table.as_bytes());
        let mut statement = conn
            .prepare(&format!("SELECT * FROM {table} ORDER BY rowid"))
            .unwrap();
        let columns = statement.column_count();
        let mut rows = statement.query([]).unwrap();
        while let Some(row) = rows.next().unwrap() {
            for column in 0..columns {
                let (tag, bytes) = match row.get_ref(column).unwrap() {
                    ValueRef::Null => (0, vec![]),
                    ValueRef::Integer(value) => (1, value.to_le_bytes().to_vec()),
                    ValueRef::Real(value) => (2, value.to_le_bytes().to_vec()),
                    ValueRef::Text(value) => (3, value.to_vec()),
                    ValueRef::Blob(value) => (4, value.to_vec()),
                };
                hash.update(&[tag]);
                hash.update(&(bytes.len() as u64).to_le_bytes());
                hash.update(&bytes);
            }
        }
    }
    (counts, hash.finalize())
}

fn uncommitted_frames(database: &Path) -> usize {
    let bytes = fs::read(database.with_file_name(format!(
        "{}-wal",
        database.file_name().unwrap().to_str().unwrap()
    )))
    .unwrap_or_default();
    if bytes.len() < 32 {
        return 0;
    }
    let size = u32::from_be_bytes(bytes[8..12].try_into().unwrap()) as usize;
    let size = if size == 1 { 65536 } else { size };
    assert!(size.is_power_of_two() && (512..=65536).contains(&size));
    let mut uncommitted = 0;
    for frame in bytes[32..].chunks_exact(size + 24) {
        if frame[8..16] != bytes[16..24] {
            break;
        }
        if frame[4..8] == [0; 4] {
            uncommitted += 1;
        } else {
            uncommitted = 0;
        }
    }
    uncommitted
}

pub(super) fn crash_at(case: &Crash<'_>) -> Observation {
    let work = tempfile::tempdir_in(case.database.parent().unwrap()).unwrap();
    let marker = work.path().join("ready.json");
    let log = work.path().join("child.log");
    let output = File::create(&log).unwrap();
    let mut child = ChildGuard(
        Command::new(env!("CARGO_BIN_EXE_dedupit"))
            .arg("--database")
            .arg(case.database)
            .args(["--json", "--chunk-size", "4096", "--scanner"])
            .arg(if case.portable { "portable" } else { "btrfs" })
            .arg("--commit-batch-size")
            .arg(case.batch_size.to_string())
            .args(if case.apply { vec!["--apply"] } else { vec![] })
            .arg(case.root)
            .env("DEDUPIT_TEST_FAILPOINT", case.event)
            .env("DEDUPIT_TEST_HIT", case.hit.to_string())
            .env("DEDUPIT_TEST_READY", &marker)
            .env("DEDUPIT_TEST_SMALL_CACHE", "1")
            .stdout(Stdio::from(output.try_clone().unwrap()))
            .stderr(Stdio::from(output))
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(60);
    let staged = loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            panic!(
                "failpoint {}#{} not reached, status={status}: {}",
                case.event,
                case.hit,
                fs::read_to_string(&log).unwrap()
            );
        }
        let state =
            fs::read_to_string(format!("/proc/{}/status", child.0.id())).unwrap_or_default();
        if state.lines().any(|line| line.starts_with("State:\tT")) {
            break serde_json::from_slice::<Value>(&fs::read(&marker).unwrap()).unwrap();
        }
        assert!(
            Instant::now() < deadline,
            "failpoint {}#{} timed out: {}",
            case.event,
            case.hit,
            fs::read_to_string(&log).unwrap()
        );
        thread::sleep(Duration::from_millis(5));
    };
    assert_eq!(staged["event"], case.event);
    assert_eq!(staged["autocommit"], case.event.ends_with("after-commit"));
    let (durable, before) = durable_state(case.database);
    let uncommitted_frames = uncommitted_frames(case.database);
    child.0.kill().unwrap();
    assert_eq!(child.0.wait().unwrap().signal(), Some(libc::SIGKILL));
    let (recovered, after) = durable_state(case.database);
    assert_eq!(
        recovered, durable,
        "partial batch became visible after SIGKILL"
    );
    assert_eq!(
        before, after,
        "durable hashes/queue/cursor/completions changed on recovery"
    );
    eprintln!("BATCH CRASH {}#{}: staged={staged}, durable={durable}, uncommitted WAL frames={uncommitted_frames}", case.event, case.hit);
    Observation {
        staged,
        durable,
        uncommitted_frames,
    }
}

type Contents = BTreeMap<PathBuf, Vec<u8>>;
fn fixture(portable: bool) -> Option<(tempfile::TempDir, Options, Contents)> {
    let (dir, mut options) = setup();
    if portable {
        options.scanner = Scanner::Portable;
    } else {
        first(&options)?;
    }
    let mut contents = BTreeMap::new();
    let block = data(4096);
    for i in 0..128 {
        contents.insert(options.roots[0].join(format!("file-{i:03}")), block.clone());
    }
    for (name, first, last) in [("partial-a", 41, 42), ("partial-b", 43, 44)] {
        let mut bytes = vec![first; 4096];
        bytes.extend_from_slice(&block);
        bytes.extend_from_slice(&vec![last; 4096]);
        contents.insert(options.roots[0].join(name), bytes);
    }
    for (path, bytes) in &contents {
        fs::write(path, bytes).unwrap();
    }
    Some((dir, options, contents))
}
fn run(options: &Options, apply: bool) -> Value {
    let output = Command::new(env!("CARGO_BIN_EXE_dedupit"))
        .arg("--database")
        .arg(&options.database)
        .args(["--json", "--chunk-size", "4096", "--scanner"])
        .arg(if options.scanner == Scanner::Portable {
            "portable"
        } else {
            "btrfs"
        })
        .arg("--commit-batch-size")
        .arg(options.commit_batch_size.to_string())
        .args(if apply { vec!["--apply"] } else { vec![] })
        .arg(&options.roots[0])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stats: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(stats["errors"], serde_json::json!([]));
    stats
}
fn verify(options: &Options, contents: &Contents) {
    for (path, bytes) in contents {
        assert_eq!(
            fs::read(path).unwrap(),
            *bytes,
            "contents changed: {path:?}"
        );
    }
    let fresh = options.database.with_file_name("oracle.sqlite");
    stress::run(&fresh, &options.roots[0], true, false);
    assert_eq!(
        stress::inventory(&options.database, &options.roots[0]),
        stress::inventory(&fresh, &options.roots[0])
    );
    let idle = run(options, true);
    assert_eq!(idle["bytes_hashed"], 0);
    assert_eq!(idle["jobs_attempted"], 0);
}

#[test]
fn portable_batches_recover_uncommitted_spilled_wal_and_keep_committed_files() {
    for (event, hit) in [
        ("portable:staged", 63),
        ("portable:before-commit", 1),
        ("portable:after-commit", 1),
    ] {
        let (_dir, options, contents) = fixture(true).unwrap();
        let observation = crash_at(&Crash {
            database: &options.database,
            root: &options.roots[0],
            portable: true,
            apply: false,
            event,
            hit,
            batch_size: 64,
        });
        if event == "portable:staged" {
            assert!(
                observation.uncommitted_frames > 0,
                "must exercise WAL spill, not just in-memory rollback"
            );
        }
        let durable = observation.durable["files"].as_u64().unwrap();
        if event.ends_with("after-commit") {
            assert!(durable > 0);
        } else {
            assert!(observation.staged["files"].as_u64().unwrap() > durable);
        }
        let resumed = run(&options, true);
        assert_eq!(
            resumed["files_hashed"].as_u64().unwrap(),
            contents.len() as u64 - durable
        );
        verify(&options, &contents);
    }
}

#[test]
fn native_batches_keep_discovery_cursors_and_hash_acknowledgements_atomic() {
    for event in [
        "discovery:before-commit",
        "discovery:after-commit",
        "native-file:staged",
        "native-file:before-commit",
        "native-file:after-commit",
    ] {
        let Some((_dir, options, mut contents)) = fixture(false) else {
            return;
        };
        let observation = crash_at(&Crash {
            database: &options.database,
            root: &options.roots[0],
            portable: false,
            apply: false,
            event,
            hit: 1,
            batch_size: 64,
        });
        if event == "native-file:before-commit" {
            assert!(
                observation.staged["files"].as_u64().unwrap()
                    > observation.durable["files"].as_u64().unwrap()
            );
            assert!(
                observation.staged["discovery"].as_u64().unwrap()
                    < observation.durable["discovery"].as_u64().unwrap()
            );
        }
        // Writes/deletions during downtime must be reconciled even if their
        // original generation was in the unfinished discovery window.
        let path = options.roots[0].join("file-000");
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .write_all_at(&[99; 32], 0)
            .unwrap();
        contents.get_mut(&path).unwrap()[..32].fill(99);
        let deleted = options.roots[0].join("file-001");
        fs::remove_file(&deleted).unwrap();
        contents.remove(&deleted);
        run(&options, true);
        verify(&options, &contents);
        assert_eq!(
            reader(&options.database)
                .query_row("SELECT COUNT(*) FROM discovery", [], |r| r.get::<_, u64>(0))
                .unwrap(),
            0
        );
    }
}

#[test]
fn apply_batches_replay_only_uncommitted_completions_and_preserve_cow_contents() {
    for (event, hit) in [
        ("apply:shared", 3),
        ("apply:staged", 3),
        ("apply:staged", 70),
        ("apply:before-commit", 1),
        ("apply:after-commit", 1),
    ] {
        let Some((_dir, options, mut contents)) = fixture(false) else {
            return;
        };
        run(&options, false);
        let observation = crash_at(&Crash {
            database: &options.database,
            root: &options.roots[0],
            portable: false,
            apply: true,
            event,
            hit,
            batch_size: 64,
        });
        let durable = observation.durable["completed"].as_u64().unwrap();
        if event.ends_with("after-commit") {
            assert!(durable > 0);
        } else {
            assert!(observation.staged["completed"].as_u64().unwrap() > durable);
        }
        let mutate = hit == 70;
        if mutate {
            assert!(
                durable > 0,
                "must crash in the second batch after a durable prefix"
            );
            let path = options.roots[0].join("file-000");
            OpenOptions::new()
                .write(true)
                .open(&path)
                .unwrap()
                .write_all_at(&[99; 32], 0)
                .unwrap();
            contents.get_mut(&path).unwrap()[..32].fill(99);
        }
        let resumed = run(&options, true);
        if !mutate {
            // 127 whole-file targets + the two matching partial-file chunks.
            assert_eq!(resumed["jobs_completed"].as_u64().unwrap(), 129 - durable);
        }
        verify(&options, &contents);
    }
}
