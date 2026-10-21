//! Disposable native-discovery benchmark driver; each measurement restarts the CLI.
use anyhow::{bail, ensure, Context, Result};
use clap::Parser;
use rusqlite::{Connection, OpenFlags};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::unix::fs::{FileExt, MetadataExt},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::Instant,
};

const BLOCK: usize = 4096;
const CHUNK: usize = 128 * 1024;
const LARGE: usize = 8 * 1024 * 1024;
const GROUPS: usize = 128;

#[derive(Parser)]
struct Args {
    #[arg(long)]
    cli: PathBuf,
    #[arg(long)]
    time: PathBuf,
    #[arg(long)]
    root: PathBuf,
    #[arg(long)]
    state: PathBuf,
    #[arg(long, default_value_t = 1_000_000, value_parser = clap::value_parser!(u32).range(260..=5_000_000))]
    files: u32,
    #[arg(long, default_value_t = 3, value_parser = clap::value_parser!(u16).range(1..=20))]
    repetitions: u16,
}

fn emit(kind: &str, value: Value) {
    println!("DEDUPIT_BENCH_{kind} {value}");
}
fn number(stats: &Value, key: &str) -> Result<u64> {
    stats[key]
        .as_u64()
        .with_context(|| format!("missing statistic {key}"))
}
fn noise(seed: u64, length: usize) -> Vec<u8> {
    let mut state = seed;
    (0..length / 8)
        .flat_map(|_| {
            state = state.wrapping_add(0x9e3779b97f4a7c15);
            let mut z = state;
            z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
            z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
            (z ^ (z >> 31)).to_le_bytes()
        })
        .collect()
}
fn original(id: usize, count: usize, groups: &[Vec<u8>]) -> std::borrow::Cow<'_, [u8]> {
    if id < count - 4 {
        return (&groups[id % GROUPS][..]).into();
    }
    let control = id - (count - 4);
    if control < 2 {
        return noise(0xbeef, LARGE).into();
    }
    let mut data = noise(100 + control as u64, CHUNK);
    data.extend(noise(200, CHUNK));
    data.extend(noise(300 + control as u64, CHUNK));
    data.into()
}
fn path(root: &Path, id: usize) -> PathBuf {
    root.join((id / 1000).to_string()).join(id.to_string())
}
fn identity(hash: &mut blake3::Hasher, id: usize, meta: &fs::Metadata) {
    for value in [id as u64, meta.dev(), meta.ino()] {
        hash.update(&value.to_le_bytes());
    }
}
struct Bench<'a> {
    args: &'a Args,
    sequence: usize,
}
impl Bench<'_> {
    fn run(&mut self, case: &str, database: &Path, apply: bool, portable: bool) -> Result<Value> {
        self.sequence += 1;
        emit(
            "PROGRESS",
            json!({"phase":case,"event":"start","sequence":self.sequence}),
        );
        let stdout = self.args.state.join("stdout.json");
        let stderr = self.args.state.join("stderr.txt");
        let memory = self.args.state.join("memory.txt");
        let mut command = Command::new(&self.args.time);
        command
            .args(["-f", "%M", "-o"])
            .arg(&memory)
            .arg("--")
            .arg(&self.args.cli)
            .arg("--database")
            .arg(database)
            .args(["--json", "--chunk-size", &CHUNK.to_string(), "--scanner"])
            .arg(if portable { "portable" } else { "btrfs" });
        if apply {
            command.arg("--apply");
        }
        if portable {
            command.arg("--verify");
        }
        command
            .arg(&self.args.root)
            .stdin(Stdio::null())
            .stdout(File::create(&stdout)?)
            .stderr(File::create(&stderr)?);
        let start = Instant::now();
        let mut child = command.spawn()?;
        let mut status = 0;
        // GNU time forks from a small, freshly exec'd process, avoiding inherited
        // driver RSS in the CLI's peak. wait4 retains precise aggregate CPU time.
        let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
        loop {
            let waited = unsafe { libc::wait4(child.id() as i32, &mut status, 0, &mut usage) };
            if waited >= 0 {
                break;
            }
            let error = std::io::Error::last_os_error();
            if error.kind() != std::io::ErrorKind::Interrupted {
                let _ = child.kill();
                let _ = child.wait();
                return Err(error.into());
            }
        }
        let seconds = start.elapsed().as_secs_f64();
        ensure!(
            libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0,
            "{case} failed: {}",
            fs::read_to_string(&stderr)?
        );
        let stats: Value = serde_json::from_reader(File::open(&stdout)?)?;
        ensure!(
            stats["errors"].as_array().is_some_and(Vec::is_empty),
            "{case}: {stats}"
        );
        ensure!(
            number(&stats, "stale")? == 0 && number(&stats, "unsupported")? == 0,
            "{case}: {stats}"
        );
        if !portable {
            ensure!(
                number(&stats, "btrfs_subvolumes")? > 0
                    && stats["scanner_fallbacks"]
                        .as_array()
                        .is_some_and(Vec::is_empty),
                "native discovery required: {stats}"
            );
        }
        let cpu = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
        let sample = json!({"case":case,"sequence":self.sequence,"seconds":seconds,
            "cpu_seconds":cpu(usage.ru_utime)+cpu(usage.ru_stime),
            "peak_rss_kib":fs::read_to_string(memory)?.trim().parse::<u64>()?,
            "database_bytes":database.metadata()?.len(),"stats":stats});
        emit("SAMPLE", sample);
        Ok(stats)
    }
    fn native(&mut self, case: &str, apply: bool) -> Result<Value> {
        self.run(case, &self.args.state.join("native.sqlite"), apply, false)
    }
    fn idle(&mut self, case: &str, apply: bool) -> Result<()> {
        let stats = self.native(case, apply)?;
        for key in [
            "files_seen",
            "files_hashed",
            "bytes_hashed",
            "jobs_attempted",
            "discovery_jobs",
        ] {
            ensure!(
                number(&stats, key)? == 0,
                "idle {case} performed {key}: {stats}"
            );
        }
        Ok(())
    }
    fn settle(&mut self, case: &str) -> Result<()> {
        // Mapping capture in apply must make the FIRST following invocation
        // idle. Extra settling passes would hide a performance regression.
        self.idle(case, true)
    }
}
fn digest(database: &Path) -> Result<String> {
    let conn = Connection::open_with_flags(database, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let integrity: String = conn.query_row("PRAGMA integrity_check", [], |r| r.get(0))?;
    ensure!(integrity == "ok", "integrity: {integrity}");
    ensure!(
        !conn.prepare("PRAGMA foreign_key_check")?.exists([])?,
        "foreign key violation"
    );
    let mut statement = conn.prepare("SELECT f.path,f.dev,f.ino,f.fingerprint,r.kind,r.offset,r.length,r.hash FROM files f JOIN ranges r ON r.file_id=f.id ORDER BY f.path,r.kind,r.offset")?;
    let mut rows = statement.query([])?;
    let mut hash = blake3::Hasher::new();
    while let Some(row) = rows.next()? {
        for column in 0..8 {
            match row.get_ref(column)? {
                rusqlite::types::ValueRef::Blob(bytes) => {
                    hash.update(&[0]);
                    hash.update(&(bytes.len() as u64).to_le_bytes());
                    hash.update(bytes);
                }
                rusqlite::types::ValueRef::Integer(value) => {
                    hash.update(&[1]);
                    hash.update(&value.to_le_bytes());
                }
                _ => bail!("unexpected inventory value"),
            }
        }
    }
    Ok(hash.finalize().to_hex().to_string())
}
fn main() -> Result<()> {
    let args = Args::parse();
    let count = args.files as usize;
    ensure!(!args.root.exists(), "fixture root must be new");
    ensure!(
        !args.state.join("native.sqlite").exists(),
        "index must be new"
    );
    fs::create_dir(&args.root)?;
    let groups: Vec<_> = (0..GROUPS)
        .map(|i| noise(i as u64 + 20261020, BLOCK))
        .collect();
    let mut identities = blake3::Hasher::new();
    let started = Instant::now();
    for id in 0..count {
        let file_path = path(&args.root, id);
        if id % 1000 == 0 {
            fs::create_dir(file_path.parent().unwrap())?;
        }
        let mut file = File::create(file_path)?;
        file.write_all(&original(id, count, &groups))?;
        identity(&mut identities, id, &file.metadata()?);
        if (id + 1) % 10000 == 0 || id + 1 == count {
            emit(
                "PROGRESS",
                json!({"phase":"fixture","files":id+1,"seconds":started.elapsed().as_secs_f64()}),
            );
        }
    }
    emit(
        "META",
        json!({"files":count,"regular_files":count-4,"file_bytes":BLOCK,"groups":GROUPS,
        "kernel_release":fs::read_to_string("/proc/sys/kernel/osrelease")?.trim(),"sqlite_version":rusqlite::version(),
        "large_controls":2,"large_bytes":LARGE,"partial_controls":2,"partial_bytes":CHUNK*3,
        "chunk_bytes":CHUNK,"repetitions":args.repetitions,"fixture_seconds":started.elapsed().as_secs_f64()}),
    );
    let mut bench = Bench {
        args: &args,
        sequence: 0,
    };
    let cold = bench.native("initial-native-index", false)?;
    ensure!(
        number(&cold, "files_hashed")? == count as u64,
        "initial file count: {cold}"
    );
    for _ in 0..args.repetitions {
        bench.idle("idle-before-apply", false)?;
    }
    let apply = bench.native("initial-apply", true)?;
    let expected_jobs = count - 4 - GROUPS + 2;
    ensure!(
        number(&apply, "jobs_completed")? == expected_jobs as u64,
        "sharing jobs: expected {expected_jobs}: {apply}"
    );
    ensure!(
        number(&apply, "bytes_deduped")? > 0,
        "must actually apply native dedupe"
    );
    let conn = Connection::open_with_flags(
        args.state.join("native.sqlite"),
        OpenFlags::SQLITE_OPEN_READ_ONLY,
    )?;
    let partial_jobs: u64 = conn.query_row(
        "SELECT COUNT(*) FROM completed c JOIN ranges r ON r.id=c.target WHERE r.kind=0",
        [],
        |r| r.get(0),
    )?;
    ensure!(
        partial_jobs == 1,
        "must complete the controlled partial-range job"
    );
    drop(conn);
    bench.settle("post-apply-reconcile")?;
    for _ in 0..args.repetitions {
        bench.idle("idle-after-apply", true)?;
    }
    let mut patches = BTreeMap::<usize, Vec<(u64, Vec<u8>)>>::new();
    for repetition in 0..args.repetitions as usize {
        let id = count - 5; // Non-representative small file, already deduplicated.
        let patch = vec![repetition as u8 ^ 0xa5; 8];
        OpenOptions::new()
            .write(true)
            .open(path(&args.root, id))?
            .write_all_at(&patch, 32)?;
        patches.entry(id).or_default().push((32, patch));
        let stats = bench.native("small-file-write-apply", true)?;
        ensure!(
            number(&stats, "files_hashed")? == 1 && number(&stats, "bytes_hashed")? == BLOCK as u64,
            "small write: {stats}"
        );
        let id = count - 3; // Previously shared destination of the 8 MiB pair.
        let patch = noise(900 + repetition as u64, BLOCK);
        OpenOptions::new()
            .write(true)
            .open(path(&args.root, id))?
            .write_all_at(&patch, (LARGE / 2) as u64)?;
        patches
            .entry(id)
            .or_default()
            .push(((LARGE / 2) as u64, patch));
        let stats = bench.native("large-file-4k-write-apply", true)?;
        ensure!(
            number(&stats, "files_hashed")? == 1 && number(&stats, "bytes_hashed")? <= LARGE as u64,
            "large write: {stats}"
        );
        // Report first-write baseline refresh costs; later writes must reuse
        // unaffected chunks, rather than quietly falling back to full rehashes.
        if repetition > 0 {
            ensure!(
                number(&stats, "bytes_hashed")? <= (CHUNK * 2) as u64
                    && number(&stats, "chunks_reused")? > 0,
                "incremental extent reuse: {stats}"
            );
        }
        let new_path = args.root.join("0").join(format!("new-{repetition}"));
        fs::write(new_path, &groups[repetition + 1])?;
        let stats = bench.native("new-duplicate-apply", true)?;
        ensure!(
            number(&stats, "files_hashed")? == 1 && number(&stats, "jobs_completed")? == 1,
            "new duplicate: {stats}"
        );
        bench.settle("post-new-duplicate-reconcile")?;
    }
    let database = args.state.join("native.sqlite");
    let conn = Connection::open_with_flags(&database, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
    let dependents: u64 = conn.query_row("SELECT COUNT(*) FROM completed c JOIN ranges r ON r.id=c.source JOIN files f ON f.id=r.file_id WHERE f.path=?1 AND r.kind=1", [path(&args.root,0).as_os_str().as_encoded_bytes()], |r|r.get(0))?;
    ensure!(
        dependents > 0,
        "source control must have dependent completion records"
    );
    drop(conn);
    let patch = vec![0x5a; 8];
    OpenOptions::new()
        .write(true)
        .open(path(&args.root, 0))?
        .write_all_at(&patch, 32)?;
    patches.entry(0).or_default().push((32, patch));
    let stats = bench.native("representative-write-apply", true)?;
    ensure!(
        number(&stats, "files_hashed")? == 1 && number(&stats, "bytes_hashed")? == BLOCK as u64,
        "representative: {stats}"
    );
    ensure!(
        number(&stats, "jobs_completed")? == dependents - 1,
        "source fanout replay: {stats}"
    );
    emit(
        "PROGRESS",
        json!({"phase":"source-fanout","previous_dependents":dependents,"replayed_jobs":number(&stats,"jobs_completed")?}),
    );
    bench.settle("post-change-reconcile")?;
    for _ in 0..args.repetitions {
        bench.idle("idle-after-changes", true)?;
    }
    emit(
        "PROGRESS",
        json!({"phase":"verify-contents","event":"start"}),
    );
    let mut observed = blake3::Hasher::new();
    let mut buffer = Vec::new();
    for id in 0..count {
        let mut file = File::open(path(&args.root, id))?;
        let meta = file.metadata()?;
        ensure!(
            meta.nlink() == 1 && meta.mode() & 0o777 == 0o644,
            "file identity/mode changed: {id}"
        );
        identity(&mut observed, id, &meta);
        let mut expected = original(id, count, &groups);
        if let Some(changes) = patches.get(&id) {
            for (offset, bytes) in changes {
                expected.to_mut()[*offset as usize..*offset as usize + bytes.len()]
                    .copy_from_slice(bytes);
            }
        }
        buffer.clear();
        file.read_to_end(&mut buffer)?;
        ensure!(buffer == expected.as_ref(), "file contents changed: {id}");
        if (id + 1) % 10000 == 0 {
            emit("PROGRESS", json!({"phase":"verify-contents","files":id+1}));
        }
    }
    ensure!(
        observed.finalize() == identities.finalize(),
        "original inodes changed"
    );
    for repetition in 0..args.repetitions as usize {
        ensure!(
            fs::read(args.root.join("0").join(format!("new-{repetition}")))?
                == groups[repetition + 1],
            "new file changed"
        );
    }
    let oracle = args.state.join("oracle.sqlite");
    let stats = bench.run("oracle-full-scan", &oracle, false, true)?;
    let total = count as u64 + args.repetitions as u64;
    ensure!(
        number(&stats, "files_hashed")? == total,
        "oracle file count: {stats}"
    );
    emit(
        "PROGRESS",
        json!({"phase":"compare-inventories","event":"start"}),
    );
    let native = digest(&database)?;
    ensure!(
        native == digest(&oracle)?,
        "native inventory differs from fresh full scan"
    );
    emit(
        "RESULT",
        json!({"files":count,"verified_files":total,"contents_verified":true,
        "identities_preserved":true,"native_matches_oracle":true,"inventory_blake3":native,
        "initial_apply_jobs":number(&apply,"jobs_completed")?,"initial_deduped_bytes":number(&apply,"bytes_deduped")?,
        "initial_partial_range_jobs":partial_jobs,"initial_whole_file_jobs":number(&apply,"jobs_completed")?-partial_jobs}),
    );
    Ok(())
}
