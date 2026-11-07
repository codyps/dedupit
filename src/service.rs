//! Native foreground service: one scanning process, durable reports, no helpers.
use anyhow::{bail, ensure, Context, Result};
use dedupit::{Granularity, Options, RunObserver, Scanner, Stats};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::{
    cell::{Cell, RefCell},
    ffi::CString,
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, BufWriter, Write},
    os::unix::{
        ffi::OsStrExt,
        fs::{DirBuilderExt, OpenOptionsExt},
    },
    path::{Path, PathBuf},
    rc::Rc,
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

static STOP: AtomicBool = AtomicBool::new(false);
extern "C" fn stop(_: libc::c_int) {
    STOP.store(true, Ordering::Relaxed);
}

#[derive(clap::Args)]
pub struct ServiceArgs {
    #[arg(long, value_enum, default_value = "cow")]
    pub database_storage: dedupit::storage::Storage,
    /// Private persistent directory for the index, status and cumulative totals.
    #[arg(long, default_value = "/var/lib/dedupit")]
    state_directory: PathBuf,
    /// Btrfs block device. Privately mounts subvolid=5 to cover every subvolume.
    #[arg(long, conflicts_with = "roots")]
    device: Option<PathBuf>,
    /// Stable private mount location; defaults to STATE_DIRECTORY/filesystem.
    #[arg(long, requires = "device")]
    mount_directory: Option<PathBuf>,
    /// Seconds to wait after a run (or a space pause) before trying again.
    #[arg(long, default_value_t = 3600, value_parser = clap::value_parser!(u64).range(1..))]
    interval: u64,
    /// Stop the current run if available space on the index filesystem drops below this.
    #[arg(long, default_value_t = 5368709120)]
    minimum_free_bytes: u64,
    /// Share matching data. Omit for a report-only service.
    #[arg(long)]
    apply: bool,
    #[arg(long, default_value_t = 1048576)]
    chunk_size: usize,
    #[arg(long, value_enum, default_value = "both")]
    granularity: Granularity,
    #[arg(long, value_enum, default_value = "auto")]
    scanner: Scanner,
    #[arg(long, default_value_t = 1024, value_parser = clap::value_parser!(u16).range(1..=4096))]
    commit_batch_size: u16,
    #[arg(required_unless_present = "device")]
    roots: Vec<PathBuf>,
}

#[derive(clap::Args)]
pub struct StatsArgs {
    #[arg(long, default_value = "/var/lib/dedupit")]
    state_directory: PathBuf,
    #[arg(long)]
    json: bool,
    /// Stream full file-error details instead of the bounded preview.
    #[arg(long, conflicts_with = "json")]
    errors: bool,
}

#[derive(Debug)]
enum Paused {
    Signal,
    Space,
}
impl std::fmt::Display for Paused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Signal => "stop requested",
            Self::Space => "minimum free space reached",
        })
    }
}
impl std::error::Error for Paused {}

#[derive(Default, Serialize, Deserialize)]
#[serde(default)]
struct Totals {
    completed_runs: u64,
    partial_runs: u64,
    failed_runs: u64,
    paused_runs: u64,
    interrupted_runs: u64,
    files_hashed: u64,
    bytes_hashed: u64,
    jobs_completed: u64,
    bytes_deduped: u64,
    bytes_already_shared: u64,
    estimated_bytes_saved: u64,
    savings_measured_bytes: u64,
}

// Counters are updated by snapshot deltas, not whole-run additions. Persisting
// them alongside the snapshot makes restart recovery idempotent.
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(default)]
struct Activity {
    scans_started: u64,
    files_seen: u64,
    files_hashed: u64,
    bytes_hashed: u64,
    jobs_completed: u64,
    bytes_deduped: u64,
    bytes_already_shared: u64,
    estimated_bytes_saved: u64,
    savings_measured_bytes: u64,
}
impl Activity {
    fn observe(&mut self, before: &Stats, after: &Stats) {
        self.estimated_bytes_saved += after
            .estimated_bytes_saved
            .saturating_sub(before.estimated_bytes_saved);
        self.savings_measured_bytes += after
            .savings_measured_bytes
            .saturating_sub(before.savings_measured_bytes);
        self.files_seen += after.files_seen.saturating_sub(before.files_seen);
        self.files_hashed += after.files_hashed.saturating_sub(before.files_hashed);
        self.bytes_hashed += after.bytes_hashed.saturating_sub(before.bytes_hashed);
        self.jobs_completed += after.jobs_completed.saturating_sub(before.jobs_completed);
        self.bytes_deduped += after.bytes_deduped.saturating_sub(before.bytes_deduped);
        self.bytes_already_shared += after
            .bytes_already_shared
            .saturating_sub(before.bytes_already_shared);
    }
}

fn recover_activity(previous: Option<&serde_json::Value>) -> Result<Activity> {
    let Some(previous) = previous else {
        return Ok(Activity::default());
    };
    if let Some(activity) = previous.get("activity") {
        return Ok(serde_json::from_value(activity.clone())?);
    }
    // Migrate the old completed-only totals and the last saved unfinished run.
    let totals: Totals = previous
        .get("totals")
        .map(Totals::deserialize)
        .transpose()?
        .unwrap_or_default();
    let mut activity = Activity {
        scans_started: totals.completed_runs
            + totals.failed_runs
            + totals.interrupted_runs
            + totals.paused_runs
            + u64::from(previous["state"] == "running"),
        files_hashed: totals.files_hashed,
        bytes_hashed: totals.bytes_hashed,
        jobs_completed: totals.jobs_completed,
        bytes_deduped: totals.bytes_deduped,
        bytes_already_shared: totals.bytes_already_shared,
        estimated_bytes_saved: totals.estimated_bytes_saved,
        savings_measured_bytes: totals.savings_measured_bytes,
        ..Default::default()
    };
    let finished_successfully = matches!(
        previous["state"].as_str(),
        Some("completed" | "finished_with_errors")
    ) || (previous["state"] == "stopped"
        && previous["finished_at"].as_u64().is_some()
        && previous["error"].is_null());
    if !finished_successfully {
        if let Some(progress) = previous.get("progress") {
            activity.observe(&Stats::default(), &Stats::deserialize(progress)?);
        }
    }
    Ok(activity)
}

#[derive(Serialize)]
struct Status {
    state: &'static str,
    pid: u32,
    started_at: u64,
    updated_at: u64,
    finished_at: Option<u64>,
    free_bytes: u64,
    error: Option<String>,
    progress: Stats,
    totals: Totals,
    activity: Activity,
    phase: &'static str,
}

pub(super) fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub(super) fn save(path: &Path, value: &impl Serialize) -> Result<()> {
    // Same-directory rename gives readers a complete old or new report. A
    // unique, exclusively created temporary file also survives stale crash debris.
    let stamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let temporary = path.with_extension(format!("{}.{}.tmp", std::process::id(), stamp));
    let result = (|| -> Result<()> {
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary)?;
        let mut output = BufWriter::with_capacity(64 * 1024, file);
        serde_json::to_writer_pretty(&mut output, value)?;
        writeln!(output)?;
        output.flush()?;
        output.get_ref().sync_all()?;
        fs::rename(&temporary, path)?;
        File::open(path.parent().context("report has no parent")?)?.sync_all()?;
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result.with_context(|| format!("save {}", path.display()))
}

fn read_optional<T: serde::de::DeserializeOwned>(path: &Path) -> Result<Option<T>> {
    match File::open(path) {
        Ok(file) => Ok(Some(
            serde_json::from_reader(BufReader::with_capacity(64 * 1024, file))
                .with_context(|| format!("read {}", path.display()))?,
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).with_context(|| format!("open {}", path.display())),
    }
}

pub(super) fn free_bytes(path: &Path) -> Result<u64> {
    let path = CString::new(path.as_os_str().as_bytes())?;
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    // SAFETY: valid NUL-terminated path and writable statvfs output.
    if unsafe { libc::statvfs(path.as_ptr(), stat.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error().into());
    }
    // SAFETY: successful statvfs initialized the entire structure.
    let stat = unsafe { stat.assume_init() };
    let available = u128::from(stat.f_bavail) * u128::from(stat.f_frsize);
    Ok(available.min(u128::from(u64::MAX)) as u64)
}

struct Monitor {
    directory: PathBuf,
    minimum: u64,
    status: RefCell<Status>,
    checked: RefCell<Instant>,
    reported: RefCell<Instant>,
    inventory: RefCell<Option<crate::inventory::Worker>>,
    inventory_hardlinks: bool,
    logged_errors: Cell<usize>,
}
impl Monitor {
    fn write(&self) -> Result<()> {
        save(
            &self.directory.join("filesystem-status.json"),
            &*self.status.borrow(),
        )
    }
    fn space(&self) -> Result<()> {
        let free = free_bytes(&self.directory)?;
        self.status.borrow_mut().free_bytes = free;
        *self.checked.borrow_mut() = Instant::now();
        if free < self.minimum {
            return Err(Paused::Space.into());
        }
        Ok(())
    }
}
impl Monitor {
    fn record(&self, stats: &Stats) -> Result<()> {
        let logged = self.logged_errors.get().min(stats.errors.len());
        if logged < stats.errors.len() {
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .mode(0o600)
                .open(self.directory.join("filesystem-errors.jsonl"))?;
            let mut output = BufWriter::with_capacity(64 * 1024, file);
            for error in &stats.errors[logged..] {
                serde_json::to_writer(&mut output, error)?;
                writeln!(output)?;
            }
            output.flush()?;
            output.get_ref().sync_all()?;
            self.logged_errors.set(stats.errors.len());
        }
        {
            let mut status = self.status.borrow_mut();
            let previous = std::mem::replace(&mut status.progress, stats.summary(10));
            status.activity.observe(&previous, stats);
            status.updated_at = now();
        }
        self.write()?;
        *self.reported.borrow_mut() = Instant::now();
        Ok(())
    }
}
impl Monitor {
    fn stop_inventory(&self) {
        let worker = self.inventory.borrow_mut().take();
        drop(worker);
    }
    fn start_inventory(&self) {
        if self.inventory.borrow().is_none() {
            *self.inventory.borrow_mut() = Some(crate::inventory::Worker::start(
                self.directory.clone(),
                self.inventory_hardlinks,
                self.minimum,
            ));
        }
    }
}
impl RunObserver for Monitor {
    fn after_commit(&self) {
        if let Some(worker) = self.inventory.borrow().as_ref() {
            worker.yield_to_writer();
        }
    }
    fn before_index_close(&self) {
        self.stop_inventory();
    }
    fn phase(&self, phase: &'static str) -> Result<()> {
        self.checkpoint()?;
        if phase == "Opening index" {
            self.stop_inventory();
        }
        // Start only after Index::open has queued any unconfirmed mappings
        // from a previous process. During this live process, newly captured
        // provisional maps reflect successful sharing and belong in the estimate.
        if phase == "Scanning files" {
            self.start_inventory();
        }
        {
            let mut status = self.status.borrow_mut();
            status.phase = phase;
            status.updated_at = now();
        }
        self.write()
    }
    fn finished(&self, stats: &Stats) -> Result<()> {
        self.record(stats)
    }
    fn checkpoint(&self) -> Result<()> {
        if STOP.load(Ordering::Relaxed) {
            return Err(Paused::Signal.into());
        }
        if self.checked.borrow().elapsed() >= Duration::from_secs(30) {
            self.space()?;
        }
        Ok(())
    }
    fn progress(&self, stats: &Stats) -> Result<()> {
        self.checkpoint()?;
        if self.reported.borrow().elapsed() >= Duration::from_secs(5) {
            self.record(stats)?;
        }
        Ok(())
    }
}

// The service owns a mount namespace; no helper executables or global mounts.
struct Volume {
    point: PathBuf,
}
#[cfg(target_os = "linux")]
impl Volume {
    fn mount(device: &Path, point: &Path) -> Result<Self> {
        use std::os::unix::fs::FileTypeExt;
        ensure!(
            fs::metadata(device)?.file_type().is_block_device(),
            "device must be a block device"
        );
        fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(point)?;
        ensure!(
            fs::read_dir(point)?.next().is_none(),
            "mount directory must be empty"
        );
        let source = CString::new(device.as_os_str().as_bytes())?;
        let target = CString::new(point.as_os_str().as_bytes())?;
        // SAFETY: creates a mount namespace for this single-threaded process;
        // all mount arguments are valid C strings. Make propagation private
        // before attaching anything so no mount can appear in the host namespace.
        unsafe {
            if libc::unshare(libc::CLONE_NEWNS) != 0 {
                return Err(io::Error::last_os_error().into());
            }
            if libc::mount(
                std::ptr::null(),
                c"/".as_ptr(),
                std::ptr::null(),
                libc::MS_REC | libc::MS_PRIVATE,
                std::ptr::null(),
            ) != 0
            {
                return Err(io::Error::last_os_error().into());
            }
            if libc::mount(
                source.as_ptr(),
                target.as_ptr(),
                c"btrfs".as_ptr(),
                libc::MS_NODEV | libc::MS_NOSUID | libc::MS_NOEXEC,
                c"subvolid=5".as_ptr().cast(),
            ) != 0
            {
                return Err(io::Error::last_os_error().into());
            }
        }
        Ok(Self {
            point: point.to_owned(),
        })
    }
}
#[cfg(not(target_os = "linux"))]
impl Volume {
    fn mount(_: &Path, _: &Path) -> Result<Self> {
        bail!("whole-volume service mode requires Linux");
    }
}
impl Drop for Volume {
    fn drop(&mut self) {
        #[cfg(target_os = "linux")]
        if let Ok(point) = CString::new(self.point.as_os_str().as_bytes()) {
            // SAFETY: valid path owned by this private namespace. Namespace
            // teardown releases it even if detaching here fails.
            unsafe {
                libc::umount2(point.as_ptr(), libc::MNT_DETACH);
            }
        }
    }
}

fn signals() -> Result<()> {
    // SAFETY: sigaction is initialized, the handler only stores an atomic flag,
    // and no Rust references or non-signal-safe functions enter the handler.
    unsafe {
        let mut action: libc::sigaction = std::mem::zeroed();
        action.sa_sigaction = stop as *const () as usize;
        libc::sigemptyset(&mut action.sa_mask);
        for signal in [libc::SIGTERM, libc::SIGINT] {
            if libc::sigaction(signal, &action, std::ptr::null_mut()) != 0 {
                return Err(io::Error::last_os_error().into());
            }
        }
    }
    Ok(())
}

pub fn run(args: ServiceArgs) -> Result<()> {
    ensure!(
        (4096..=16777216).contains(&args.chunk_size) && args.chunk_size.is_power_of_two(),
        "chunk size must be a power of two between 4096 and 16777216"
    );
    // SAFETY: service startup is single threaded; indexes and reports are private.
    unsafe {
        libc::umask(0o077);
    }
    fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(&args.state_directory)?;
    let directory = fs::canonicalize(&args.state_directory)?;
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(directory.join("service.lock"))?;
    lock.try_lock_exclusive()
        .context("another dedupit service owns this state directory")?;
    signals()?;
    let mount_point = args
        .mount_directory
        .unwrap_or_else(|| directory.join("filesystem"));
    let volume = args
        .device
        .as_ref()
        .map(|device| Volume::mount(device, &mount_point))
        .transpose()?;
    let options = Options {
        database: directory.join("filesystem.sqlite"),
        storage: args.database_storage,
        roots: if volume.is_some() {
            vec![mount_point]
        } else {
            args.roots
        },
        chunk_size: args.chunk_size,
        granularity: args.granularity,
        verify: false,
        scanner: args.scanner,
        commit_batch_size: args.commit_batch_size as usize,
    };
    // Status and totals are one atomic document, so a crash cannot apply the
    // same completed run twice to cumulative totals. Legacy reports remain usable.
    let previous = read_optional::<serde_json::Value>(&directory.join("filesystem-status.json"))?;
    let activity = recover_activity(previous.as_ref())?;
    let mut totals: Totals = previous
        .as_ref()
        .and_then(|v| v.get("totals").cloned())
        .map(serde_json::from_value)
        .transpose()?
        .unwrap_or_default();
    if previous
        .as_ref()
        .is_some_and(|s| s["state"] == "running" && s["finished_at"].is_null())
    {
        // The last daemon did not finish or stop gracefully. Its saved activity
        // is already included; count the abandoned scan without adding it again.
        totals.interrupted_runs += 1;
    }
    // Retain oversized legacy diagnostics without copying hundreds of MiB or
    // creating a gap in the live status file before its atomic replacement.
    if previous
        .as_ref()
        .and_then(|s| s["progress"]["errors"].as_array())
        .is_some_and(|e| e.len() > 10)
    {
        let archive = directory.join(format!("filesystem-status-legacy-{}.json", now()));
        if !archive.exists() {
            fs::hard_link(directory.join("filesystem-status.json"), archive)?;
        }
    }
    drop(previous);
    if !directory.join("filesystem-result-summary.json").exists() {
        if let Some(result) = read_optional::<Stats>(&directory.join("filesystem-result.json"))? {
            save(
                &directory.join("filesystem-result-summary.json"),
                &result.summary(10),
            )?;
        }
    }
    let monitor = Rc::new(Monitor {
        directory,
        minimum: args.minimum_free_bytes,
        status: RefCell::new(Status {
            state: "starting",
            pid: std::process::id(),
            started_at: now(),
            updated_at: now(),
            finished_at: None,
            free_bytes: 0,
            error: None,
            progress: Stats::default(),
            totals,
            activity,
            phase: "Starting",
        }),
        checked: RefCell::new(Instant::now()),
        reported: RefCell::new(Instant::now()),
        inventory: RefCell::new(None),
        inventory_hardlinks: args.device.is_some(),
        logged_errors: Cell::new(0),
    });
    while !STOP.load(Ordering::Relaxed) {
        let error_log = monitor.directory.join("filesystem-errors.jsonl");
        if error_log.exists() {
            fs::rename(
                &error_log,
                monitor.directory.join("filesystem-errors-previous.jsonl"),
            )?;
        }
        OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .mode(0o600)
            .open(&error_log)?;
        monitor.logged_errors.set(0);
        {
            let mut status = monitor.status.borrow_mut();
            status.state = "running";
            status.activity.scans_started += 1;
            status.phase = "Checking free space";
            status.started_at = now();
            status.updated_at = now();
            status.finished_at = None;
            status.progress = Stats::default();
            status.error = None;
        }
        monitor.write()?;
        let result = monitor
            .space()
            .and_then(|()| dedupit::run_observed(&options, args.apply, monitor.clone()));
        {
            let mut status = monitor.status.borrow_mut();
            status.updated_at = now();
            status.finished_at = Some(now());
            match result {
                Ok(stats) => {
                    status.state = if stats.errors.is_empty() {
                        "completed"
                    } else {
                        "finished_with_errors"
                    };
                    // Keep the last complete report if a future run is interrupted.
                    save(&monitor.directory.join("filesystem-result.json"), &stats)?;
                    save(
                        &monitor.directory.join("filesystem-result-summary.json"),
                        &stats.summary(10),
                    )?;
                    status.totals.completed_runs += 1;
                    status.totals.partial_runs += u64::from(!stats.errors.is_empty());
                    status.totals.files_hashed += stats.files_hashed;
                    status.totals.bytes_hashed += stats.bytes_hashed;
                    status.totals.jobs_completed += stats.jobs_completed;
                    status.totals.bytes_deduped += stats.bytes_deduped;
                    status.totals.bytes_already_shared += stats.bytes_already_shared;
                    status.totals.estimated_bytes_saved += stats.estimated_bytes_saved;
                    status.totals.savings_measured_bytes += stats.savings_measured_bytes;
                    status.progress = stats.summary(10);
                }
                Err(error) => {
                    status.state = match error.downcast_ref::<Paused>() {
                        Some(Paused::Signal) => {
                            status.totals.interrupted_runs += 1;
                            "interrupted"
                        }
                        Some(Paused::Space) => {
                            status.totals.paused_runs += 1;
                            "paused_for_space"
                        }
                        None => {
                            status.totals.failed_runs += 1;
                            "failed"
                        }
                    };
                    status.error = Some(format!("{error:#}"));
                }
            }
            eprintln!(
                "dedupit: {}: {} files hashed, {} logical bytes deduped{}",
                status.state,
                status.progress.files_hashed,
                status.progress.bytes_deduped,
                status
                    .error
                    .as_ref()
                    .map(|e| format!(" ({e})"))
                    .unwrap_or_default()
            );
        }
        monitor.write()?;
        // Idle inventories can finish even when the scan itself was brief.
        // Stop them before the next index opens raw SQLite/WAL guard handles.
        if !STOP.load(Ordering::Relaxed)
            && matches!(
                monitor.status.borrow().state,
                "completed" | "finished_with_errors"
            )
        {
            monitor.start_inventory();
        }
        let deadline = Instant::now() + Duration::from_secs(args.interval);
        while !STOP.load(Ordering::Relaxed) && Instant::now() < deadline {
            std::thread::sleep(
                Duration::from_millis(200).min(deadline.saturating_duration_since(Instant::now())),
            );
        }
    }
    // Keep the last run outcome and counters while recording that this daemon
    // has exited. A completed/paused run does not become an interrupted run.
    {
        let mut status = monitor.status.borrow_mut();
        status.state = "stopped";
        status.updated_at = now();
    }
    monitor.write()?;
    Ok(())
}

#[derive(Serialize)]
struct DatabaseSize {
    main_bytes: u64,
    wal_bytes: u64,
    shm_bytes: u64,
    total_bytes: u64,
}

fn database_size(directory: &Path) -> Result<Option<DatabaseSize>> {
    let length = |name: &str| -> Result<Option<u64>> {
        match fs::metadata(directory.join(name)) {
            Ok(metadata) => Ok(Some(metadata.len())),
            // SQLite can remove sidecars while the service is committing.
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error).with_context(|| format!("inspect {name}")),
        }
    };
    let main = length("filesystem.sqlite")?;
    let wal = length("filesystem.sqlite-wal")?;
    let shm = length("filesystem.sqlite-shm")?;
    if main.is_none() && wal.is_none() && shm.is_none() {
        return Ok(None);
    }
    let main_bytes = main.unwrap_or(0);
    let wal_bytes = wal.unwrap_or(0);
    let shm_bytes = shm.unwrap_or(0);
    Ok(Some(DatabaseSize {
        main_bytes,
        wal_bytes,
        shm_bytes,
        total_bytes: main_bytes
            .saturating_add(wal_bytes)
            .saturating_add(shm_bytes),
    }))
}

pub fn stats(args: StatsArgs) -> Result<()> {
    let status =
        read_optional::<serde_json::Value>(&args.state_directory.join("filesystem-status.json"))?;
    let result =
        match read_optional::<Stats>(&args.state_directory.join("filesystem-result-summary.json"))?
        {
            Some(summary) => Some(summary),
            None => read_optional::<Stats>(&args.state_directory.join("filesystem-result.json"))?,
        };
    if status.is_none() && result.is_none() {
        bail!(
            "no recorded statistics in {}",
            args.state_directory.display()
        );
    }
    let database = database_size(&args.state_directory)?;
    let inventory = crate::inventory::read(&args.state_directory)?;
    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(
                &serde_json::json!({"status": status, "last_result": result, "database": database, "sharing_inventory": inventory})
            )?
        );
        return Ok(());
    }
    let activity = recover_activity(status.as_ref())?;
    let progress = status
        .as_ref()
        .and_then(|s| s.get("progress"))
        .map(|v| serde_json::from_value::<Stats>(v.clone()))
        .transpose()?;
    let totals = status
        .as_ref()
        .and_then(|s| s.get("totals"))
        .map(|v| serde_json::from_value::<Totals>(v.clone()))
        .transpose()?;
    println!("Dedupit");
    if let Some(status) = &status {
        let state = match status["state"].as_str().unwrap_or("unknown") {
            "running" => "Running",
            "starting" => "Starting",
            "completed" => "Waiting (last run completed)",
            "finished_with_errors" => "Waiting (last run had file errors)",
            "paused_for_space" => "Paused (low disk space)",
            "interrupted" => "Interrupted",
            "failed" => "Waiting (last run failed)",
            "stopped" => "Stopped",
            _ => "Unknown",
        };
        println!("  Status       {state}");
        if status["state"] == "running" {
            if let Some(phase) = status["phase"].as_str() {
                println!("  Phase        {phase}");
            }
        }
        let current_time = now();
        if let Some(updated) = status["updated_at"].as_u64() {
            println!(
                "  Updated      {} ago",
                elapsed(current_time.saturating_sub(updated))
            );
        }
        if let Some(started) = status["started_at"].as_u64() {
            let end = status["finished_at"].as_u64().unwrap_or(current_time);
            println!("  Run duration {}", elapsed(end.saturating_sub(started)));
        }
        if let Some(free) = status["free_bytes"].as_u64() {
            println!("  Free space   {}", size(free));
        }
        if let Some(error) = status["error"].as_str() {
            println!("  Last error   {error}");
        }
    }
    if let Some(database) = &database {
        println!(
            "  Database     {} total ({} DB + {} WAL + {} SHM)",
            size(database.total_bytes),
            size(database.main_bytes),
            size(database.wal_bytes),
            size(database.shm_bytes)
        );
    } else {
        println!("  Database     Not created yet");
    }
    println!("\nCurrent sharing estimate (indexed files)");
    if let Some(inventory) = &inventory {
        let best = inventory
            .estimate
            .as_ref()
            .or_else(|| (inventory.refresh.mapped_data_bytes > 0).then_some(&inventory.refresh));
        if let Some(best) = best {
            println!(
                "  Saved (uncompressed)   {}",
                size(best.saved_uncompressed_bytes)
            );
            println!(
                "  Physical estimate      {}",
                if best.physical_covered_bytes > 0 {
                    size(best.physical_estimate_bytes)
                } else {
                    "-".into()
                }
            );
            let pct = if best.referenced_data_bytes == 0 {
                0.0
            } else {
                best.physical_covered_bytes as f64 / best.referenced_data_bytes as f64 * 100.0
            };
            println!(
                "    Coverage             {} / {} ({pct:.1}%)",
                size(best.physical_covered_bytes),
                size(best.referenced_data_bytes)
            );
            println!(
                "  Based on               {} / {} indexed files · {} checked · {} ago",
                count(best.verified_files),
                count(best.indexed_files),
                size(best.checked_file_bytes),
                elapsed(now().saturating_sub(best.as_of))
            );
            println!(
                "  Index reconciliation   {} queued jobs",
                count(best.pending_jobs)
            );
        }
        if inventory.refresh.incremental {
            println!(
                "  Inventory               {} · {} file updates · {} processed",
                if inventory.refresh.bootstrap_complete {
                    "incremental"
                } else {
                    "bootstrapping"
                },
                count(inventory.refresh.changed_files),
                size(inventory.refresh.changed_file_bytes)
            );
            println!(
                "    Pending              {} files · {} allocation groups",
                count(inventory.refresh.pending_files),
                count(inventory.refresh.pending_groups)
            );
        } else if inventory.state != "Complete" {
            println!(
                "  {}: {} / {} indexed files · {} checked · {} extent data examined",
                inventory.state,
                count(inventory.refresh.checked_files),
                count(inventory.refresh.indexed_files),
                size(inventory.refresh.checked_file_bytes),
                size(inventory.refresh.mapped_data_bytes)
            );
        }
        if let Some(error) = &inventory.error {
            println!("  Inventory error        {error}");
        }
    } else {
        println!("  Inventory starting");
    }
    println!("\nDedupit activity history");
    println!();
    println!(
        "  {:<22} {:>17} {:>17} {:>17}",
        "", "This scan", "Last complete", "Recorded work"
    );
    let row = |label: &str, a: Option<u64>, b: Option<u64>, c: Option<u64>, bytes: bool| {
        let value = |n: Option<u64>| {
            n.map(|n| if bytes { size(n) } else { count(n) })
                .unwrap_or_else(|| "-".into())
        };
        println!(
            "  {label:<22} {:>17} {:>17} {:>17}",
            value(a),
            value(b),
            value(c)
        );
    };
    row(
        "Files scanned",
        progress.as_ref().map(|s| s.files_seen),
        result.as_ref().map(|s| s.files_seen),
        Some(activity.files_seen),
        false,
    );
    row(
        "Files hashed",
        progress.as_ref().map(|s| s.files_hashed),
        result.as_ref().map(|s| s.files_hashed),
        Some(activity.files_hashed),
        false,
    );
    row(
        "Data read",
        progress.as_ref().map(|s| s.bytes_hashed),
        result.as_ref().map(|s| s.bytes_hashed),
        Some(activity.bytes_hashed),
        true,
    );
    row(
        "Dedupe accepted",
        progress.as_ref().map(|s| s.bytes_deduped),
        result.as_ref().map(|s| s.bytes_deduped),
        Some(activity.bytes_deduped),
        true,
    );
    row(
        "Already shared",
        progress.as_ref().map(|s| s.bytes_already_shared),
        result.as_ref().map(|s| s.bytes_already_shared),
        Some(activity.bytes_already_shared),
        true,
    );
    row(
        "Sharing jobs",
        progress.as_ref().map(|s| s.jobs_completed),
        result.as_ref().map(|s| s.jobs_completed),
        Some(activity.jobs_completed),
        false,
    );
    // Duplicate group counts are summarized only after scanning finishes.
    let summarized = status
        .as_ref()
        .is_some_and(|s| s["finished_at"].as_u64().is_some());
    row(
        "Duplicate files",
        progress
            .as_ref()
            .filter(|_| summarized)
            .map(|s| s.duplicate_files),
        result.as_ref().map(|s| s.duplicate_files),
        None,
        false,
    );
    row(
        "Duplicate chunks",
        progress
            .as_ref()
            .filter(|_| summarized)
            .map(|s| s.duplicate_chunks),
        result.as_ref().map(|s| s.duplicate_chunks),
        None,
        false,
    );
    row(
        "Incompatible ranges",
        progress.as_ref().map(|s| s.incompatible_ranges),
        result.as_ref().map(|s| s.incompatible_ranges),
        None,
        false,
    );
    row(
        "Read-only skips",
        progress.as_ref().map(|s| s.readonly_ranges),
        result.as_ref().map(|s| s.readonly_ranges),
        None,
        false,
    );
    row(
        "File errors",
        progress.as_ref().map(Stats::error_count),
        result.as_ref().map(Stats::error_count),
        None,
        false,
    );
    if let Some(totals) = &totals {
        println!(
            "\n  Scans        {} started · {} completed ({} with file errors)",
            count(activity.scans_started),
            count(totals.completed_runs),
            count(totals.partial_runs)
        );
        println!(
            "               {} failed · {} space pauses · {} interrupted",
            count(totals.failed_runs),
            count(totals.paused_runs),
            count(totals.interrupted_runs)
        );
    }
    let current_errors = progress.as_ref().is_some_and(|s| s.error_count() > 0);
    if args.errors {
        println!("\n  File errors");
        if current_errors {
            match File::open(args.state_directory.join("filesystem-errors.jsonl")) {
                Ok(file) => {
                    let mut output = BufWriter::new(io::stdout().lock());
                    for line in BufReader::with_capacity(64 * 1024, file).lines() {
                        let error: String = serde_json::from_str(&line?)?;
                        writeln!(output, "  {error}")?;
                    }
                    output.flush()?;
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    if let Some(stats) = &progress {
                        for error in &stats.errors {
                            println!("  {error}");
                        }
                    }
                }
                Err(error) => return Err(error.into()),
            }
        } else if let Some(full) =
            read_optional::<Stats>(&args.state_directory.join("filesystem-result.json"))?
        {
            for error in &full.errors {
                println!("  {error}");
            }
        }
    } else {
        let report = if current_errors {
            progress.as_ref()
        } else {
            result.as_ref()
        };
        if let Some(report) = report.filter(|s| s.error_count() > 0) {
            println!("\n  Recent file errors");
            for error in report.errors.iter().take(10) {
                println!("    {error}");
            }
            let shown = report.errors.len().min(10) as u64;
            if report.error_count() > shown {
                println!(
                    "    … {} more (use --errors for full details)",
                    count(report.error_count() - shown)
                );
            }
        }
    }

    Ok(())
}

fn count(value: u64) -> String {
    let digits = value.to_string();
    let mut formatted = String::new();
    for (i, digit) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i) % 3 == 0 {
            formatted.push(',');
        }
        formatted.push(digit);
    }
    formatted
}

fn size(bytes: u64) -> String {
    const UNITS: [&str; 7] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"];
    if bytes < 1024 {
        return format!("{bytes} B");
    }
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    format!("{value:.2} {}", UNITS[unit])
}

fn elapsed(seconds: u64) -> String {
    if seconds >= 86400 {
        format!("{}d {}h", seconds / 86400, seconds % 86400 / 3600)
    } else if seconds >= 3600 {
        format!("{}h {}m", seconds / 3600, seconds % 3600 / 60)
    } else if seconds >= 60 {
        format!("{}m {}s", seconds / 60, seconds % 60)
    } else {
        format!("{seconds}s")
    }
}

#[cfg(test)]
mod progress_tests {
    use super::*;

    #[test]
    fn live_totals_use_deltas_and_retain_interrupted_scans() {
        let mut activity = Activity::default();
        let first = Stats {
            files_seen: 10,
            bytes_hashed: 4096,
            bytes_deduped: 1024,
            estimated_bytes_saved: 512,
            savings_measured_bytes: 1024,
            ..Default::default()
        };
        activity.observe(&Stats::default(), &first);
        activity.observe(&first, &first); // Reporting the same snapshot is idempotent.
        let last = Stats {
            files_seen: 20,
            bytes_hashed: 8192,
            bytes_deduped: 2048,
            estimated_bytes_saved: 1024,
            savings_measured_bytes: 2048,
            ..Default::default()
        };
        activity.observe(&first, &last);
        let persisted = serde_json::json!({"activity": activity});
        let mut restarted = recover_activity(Some(&persisted)).unwrap();
        restarted.observe(&Stats::default(), &first); // A new scan starts from zero.
        assert_eq!(restarted.files_seen, 30);
        assert_eq!(restarted.bytes_hashed, 12288);
        assert_eq!(restarted.bytes_deduped, 3072);
        assert_eq!(restarted.estimated_bytes_saved, 1536);
        assert_eq!(restarted.savings_measured_bytes, 3072);
    }

    #[test]
    fn migration_adds_unfinished_work_but_never_recounts_completed_work() {
        let base = serde_json::json!({"state":"stopped", "finished_at":123, "error":null,
            "totals":{"completed_runs":1,"bytes_hashed":8192}, "progress":{"bytes_hashed":8192}});
        assert_eq!(recover_activity(Some(&base)).unwrap().bytes_hashed, 8192);
        let mut interrupted = base.clone();
        interrupted["error"] = serde_json::json!("stop requested");
        interrupted["progress"]["bytes_hashed"] = serde_json::json!(4096);
        assert_eq!(
            recover_activity(Some(&interrupted)).unwrap().bytes_hashed,
            12288
        );
        interrupted["state"] = serde_json::json!("running");
        assert_eq!(
            recover_activity(Some(&interrupted)).unwrap().scans_started,
            2
        );
    }
}

#[cfg(test)]
mod summary_io_tests {
    use super::*;
    #[test]
    fn live_summary_stays_small_and_full_error_log_is_incremental() {
        let directory = tempfile::tempdir().unwrap();
        let monitor = Monitor {
            directory: directory.path().to_owned(),
            minimum: 0,
            status: RefCell::new(Status {
                state: "running",
                pid: 1,
                started_at: 0,
                updated_at: 0,
                finished_at: None,
                free_bytes: 0,
                error: None,
                progress: Stats::default(),
                totals: Totals::default(),
                activity: Activity::default(),
                phase: "test",
            }),
            checked: RefCell::new(Instant::now()),
            reported: RefCell::new(Instant::now()),
            inventory: RefCell::new(None),
            inventory_hardlinks: false,
            logged_errors: Cell::new(0),
        };
        let mut stats = Stats {
            files_seen: 123,
            errors: (0..200)
                .map(|n| format!("error {n}: {}", "x".repeat(1000)))
                .collect(),
            ..Default::default()
        };
        monitor.record(&stats).unwrap();
        monitor.record(&stats).unwrap();
        let status: serde_json::Value =
            read_optional(&directory.path().join("filesystem-status.json"))
                .unwrap()
                .unwrap();
        assert_eq!(status["progress"]["errors_total"], 200);
        assert_eq!(status["progress"]["errors"].as_array().unwrap().len(), 10);
        assert!(
            fs::metadata(directory.path().join("filesystem-status.json"))
                .unwrap()
                .len()
                < 16000
        );
        let log = directory.path().join("filesystem-errors.jsonl");
        assert_eq!(
            BufReader::new(File::open(&log).unwrap()).lines().count(),
            200
        );
        stats.errors.push("one new error".into());
        monitor.record(&stats).unwrap();
        assert_eq!(
            BufReader::new(File::open(&log).unwrap()).lines().count(),
            201
        );
        assert_eq!(monitor.status.borrow().progress.error_count(), 201);
    }
}
