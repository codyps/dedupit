//! Native foreground service: one scanning process, durable reports, no helpers.
use anyhow::{bail, ensure, Context, Result};
use dedupit::{Granularity, Options, RunObserver, Scanner, Stats};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use std::{
    cell::RefCell,
    ffi::CString,
    fs::{self, File, OpenOptions},
    io::{self, Write},
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
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn save(path: &Path, value: &impl Serialize) -> Result<()> {
    // Same-directory rename gives readers a complete old or new report. A
    // unique, exclusively created temporary file also survives stale crash debris.
    let stamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let temporary = path.with_extension(format!("{}.{}.tmp", std::process::id(), stamp));
    let result = (|| -> Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary)?;
        serde_json::to_writer_pretty(&mut file, value)?;
        writeln!(file)?;
        file.sync_all()?;
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
            serde_json::from_reader(file).with_context(|| format!("read {}", path.display()))?,
        )),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error).with_context(|| format!("open {}", path.display())),
    }
}

fn free_bytes(path: &Path) -> Result<u64> {
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
impl RunObserver for Monitor {
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
            {
                let mut status = self.status.borrow_mut();
                status.progress = stats.clone();
                status.updated_at = now();
            }
            self.write()?;
            *self.reported.borrow_mut() = Instant::now();
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
    let totals = previous
        .and_then(|v| v.get("totals").cloned())
        .map(serde_json::from_value)
        .transpose()?
        .unwrap_or_default();
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
        }),
        checked: RefCell::new(Instant::now()),
        reported: RefCell::new(Instant::now()),
    });
    while !STOP.load(Ordering::Relaxed) {
        {
            let mut status = monitor.status.borrow_mut();
            status.state = "running";
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
                    status.totals.completed_runs += 1;
                    status.totals.partial_runs += u64::from(!stats.errors.is_empty());
                    status.totals.files_hashed += stats.files_hashed;
                    status.totals.bytes_hashed += stats.bytes_hashed;
                    status.totals.jobs_completed += stats.jobs_completed;
                    status.totals.bytes_deduped += stats.bytes_deduped;
                    status.totals.bytes_already_shared += stats.bytes_already_shared;
                    status.progress = stats;
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

pub fn stats(args: StatsArgs) -> Result<()> {
    let status =
        read_optional::<serde_json::Value>(&args.state_directory.join("filesystem-status.json"))?;
    let result = read_optional::<Stats>(&args.state_directory.join("filesystem-result.json"))?;
    if status.is_none() && result.is_none() {
        bail!(
            "no recorded statistics in {}",
            args.state_directory.display()
        );
    }
    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(
                &serde_json::json!({"status": status, "last_result": result})
            )?
        );
        return Ok(());
    }
    if let Some(status) = &status {
        println!("State: {}", status["state"].as_str().unwrap_or("unknown"));
        if let Some(updated) = status["updated_at"].as_u64() {
            println!("Updated: {} seconds ago", now().saturating_sub(updated));
        }
        if let Some(free) = status["free_bytes"].as_u64() {
            println!("Available: {free} bytes");
        }
        if let Some(error) = status["error"].as_str() {
            println!("Last error: {error}");
        }
        if let Some(progress) = status.get("progress") {
            println!("Current/latest run: {} files hashed, {} bytes read, {} sharing jobs, {} logical bytes deduped", progress["files_hashed"], progress["bytes_hashed"], progress["jobs_completed"], progress["bytes_deduped"]);
        }
        if let Some(totals) = status.get("totals") {
            let totals: Totals = serde_json::from_value(totals.clone())?;
            println!("Recorded runs: {} completed ({} with file errors), {} failed, {} space pauses, {} interrupted", totals.completed_runs, totals.partial_runs, totals.failed_runs, totals.paused_runs, totals.interrupted_runs);
            println!("Completed-run totals: {} files hashed, {} bytes read, {} sharing jobs, {} logical bytes deduped, {} already shared", totals.files_hashed, totals.bytes_hashed, totals.jobs_completed, totals.bytes_deduped, totals.bytes_already_shared);
        }
    }
    if let Some(result) = result {
        println!("Last complete report: {} files seen, {} files hashed, {} duplicate files, {} duplicate chunks, {} errors", result.files_seen, result.files_hashed, result.duplicate_files, result.duplicate_chunks, result.errors.len());
        for error in result.errors.iter().take(10) {
            println!("  {error}");
        }
    }
    println!("Deduped bytes are logical bytes accepted by the kernel, not measured physical savings. Totals cover completed runs recorded by this service.");
    Ok(())
}
