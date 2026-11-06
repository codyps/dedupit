//! Incremental duplicate discovery on Unix filesystems, and safe native sharing on Linux.
#![cfg(unix)]

pub mod backend;
#[cfg(target_os = "linux")]
mod btrfs_io;
#[cfg(target_os = "linux")]
mod btrfs_mount;
#[cfg(target_os = "linux")]
mod btrfs_scan;
#[cfg(feature = "test-failpoints")]
mod failpoints;
mod index;
mod scan;

use anyhow::{ensure, Context, Result};
use serde::Serialize;
use std::{path::PathBuf, rc::Rc};

pub use backend::{Backend, NativeBackend, Outcome, Range};

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum Granularity {
    File,
    Chunk,
    Both,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum Scanner {
    /// Use generation-based btrfs discovery when its ioctls are available.
    Auto,
    /// Always walk directories and compare file metadata.
    Portable,
    /// Require generation-based btrfs discovery; fail rather than fall back.
    Btrfs,
}

#[derive(Clone, Debug)]
pub struct Options {
    pub database: PathBuf,
    pub roots: Vec<PathBuf>,
    /// Logical chunk size; independent of physical extent boundaries.
    pub chunk_size: usize,
    pub granularity: Granularity,
    /// Re-read every file, even when its metadata fingerprint is unchanged.
    pub verify: bool,
    pub scanner: Scanner,
    /// Operations per durable database batch (1..=4096). May change on restart.
    pub commit_batch_size: usize,
}

#[derive(Clone, Debug, Default, Serialize, serde::Deserialize)]
#[serde(default)]
pub struct Stats {
    /// Explicit scan/apply batch commits, excluding startup and maintenance SQL.
    pub database_batches: u64,
    pub btrfs_subvolumes: u64,
    pub btrfs_search_pages: u64,
    pub btrfs_mapping_updates: u64,
    pub btrfs_events_reused: u64,
    pub discovery_jobs: u64,
    pub chunks_reused: u64,
    pub scanner_fallbacks: Vec<String>,
    pub files_seen: u64,
    pub files_hashed: u64,
    pub files_unchanged: u64,
    pub hardlinks_skipped: u64,
    pub files_removed: u64,
    pub bytes_hashed: u64,
    pub hole_bytes_skipped: u64,
    pub duplicate_files: u64,
    pub duplicate_file_bytes: u64,
    pub duplicate_chunks: u64,
    pub duplicate_chunk_bytes: u64,
    pub jobs_attempted: u64,
    pub jobs_completed: u64,
    /// Bounded sharing passes performed before scanning finished.
    pub interleaved_batches: u64,
    /// Logical bytes accepted by the kernel, NOT measured physical savings.
    pub bytes_deduped: u64,
    pub bytes_already_shared: u64,
    pub unsupported: u64,
    pub stale: u64,
    pub errors: Vec<String>,
}

#[derive(Debug)]
pub struct Duplicate {
    pub source: PathBuf,
    pub source_offset: u64,
    pub destination: PathBuf,
    pub destination_offset: u64,
    pub length: u64,
    pub whole_file: bool,
}

/// Stream candidate duplicates from the current index. A group with N ranges
/// produces N-1 records, rather than all pairs. File and chunk records overlap.
/// Native application performs the authoritative byte comparison.
pub fn list_duplicates(
    options: &Options,
    mut visit: impl FnMut(Duplicate) -> Result<()>,
) -> Result<()> {
    let index = index::Index::open(options)?;
    index.list_duplicates(&mut visit)
}

/// Cooperative service control. Errors abort the current batch, leaving committed
/// progress reusable. Callbacks run on the scanning thread; they must not block.
pub trait RunObserver {
    fn phase(&self, _phase: &'static str) -> Result<()> {
        self.checkpoint()
    }
    /// Final counters on success or failure. This must be allowed even after
    /// cancellation so the last observed work can be recorded before exit.
    fn finished(&self, _stats: &Stats) -> Result<()> {
        Ok(())
    }
    fn checkpoint(&self) -> Result<()>;
    fn progress(&self, _stats: &Stats) -> Result<()> {
        self.checkpoint()
    }
}

/// Run with cooperative cancellation and progress reporting, without spawning
/// a worker or requiring an external supervisor.
pub fn run_observed(
    options: &Options,
    apply: bool,
    observer: Rc<dyn RunObserver>,
) -> Result<Stats> {
    run_inner(options, apply, &mut NativeBackend, Some(observer))
}

/// Discovery commits each stable file atomically. Applying is restartable and
/// only records work after a backend reports full success.
pub fn run(options: &Options, apply: bool) -> Result<Stats> {
    run_with_backend(options, apply, &mut NativeBackend)
}

pub fn run_with_backend(
    options: &Options,
    apply: bool,
    backend: &mut impl Backend,
) -> Result<Stats> {
    run_inner(options, apply, backend, None)
}

fn run_inner(
    options: &Options,
    apply: bool,
    backend: &mut impl Backend,
    observer: Option<Rc<dyn RunObserver>>,
) -> Result<Stats> {
    if let Some(observer) = &observer {
        observer.checkpoint()?;
    }
    ensure!(
        (4096..=16 * 1024 * 1024).contains(&options.chunk_size)
            && options.chunk_size.is_power_of_two(),
        "chunk size must be a power of two between 4096 and 16777216"
    );
    ensure!(!options.roots.is_empty(), "at least one root is required");
    ensure!(
        (1..=4096).contains(&options.commit_batch_size),
        "commit batch size must be between 1 and 4096"
    );
    let mut stats = Stats::default();
    let outcome = (|| -> Result<()> {
        if let Some(observer) = &observer {
            observer.phase("Opening index")?;
        }
        let mut index = index::Index::open(options)?;
        index.observer = observer.clone();
        let mut sharing = index::Interleaver::new(if apply { Some(backend) } else { None });
        if let Some(observer) = &observer {
            observer.phase("Scanning files")?;
        }
        scan::scan(&mut index, options, &mut stats, &mut sharing).context("scanning files")?;
        index.flush_batch()?;
        if let Some(observer) = &observer {
            observer.phase("Summarizing duplicates")?;
        }
        index.summarize(&mut stats)?;
        if let Some(observer) = &observer {
            observer.phase("Sharing remaining ranges")?;
        }
        sharing.finish(&mut index, &mut stats)?;
        index.flush_batch()?;
        stats.database_batches = index.batch_commits();
        Ok(())
    })();
    if let Some(observer) = &observer {
        observer.finished(&stats)?;
    }
    outcome?;
    Ok(stats)
}
