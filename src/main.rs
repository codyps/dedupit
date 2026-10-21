use anyhow::Result;
use clap::Parser;
use dedupit::{Granularity, Options, Scanner};
use std::{
    borrow::Cow,
    io::{self, BufWriter, Write},
    path::PathBuf,
};

// Borrow report fields instead of allocating a JSON value tree (including
// numeric arrays for both paths) for every range. Keep the existing key order.
#[derive(serde::Serialize)]
struct DuplicateRecord<'a> {
    destination: Cow<'a, str>,
    destination_offset: u64,
    destination_path_bytes: &'a [u8],
    length: u64,
    source: Cow<'a, str>,
    source_offset: u64,
    source_path_bytes: &'a [u8],
    #[serde(rename = "type")]
    record_type: &'static str,
    whole_file: bool,
}

#[derive(Parser)]
#[command(
    version,
    about = "Incremental full-file and extent deduplication. Reports duplicates unless --apply is given."
)]
struct Cli {
    /// Persistent SQLite index. Use the same index and roots on subsequent runs.
    #[arg(long, default_value = "dedupit.sqlite")]
    database: PathBuf,
    /// Apply native, kernel-verified sharing where supported.
    #[arg(long)]
    apply: bool,
    #[arg(long, value_enum, default_value = "both")]
    granularity: Granularity,
    /// Bytes per aligned logical chunk (power of two, 4 KiB through 16 MiB).
    #[arg(long, default_value_t = 131072)]
    chunk_size: usize,
    /// Rehash all files; useful on filesystems with unreliable metadata timestamps.
    #[arg(long)]
    verify: bool,
    /// Change discovery strategy. Btrfs tree searches require CAP_SYS_ADMIN.
    #[arg(long, value_enum, default_value = "auto")]
    scanner: Scanner,
    /// Operations per durable commit; unfinished batches are replayed after a crash.
    #[arg(long, default_value_t = 1024, value_parser = clap::value_parser!(u16).range(1..=4096))]
    commit_batch_size: u16,
    /// Emit machine-readable run statistics.
    #[arg(long)]
    json: bool,
    /// List duplicate paths and byte ranges (JSON Lines when used with --json).
    #[arg(long)]
    list: bool,
    #[arg(required = true)]
    roots: Vec<PathBuf>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let options = Options {
        database: cli.database,
        roots: cli.roots,
        chunk_size: cli.chunk_size,
        granularity: cli.granularity,
        verify: cli.verify,
        scanner: cli.scanner,
        commit_batch_size: cli.commit_batch_size as usize,
    };
    let stats = dedupit::run(&options, cli.apply)?;
    if cli.list {
        let stdout = io::stdout();
        let mut output = BufWriter::with_capacity(64 * 1024, stdout.lock());
        dedupit::list_duplicates(&options, |duplicate| {
            if cli.json {
                use std::os::unix::ffi::OsStrExt;
                serde_json::to_writer(
                    &mut output,
                    &DuplicateRecord {
                        destination: duplicate.destination.to_string_lossy(),
                        destination_offset: duplicate.destination_offset,
                        destination_path_bytes: duplicate.destination.as_os_str().as_bytes(),
                        length: duplicate.length,
                        source: duplicate.source.to_string_lossy(),
                        source_offset: duplicate.source_offset,
                        source_path_bytes: duplicate.source.as_os_str().as_bytes(),
                        record_type: "duplicate",
                        whole_file: duplicate.whole_file,
                    },
                )?;
                writeln!(output)?;
            } else {
                writeln!(
                    output,
                    "{}: {:?}@{} -> {:?}@{} ({} bytes)",
                    if duplicate.whole_file {
                        "file"
                    } else {
                        "chunk"
                    },
                    duplicate.source,
                    duplicate.source_offset,
                    duplicate.destination,
                    duplicate.destination_offset,
                    duplicate.length
                )?;
            }
            Ok(())
        })?;
        // Reports precede the final stats record, including with redirected
        // stdout; propagate write errors instead of silently losing a tail.
        output.flush()?;
    }
    if cli.json {
        if cli.list {
            println!("{}", serde_json::json!({"type":"stats","stats":stats}));
        } else {
            println!("{}", serde_json::to_string_pretty(&stats)?);
        }
    } else {
        if stats.btrfs_subvolumes > 0 {
            println!("Btrfs discovery: {} subvolumes, {} metadata pages, {} queued observations, {} cached chunks reused",
                stats.btrfs_subvolumes,stats.btrfs_search_pages,stats.discovery_jobs,stats.chunks_reused);
        }
        println!(
            "Files: {} seen, {} hashed, {} unchanged, {} removed; {} bytes read, {} hole bytes skipped",
            stats.files_seen, stats.files_hashed, stats.files_unchanged, stats.files_removed,
            stats.bytes_hashed, stats.hole_bytes_skipped
        );
        println!(
            "Duplicates: {} whole files ({} bytes), {} chunks ({} bytes); these counts overlap",
            stats.duplicate_files,
            stats.duplicate_file_bytes,
            stats.duplicate_chunks,
            stats.duplicate_chunk_bytes
        );
        if cli.apply {
            println!(
                "Native sharing: {}/{} jobs completed, {} logical bytes accepted, {} already shared, {} unsupported, {} stale",
                stats.jobs_completed, stats.jobs_attempted, stats.bytes_deduped,
                stats.bytes_already_shared, stats.unsupported, stats.stale
            );
        }
        for error in &stats.errors {
            eprintln!("warning: {error}");
        }
        for fallback in &stats.scanner_fallbacks {
            eprintln!("scanner: {fallback}");
        }
    }
    if !stats.errors.is_empty() {
        std::process::exit(2);
    }
    Ok(())
}
