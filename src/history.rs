//! Per-run summaries and append-only error logs for the native service.
use crate::service::{count, read_optional, save, size};
use anyhow::{Context, Result};
use dedupit::Stats;
use serde::{
    de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor},
    Deserialize, Serialize,
};
use std::{
    fmt,
    fs::{self, File, OpenOptions},
    io::{self, BufRead, BufReader, BufWriter, Write},
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
    time::UNIX_EPOCH,
};

#[derive(Clone, Serialize, Deserialize)]
struct RunReport {
    run_id: u64,
    state: String,
    started_at: Option<u64>,
    updated_at: u64,
    finished_at: Option<u64>,
    error: Option<String>,
    progress: Stats,
}

fn root(directory: &Path) -> PathBuf {
    directory.join("runs")
}

fn run_directory(directory: &Path, id: u64) -> PathBuf {
    root(directory).join(id.to_string())
}

fn summary_path(directory: &Path, id: u64) -> PathBuf {
    run_directory(directory, id).join("summary.json")
}

pub(crate) fn error_log(directory: &Path, id: u64) -> PathBuf {
    run_directory(directory, id).join("errors.jsonl")
}

fn ids(directory: &Path) -> Result<Vec<u64>> {
    let entries = match fs::read_dir(root(directory)) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error).context("read run history"),
    };
    let mut ids = Vec::new();
    for entry in entries {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            let name = entry.file_name();
            if let Some(id) = name.to_str().and_then(|name| name.parse::<u64>().ok()) {
                if name == id.to_string().as_str() {
                    ids.push(id);
                }
            }
        }
    }
    ids.sort_unstable_by(|a, b| b.cmp(a));
    Ok(ids)
}

pub(crate) fn latest_id(directory: &Path) -> Result<u64> {
    Ok(ids(directory)?.first().copied().unwrap_or(0))
}

pub(crate) fn begin(directory: &Path, id: u64) -> Result<()> {
    fs::create_dir_all(root(directory))?;
    fs::create_dir(run_directory(directory, id))?;
    let log = error_log(directory, id);
    OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&log)?
        .sync_all()?;
    File::open(run_directory(directory, id))?.sync_all()?;
    File::open(root(directory))?.sync_all()?;

    // Keep the old paths usable without copying the potentially large logs.
    let current = directory.join("filesystem-errors.jsonl");
    match fs::rename(&current, directory.join("filesystem-errors-previous.jsonl")) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error).context("rotate latest file error log"),
    }
    fs::hard_link(&log, &current)?;
    Ok(())
}

pub(crate) fn finish(directory: &Path, id: u64, status: &impl Serialize) -> Result<()> {
    save(&summary_path(directory, id), status)
}

pub(crate) fn recover(directory: &Path, previous: Option<&serde_json::Value>) -> Result<()> {
    if let Some(previous) = previous.filter(|status| status["run_id"].as_u64().is_some()) {
        let mut report = RunReport::deserialize(previous)?;
        if !summary_path(directory, report.run_id).exists() {
            if report.state == "running" {
                report.state = "interrupted".into();
                report.error = Some("Service exited before the scan finished".into());
            }
            finish(directory, report.run_id, &report)?;
        }
    } else {
        preserve_legacy_result(directory)?;
    }
    Ok(())
}

fn preserve_legacy_result(directory: &Path) -> Result<()> {
    // ID 0 is reserved for the one completed result available before history.
    if summary_path(directory, 0).exists() {
        return Ok(());
    }
    let Some(progress) = read_optional::<Stats>(&directory.join("filesystem-result-summary.json"))?
    else {
        return Ok(());
    };
    let legacy_dir = run_directory(directory, 0);
    fs::create_dir_all(&legacy_dir)?;
    let full = directory.join("filesystem-result.json");
    let archived = legacy_dir.join("legacy-result.json");
    match fs::hard_link(&full, &archived) {
        Ok(()) => {}
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::NotFound | io::ErrorKind::AlreadyExists
            ) => {}
        Err(error) => return Err(error).context("preserve last pre-history report"),
    }
    let updated_at = fs::metadata(&archived)
        .or_else(|_| fs::metadata(directory.join("filesystem-result-summary.json")))?
        .modified()?
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let state = if progress.error_count() == 0 {
        "completed"
    } else {
        "finished_with_errors"
    };
    let report = RunReport {
        run_id: 0,
        state: state.into(),
        started_at: None,
        updated_at,
        finished_at: None,
        error: None,
        progress,
    };
    finish(directory, 0, &report)?;
    File::open(root(directory))?.sync_all()?;
    File::open(directory)?.sync_all()?;
    Ok(())
}

fn run_report(directory: &Path, id: u64, live: Option<&RunReport>) -> Result<Option<RunReport>> {
    Ok(read_optional(&summary_path(directory, id))?
        .or_else(|| live.filter(|report| report.run_id == id).cloned()))
}

fn live_report(directory: &Path) -> Result<Option<RunReport>> {
    read_optional::<serde_json::Value>(&directory.join("filesystem-status.json"))?
        .filter(|status| status["run_id"].as_u64().is_some())
        .map(|status| serde_json::from_value(status).context("read latest run"))
        .transpose()
}

fn outcome(state: &str) -> &str {
    match state {
        "running" => "Running",
        "completed" => "Completed",
        "finished_with_errors" => "Completed with file errors",
        "paused_for_space" => "Paused (low space)",
        "interrupted" => "Interrupted",
        "failed" => "Failed",
        _ => "Unknown",
    }
}

fn timestamp(seconds: u64) -> String {
    let Ok(time) = libc::time_t::try_from(seconds) else {
        return seconds.to_string();
    };
    let mut calendar = std::mem::MaybeUninit::<libc::tm>::uninit();
    // SAFETY: gmtime_r initializes the out parameter on success.
    let calendar = unsafe {
        if libc::gmtime_r(&time, calendar.as_mut_ptr()).is_null() {
            return seconds.to_string();
        }
        calendar.assume_init()
    };
    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02} UTC",
        i64::from(calendar.tm_year) + 1900,
        calendar.tm_mon + 1,
        calendar.tm_mday,
        calendar.tm_hour,
        calendar.tm_min,
        calendar.tm_sec
    )
}

pub(crate) fn list(directory: &Path, json: bool) -> Result<()> {
    let live = live_report(directory)?;
    let mut reports = Vec::new();
    for id in ids(directory)? {
        if let Some(report) = run_report(directory, id, live.as_ref())? {
            reports.push(report);
        }
    }
    if json {
        println!("{}", serde_json::to_string_pretty(&reports)?);
    } else if reports.is_empty() {
        println!("No saved runs");
    } else {
        println!(
            "  {:>6}  {:<25}  {:<27}  {:>12}",
            "Run", "Started or recorded", "Outcome", "File errors"
        );
        for report in reports {
            let started = timestamp(report.started_at.unwrap_or(report.updated_at));
            println!(
                "  {:>6}  {:<25}  {:<27}  {:>12}",
                report.run_id,
                started,
                outcome(&report.state),
                count(report.progress.error_count())
            );
        }
    }
    Ok(())
}

pub(crate) fn show(directory: &Path, id: u64, json: bool, errors: bool) -> Result<()> {
    let report: RunReport = match read_optional(&summary_path(directory, id))? {
        Some(report) => report,
        None => live_report(directory)?
            .filter(|report| report.run_id == id)
            .with_context(|| format!("run {id} is not in history"))?,
    };
    if json {
        println!("{}", serde_json::to_string_pretty(&report)?);
        return Ok(());
    }

    println!("Run {id}");
    println!("  Outcome                 {}", outcome(&report.state));
    if let Some(started) = report.started_at {
        println!("  Started                 {}", timestamp(started));
    }
    if let Some(finished) = report.finished_at {
        println!("  Finished                {}", timestamp(finished));
    }
    println!("  Last recorded           {}", timestamp(report.updated_at));
    if let Some(error) = &report.error {
        println!("  Scan error              {error}");
    }
    let stats = &report.progress;
    println!("  Files scanned           {}", count(stats.files_seen));
    println!("  Files hashed            {}", count(stats.files_hashed));
    println!("  Data read               {}", size(stats.bytes_hashed));
    println!("  Logical bytes shared    {}", size(stats.bytes_deduped));
    println!(
        "  Already shared          {}",
        size(stats.bytes_already_shared)
    );
    println!(
        "  Sharing jobs            {} of {} attempted",
        count(stats.jobs_completed),
        count(stats.jobs_attempted)
    );
    println!("  Duplicate files         {}", count(stats.duplicate_files));
    println!(
        "  Duplicate chunks        {}",
        count(stats.duplicate_chunks)
    );
    println!(
        "  Incompatible ranges     {}",
        count(stats.incompatible_ranges)
    );
    println!(
        "  Read-only skips          {}",
        count(stats.readonly_ranges)
    );
    println!("  File errors              {}", count(stats.error_count()));
    if stats.error_count() == 0 {
        return Ok(());
    }
    println!("\n  File errors from run {id}");
    if errors {
        let mut output = BufWriter::with_capacity(64 * 1024, io::stdout().lock());
        let legacy = run_directory(directory, id).join("legacy-result.json");
        if id == 0 && legacy.is_file() {
            stream_legacy_errors(&legacy, &mut output)?;
        } else {
            match File::open(error_log(directory, id)) {
                Ok(log) => write_error_log(log, &mut output)?,
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    for error in &stats.errors {
                        writeln!(output, "    {error}")?;
                    }
                    if stats.error_count() > stats.errors.len() as u64 {
                        writeln!(output, "    Full error details unavailable")?;
                    }
                }
                Err(error) => return Err(error).context("open run error log"),
            }
        }
        output.flush()?;
    } else {
        for error in &stats.errors {
            println!("    {error}");
        }
        let remaining = stats
            .error_count()
            .saturating_sub(stats.errors.len() as u64);
        if remaining > 0 {
            println!(
                "    … {} more (use --errors for full details)",
                count(remaining)
            );
        }
    }
    Ok(())
}

pub(crate) fn write_error_log(log: File, output: &mut impl Write) -> Result<()> {
    let mut reader = BufReader::with_capacity(64 * 1024, log);
    let mut line = String::new();
    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            break;
        }
        // A concurrent append or crash can leave an incomplete final record.
        let error: String = match serde_json::from_str(&line) {
            Err(error) if error.is_eof() && !line.ends_with('\n') => break,
            result => result?,
        };
        writeln!(output, "    {error}")?;
    }
    Ok(())
}

fn stream_legacy_errors(path: &Path, output: &mut impl Write) -> Result<()> {
    struct Report<'a, W>(&'a mut W);
    struct Errors<'a, W>(&'a mut W);

    impl<'de, W: Write> Visitor<'de> for Report<'_, W> {
        type Value = ();

        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("a completed scan report")
        }

        fn visit_map<M: MapAccess<'de>>(self, mut map: M) -> Result<(), M::Error> {
            while let Some(key) = map.next_key::<String>()? {
                if key == "errors" {
                    map.next_value_seed(Errors(self.0))?;
                } else {
                    map.next_value::<de::IgnoredAny>()?;
                }
            }
            Ok(())
        }
    }

    impl<'de, W: Write> DeserializeSeed<'de> for Errors<'_, W> {
        type Value = ();

        fn deserialize<D: serde::Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
            deserializer.deserialize_seq(self)
        }
    }

    impl<'de, W: Write> Visitor<'de> for Errors<'_, W> {
        type Value = ();

        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("a list of file errors")
        }

        fn visit_seq<S: SeqAccess<'de>>(self, mut seq: S) -> Result<(), S::Error> {
            while let Some(error) = seq.next_element::<String>()? {
                writeln!(self.0, "    {error}").map_err(de::Error::custom)?;
            }
            Ok(())
        }
    }

    use serde::Deserializer;
    let input = BufReader::with_capacity(64 * 1024, File::open(path)?);
    let mut parser = serde_json::Deserializer::from_reader(input);
    parser.deserialize_map(Report(output))?;
    parser.end()?;
    Ok(())
}
