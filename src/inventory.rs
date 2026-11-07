//! Current sharing among indexed files. Each refresh is rebuilt from a single
//! SQLite snapshot, rather than accumulating results from deduplication runs.
use anyhow::{ensure, Context, Result};
use rusqlite::{Connection, OpenFlags};
use serde::{Deserialize, Serialize};
use std::{
    fs,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct Estimate {
    pub as_of: u64,
    pub completed_at: Option<u64>,
    pub indexed_files: u64,
    pub checked_files: u64,
    pub verified_files: u64,
    pub checked_file_bytes: u64,
    pub mapped_data_bytes: u64,
    pub referenced_data_bytes: u64,
    pub saved_uncompressed_bytes: u64,
    pub physical_estimate_bytes: u64,
    pub physical_covered_bytes: u64,
    pub pending_jobs: u64,
    pub include_hardlinks: bool,
}
#[derive(Default, Serialize, Deserialize)]
#[serde(default)]
pub(crate) struct Report {
    pub state: String,
    pub updated_at: u64,
    pub estimate: Option<Estimate>,
    pub refresh: Estimate,
    pub error: Option<String>,
}

pub(crate) fn read(directory: &Path) -> Result<Option<Report>> {
    match fs::File::open(directory.join("sharing-inventory.json")) {
        Ok(file) => Ok(Some(serde_json::from_reader(
            std::io::BufReader::with_capacity(64 * 1024, file),
        )?)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}
fn write(directory: &Path, report: &mut Report) -> Result<()> {
    report.updated_at = crate::service::now();
    crate::service::save(&directory.join("sharing-inventory.json"), report)
}
fn le64(bytes: &[u8], offset: usize) -> Option<u64> {
    Some(u64::from_le_bytes(
        bytes.get(offset..offset + 8)?.try_into().ok()?,
    ))
}
fn fingerprint(metadata: &fs::Metadata) -> Vec<u8> {
    [
        metadata.dev(),
        metadata.ino(),
        metadata.len(),
        metadata.mtime() as u64,
        metadata.mtime_nsec() as u64,
        metadata.ctime() as u64,
        metadata.ctime_nsec() as u64,
    ]
    .into_iter()
    .flat_map(u64::to_le_bytes)
    .collect()
}

#[derive(Default)]
struct Group {
    key: Option<(Vec<u8>, u64)>,
    disk: u64,
    ram: u64,
    encoded: bool,
    consistent: bool,
    referenced: u64,
    spans: Vec<(u64, u64)>,
}
impl Group {
    fn add(&mut self, disk: u64, ram: u64, encoded: bool, start: u64, length: u64, links: u64) {
        if self.spans.is_empty() {
            self.disk = disk;
            self.ram = ram;
            self.encoded = encoded;
            self.consistent = true;
        }
        self.consistent &= self.disk == disk && self.ram == ram && self.encoded == encoded;
        self.referenced = self.referenced.saturating_add(length.saturating_mul(links));
        self.spans.push((start, start + length));
    }
    fn finish(&mut self, estimate: &mut Estimate) {
        if self.consistent && !self.spans.is_empty() {
            self.spans.sort_unstable();
            let mut unique = 0u64;
            let mut end = 0u64;
            for &(start, finish) in &self.spans {
                unique += finish.saturating_sub(start.max(end));
                end = end.max(finish);
            }
            let redundant = self.referenced.saturating_sub(unique);
            estimate.referenced_data_bytes += self.referenced;
            estimate.saved_uncompressed_bytes += redundant;
            // Uncompressed overlaps have a direct byte correspondence. For
            // encoded allocations, only estimate groups covering the complete
            // decoded allocation; partial compressed slices remain uncovered.
            if !self.encoded || unique == self.ram {
                estimate.physical_estimate_bytes += if self.encoded {
                    (u128::from(self.disk) * u128::from(redundant) / u128::from(self.ram)) as u64
                } else {
                    redundant
                };
                estimate.physical_covered_bytes += self.referenced;
            }
        }
        self.spans.clear();
        self.referenced = 0;
    }
}

pub(crate) struct Worker {
    stop: Arc<AtomicBool>,
    interrupt: Arc<Mutex<Option<rusqlite::InterruptHandle>>>,
    thread: Option<JoinHandle<()>>,
}
impl Worker {
    pub(crate) fn start(
        directory: PathBuf,
        include_hardlinks: bool,
        minimum_free_bytes: u64,
    ) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let interrupt = Arc::new(Mutex::new(None));
        let worker_stop = stop.clone();
        let worker_interrupt = interrupt.clone();
        let thread = thread::spawn(move || {
            let mut report = read(&directory).ok().flatten().unwrap_or_default();
            while !worker_stop.load(Ordering::Relaxed) {
                if !directory.join("filesystem.sqlite").exists() {
                    thread::sleep(Duration::from_millis(200));
                    continue;
                }
                report.refresh = Estimate {
                    as_of: crate::service::now(),
                    include_hardlinks,
                    ..Default::default()
                };
                report.state = "Checking indexed files".into();
                report.error = None;
                let outcome = refresh(
                    &directory,
                    &worker_stop,
                    &worker_interrupt,
                    &mut report,
                    minimum_free_bytes,
                );
                *worker_interrupt.lock().unwrap() = None;
                if worker_stop.load(Ordering::Relaxed) {
                    break;
                }
                match outcome {
                    Ok(()) if report.refresh.verified_files == 0 => {
                        report.state = "Waiting for verified native extent mappings".into();
                    }
                    Ok(()) => {
                        report.refresh.completed_at = Some(crate::service::now());
                        report.estimate = Some(report.refresh.clone());
                        report.state = "Complete".into();
                    }
                    Err(error) => {
                        report.state = "Refresh failed".into();
                        report.error = Some(format!("{error:#}"));
                    }
                }
                let _ = write(&directory, &mut report);
                // Failed/incomplete bootstrap refreshes retry sooner. A fresh
                // full metadata inventory otherwise refreshes every five minutes.
                let wait = if report.error.is_some()
                    || report.refresh.indexed_files == 0
                    || report.refresh.verified_files == 0
                {
                    10
                } else {
                    300
                };
                let deadline = Instant::now() + Duration::from_secs(wait);
                while !worker_stop.load(Ordering::Relaxed) && Instant::now() < deadline {
                    thread::sleep(Duration::from_millis(200));
                }
            }
        });
        Self {
            stop,
            interrupt,
            thread: Some(thread),
        }
    }
}
impl Drop for Worker {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(interrupt) = self.interrupt.lock().unwrap().as_ref() {
            interrupt.interrupt();
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn capture(connection: &Connection) -> Result<()> {
    connection.execute_batch(
        "CREATE TEMP TABLE inventory_files(file_id INTEGER PRIMARY KEY,path BLOB,fingerprint BLOB,fskey BLOB);
         INSERT INTO temp.inventory_files
           SELECT f.id,f.path,f.fingerprint,f.fskey FROM file_paths f
           JOIN extent_state e ON e.file_id=f.id
           JOIN btrfs_files b ON b.file_id=f.id
           WHERE NOT EXISTS(SELECT 1 FROM discovery d WHERE d.crawl_id=b.crawl_id AND d.ino=b.ino);
         CREATE TEMP TABLE inventory_extents AS
           SELECT e.file_id,e.offset,e.length,substr(e.descriptor,1,53) AS descriptor
           FROM file_extents e CROSS JOIN temp.inventory_files f WHERE f.file_id=e.file_id;
         COMMIT;",
    )?;
    ensure!(
        connection.is_autocommit(),
        "inventory retained its main snapshot"
    );
    Ok(())
}

fn refresh(
    directory: &Path,
    stop: &AtomicBool,
    interrupt: &Mutex<Option<rusqlite::InterruptHandle>>,
    report: &mut Report,
    minimum_free_bytes: u64,
) -> Result<()> {
    ensure!(
        crate::service::free_bytes(directory)? >= minimum_free_bytes,
        "inventory paused for free space"
    );
    let connection = Connection::open_with_flags(
        directory.join("filesystem.sqlite"),
        OpenFlags::SQLITE_OPEN_READ_ONLY,
    )?;
    connection.busy_timeout(Duration::from_secs(2))?;
    *interrupt.lock().unwrap() = Some(connection.get_interrupt_handle());
    // A read snapshot protects consistency across old and current scan batches.
    // Sorting spills to private SQLite temp files, not an unbounded Rust map.
    connection.execute_batch("PRAGMA temp_store=FILE; PRAGMA cache_size=-8192; PRAGMA temp.cache_size=-8192;
        CREATE TEMP TABLE verified(file_id INTEGER PRIMARY KEY, size INTEGER NOT NULL, links INTEGER NOT NULL);
        BEGIN;")?;
    report.refresh.indexed_files =
        connection.query_row("SELECT COUNT(*) FROM files", [], |r| r.get(0))?;
    report.refresh.pending_jobs =
        connection.query_row("SELECT COUNT(*) FROM discovery", [], |r| r.get(0))?;
    write(directory, report)?;
    report.state = "Capturing indexed extent snapshot".into();
    write(directory, report)?;
    // Materialize only inventory inputs, then release the main WAL snapshot.
    // Slow filesystem validation and sorting use TEMP exclusively. Inline file
    // contents are not needed: all decoded extent fields fit in 53 bytes.
    capture(&connection)?;
    report.state = "Verifying indexed files".into();
    write(directory, report)?;
    let mut last_report = Instant::now();
    let mut files =
        connection.prepare("SELECT file_id,path,fingerprint FROM temp.inventory_files")?;
    let mut rows = files.query([])?;
    while let Some(row) = rows.next()? {
        ensure!(!stop.load(Ordering::Relaxed), "inventory stopped");
        report.refresh.checked_files += 1;
        let path: Vec<u8> = row.get(1)?;
        let path = PathBuf::from(std::ffi::OsString::from_vec(path));
        let stored: Vec<u8> = row.get(2)?;
        if let Ok(metadata) = fs::symlink_metadata(path) {
            if metadata.is_file() && fingerprint(&metadata) == stored {
                let links = if report.refresh.include_hardlinks {
                    metadata.nlink().max(1)
                } else {
                    1
                };
                connection.execute(
                    "INSERT INTO temp.verified VALUES (?1,?2,?3)",
                    rusqlite::params![row.get::<_, i64>(0)?, metadata.len(), links],
                )?;
                report.refresh.verified_files += 1;
                report.refresh.checked_file_bytes += metadata.len();
            }
        }
        if last_report.elapsed() >= Duration::from_secs(2) {
            ensure!(
                crate::service::free_bytes(directory)? >= minimum_free_bytes,
                "inventory paused for free space"
            );
            write(directory, report)?;
            last_report = Instant::now();
        }
    }
    drop(rows);
    drop(files);
    report.state = "Summarizing shared extents".into();
    write(directory, report)?;
    let mut extents = connection.prepare(
        "SELECT f.fskey,e.offset,e.length,e.descriptor,v.size,v.links
        FROM temp.inventory_extents e JOIN temp.inventory_files f ON f.file_id=e.file_id JOIN temp.verified v ON v.file_id=f.file_id
        ORDER BY f.fskey,substr(e.descriptor,22,8)",
    )?;
    let mut rows = extents.query([])?;
    let mut group = Group::default();
    while let Some(row) = rows.next()? {
        ensure!(!stop.load(Ordering::Relaxed), "inventory stopped");
        let descriptor: Vec<u8> = row.get(3)?;
        let offset: u64 = row.get(1)?;
        let length: u64 = row.get(2)?;
        let file_size: u64 = row.get(4)?;
        let links: u64 = row.get(5)?;
        let used = length.min(file_size.saturating_sub(offset));
        if used == 0 || descriptor.len() < 21 {
            continue;
        }
        if descriptor[20] == 0 {
            report.refresh.mapped_data_bytes += used;
            report.refresh.referenced_data_bytes += used.saturating_mul(links);
            report.refresh.saved_uncompressed_bytes += used.saturating_mul(links.saturating_sub(1));
            continue;
        }
        if descriptor[20] != 1 || descriptor.len() < 53 || descriptor[17..20] != [0, 0, 0] {
            continue;
        }
        let physical = le64(&descriptor, 21).context("short extent")?;
        if physical == 0 {
            continue;
        } // Holes aren't deduplication savings.
        let disk = le64(&descriptor, 29).context("short allocation")?;
        let encoded = descriptor[16] != 0;
        let ram = if encoded {
            le64(&descriptor, 8).context("short decoded extent")?
        } else {
            disk
        };
        let start = le64(&descriptor, 37).context("short extent offset")?;
        if ram == 0 || start.checked_add(used).is_none_or(|end| end > ram) {
            continue;
        }
        let key = (row.get::<_, Vec<u8>>(0)?, physical);
        if group.key.as_ref() != Some(&key) {
            group.finish(&mut report.refresh);
            group.key = Some(key);
        }
        group.add(disk, ram, encoded, start, used, links);
        report.refresh.mapped_data_bytes += used;
        if last_report.elapsed() >= Duration::from_secs(2) {
            ensure!(
                crate::service::free_bytes(directory)? >= minimum_free_bytes,
                "inventory paused for free space"
            );
            write(directory, report)?;
            last_report = Instant::now();
        }
    }
    group.finish(&mut report.refresh);
    Ok(())
}

use std::os::unix::ffi::OsStringExt;

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn current_estimate_unions_ranges_and_counts_existing_links_once() {
        let mut estimate = Estimate::default();
        let mut group = Group::default();
        group.add(4096, 4096, false, 0, 4096, 3);
        group.add(4096, 4096, false, 0, 4096, 1);
        group.finish(&mut estimate);
        assert_eq!(estimate.saved_uncompressed_bytes, 12288);
        assert_eq!(estimate.physical_estimate_bytes, 12288);
        let mut fresh = Estimate::default();
        group.add(4096, 4096, false, 0, 4096, 1);
        group.finish(&mut fresh);
        assert_eq!(fresh.saved_uncompressed_bytes, 0); // No accumulated history.
    }
    #[test]
    fn compressed_savings_use_allocation_ratio_and_partial_overlap_union() {
        let mut estimate = Estimate::default();
        let mut group = Group::default();
        group.add(4096, 131072, true, 0, 131072, 2);
        group.finish(&mut estimate);
        assert_eq!(estimate.saved_uncompressed_bytes, 131072);
        assert_eq!(estimate.physical_estimate_bytes, 4096);
        let mut partial = Estimate::default();
        group.add(8192, 8192, false, 0, 4096, 1);
        group.add(8192, 8192, false, 2048, 4096, 1);
        group.finish(&mut partial);
        assert_eq!(partial.saved_uncompressed_bytes, 2048);
    }
}

#[cfg(test)]
mod snapshot_tests {
    use super::*;
    #[test]
    fn readonly_snapshot_includes_preexisting_hardlinks_and_reconciles_deletions() {
        let directory = tempfile::tempdir().unwrap();
        let a = directory.path().join("a");
        let b = directory.path().join("b");
        fs::write(&a, vec![42; 4096]).unwrap();
        fs::write(&b, vec![42; 4096]).unwrap();
        fs::hard_link(&a, directory.path().join("alias")).unwrap();
        let connection = Connection::open(directory.path().join("filesystem.sqlite")).unwrap();
        connection.execute_batch("PRAGMA journal_mode=WAL;
            CREATE TABLE files(id INTEGER PRIMARY KEY,path BLOB,fingerprint BLOB,fskey BLOB);
            CREATE VIEW file_paths AS SELECT * FROM files;
            CREATE TABLE extent_state(file_id INTEGER);
            CREATE TABLE btrfs_files(file_id INTEGER,crawl_id INTEGER,ino INTEGER);
            CREATE TABLE discovery(crawl_id INTEGER,ino INTEGER);
            CREATE TABLE apply_mappings(file_id INTEGER);
            CREATE TABLE file_extents(file_id INTEGER,offset INTEGER,length INTEGER,descriptor BLOB);").unwrap();
        let mut descriptor = vec![0; 53];
        descriptor[20] = 1;
        descriptor[21..29].copy_from_slice(&65536u64.to_le_bytes());
        descriptor[29..37].copy_from_slice(&4096u64.to_le_bytes());
        descriptor[45..53].copy_from_slice(&4096u64.to_le_bytes());
        for (id, path) in [(1, &a), (2, &b)] {
            use std::os::unix::ffi::OsStrExt;
            connection
                .execute(
                    "INSERT INTO files VALUES (?1,?2,?3,?4)",
                    rusqlite::params![
                        id,
                        path.as_os_str().as_bytes(),
                        fingerprint(&fs::metadata(path).unwrap()),
                        b"btrfs:fixture".as_slice()
                    ],
                )
                .unwrap();
            connection
                .execute("INSERT INTO extent_state VALUES (?1)", [id])
                .unwrap();
            connection
                .execute("INSERT INTO btrfs_files VALUES (?1,1,?1)", [id])
                .unwrap();
            connection
                .execute(
                    "INSERT INTO file_extents VALUES (?1,0,4096,?2)",
                    rusqlite::params![id, &descriptor],
                )
                .unwrap();
        }
        let stop = AtomicBool::new(false);
        let interrupt = Mutex::new(None);
        let run = || {
            let mut report = Report {
                refresh: Estimate {
                    include_hardlinks: true,
                    ..Default::default()
                },
                ..Default::default()
            };
            refresh(directory.path(), &stop, &interrupt, &mut report, 0).unwrap();
            report.refresh
        };
        let first = run();
        assert_eq!(first.verified_files, 2);
        assert_eq!(first.saved_uncompressed_bytes, 8192);
        assert_eq!(run().saved_uncompressed_bytes, 8192); // Not an additive history.
        fs::remove_file(&b).unwrap();
        let after = run();
        assert_eq!(after.verified_files, 1);
        assert_eq!(after.saved_uncompressed_bytes, 4096);
    }
}

#[cfg(test)]
mod checkpoint_tests {
    use super::*;
    #[test]
    fn materialized_inventory_survives_source_changes_without_pinning_wal() {
        let directory = tempfile::tempdir().unwrap();
        let db = directory.path().join("index.sqlite");
        let writer = Connection::open(&db).unwrap();
        writer.execute_batch("PRAGMA journal_mode=WAL;
          CREATE TABLE files(id INTEGER PRIMARY KEY,path BLOB,fingerprint BLOB,fskey BLOB);
          CREATE VIEW file_paths AS SELECT * FROM files;
          CREATE TABLE extent_state(file_id INTEGER);
          CREATE TABLE btrfs_files(file_id INTEGER,crawl_id INTEGER,ino INTEGER);
          CREATE TABLE discovery(crawl_id INTEGER,ino INTEGER);
          CREATE TABLE file_extents(file_id INTEGER,offset INTEGER,length INTEGER,descriptor BLOB);
          INSERT INTO files VALUES(1,x'2f61',x'01',x'02'); INSERT INTO extent_state VALUES(1);
          INSERT INTO btrfs_files VALUES(1,1,1); INSERT INTO file_extents VALUES(1,0,4096,x'0304');").unwrap();
        let reader = Connection::open_with_flags(&db, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap();
        reader
            .execute_batch("PRAGMA temp_store=FILE; BEGIN;")
            .unwrap();
        capture(&reader).unwrap();
        writer
            .execute_batch("UPDATE files SET fingerprint=x'ff'; DELETE FROM file_extents;")
            .unwrap();
        let busy: i64 = writer
            .query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            busy, 0,
            "inventory must release the main WAL before slow validation/sorting"
        );
        assert_eq!(
            reader
                .query_row(
                    "SELECT hex(fingerprint) FROM temp.inventory_files",
                    [],
                    |r| r.get::<_, String>(0)
                )
                .unwrap(),
            "01"
        );
        assert_eq!(
            reader
                .query_row(
                    "SELECT hex(descriptor) FROM temp.inventory_extents",
                    [],
                    |r| r.get::<_, String>(0)
                )
                .unwrap(),
            "0304"
        );
    }
}
