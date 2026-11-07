//! Incremental current sharing, invalidated by the generation-driven Btrfs index.
//! Only changed files are verified; only affected allocation groups are reduced.
use anyhow::{ensure, Context, Result};
use dedupit::inventory_state::SqlCache;
use rusqlite::{Connection, OpenFlags, OptionalExtension};
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
    pub incremental: bool,
    pub bootstrap_complete: bool,
    pub changed_files: u64,
    pub changed_file_bytes: u64,
    pub pending_files: u64,
    pub pending_groups: u64,
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
                // The scanner may already be idle when the last inventory
                // reader releases its snapshot. Do not wait an hour for its
                // next write to reclaim the WAL's retained allocation.
                let _ = checkpoint(&directory);
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
                // Poll the durable change queue; idle polls read only small inventory tables.
                let wait = if report.error.is_some()
                    || report.refresh.indexed_files == 0
                    || report.refresh.verified_files == 0
                {
                    10
                } else {
                    30
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

fn checkpoint(directory: &Path) -> Result<()> {
    let connection = Connection::open_with_flags(
        directory.join("filesystem.sqlite"),
        OpenFlags::SQLITE_OPEN_READ_WRITE,
    )?;
    // A live writer/reader is allowed to defer this attempt. SQLite's locks
    // coordinate checkpoints; no scan rows or durable progress are modified.
    connection.busy_timeout(Duration::ZERO)?;
    connection.execute_batch("PRAGMA journal_size_limit=67108864;")?;
    let _: i64 = connection.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| r.get(0))?;
    Ok(())
}

// No filesystem IO is performed while holding a main-database read snapshot
// or writer transaction. Revision checks reject captures invalidated meanwhile.
struct Candidate {
    id: i64,
    revision: i64,
    fingerprint: Vec<u8>,
    metadata: Option<fs::Metadata>,
}

fn file_batch(connection: &mut Connection, hardlinks: bool, stop: &AtomicBool) -> Result<usize> {
    file_batch_with(connection, hardlinks, stop, || {})
}
fn file_batch_with(
    connection: &mut Connection,
    hardlinks: bool,
    stop: &AtomicBool,
    after_stat: impl FnOnce(),
) -> Result<usize> {
    let (cursor, ceiling): (i64, i64) = connection.query_row_cached(
        "SELECT cursor,ceiling FROM inventory_progress WHERE id=1",
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    let tx = connection.transaction()?;
    let bootstrap = tx
        .prepare_cached("SELECT id FROM files WHERE id>?1 AND id<=?2 ORDER BY id LIMIT 512")?
        .query_map([cursor, ceiling], |r| r.get::<_, i64>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let dirty = tx
        .prepare_cached("SELECT file_id FROM inventory_dirty WHERE retry_after<=unixepoch() ORDER BY retry_after,file_id LIMIT 256")?
        .query_map([], |r| r.get::<_, i64>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut ids = bootstrap.clone();
    ids.extend(dirty);
    ids.sort_unstable();
    ids.dedup();
    let mut captured = Vec::new();
    for id in ids {
        let revision = tx.query_row_cached(
            "SELECT coalesce((SELECT revision FROM inventory_dirty WHERE file_id=?1),0)",
            [id],
            |r| r.get(0),
        )?;
        let value = tx.query_row_cached("SELECT f.path,f.fingerprint FROM file_paths f JOIN extent_state e ON e.file_id=f.id JOIN btrfs_files b ON b.file_id=f.id
            WHERE f.id=?1 AND NOT EXISTS(SELECT 1 FROM discovery d WHERE d.crawl_id=b.crawl_id AND d.ino=b.ino)", [id],
            |r| Ok((r.get::<_,Vec<u8>>(0)?,r.get::<_,Vec<u8>>(1)?))).optional()?;
        captured.push((id, revision, value));
    }
    tx.commit()?;
    let mut candidates = Vec::new();
    for (id, revision, value) in captured {
        ensure!(!stop.load(Ordering::Relaxed), "inventory stopped");
        let (stored, metadata) = match value {
            Some((path, stored)) => {
                let metadata =
                    fs::symlink_metadata(PathBuf::from(std::ffi::OsString::from_vec(path)))
                        .ok()
                        .filter(|m| m.is_file() && fingerprint(m) == stored);
                (stored, metadata)
            }
            None => (Vec::new(), None),
        };
        candidates.push(Candidate {
            id,
            revision,
            fingerprint: stored,
            metadata,
        });
    }
    after_stat();
    if candidates.is_empty() && cursor >= ceiling {
        return Ok(0);
    }
    let tx = connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    let mut checked_bytes = 0u64;
    for candidate in &candidates {
        let id = candidate.id;
        let revision: i64 = tx.query_row_cached(
            "SELECT coalesce((SELECT revision FROM inventory_dirty WHERE file_id=?1),0)",
            [id],
            |r| r.get(0),
        )?;
        if revision != candidate.revision {
            continue;
        }
        // Failed or racing metadata checks are retried without revisiting clean files.
        // Recheck eligibility in the same transaction that acknowledges it.
        tx.execute_cached("DELETE FROM inventory_verified WHERE file_id=?1", [id])?;
        if let Some(metadata) = &candidate.metadata {
            let current: bool = tx.query_row_cached("SELECT EXISTS(SELECT 1 FROM files f JOIN extent_state e ON e.file_id=f.id JOIN btrfs_files b ON b.file_id=f.id
                WHERE f.id=?1 AND f.fingerprint=?2 AND NOT EXISTS(SELECT 1 FROM discovery d WHERE d.crawl_id=b.crawl_id AND d.ino=b.ino))",
                rusqlite::params![id,candidate.fingerprint], |r| r.get(0))?;
            if !current {
                continue;
            }
            let links = if hardlinks {
                metadata.nlink().max(1)
            } else {
                1
            };
            let inline: u64 = tx.query_row_cached("SELECT coalesce(sum(min(length,max(0,?2-offset))),0) FROM file_extents WHERE file_id=?1 AND substr(descriptor,21,1)=x'00'",
                rusqlite::params![id,metadata.len()], |r| r.get(0))?;
            tx.execute_cached(
                "INSERT INTO inventory_verified VALUES(?1,?2,?3,?4,?5,?6)",
                rusqlite::params![
                    id,
                    metadata.len(),
                    links,
                    inline,
                    inline.saturating_mul(links),
                    inline.saturating_mul(links - 1)
                ],
            )?;
            tx.execute_cached("INSERT OR IGNORE INTO inventory_dirty_groups SELECT f.fskey,substr(e.descriptor,22,8) FROM file_extents e JOIN files f ON f.id=e.file_id WHERE e.file_id=?1 AND substr(e.descriptor,21,1)=x'01'", [id])?;
            checked_bytes += metadata.len();
        }
        if candidate.metadata.is_none() && !candidate.fingerprint.is_empty() {
            tx.execute_cached("INSERT INTO inventory_dirty(file_id,revision,retry_after) VALUES(?1,1,unixepoch()+30) ON CONFLICT(file_id) DO UPDATE SET retry_after=unixepoch()+30",[id])?;
            continue;
        }
        tx.execute_cached(
            "DELETE FROM inventory_dirty WHERE file_id=?1 AND revision=?2",
            rusqlite::params![id, revision],
        )?;
    }
    let next = bootstrap.last().copied().unwrap_or(ceiling);
    tx.execute_cached("UPDATE inventory_progress SET cursor=?1,checked=checked+?2,checked_bytes=checked_bytes+?3 WHERE id=1",
        rusqlite::params![next,candidates.len() as u64,checked_bytes])?;
    tx.commit()?;
    Ok(candidates.len())
}

fn group_batch(connection: &mut Connection, stop: &AtomicBool) -> Result<usize> {
    let keys = connection
        .prepare_cached("SELECT fskey,physical FROM inventory_dirty_groups LIMIT 512")?
        .query_map([], |r| {
            Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, Vec<u8>>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if keys.is_empty() {
        return Ok(0);
    }
    let tx = connection.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    for (fskey, physical) in &keys {
        ensure!(!stop.load(Ordering::Relaxed), "inventory stopped");
        let mut statement = tx.prepare_cached("SELECT e.offset,e.length,e.descriptor,v.size,v.links
          FROM file_extents e INDEXED BY inventory_physical JOIN inventory_verified v ON v.file_id=e.file_id JOIN files f ON f.id=e.file_id
          WHERE substr(e.descriptor,21,1)=x'01' AND substr(e.descriptor,22,8)=?1 AND f.fskey=?2")?;
        let mut rows = statement.query(rusqlite::params![physical, fskey])?;
        let mut group = Group::default();
        let mut estimate = Estimate::default();
        while let Some(row) = rows.next()? {
            let descriptor: Vec<u8> = row.get(2)?;
            let offset: u64 = row.get(0)?;
            let length: u64 = row.get(1)?;
            let size: u64 = row.get(3)?;
            let links: u64 = row.get(4)?;
            let used = length.min(size.saturating_sub(offset));
            if used == 0
                || descriptor.len() < 53
                || descriptor[17..20] != [0, 0, 0]
                || le64(&descriptor, 21) == Some(0)
            {
                continue;
            }
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
            group.add(disk, ram, encoded, start, used, links);
            estimate.mapped_data_bytes += used;
        }
        group.finish(&mut estimate);
        // DELETE + INSERT deliberately invokes the totals delta triggers.
        tx.execute_cached(
            "DELETE FROM inventory_groups WHERE fskey=?1 AND physical=?2",
            rusqlite::params![fskey, physical],
        )?;
        if estimate.mapped_data_bytes > 0 {
            tx.execute_cached(
                "INSERT INTO inventory_groups VALUES(?1,?2,?3,?4,?5,?6,?7)",
                rusqlite::params![
                    fskey,
                    physical,
                    estimate.mapped_data_bytes,
                    estimate.referenced_data_bytes,
                    estimate.saved_uncompressed_bytes,
                    estimate.physical_estimate_bytes,
                    estimate.physical_covered_bytes
                ],
            )?;
        }
        tx.execute_cached(
            "DELETE FROM inventory_dirty_groups WHERE fskey=?1 AND physical=?2",
            rusqlite::params![fskey, physical],
        )?;
    }
    tx.commit()?;
    Ok(keys.len())
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
    let mut connection = Connection::open_with_flags(
        directory.join("filesystem.sqlite"),
        OpenFlags::SQLITE_OPEN_READ_WRITE,
    )?;
    connection.busy_timeout(Duration::from_secs(2))?;
    connection.set_prepared_statement_cache_capacity(64);
    connection.execute_batch("PRAGMA foreign_keys=ON; PRAGMA cache_size=-8192;")?;
    *interrupt.lock().unwrap() = Some(connection.get_interrupt_handle());
    dedupit::inventory_state::initialize(&connection)?;
    let hardlinks = report.refresh.include_hardlinks;
    let stored: Option<bool> = connection.query_row_cached(
        "SELECT hardlinks FROM inventory_progress WHERE id=1",
        [],
        |r| r.get(0),
    )?;
    if stored.is_some_and(|value| value != hardlinks) {
        report.estimate = None;
        // Changing scope requires re-verification, never reuse incompatible totals.
        connection.execute_batch("BEGIN IMMEDIATE; DELETE FROM inventory_verified; DELETE FROM inventory_groups; DELETE FROM inventory_dirty_groups;
            UPDATE inventory_progress SET cursor=0,ceiling=(SELECT coalesce(max(id),0) FROM files),checked=0,checked_bytes=0; COMMIT;")?;
    }
    if stored != Some(hardlinks) {
        connection.execute_cached(
            "UPDATE inventory_progress SET hardlinks=?1 WHERE id=1",
            [hardlinks],
        )?;
    }
    let mut last_report = Instant::now() - Duration::from_secs(2);
    let mut worked = false;
    loop {
        ensure!(!stop.load(Ordering::Relaxed), "inventory stopped");
        ensure!(
            crate::service::free_bytes(directory)? >= minimum_free_bytes,
            "inventory paused for free space"
        );
        let groups_backlogged: bool = connection.query_row_cached(
            "SELECT EXISTS(SELECT 1 FROM inventory_dirty_groups LIMIT 1 OFFSET 4096)",
            [],
            |r| r.get(0),
        )?;
        let files = if groups_backlogged {
            0
        } else {
            file_batch(&mut connection, hardlinks, stop)?
        };
        let groups = group_batch(&mut connection, stop)?;
        worked |= files > 0 || groups > 0;
        if last_report.elapsed() >= Duration::from_secs(2) || (files == 0 && groups == 0) {
            let (cursor,ceiling,checked,bytes,indexed,pending):(u64,u64,u64,u64,u64,u64)=connection.query_row_cached(
                "SELECT cursor,ceiling,checked,checked_bytes,indexed,pending FROM inventory_progress WHERE id=1",[],
                |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?)))?;
            let totals: (u64,u64,u64,u64,u64,u64,u64)=connection.query_row_cached(
                "SELECT files,bytes,mapped,referenced,saved,physical_saved,covered FROM inventory_totals WHERE id=1",[],
                |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?,r.get(5)?,r.get(6)?)))?;
            report.refresh = Estimate {
                as_of: crate::service::now(),
                indexed_files: indexed,
                checked_files: checked,
                verified_files: totals.0,
                checked_file_bytes: totals.1,
                mapped_data_bytes: totals.2,
                referenced_data_bytes: totals.3,
                saved_uncompressed_bytes: totals.4,
                physical_estimate_bytes: totals.5,
                physical_covered_bytes: totals.6,
                pending_jobs: pending,
                include_hardlinks: hardlinks,
                incremental: true,
                bootstrap_complete: cursor >= ceiling,
                changed_files: checked,
                changed_file_bytes: bytes,
                pending_files: connection.query_row_cached(
                    "SELECT count(*) FROM inventory_dirty",
                    [],
                    |r| r.get(0),
                )?,
                pending_groups: connection.query_row_cached(
                    "SELECT count(*) FROM inventory_dirty_groups",
                    [],
                    |r| r.get(0),
                )?,
                ..Default::default()
            };
            report.state = if cursor < ceiling {
                "Bootstrapping incremental inventory"
            } else {
                "Updating changed files and allocations"
            }
            .into();
            // During bootstrap retain the previous full estimate until coverage
            // catches up; progress is durable even when interrupted.
            if cursor >= ceiling {
                report.estimate = Some(report.refresh.clone());
            }
            write(directory, report)?;
            last_report = Instant::now();
        }
        if files == 0 && groups == 0 {
            break;
        }
        // Give the scanner writer a chance between bounded transactions.
        thread::yield_now();
    }
    if !worked {
        report.state = "Complete".into();
    }
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
    fn bootstrap_resumes_and_concurrent_changes_cannot_acknowledge_stale_captures() {
        use std::os::unix::ffi::OsStrExt;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("data");
        fs::write(&path, vec![1; 4096]).unwrap();
        let db = directory.path().join("filesystem.sqlite");
        let writer = Connection::open(&db).unwrap();
        writer.execute_batch("PRAGMA journal_mode=WAL;
          CREATE TABLE files(id INTEGER PRIMARY KEY,path BLOB,fingerprint BLOB,fskey BLOB);
          CREATE VIEW file_paths AS SELECT * FROM files;
          CREATE TABLE extent_state(file_id INTEGER PRIMARY KEY);
          CREATE TABLE btrfs_files(file_id INTEGER PRIMARY KEY,crawl_id INTEGER,ino INTEGER);
          CREATE TABLE discovery(crawl_id INTEGER,ino INTEGER,PRIMARY KEY(crawl_id,ino));
          CREATE TABLE file_extents(file_id INTEGER,offset INTEGER,length INTEGER,descriptor BLOB,PRIMARY KEY(file_id,offset));
          CREATE TABLE crawls(id INTEGER PRIMARY KEY,completed INTEGER,low INTEGER,high INTEGER,cursor BLOB);
          INSERT INTO crawls VALUES(1,42,43,100,x'1234');").unwrap();
        let stored = fingerprint(&fs::metadata(&path).unwrap());
        let mut descriptor = vec![0; 53];
        descriptor[20] = 1;
        descriptor[21..29].copy_from_slice(&65536u64.to_le_bytes());
        descriptor[29..37].copy_from_slice(&4096u64.to_le_bytes());
        for id in 1..=1300 {
            writer
                .execute(
                    "INSERT INTO files VALUES(?1,?2,?3,x'01')",
                    rusqlite::params![id, path.as_os_str().as_bytes(), stored],
                )
                .unwrap();
            writer
                .execute("INSERT INTO extent_state VALUES(?1)", [id])
                .unwrap();
            writer
                .execute("INSERT INTO btrfs_files VALUES(?1,1,?1)", [id])
                .unwrap();
            writer
                .execute(
                    "INSERT INTO file_extents VALUES(?1,0,4096,?2)",
                    rusqlite::params![id, descriptor],
                )
                .unwrap();
        }
        dedupit::inventory_state::initialize(&writer).unwrap();
        let stop = AtomicBool::new(false);
        let mut worker = Connection::open(&db).unwrap();
        assert_eq!(file_batch(&mut worker, false, &stop).unwrap(), 512);
        assert_eq!(
            worker
                .query_row("SELECT cursor FROM inventory_progress", [], |r| r
                    .get::<_, u64>(0))
                .unwrap(),
            512
        );
        drop(worker);
        let mut worker = Connection::open(&db).unwrap();
        dedupit::inventory_state::initialize(&worker).unwrap();
        assert_eq!(file_batch(&mut worker, false, &stop).unwrap(), 512);
        assert_eq!(
            worker
                .query_row("SELECT cursor FROM inventory_progress", [], |r| r
                    .get::<_, u64>(0))
                .unwrap(),
            1024
        );
        // Invalidate a captured verified file after stat but before writer lock.
        writer
            .execute(
                "INSERT INTO inventory_dirty(file_id,revision) VALUES(1,1)",
                [],
            )
            .unwrap();
        file_batch_with(&mut worker, false, &stop, || {
            writer
                .execute("UPDATE files SET fingerprint=x'ff' WHERE id=1", [])
                .unwrap();
        })
        .unwrap();
        assert!(!worker
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM inventory_verified WHERE file_id=1)",
                [],
                |r| r.get::<_, bool>(0)
            )
            .unwrap());
        assert!(worker
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM inventory_dirty WHERE file_id=1)",
                [],
                |r| r.get::<_, bool>(0)
            )
            .unwrap());
        let mut report = Report::default();
        refresh(directory.path(), &stop, &Mutex::new(None), &mut report, 0).unwrap();
        assert_eq!(report.refresh.verified_files, 1299);
        assert_eq!(report.refresh.saved_uncompressed_bytes, 1298 * 4096);
        assert!(report.refresh.bootstrap_complete);
        let before = worker.total_changes();
        assert_eq!(file_batch(&mut worker, false, &stop).unwrap(), 0);
        assert_eq!(group_batch(&mut worker, &stop).unwrap(), 0);
        assert_eq!(
            before,
            worker.total_changes(),
            "idle inventory must not write SQLite"
        );
        assert_eq!(
            writer
                .query_row(
                    "SELECT completed,low,high,hex(cursor) FROM crawls",
                    [],
                    |r| Ok((
                        r.get::<_, u64>(0)?,
                        r.get::<_, u64>(1)?,
                        r.get::<_, u64>(2)?,
                        r.get::<_, String>(3)?
                    ))
                )
                .unwrap(),
            (42, 43, 100, "1234".into())
        );
        // A completed source deletion invalidates savings and preserves the queue.
        writer.execute("DELETE FROM files WHERE id=2", []).unwrap();
        assert!(writer
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM inventory_dirty_groups)",
                [],
                |r| r.get::<_, bool>(0)
            )
            .unwrap());
    }
    #[test]
    fn incremental_inventory_reconciles_changes_and_resumes_without_rechecking_clean_files() {
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
        let unchanged = run();
        assert_eq!(unchanged.saved_uncompressed_bytes, 8192);
        assert_eq!(unchanged.changed_files, first.changed_files); // No stats or index walk.
        assert_eq!(unchanged.changed_file_bytes, first.changed_file_bytes);
        assert_eq!(unchanged.checked_file_bytes, 8192); // Represented bytes, not lifetime IO.
                                                        // A native change event makes the old record ineligible immediately.
        connection
            .execute("INSERT INTO discovery VALUES(1,2)", [])
            .unwrap();
        let pending = run();
        assert_eq!(pending.verified_files, 1);
        assert_eq!(pending.saved_uncompressed_bytes, 4096);
        assert_eq!(pending.pending_jobs, 1);
        connection
            .execute("DELETE FROM discovery WHERE ino=2", [])
            .unwrap();
        assert_eq!(run().saved_uncompressed_bytes, 8192);
        // External reflink/remap: old and new allocation groups must both update.
        let mut moved = descriptor.clone();
        moved[21..29].copy_from_slice(&131072u64.to_le_bytes());
        connection
            .execute(
                "UPDATE file_extents SET descriptor=?1 WHERE file_id=2",
                [&moved],
            )
            .unwrap();
        let remapped = run();
        assert_eq!(remapped.saved_uncompressed_bytes, 4096);
        assert_eq!(remapped.physical_estimate_bytes, 4096);
        assert_eq!(remapped.changed_files, pending.changed_files + 2);
        connection
            .execute(
                "UPDATE file_extents SET descriptor=?1 WHERE file_id=2",
                [&descriptor],
            )
            .unwrap();
        // Stop after acknowledging file work, before reducing its groups. A new
        // worker/connection must recover the durable queue and exact totals.
        let mut worker = Connection::open(directory.path().join("filesystem.sqlite")).unwrap();
        assert_eq!(file_batch(&mut worker, true, &stop).unwrap(), 1);
        assert!(
            worker
                .query_row("SELECT count(*) FROM inventory_dirty_groups", [], |r| r
                    .get::<_, u64>(0))
                .unwrap()
                > 0
        );
        drop(worker);
        assert_eq!(run().saved_uncompressed_bytes, 8192);
        // Hardlink count changes arrive as inode metadata events.
        fs::remove_file(directory.path().join("alias")).unwrap();
        connection
            .execute(
                "UPDATE files SET fingerprint=?1 WHERE id=1",
                [fingerprint(&fs::metadata(&a).unwrap())],
            )
            .unwrap();
        assert_eq!(run().saved_uncompressed_bytes, 4096);
        fs::hard_link(&a, directory.path().join("alias")).unwrap();
        connection
            .execute(
                "UPDATE files SET fingerprint=?1 WHERE id=1",
                [fingerprint(&fs::metadata(&a).unwrap())],
            )
            .unwrap();
        assert_eq!(run().saved_uncompressed_bytes, 8192);
        fs::remove_file(&b).unwrap();
        connection
            .execute("DELETE FROM files WHERE id=2", [])
            .unwrap();
        let after = run();
        assert_eq!(after.verified_files, 1);
        assert_eq!(after.saved_uncompressed_bytes, 4096);
        assert_eq!(after.indexed_files, 1);
        assert_eq!(after.pending_files, 0);
        assert_eq!(after.pending_groups, 0);
        assert!(after.bootstrap_complete);
        // Scope changes rebuild derived records without touching source work.
        let mut report = Report::default();
        refresh(directory.path(), &stop, &interrupt, &mut report, 0).unwrap();
        assert_eq!(report.refresh.saved_uncompressed_bytes, 0);
        assert_eq!(report.refresh.verified_files, 1);
    }
}
