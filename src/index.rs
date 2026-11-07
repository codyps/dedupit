use crate::{backend, scan, Backend, Duplicate, Granularity, Options, Outcome, Range, Stats};
use anyhow::{bail, ensure, Context, Result};
use fs2::FileExt;
use rusqlite::{params, Connection, OptionalExtension, Params, Row};
use std::{
    fs::{self, File, OpenOptions},
    os::unix::{ffi::OsStrExt, fs::OpenOptionsExt},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

pub(crate) struct Index {
    pub observer: Option<std::rc::Rc<dyn crate::RunObserver>>,
    pub conn: Connection,
    pub roots: Vec<PathBuf>,
    pub excluded: Vec<PathBuf>,
    pub buffer: Vec<u8>,
    batch: Batch,
    pub scan_phase: ScanPhase,
    #[cfg(target_os = "linux")]
    pub mapping_sync: std::collections::HashMap<i64, File>,
    #[cfg(target_os = "linux")]
    pub crawl_prefixes: std::collections::HashMap<i64, PathBuf>,
    _lock: File,
    _database: File,
}

pub(crate) enum ScanPhase {
    Complete,
    Portable,
    #[cfg(target_os = "linux")]
    Native,
}

// Only flush_batch commits the outer transaction. Dropping Index on an error
// or panic lets SQLite roll it back; releasing a metadata savepoint never makes
// that operation durable independently of its batch.
struct Batch {
    active: bool,
    phase: &'static str,
    started: Instant,
    operations: usize,
    bytes: u64,
    limit: usize,
    commits: u64,
    poisoned: bool,
}

/// Sharing only starts between committed scan operations. Each pass seeks
/// forward through newly pending ranges, rather than sweeping the index again.
/// The final pass also picks up old IDs requeued by source invalidation.
pub(crate) struct Interleaver<'a> {
    backend: Option<&'a mut dyn Backend>,
    cursors: [i64; 2],
}

impl<'a> Interleaver<'a> {
    pub fn new(backend: Option<&'a mut dyn Backend>) -> Self {
        Self {
            backend,
            cursors: [0; 2],
        }
    }

    pub fn between_batches(&mut self, index: &mut Index, stats: &mut Stats) -> Result<()> {
        index.progress(stats)?;
        let Some(backend) = self.backend.as_deref_mut() else {
            return Ok(());
        };
        if index.batch.active {
            return Ok(());
        }
        // All rows up to this bound belong to complete, committed file hashes.
        // New work gets a later AUTOINCREMENT ID; failed attempts stay retryable
        // next invocation but do not get retried at every sharing boundary.
        let high: i64 =
            index
                .conn
                .query_row_cached("SELECT COALESCE(MAX(id),0) FROM ranges", [], |r| r.get(0))?;
        let mut visited = 0;
        for kind in [1, 0] {
            let started = Instant::now();
            let bytes_before = stats.bytes_deduped + stats.bytes_already_shared;
            for _ in 0..1024 {
                // Chunks of newly indexed files wait until their whole-file
                // job has been considered. A whole-file success consumes them.
                let ceiling = if kind == 1 {
                    high
                } else {
                    high.min(self.cursors[1])
                };
                let target = index
                    .conn
                    .query_row_cached(
                        &format!(
                            "{JOB_SELECT} WHERE r.pending=1 AND r.kind=?1 AND r.id>?2 AND r.id<=?3
                     AND NOT EXISTS(SELECT 1 FROM apply_failed a WHERE a.id=r.id)
                     AND ({ready})
                     AND (?1=1 OR NOT EXISTS(SELECT 1 FROM ranges w WHERE w.file_id=r.file_id
                         AND w.kind=1 AND w.pending=1
                         AND NOT EXISTS(SELECT 1 FROM apply_failed a WHERE a.id=w.id)))
                     ORDER BY r.id LIMIT 1",
                            ready = index.ready()
                        ),
                        params![kind, self.cursors[kind as usize], ceiling],
                        Job::from_row,
                    )
                    .optional()?;
                let Some(target) = target else {
                    self.cursors[kind as usize] = self.cursors[kind as usize].max(ceiling);
                    break;
                };
                self.cursors[kind as usize] = target.id;
                index.begin_batch("apply")?;
                let before = stats.bytes_deduped + stats.bytes_already_shared;
                index.attempt_target(&target, backend, stats)?;
                index.batch_step(
                    (stats.bytes_deduped + stats.bytes_already_shared).saturating_sub(before),
                )?;
                visited += 1;
                if stats.bytes_deduped + stats.bytes_already_shared - bytes_before
                    >= 64 * 1024 * 1024
                    || started.elapsed() >= Duration::from_secs(1)
                {
                    break;
                }
            }
            index.flush_batch()?;
        }
        if visited > 0 {
            stats.interleaved_batches += 1;
            index.test_hook("interleave:shared")?;
        }
        // Extent maps remain provisional. The final apply pass performs the
        // filesystem commit/confirmation barrier once, not once per scan batch.
        Ok(())
    }

    pub fn finish(&mut self, index: &mut Index, stats: &mut Stats) -> Result<()> {
        index.scan_phase = ScanPhase::Complete;
        if let Some(backend) = self.backend.as_deref_mut() {
            index.apply(backend, stats)?;
        }
        Ok(())
    }
}

/// Repeated scan/job statements share a bounded connection-local cache, also
/// through transactions and savepoints. Dropping a cached statement resets it
/// and clears bindings before reuse; these helpers do not alter transaction boundaries.
pub(crate) trait SqlCache {
    fn execute_cached(&self, sql: &str, params: impl Params) -> rusqlite::Result<usize>;
    fn query_row_cached<T>(
        &self,
        sql: &str,
        params: impl Params,
        read: impl FnOnce(&Row<'_>) -> rusqlite::Result<T>,
    ) -> rusqlite::Result<T>;
}

impl SqlCache for Connection {
    fn execute_cached(&self, sql: &str, params: impl Params) -> rusqlite::Result<usize> {
        self.prepare_cached(sql)?.execute(params)
    }

    fn query_row_cached<T>(
        &self,
        sql: &str,
        params: impl Params,
        read: impl FnOnce(&Row<'_>) -> rusqlite::Result<T>,
    ) -> rusqlite::Result<T> {
        self.prepare_cached(sql)?.query_row(params, read)
    }
}

pub(crate) fn path_bytes(path: &Path) -> &[u8] {
    path.as_os_str().as_bytes()
}

pub(crate) fn decode_path(bytes: Vec<u8>) -> PathBuf {
    use std::os::unix::ffi::OsStringExt;
    std::ffi::OsString::from_vec(bytes).into()
}

impl Index {
    pub fn open(options: &Options) -> Result<Self> {
        let mut roots: Vec<_> = options
            .roots
            .iter()
            .map(|root| fs::canonicalize(root).with_context(|| format!("open root {root:?}")))
            .collect::<Result<_>>()?;
        roots.sort();
        roots.dedup();
        // Descendant roots are already covered; avoid walking them twice.
        let mut reduced = Vec::<PathBuf>::new();
        for root in roots {
            if !reduced.iter().any(|parent| root.starts_with(parent)) {
                reduced.push(root);
            }
        }
        let parent = options
            .database
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let parent = fs::canonicalize(parent).context("index directory must exist")?;
        let database = parent.join(
            options
                .database
                .file_name()
                .context("index needs a filename")?,
        );
        let mut excluded = vec![database.clone()];
        for suffix in ["-wal", "-shm", "-journal", ".dedupit-lock"] {
            let mut name = database.as_os_str().to_os_string();
            name.push(suffix);
            excluded.push(PathBuf::from(name));
        }
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(excluded.last().unwrap())?;
        ensure!(
            lock.metadata()?.is_file(),
            "index lock is not a regular file"
        );
        FileExt::try_lock_exclusive(&lock)
            .context("index is already in use (or locking is unsupported)")?;
        // Create privately before SQLite opens it, and refuse symlinks/devices.
        let db_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(&database)?;
        ensure!(db_file.metadata()?.is_file(), "index is not a regular file");
        // Also lock the database inode: two different hardlinked names must not
        // bypass the single-writer guard by acquiring different sidecar locks.
        FileExt::try_lock_exclusive(&db_file).context("database inode is already in use")?;
        let conn = Connection::open(&database)?;
        // Enough for the scan/apply working set without growing with file count.
        conn.set_prepared_statement_cache_capacity(64);
        conn.execute_batch(
            "PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;",
        )?;
        let version: i64 = conn.query_row("PRAGMA user_version", [], |r| r.get(0))?;
        ensure!(
            (0..=4).contains(&version),
            "unsupported index schema version {version}"
        );
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value BLOB NOT NULL);
             CREATE TABLE IF NOT EXISTS files (
               id INTEGER PRIMARY KEY AUTOINCREMENT, path BLOB NOT NULL UNIQUE,
               dev INTEGER NOT NULL, ino INTEGER NOT NULL, fingerprint BLOB NOT NULL,
               fskey BLOB NOT NULL, alignment INTEGER NOT NULL,
               UNIQUE(dev, ino));
             CREATE TABLE IF NOT EXISTS file_policies (
               file_id INTEGER PRIMARY KEY REFERENCES files(id) ON DELETE CASCADE,
               policy INTEGER NOT NULL);
             CREATE TABLE IF NOT EXISTS ranges (
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               file_id INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
               kind INTEGER NOT NULL, offset INTEGER NOT NULL, length INTEGER NOT NULL,
               hash BLOB NOT NULL, pending INTEGER NOT NULL DEFAULT 1,
               UNIQUE(file_id, kind, offset));
             CREATE INDEX IF NOT EXISTS matches ON ranges(kind, hash, length, id);
             CREATE INDEX IF NOT EXISTS pending ON ranges(kind, id) WHERE pending=1;
             CREATE TABLE IF NOT EXISTS completed (
               target INTEGER PRIMARY KEY REFERENCES ranges(id) ON DELETE CASCADE,
               source INTEGER NOT NULL REFERENCES ranges(id) ON DELETE CASCADE);
             CREATE INDEX IF NOT EXISTS completed_source ON completed(source);
             CREATE TRIGGER IF NOT EXISTS invalidate_source BEFORE DELETE ON ranges BEGIN
               UPDATE ranges SET pending=1 WHERE id IN
                 (SELECT target FROM completed WHERE source=OLD.id);
             END;
             CREATE TRIGGER IF NOT EXISTS invalidate_summary_insert AFTER INSERT ON ranges BEGIN
               DELETE FROM config WHERE key='summary';
             END;
             CREATE TRIGGER IF NOT EXISTS invalidate_summary_delete AFTER DELETE ON ranges BEGIN
               DELETE FROM config WHERE key='summary';
             END;
             CREATE TRIGGER IF NOT EXISTS invalidate_summary_update AFTER UPDATE OF hash,length,kind ON ranges BEGIN
               DELETE FROM config WHERE key='summary';
             END;
             CREATE TEMP TABLE seen (file_id INTEGER PRIMARY KEY);
             CREATE TEMP TABLE apply_failed (id INTEGER PRIMARY KEY);
             CREATE TEMP TABLE apply_crawls (id INTEGER PRIMARY KEY);
             CREATE TEMP TABLE class_sources (
               kind INTEGER, hash BLOB, length INTEGER, fskey BLOB, policy INTEGER, source INTEGER,
               PRIMARY KEY(kind,hash,length,fskey,policy));",
        )?;
        if version < 2 {
            conn.execute_batch(
                "BEGIN IMMEDIATE;
              CREATE TABLE chunk_cache (
                file_id INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
                offset INTEGER NOT NULL, length INTEGER NOT NULL, hash BLOB NOT NULL,
                nonzero INTEGER NOT NULL, PRIMARY KEY(file_id,offset));
              CREATE TABLE extent_state (
                file_id INTEGER PRIMARY KEY REFERENCES files(id) ON DELETE CASCADE,
                incarnation BLOB NOT NULL);
              CREATE TABLE file_extents (
                file_id INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
                offset INTEGER NOT NULL, length INTEGER NOT NULL, descriptor BLOB NOT NULL,
                PRIMARY KEY(file_id,offset));
              CREATE TABLE crawls (
                id INTEGER PRIMARY KEY, identity BLOB NOT NULL UNIQUE, path BLOB NOT NULL,
                completed INTEGER NOT NULL DEFAULT 0, low INTEGER, high INTEGER,
                cursor BLOB, enumerated INTEGER NOT NULL DEFAULT 0,
                initialized INTEGER NOT NULL DEFAULT 0);
              CREATE TABLE discovery (
                crawl_id INTEGER NOT NULL REFERENCES crawls(id) ON DELETE CASCADE,
                ino INTEGER NOT NULL, directory INTEGER NOT NULL,
                PRIMARY KEY(crawl_id,ino));
              CREATE TABLE btrfs_files (
                file_id INTEGER PRIMARY KEY REFERENCES files(id) ON DELETE CASCADE,
                crawl_id INTEGER NOT NULL REFERENCES crawls(id) ON DELETE CASCADE,
                ino INTEGER NOT NULL, parent BLOB NOT NULL,
                UNIQUE(crawl_id,ino));
              CREATE INDEX btrfs_file_parent ON btrfs_files(crawl_id,parent);
              CREATE TABLE btrfs_dirs (
                crawl_id INTEGER NOT NULL REFERENCES crawls(id) ON DELETE CASCADE,
                ino INTEGER NOT NULL, path BLOB NOT NULL, parent BLOB NOT NULL,
                PRIMARY KEY(crawl_id,ino));
              CREATE INDEX btrfs_dir_parent ON btrfs_dirs(crawl_id,parent);
              CREATE TRIGGER invalidate_extent_baseline AFTER DELETE ON chunk_cache BEGIN
                DELETE FROM extent_state WHERE file_id=OLD.file_id;
              END;
              PRAGMA user_version=2;
              COMMIT;",
            )?;
        }
        // Configuration changes require a fresh index, never silent stale reuse.
        let config = serde_json::to_vec(&(
            options.chunk_size,
            match options.granularity {
                Granularity::File => 1,
                Granularity::Chunk => 2,
                Granularity::Both => 3,
            },
            reduced.iter().map(|p| path_bytes(p)).collect::<Vec<_>>(),
            "blake3-chunk-tree-v1",
        ))?;
        let existing: Option<Vec<u8>> = conn
            .query_row("SELECT value FROM config WHERE key='scan'", [], |r| {
                r.get(0)
            })
            .optional()?;
        if let Some(existing) = existing {
            ensure!(
                config == existing,
                "roots, chunk size, or granularity differ from this index; use a new database"
            );
        } else {
            conn.execute("INSERT INTO config VALUES ('scan', ?1)", [&config])?;
        }
        if version < 3 {
            let tx = conn.unchecked_transaction()?;
            let lengths = tx
                .prepare("SELECT DISTINCT length FROM ranges WHERE kind=0 ORDER BY length")?
                .query_map([], |r| r.get::<_, usize>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            // Migrate cached v1/v2 ranges without reading files. Incrementally
            // hash prefixes of one zero buffer, so even many distinct EOF tail
            // lengths require hashing at most one chunk's worth of zero bytes.
            let maximum = lengths.last().copied().unwrap_or(0);
            ensure!(
                maximum <= options.chunk_size && maximum <= 16 * 1024 * 1024,
                "invalid cached chunk length"
            );
            let zeros = vec![0; maximum];
            let mut hasher = blake3::Hasher::new();
            let mut previous = 0;
            for length in lengths {
                hasher.update(&zeros[previous..length]);
                let hash = hasher.finalize();
                tx.execute(
                    "DELETE FROM ranges WHERE kind=0 AND length=?1 AND hash=?2",
                    params![length, hash.as_bytes().as_slice()],
                )?;
                previous = length;
            }
            tx.pragma_update(None, "user_version", 3)?;
            tx.commit()?;
        }
        // Directory reconciliation precedes file jobs. Without this index,
        // finding each directory can walk all earlier queued regular files.
        conn.execute_batch(
            "CREATE INDEX IF NOT EXISTS discovery_kind ON discovery(crawl_id,directory,ino);",
        )?;
        if version < 4 {
            conn.execute_batch(
                "BEGIN IMMEDIATE;
                CREATE TABLE IF NOT EXISTS apply_mappings (
                    file_id INTEGER PRIMARY KEY REFERENCES files(id) ON DELETE CASCADE);
                PRAGMA user_version=4; COMMIT;",
            )?;
        }
        // A process or machine crash may have preserved SQLite's provisional
        // maps but lost btrfs's transaction. Revisit them even if that rollback
        // leaves no new inode event. Never authorize skipping with these rows.
        conn.execute_cached("INSERT OR IGNORE INTO discovery
            SELECT b.crawl_id,b.ino,0 FROM apply_mappings a JOIN btrfs_files b ON b.file_id=a.file_id", [])?;
        #[cfg(feature = "test-failpoints")]
        if std::env::var_os("DEDUPIT_TEST_SMALL_CACHE").is_some() {
            // Force uncommitted WAL spill in crash tests, not just lost in-memory rows.
            conn.pragma_update(None, "cache_size", 8)?;
        }
        Ok(Self {
            observer: None,
            conn,
            roots: reduced,
            excluded,
            buffer: Vec::new(),
            scan_phase: ScanPhase::Complete,
            #[cfg(target_os = "linux")]
            mapping_sync: std::collections::HashMap::new(),
            #[cfg(target_os = "linux")]
            crawl_prefixes: std::collections::HashMap::new(),
            batch: Batch {
                active: false,
                phase: "",
                started: Instant::now(),
                operations: 0,
                bytes: 0,
                limit: options.commit_batch_size,
                commits: 0,
                poisoned: false,
            },
            _lock: lock,
            _database: db_file,
        })
    }

    pub fn checkpoint(&self) -> Result<()> {
        if let Some(observer) = &self.observer {
            observer.checkpoint()?;
        }
        Ok(())
    }

    pub fn progress(&self, stats: &Stats) -> Result<()> {
        if let Some(observer) = &self.observer {
            observer.progress(stats)?;
        }
        Ok(())
    }

    pub fn begin_batch(&mut self, phase: &'static str) -> Result<()> {
        self.check_batch()?;
        if self.batch.active {
            ensure!(
                !self.conn.is_autocommit(),
                "database batch was unexpectedly rolled back"
            );
            ensure!(
                self.batch.phase == phase,
                "database batch crossed a phase boundary"
            );
        } else {
            ensure!(
                self.conn.is_autocommit(),
                "unexpected open database transaction"
            );
            self.conn.execute_batch("BEGIN IMMEDIATE")?;
            self.batch.active = true;
            self.batch.phase = phase;
            self.batch.started = Instant::now();
            self.batch.operations = 0;
            self.batch.bytes = 0;
        }
        Ok(())
    }

    pub fn batch_step(&mut self, bytes: u64) -> Result<()> {
        self.check_batch()?;
        ensure!(
            self.batch.active && !self.conn.is_autocommit(),
            "database batch was unexpectedly rolled back"
        );
        self.batch.operations += 1;
        self.batch.bytes = self.batch.bytes.saturating_add(bytes);
        #[cfg(feature = "test-failpoints")]
        self.test_hook(&format!("{}:staged", self.batch.phase))?;
        // Check only between atomic operations: a single large file, metadata
        // page or ioctl job can exceed the byte/time budgets on its own.
        if self.batch.operations >= self.batch.limit
            || self.batch.bytes >= 64 * 1024 * 1024
            || self.batch.started.elapsed() >= Duration::from_secs(1)
        {
            self.flush_batch()?;
        }
        Ok(())
    }

    pub fn flush_batch(&mut self) -> Result<()> {
        self.check_batch()?;
        if self.batch.active {
            #[cfg(feature = "test-failpoints")]
            self.test_hook(&format!("{}:before-commit", self.batch.phase))?;
            self.conn.execute_batch("COMMIT")?;
            self.batch.active = false;
            self.batch.commits += 1;
            #[cfg(feature = "test-failpoints")]
            self.test_hook(&format!("{}:after-commit", self.batch.phase))?;
        }
        Ok(())
    }

    pub fn batch_commits(&self) -> u64 {
        self.batch.commits
    }

    pub fn check_batch(&self) -> Result<()> {
        self.checkpoint()?;
        ensure!(
            !self.batch.poisoned,
            "file cleanup failed; database batch must be rolled back on restart"
        );
        ensure!(
            !self.batch.active || !self.conn.is_autocommit(),
            "database batch was unexpectedly rolled back"
        );
        Ok(())
    }

    /// On a failed file, invalidate both its old and partially written records
    /// before any later batch commit. A cleanup failure is fatal: never allow
    /// a caller collecting per-file errors to commit the incomplete file.
    pub fn discard_file(&mut self, path: &Path) -> Result<()> {
        self.check_batch()?;
        if let Err(error) = self
            .conn
            .execute_cached("DELETE FROM files WHERE path=?1", [path_bytes(path)])
        {
            self.batch.poisoned = true;
            return Err(error.into());
        }
        Ok(())
    }

    pub fn test_hook(&self, event: &str) -> Result<()> {
        #[cfg(feature = "test-failpoints")]
        crate::failpoints::hit(&self.conn, event)?;
        #[cfg(not(feature = "test-failpoints"))]
        let _ = event;
        Ok(())
    }

    pub fn summarize(&self, stats: &mut Stats) -> Result<()> {
        let cached: Option<Vec<u8>> = self
            .conn
            .query_row_cached("SELECT value FROM config WHERE key='summary'", [], |r| {
                r.get(0)
            })
            .optional()?;
        if let Some(cached) = cached {
            let [files, file_bytes, chunks, chunk_bytes]: [u64; 4] =
                serde_json::from_slice(&cached)?;
            stats.duplicate_files = files;
            stats.duplicate_file_bytes = file_bytes;
            stats.duplicate_chunks = chunks;
            stats.duplicate_chunk_bytes = chunk_bytes;
            return Ok(());
        }
        for kind in [0, 1] {
            let (count, bytes): (u64, u64) = self.conn.query_row_cached(
                "SELECT COALESCE(SUM(n-1),0), COALESCE(SUM((n-1)*length),0)
                 FROM (SELECT COUNT(*) n, length FROM ranges WHERE kind=?1
                       GROUP BY hash,length HAVING COUNT(*)>1)",
                [kind],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            if kind == 0 {
                stats.duplicate_chunks = count;
                stats.duplicate_chunk_bytes = bytes;
            } else {
                stats.duplicate_files = count;
                stats.duplicate_file_bytes = bytes;
            }
        }
        // Triggers invalidate this cache in the same transaction as range
        // changes, including after an interrupted scan or a stale-file removal.
        let cached = serde_json::to_vec(&[
            stats.duplicate_files,
            stats.duplicate_file_bytes,
            stats.duplicate_chunks,
            stats.duplicate_chunk_bytes,
        ])?;
        self.conn.execute_cached(
            "INSERT OR REPLACE INTO config(key,value) VALUES ('summary',?1)",
            [cached],
        )?;
        Ok(())
    }

    pub fn list_duplicates(&self, visit: &mut impl FnMut(Duplicate) -> Result<()>) -> Result<()> {
        // Stream duplicate groups from the covering matches index, then seek
        // each group's ranges in ID order. Joining all groups in one statement
        // made SQLite sort every matching path before yielding any output.
        // Singleton groups never need a file/path lookup.
        let mut groups = self.conn.prepare_cached(DUPLICATE_GROUPS)?;
        let mut candidates = self.conn.prepare_cached(DUPLICATE_RANGES)?;
        let mut groups = groups.query([])?;
        while let Some(group) = groups.next()? {
            let kind: i64 = group.get(0)?;
            let hash: Vec<u8> = group.get(1)?;
            let length: u64 = group.get(2)?;
            let mut rows = candidates.query(params![kind, hash, length])?;
            let Some(first) = rows.next()? else { continue };
            let source = decode_path(first.get(0)?);
            let source_offset = first.get(1)?;
            while let Some(row) = rows.next()? {
                visit(Duplicate {
                    source: source.clone(),
                    source_offset,
                    destination: decode_path(row.get(0)?),
                    destination_offset: row.get(1)?,
                    length,
                    whole_file: kind == 1,
                })?;
            }
        }
        Ok(())
    }

    pub fn apply(&mut self, backend: &mut dyn Backend, stats: &mut Stats) -> Result<()> {
        // Clear unique ranges in one durable operation, rather than committing
        // once per unique chunk. Only changed/new ranges are pending on reruns.
        self.conn.execute_cached(
            "UPDATE ranges AS t SET pending=0 WHERE t.pending=1 AND NOT EXISTS (
               SELECT 1 FROM ranges s JOIN files f ON f.id=s.file_id
               WHERE s.kind=t.kind AND s.hash=t.hash AND s.length=t.length AND s.id<t.id
                 AND f.fskey=(SELECT fskey FROM files WHERE id=t.file_id))",
            [],
        )?;
        // Whole files first. A successful whole-file job consumes chunk jobs for
        // that destination so the same data isn't processed twice.
        for kind in [1, 0] {
            let mut cursor = 0;
            loop {
                self.progress(stats)?;
                let target = self.next_pending(kind, cursor)?;
                let Some(target) = target else { break };
                cursor = target.id;
                self.begin_batch("apply")?;
                let bytes_before = stats.bytes_deduped + stats.bytes_already_shared;
                self.attempt_target(&target, backend, stats)?;
                self.batch_step(
                    (stats.bytes_deduped + stats.bytes_already_shared).saturating_sub(bytes_before),
                )?;
            }
            self.flush_batch()?;
        }
        #[cfg(target_os = "linux")]
        crate::btrfs_scan::confirm_apply_mappings(self, stats)?;
        Ok(())
    }

    fn next_pending(&self, kind: i64, cursor: i64) -> Result<Option<Job>> {
        Ok(self
            .conn
            .query_row_cached(
                &format!(
                    "{JOB_SELECT} WHERE r.pending=1 AND r.kind=?1 AND r.id>?2
                AND NOT EXISTS(SELECT 1 FROM apply_failed a WHERE a.id=r.id) ORDER BY r.id LIMIT 1"
                ),
                params![kind, cursor],
                Job::from_row,
            )
            .optional()?)
    }

    fn attempt_target(
        &mut self,
        target: &Job,
        backend: &mut dyn Backend,
        stats: &mut Stats,
    ) -> Result<()> {
        let attempted = stats.jobs_attempted;
        let stale = stats.stale;
        let readonly = stats.readonly_ranges;
        self.apply_target(target, backend, stats)?;
        if stats.jobs_attempted != attempted
            || stats.stale != stale
            || stats.readonly_ranges != readonly
        {
            self.conn.execute_cached(
                "INSERT OR IGNORE INTO apply_failed SELECT id FROM ranges WHERE id=?1 AND pending=1", [target.id],
            )?;
        }
        Ok(())
    }

    // During scanning, cached paths/hashes must first be reconciled. Native
    // clean inodes need no reopening, but changed inodes and renamed descendants
    // remain in discovery until validated. Other subvolumes wait for their turn.
    fn ready(&self) -> &'static str {
        match self.scan_phase {
            ScanPhase::Complete => "1",
            ScanPhase::Portable => "EXISTS(SELECT 1 FROM seen WHERE file_id=f.id)",
            #[cfg(target_os = "linux")]
            ScanPhase::Native => {
                "EXISTS(SELECT 1 FROM btrfs_files b JOIN apply_crawls c ON c.id=b.crawl_id
                WHERE b.file_id=f.id AND NOT EXISTS(SELECT 1 FROM discovery d
                    WHERE d.crawl_id=b.crawl_id AND d.ino=b.ino))"
            }
        }
    }

    fn policy(&self, id: i64, file: &File, backend: &mut dyn Backend) -> Result<Option<u64>> {
        let cached = self
            .conn
            .query_row_cached(
                "SELECT policy FROM file_policies WHERE file_id=?1",
                [id],
                |r| r.get::<_, u64>(0),
            )
            .optional()?;
        if cached.is_some() {
            return Ok(cached);
        }
        let policy = backend.sharing_policy(file)?;
        if let Some(policy) = policy {
            save_sharing_policy(&self.conn, id, policy)?;
        }
        Ok(policy)
    }
    fn class_source(&self, target: &Job, policy: u64) -> Result<Option<i64>> {
        Ok(self.conn.query_row_cached("SELECT source FROM class_sources WHERE kind=?1 AND hash=?2 AND length=?3 AND fskey=?4 AND policy=?5",
            params![target.kind,target.hash,target.len,target.fskey,policy], |r| r.get(0)).optional()?)
    }
    fn remember_class_source(&self, target: &Job, policy: u64, source: i64) -> Result<()> {
        self.conn.execute_cached(
            "INSERT OR REPLACE INTO class_sources VALUES (?1,?2,?3,?4,?5,?6)",
            params![
                target.kind,
                target.hash,
                target.len,
                target.fskey,
                policy,
                source
            ],
        )?;
        Ok(())
    }
    fn forget_class_source(&self, target: &Job, policy: u64) -> Result<()> {
        self.conn.execute_cached("DELETE FROM class_sources WHERE kind=?1 AND hash=?2 AND length=?3 AND fskey=?4 AND policy=?5",params![target.kind,target.hash,target.len,target.fskey,policy])?;
        Ok(())
    }

    fn apply_target(
        &mut self,
        target: &Job,
        backend: &mut dyn Backend,
        stats: &mut Stats,
    ) -> Result<()> {
        self.apply_target_from(target, backend, stats, None)
    }

    fn apply_target_from(
        &mut self,
        target: &Job,
        backend: &mut dyn Backend,
        stats: &mut Stats,
        forced_source: Option<i64>,
    ) -> Result<()> {
        let mut target_policy: Option<u64> = self
            .conn
            .query_row_cached(
                "SELECT policy FROM file_policies WHERE file_id=?1",
                [target.file_id],
                |r| r.get(0),
            )
            .optional()?;
        let mut expected = forced_source
            .or(target_policy
                .map(|policy| self.class_source(target, policy))
                .transpose()?
                .flatten())
            .filter(|id| *id != target.id);
        let mut cursor = expected.map_or(0, |id| id - 1);
        let mut found = false;
        loop {
            // Prefer a cached read-only donor; otherwise use the lowest ID.
            // Keep a single representative rather than all O(n^2) pairs.
            // Seek to another source when a representative is stale. Hash
            // lookup and keyset iteration use the matches index.
            let source = if let Some(id) = expected.or(forced_source) {
                self.conn
                    .query_row_cached(&format!("{JOB_SELECT} WHERE r.id=?1"), [id], Job::from_row)
                    .optional()?
            } else {
                self
                .conn
                .query_row_cached(
                    &format!(
                        "{JOB_SELECT} WHERE r.kind=?1 AND r.hash=?2 AND r.length=?3
                          AND r.id>?4 AND r.id<?5 AND f.fskey=?6
                          AND (?7 IS NULL OR NOT EXISTS(SELECT 1 FROM file_policies p WHERE p.file_id=f.id)
                               OR EXISTS(SELECT 1 FROM file_policies p WHERE p.file_id=f.id AND p.policy=?7))
                          ORDER BY r.id LIMIT 1"
                    ),
                    params![
                        target.kind,
                        target.hash,
                        target.len,
                        cursor,
                        target.id,
                        target.fskey,
                        target_policy
                    ],
                    Job::from_row,
                )
                .optional()?
            };
            if let Some(cached) = expected.take() {
                if source.as_ref().map(|source| source.id) != Some(cached) {
                    if let Some(policy) = target_policy {
                        self.forget_class_source(target, policy)?;
                    }
                    if forced_source.is_some() {
                        return Ok(());
                    }
                    cursor = 0;
                    continue;
                }
            }
            let Some(source) = source else {
                if let Some(policy) = target_policy {
                    let older_match: bool = self.conn.query_row_cached(
                        "SELECT EXISTS(SELECT 1 FROM ranges r JOIN files f ON f.id=r.file_id JOIN file_policies p ON p.file_id=f.id WHERE r.kind=?1 AND r.hash=?2 AND r.length=?3 AND r.id<?4 AND f.fskey=?5 AND p.policy!=?6)",
                        params![target.kind,target.hash,target.len,target.id,target.fskey,policy], |r| r.get(0))?;
                    if older_match {
                        // This target is the first representative of its class.
                        // Later targets can seek directly here rather than
                        // repeatedly scanning the incompatible class prefix.
                        self.remember_class_source(target, policy, target.id)?;
                        self.conn.execute_cached(
                            "UPDATE ranges SET pending=0 WHERE id=?1",
                            [target.id],
                        )?;
                        stats.incompatible_ranges += 1;
                        return Ok(());
                    }
                }
                if !found {
                    self.conn
                        .execute_cached("UPDATE ranges SET pending=0 WHERE id=?1", [target.id])?;
                }
                return Ok(());
            };
            if !matches!(self.scan_phase, ScanPhase::Complete)
                && !self.conn.query_row_cached(
                    &format!(
                        "SELECT ({ready}) FROM files f WHERE f.id=?1",
                        ready = self.ready()
                    ),
                    [source.file_id],
                    |r| r.get::<_, bool>(0),
                )?
            {
                // Keep this match pending until its representative is validated.
                // Do not walk every member of a large unready duplicate group.
                return Ok(());
            }
            if forced_source.is_some() && found {
                return Ok(());
            }
            found = true;
            cursor = source.id;
            if source.file_id == target.file_id
                && ranges_overlap(source.offset, target.offset, target.len)
            {
                continue;
            }
            let source_file = match source.open(false) {
                Ok(file) => file,
                Err(error) => {
                    self.invalidate(source.file_id)?;
                    stats.stale += 1;
                    stats
                        .errors
                        .push(format!("source {:?}: {error:#}", source.path));
                    continue;
                }
            };
            let destination = match target.open(true) {
                Ok(file) => file,
                Err(error) => {
                    if error
                        .downcast_ref::<std::io::Error>()
                        .is_some_and(|e| e.raw_os_error() == Some(libc::EROFS))
                    {
                        stats.readonly_ranges += 1;
                        if forced_source.is_none() {
                            // A later read-only match can donate to every earlier
                            // writable member, including already completed jobs.
                            // Use the normal validation, policy, and completion path.
                            let policy =
                                self.policy(target.file_id, &target.open(false)?, backend)?;
                            // An existing read-only donor is already the best
                            // representative; two immutable copies cannot merge.
                            if self.policy(source.file_id, &source_file, backend)? == policy
                                && source.open(true).is_err_and(|e| {
                                    e.downcast_ref::<std::io::Error>()
                                        .is_some_and(|e| e.raw_os_error() == Some(libc::EROFS))
                                })
                            {
                                if let Some(policy) = policy {
                                    self.remember_class_source(target, policy, source.id)?;
                                }
                                return Ok(());
                            }
                            let mut cursor = 0;
                            loop {
                                self.progress(stats)?;
                                let candidate = self.conn.query_row_cached(
                                    &format!("{JOB_SELECT} WHERE r.kind=?1 AND r.hash=?2 AND r.length=?3 AND f.fskey=?4 AND r.id>?5 AND r.id<?6 AND ({ready}) ORDER BY r.id LIMIT 1", ready=self.ready()),
                                    params![target.kind,target.hash,target.len,target.fskey,cursor,target.id], Job::from_row,
                                ).optional()?;
                                let Some(candidate) = candidate else { break };
                                cursor = candidate.id;
                                self.apply_target_from(
                                    &candidate,
                                    backend,
                                    stats,
                                    Some(target.id),
                                )?;
                            }
                            if let Some(policy) = policy {
                                self.remember_class_source(target, policy, target.id)?;
                            }
                        }
                        return Ok(());
                    }
                    // Permission errors must remain retryable; only invalidate
                    // when a read-only reopen also fails identity validation.
                    if target.open(false).is_err() {
                        self.invalidate(target.file_id)?;
                    }
                    stats.stale += 1;
                    stats
                        .errors
                        .push(format!("destination {:?}: {error:#}", target.path));
                    return Ok(());
                }
            };
            let source_policy = self.policy(source.file_id, &source_file, backend)?;
            if target_policy.is_none() {
                target_policy = self.policy(target.file_id, &destination, backend)?;
            }
            if let (Some(source_policy), Some(policy)) = (source_policy, target_policy) {
                if source_policy != policy {
                    if let Some(cached) = self
                        .class_source(target, policy)?
                        .filter(|id| *id < target.id)
                    {
                        cursor = cached - 1;
                        expected = Some(cached);
                    }
                    continue;
                }
                self.remember_class_source(target, policy, source.id)?;
            }
            let alignment = target.alignment.max(source.alignment);
            if source.offset % alignment != 0 || target.offset % alignment != 0 {
                stats.unsupported += 1;
                return Ok(());
            }
            stats.jobs_attempted += 1;
            let mut done = 0;
            let mut complete = true;
            let mut mapping_changed = false;
            while done < target.len {
                self.progress(stats)?;
                let len = (target.len - done).min(16 * 1024 * 1024);
                let measured = backend.dedupe_with_savings(
                    &source_file,
                    &destination,
                    Range {
                        offset: source.offset + done,
                        len,
                    },
                    target.offset + done,
                );
                let mut saved = 0;
                let mut measured_bytes = 0;
                let result = measured.map(|report| {
                    saved = report.estimated_bytes_saved;
                    measured_bytes = report.savings_measured_bytes;
                    report.outcome
                });
                match result {
                    Ok(Outcome::Shared(bytes) | Outcome::AlreadyShared(bytes))
                        if bytes > 0
                            && bytes <= len
                            && (bytes == target.len - done || bytes % alignment == 0) =>
                    {
                        if matches!(result, Ok(Outcome::Shared(_))) {
                            stats.bytes_deduped += bytes;
                            stats.estimated_bytes_saved += saved;
                            stats.savings_measured_bytes += measured_bytes.min(bytes);
                            mapping_changed = true;
                        } else {
                            stats.bytes_already_shared += bytes;
                        }
                        done += bytes;
                    }
                    Ok(Outcome::Differs) => {
                        stats.stale += 1;
                        complete = false;
                        break;
                    }
                    Ok(Outcome::Unsupported) => {
                        stats.unsupported += 1;
                        complete = false;
                        break;
                    }
                    Ok(_) => {
                        stats
                            .errors
                            .push("backend returned invalid deduplication progress".into());
                        complete = false;
                        break;
                    }
                    Err(error) => {
                        stats.errors.push(format!(
                            "dedupe {:?}@{} -> {:?}@{}: {error}",
                            source.path,
                            source.offset + done,
                            target.path,
                            target.offset + done
                        ));
                        complete = false;
                        break;
                    }
                }
            }
            // Never bless a concurrently modified file's hashes. In particular,
            // do not refresh ctime after an ioctl: that could hide another writer.
            // Native dedupe does not change data timestamps on btrfs/XFS.
            let source_valid = source.valid(&source_file)?;
            let target_valid = target.valid(&destination)?;
            if !source_valid || !target_valid {
                if !source_valid {
                    self.invalidate(source.file_id)?;
                }
                if !target_valid {
                    self.invalidate(target.file_id)?;
                }
                stats.stale += 1;
                return Ok(());
            }
            if complete {
                self.test_hook("apply:shared")?;
                #[cfg(target_os = "linux")]
                if mapping_changed
                    && !crate::btrfs_scan::refresh_apply_mapping(
                        self,
                        target.file_id,
                        &destination,
                        &target.fingerprint,
                        stats,
                    )?
                {
                    return Ok(());
                }
                #[cfg(not(target_os = "linux"))]
                let _ = mapping_changed;
                if !target.valid(&destination)? {
                    self.invalidate(target.file_id)?;
                    stats.stale += 1;
                    return Ok(());
                }
                // These records are one part of the enclosing batch. Any SQL
                // failure aborts apply and rolls the entire unfinished batch
                // back, so no nested per-job rollback journal is needed.
                self.conn.execute_cached(
                    "INSERT OR REPLACE INTO completed(target,source) VALUES (?1,?2)",
                    params![target.id, source.id],
                )?;
                self.conn
                    .execute_cached("UPDATE ranges SET pending=0 WHERE id=?1", [target.id])?;
                if target.kind == 1 {
                    self.conn.execute_cached(
                        "UPDATE ranges SET pending=0 WHERE file_id=?1 AND kind=0",
                        [target.file_id],
                    )?;
                }
                stats.jobs_completed += 1;
                self.test_hook("apply:completion-staged")?;
                return Ok(());
            }
            // A failure is not a completed job. Keep it pending for the next run.
            // Do not try all pairs in a rejected group: unsupported filesystems
            // and collisions must not turn a linear group into quadratic work.
            return Ok(());
        }
    }

    fn invalidate(&mut self, file_id: i64) -> Result<()> {
        // A path can become stale after discovery's committed window (e.g. a
        // directory rename). Preserve its native identity before deleting the
        // path/hash record; no future inode event is required to rediscover it.
        self.conn.execute_cached("INSERT OR IGNORE INTO discovery SELECT crawl_id,ino,0 FROM btrfs_files WHERE file_id=?1",[file_id])?;
        self.conn
            .execute_cached("DELETE FROM files WHERE id=?1", [file_id])?;
        Ok(())
    }
}

fn ranges_overlap(a: u64, b: u64, length: u64) -> bool {
    a.abs_diff(b) < length
}

pub(crate) fn save_sharing_policy(
    connection: &Connection,
    file_id: i64,
    policy: u64,
) -> Result<()> {
    connection.execute_cached("INSERT INTO file_policies VALUES (?1,?2) ON CONFLICT(file_id) DO UPDATE SET policy=excluded.policy WHERE policy!=excluded.policy", params![file_id,policy])?;
    Ok(())
}

const DUPLICATE_GROUPS: &str = "SELECT kind,hash,length FROM ranges
    GROUP BY kind,hash,length HAVING COUNT(*)>1 ORDER BY kind,hash,length";
const DUPLICATE_RANGES: &str = "SELECT f.path,r.offset FROM ranges r
    JOIN files f ON f.id=r.file_id WHERE r.kind=?1 AND r.hash=?2 AND r.length=?3 ORDER BY r.id";

const JOB_SELECT: &str = "SELECT r.id,r.file_id,r.kind,r.offset,r.length,r.hash,
    f.path,f.fingerprint,f.fskey,f.alignment FROM ranges r JOIN files f ON f.id=r.file_id";

struct Job {
    id: i64,
    file_id: i64,
    kind: i64,
    offset: u64,
    len: u64,
    hash: Vec<u8>,
    path: PathBuf,
    fingerprint: Vec<u8>,
    fskey: Vec<u8>,
    alignment: u64,
}

impl Job {
    fn from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            id: row.get(0)?,
            file_id: row.get(1)?,
            kind: row.get(2)?,
            offset: row.get(3)?,
            len: row.get(4)?,
            hash: row.get(5)?,
            path: decode_path(row.get(6)?),
            fingerprint: row.get(7)?,
            fskey: row.get(8)?,
            alignment: row.get(9)?,
        })
    }

    fn open(&self, write: bool) -> Result<File> {
        let file = scan::open_regular(&self.path, write)?;
        ensure!(self.valid(&file)?, "file changed since indexing");
        ensure!(
            backend::filesystem(&file)?.key == self.fskey,
            "filesystem changed"
        );
        Ok(file)
    }

    fn valid(&self, file: &File) -> Result<bool> {
        let meta = file.metadata()?;
        if scan::fingerprint(&meta) != self.fingerprint {
            return Ok(false);
        }
        if self
            .offset
            .checked_add(self.len)
            .is_none_or(|end| end > meta.len())
        {
            bail!("indexed range is outside the file");
        }
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::StatementStatus;

    #[test]
    fn listing_uses_index_order_without_sorting_paths() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE files (id INTEGER PRIMARY KEY, path BLOB);
             CREATE TABLE ranges (id INTEGER PRIMARY KEY, file_id INTEGER,
               kind INTEGER, hash BLOB, length INTEGER, offset INTEGER);
             CREATE INDEX matches ON ranges(kind,hash,length,id); BEGIN;",
        )
        .unwrap();
        for id in 1..=4000 {
            conn.execute_cached("INSERT INTO files VALUES (?1,?2)", params![id, b"path"])
                .unwrap();
            conn.execute_cached(
                "INSERT INTO ranges VALUES (?1,?1,?2,?3,4096,0)",
                params![id, id % 2, (id / 4i32).to_le_bytes()],
            )
            .unwrap();
        }
        conn.execute_batch("COMMIT").unwrap();
        let mut groups = conn.prepare_cached(DUPLICATE_GROUPS).unwrap();
        let mut candidates = conn.prepare_cached(DUPLICATE_RANGES).unwrap();
        {
            let mut rows = groups.query([]).unwrap();
            while let Some(row) = rows.next().unwrap() {
                let kind: i64 = row.get(0).unwrap();
                let hash: Vec<u8> = row.get(1).unwrap();
                let length: u64 = row.get(2).unwrap();
                let mut matches = candidates.query(params![kind, hash, length]).unwrap();
                while matches.next().unwrap().is_some() {}
            }
        }
        assert_eq!(groups.get_status(StatementStatus::Sort), 0);
        assert_eq!(candidates.get_status(StatementStatus::Sort), 0);
        assert_eq!(candidates.get_status(StatementStatus::FullscanStep), 0);
    }
}
