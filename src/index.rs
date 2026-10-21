use crate::{backend, scan, Backend, Duplicate, Granularity, Options, Outcome, Range, Stats};
use anyhow::{bail, ensure, Context, Result};
use fs2::FileExt;
use rusqlite::{params, Connection, OptionalExtension, Params, Row};
use std::{
    fs::{self, File, OpenOptions},
    os::unix::{ffi::OsStrExt, fs::OpenOptionsExt},
    path::{Path, PathBuf},
};

pub(crate) struct Index {
    pub conn: Connection,
    pub roots: Vec<PathBuf>,
    pub excluded: Vec<PathBuf>,
    pub buffer: Vec<u8>,
    _lock: File,
    _database: File,
}

/// Repeated scan/job statements share a bounded connection-local cache, also
/// through Transaction's deref. Dropping a cached statement resets it and clears
/// its bindings before reuse; transaction boundaries and durability are unchanged.
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
            (0..=3).contains(&version),
            "unsupported index schema version {version}"
        );
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value BLOB NOT NULL);
             CREATE TABLE IF NOT EXISTS files (
               id INTEGER PRIMARY KEY AUTOINCREMENT, path BLOB NOT NULL UNIQUE,
               dev INTEGER NOT NULL, ino INTEGER NOT NULL, fingerprint BLOB NOT NULL,
               fskey BLOB NOT NULL, alignment INTEGER NOT NULL,
               UNIQUE(dev, ino));
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
             CREATE TEMP TABLE seen (file_id INTEGER PRIMARY KEY);",
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
        Ok(Self {
            conn,
            roots: reduced,
            excluded,
            buffer: Vec::new(),
            _lock: lock,
            _database: db_file,
        })
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
        let mut statement = self.conn.prepare_cached(
            "SELECT f.path,r.offset,r.length,r.kind,r.hash FROM ranges r
             JOIN files f ON f.id=r.file_id
             JOIN (SELECT kind,hash,length FROM ranges GROUP BY kind,hash,length
                   HAVING COUNT(*)>1) g
             ON r.kind=g.kind AND r.hash=g.hash AND r.length=g.length
             ORDER BY r.kind,r.hash,r.length,r.id",
        )?;
        let mut rows = statement.query([])?;
        let mut representative: Option<(i64, Vec<u8>, u64, PathBuf, u64)> = None;
        while let Some(row) = rows.next()? {
            let path = decode_path(row.get(0)?);
            let offset = row.get(1)?;
            let length = row.get(2)?;
            let kind = row.get(3)?;
            let hash: Vec<u8> = row.get(4)?;
            if let Some((previous_kind, previous_hash, previous_length, source, source_offset)) =
                &representative
            {
                if kind == *previous_kind && hash == *previous_hash && length == *previous_length {
                    visit(Duplicate {
                        source: source.clone(),
                        source_offset: *source_offset,
                        destination: path,
                        destination_offset: offset,
                        length,
                        whole_file: kind == 1,
                    })?;
                    continue;
                }
            }
            representative = Some((kind, hash, length, path, offset));
        }
        Ok(())
    }

    pub fn apply(&mut self, backend: &mut impl Backend, stats: &mut Stats) -> Result<()> {
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
                let target = self.next_pending(kind, cursor)?;
                let Some(target) = target else { break };
                cursor = target.id;
                self.apply_target(&target, backend, stats)?;
            }
        }
        Ok(())
    }

    fn next_pending(&self, kind: i64, cursor: i64) -> Result<Option<Job>> {
        Ok(self.conn.query_row_cached(
            &format!("{JOB_SELECT} WHERE r.pending=1 AND r.kind=?1 AND r.id>?2 ORDER BY r.id LIMIT 1"),
            params![kind, cursor], Job::from_row,
        ).optional()?)
    }

    fn apply_target(
        &mut self,
        target: &Job,
        backend: &mut impl Backend,
        stats: &mut Stats,
    ) -> Result<()> {
        let mut cursor = 0;
        let mut found = false;
        loop {
            // Deterministic lowest-ID representative, not all O(n^2) pairs.
            // Seek to another source when a representative is stale. Hash
            // lookup and keyset iteration use the matches index.
            let source = self
                .conn
                .query_row_cached(
                    &format!(
                        "{JOB_SELECT} WHERE r.kind=?1 AND r.hash=?2 AND r.length=?3
                          AND r.id>?4 AND r.id<?5 AND f.fskey=?6 ORDER BY r.id LIMIT 1"
                    ),
                    params![
                        target.kind,
                        target.hash,
                        target.len,
                        cursor,
                        target.id,
                        target.fskey
                    ],
                    Job::from_row,
                )
                .optional()?;
            let Some(source) = source else {
                if !found {
                    self.conn
                        .execute_cached("UPDATE ranges SET pending=0 WHERE id=?1", [target.id])?;
                }
                return Ok(());
            };
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
            let alignment = target.alignment.max(source.alignment);
            if source.offset % alignment != 0 || target.offset % alignment != 0 {
                stats.unsupported += 1;
                return Ok(());
            }
            stats.jobs_attempted += 1;
            let mut done = 0;
            let mut complete = true;
            while done < target.len {
                let len = (target.len - done).min(16 * 1024 * 1024);
                let result = backend.dedupe(
                    &source_file,
                    &destination,
                    Range {
                        offset: source.offset + done,
                        len,
                    },
                    target.offset + done,
                );
                match result {
                    Ok(Outcome::Shared(bytes) | Outcome::AlreadyShared(bytes))
                        if bytes > 0
                            && bytes <= len
                            && (bytes == target.len - done || bytes % alignment == 0) =>
                    {
                        if matches!(result, Ok(Outcome::Shared(_))) {
                            stats.bytes_deduped += bytes;
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
                let tx = self.conn.transaction()?;
                tx.execute_cached(
                    "INSERT OR REPLACE INTO completed(target,source) VALUES (?1,?2)",
                    params![target.id, source.id],
                )?;
                tx.execute_cached("UPDATE ranges SET pending=0 WHERE id=?1", [target.id])?;
                if target.kind == 1 {
                    tx.execute_cached(
                        "UPDATE ranges SET pending=0 WHERE file_id=?1 AND kind=0",
                        [target.file_id],
                    )?;
                }
                tx.commit()?;
                stats.jobs_completed += 1;
                return Ok(());
            }
            // A failure is not a completed job. Keep it pending for the next run.
            // Do not try all pairs in a rejected group: unsupported filesystems
            // and collisions must not turn a linear group into quadratic work.
            return Ok(());
        }
    }

    fn invalidate(&self, file_id: i64) -> Result<()> {
        // A path can become stale after discovery's committed window (e.g. a
        // directory rename). Preserve its native identity before deleting the
        // path/hash record; no future inode event is required to rediscover it.
        let tx = self.conn.unchecked_transaction()?;
        tx.execute_cached("INSERT OR IGNORE INTO discovery SELECT crawl_id,ino,0 FROM btrfs_files WHERE file_id=?1",[file_id])?;
        tx.execute_cached("DELETE FROM files WHERE id=?1", [file_id])?;
        tx.commit()?;
        Ok(())
    }
}

fn ranges_overlap(a: u64, b: u64, length: u64) -> bool {
    a.abs_diff(b) < length
}

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
