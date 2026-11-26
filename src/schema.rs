//! Ordered, transactional upgrades shared by every index writer.
use anyhow::{ensure, Context, Result};
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};

type Migration = fn(&Transaction<'_>) -> Result<()>;

// Each position is the schema version produced by that migration, starting at 1.
const MIGRATIONS: &[Migration] = &[
    create_base,
    create_incremental_tables,
    purge_zero_chunks,
    create_apply_mappings,
    crate::paths::migrate,
    crate::records::migrate,
    complete_legacy_schema,
    crate::inventory_state::repair_file_queue,
];
const PATHS_VERSION: usize = 5;

pub(crate) fn migrate(conn: &Connection, minimum_version: usize) -> Result<()> {
    let version: i64 = conn.pragma_query_value(None, "user_version", |row| row.get(0))?;
    let latest = MIGRATIONS.len();
    ensure!(
        version >= minimum_version as i64 && version <= latest as i64,
        "unsupported index schema version {version} (expected {minimum_version} through {latest})"
    );
    conn.pragma_update(None, "foreign_keys", true)?;
    for target in version as usize + 1..=latest {
        apply_step(conn, target)
            .with_context(|| format!("migrating index to schema version {target}"))?;
    }
    Ok(())
}

pub(crate) fn apply_step(conn: &Connection, target: usize) -> Result<()> {
    // SQLite table replacement needs foreign keys disabled outside the
    // transaction. The path migration checks them before the step can commit.
    if target == PATHS_VERSION {
        conn.pragma_update(None, "foreign_keys", false)?;
    }
    let result = (|| -> Result<()> {
        let tx = Transaction::new_unchecked(conn, TransactionBehavior::Immediate)?;
        MIGRATIONS[target - 1](&tx)?;
        tx.pragma_update(None, "user_version", target as i64)?;
        tx.commit()?;
        Ok(())
    })();
    if target == PATHS_VERSION {
        conn.pragma_update(None, "foreign_keys", true)?;
    }
    result
}

fn create_base(tx: &Transaction<'_>) -> Result<()> {
    tx.execute_batch(
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
         END;",
    )?;
    Ok(())
}

fn create_incremental_tables(tx: &Transaction<'_>) -> Result<()> {
    tx.execute_batch(
        "CREATE TABLE chunk_cache (
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
         END;",
    )?;
    Ok(())
}

fn purge_zero_chunks(tx: &Transaction<'_>) -> Result<()> {
    let lengths = tx
        .prepare("SELECT DISTINCT length FROM ranges WHERE kind=0 ORDER BY length")?
        .query_map([], |r| r.get::<_, usize>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    // Migrate cached v1/v2 ranges without reading files. Incrementally
    // hash prefixes of one zero buffer, so even many distinct EOF tail
    // lengths require hashing at most one chunk's worth of zero bytes.
    let Some(&maximum) = lengths.last() else {
        return Ok(());
    };
    let scan: Option<Vec<u8>> = tx
        .query_row("SELECT value FROM config WHERE key='scan'", [], |r| {
            r.get(0)
        })
        .optional()?;
    const MAX_CHUNK_SIZE: usize = 16 * 1024 * 1024;
    let chunk_size = match scan {
        Some(scan) => {
            let (chunk_size, _, _, _): (usize, u8, Vec<Vec<u8>>, String) =
                serde_json::from_slice(&scan).context("read legacy index scan configuration")?;
            chunk_size
        }
        None => MAX_CHUNK_SIZE,
    };
    ensure!(
        maximum <= chunk_size && maximum <= MAX_CHUNK_SIZE,
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
    Ok(())
}

fn create_apply_mappings(tx: &Transaction<'_>) -> Result<()> {
    tx.execute_batch(
        "CREATE TABLE IF NOT EXISTS apply_mappings (
           file_id INTEGER PRIMARY KEY REFERENCES files(id) ON DELETE CASCADE);",
    )?;
    Ok(())
}

fn complete_legacy_schema(tx: &Transaction<'_>) -> Result<()> {
    // Older scans added these outside the versioned migrations. Compaction
    // could advance the schema to version 6 without creating them.
    tx.execute_batch(
        "CREATE TABLE IF NOT EXISTS file_policies (
           file_id INTEGER PRIMARY KEY REFERENCES files(id) ON DELETE CASCADE,
           policy INTEGER NOT NULL);
         CREATE INDEX IF NOT EXISTS discovery_kind ON discovery(crawl_id,directory,ino);",
    )?;
    Ok(())
}
