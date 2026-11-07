//! Compact extent identities and one canonical record for each candidate chunk.
use anyhow::{ensure, Result};
use rusqlite::{functions::FunctionFlags, Connection};

/// Input is a raw Btrfs file-extent item. Regular/preallocated records retain
/// their kernel layout; inline items retain the header and a domain-separated
/// digest of the entire encoded payload. Call only at the raw-input boundary.
pub(crate) fn descriptor(raw: &[u8]) -> Vec<u8> {
    if raw.get(20) != Some(&0) {
        return raw.to_vec();
    }
    let mut result = raw[..21].to_vec();
    result.extend_from_slice(&blake3::derive_key(
        "dedupit inline extent payload v1",
        &raw[21..],
    ));
    result
}

pub(crate) fn migrate(conn: &Connection) -> Result<()> {
    eprintln!(
        "Consolidating chunk records and compacting inline descriptors; retaining scan progress"
    );
    conn.create_scalar_function(
        "dedupit_extent_descriptor",
        1,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let raw: Vec<u8> = ctx.get(0)?;
            Ok(descriptor(&raw))
        },
    )?;
    let tx = conn.unchecked_transaction()?;
    let inconsistent: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM chunk_cache c JOIN ranges r
           ON r.file_id=c.file_id AND r.kind=0 AND r.offset=c.offset
           WHERE c.length!=r.length OR c.hash!=r.hash OR c.nonzero!=1)",
        [],
        |r| r.get(0),
    )?;
    ensure!(
        !inconsistent,
        "chunk cache disagrees with candidate ranges; original index retained"
    );
    // No range is replaced or renumbered. Pending/completed jobs and all cached
    // hashes retain their identities; only redundant copies are discarded.
    tx.execute_batch(
        "CREATE TABLE chunk_cache_extra (
           file_id INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
           offset INTEGER NOT NULL,length INTEGER NOT NULL,hash BLOB NOT NULL,
           nonzero INTEGER NOT NULL,PRIMARY KEY(file_id,offset)) WITHOUT ROWID;
         INSERT INTO chunk_cache_extra SELECT c.* FROM chunk_cache c
           WHERE NOT EXISTS(SELECT 1 FROM ranges r WHERE r.file_id=c.file_id AND r.kind=0 AND r.offset=c.offset);
         DROP TABLE chunk_cache;
         CREATE VIEW chunk_cache AS
           SELECT file_id,offset,length,hash,1 AS nonzero FROM ranges WHERE kind=0
           UNION ALL SELECT file_id,offset,length,hash,nonzero FROM chunk_cache_extra;
         CREATE TRIGGER chunk_cache_delete INSTEAD OF DELETE ON chunk_cache BEGIN
           DELETE FROM ranges WHERE file_id=OLD.file_id AND kind=0 AND offset=OLD.offset;
           DELETE FROM chunk_cache_extra WHERE file_id=OLD.file_id AND offset=OLD.offset;
         END;
         CREATE TRIGGER invalidate_extent_baseline_range AFTER DELETE ON ranges WHEN OLD.kind=0 BEGIN
           DELETE FROM extent_state WHERE file_id=OLD.file_id;
         END;
         CREATE TRIGGER invalidate_extent_baseline_extra AFTER DELETE ON chunk_cache_extra BEGIN
           DELETE FROM extent_state WHERE file_id=OLD.file_id;
         END;
         CREATE TABLE file_extents_compact (
           file_id INTEGER NOT NULL REFERENCES files(id) ON DELETE CASCADE,
           offset INTEGER NOT NULL,length INTEGER NOT NULL,descriptor BLOB NOT NULL,
           PRIMARY KEY(file_id,offset)) WITHOUT ROWID;
         INSERT INTO file_extents_compact
           SELECT file_id,offset,length,
             CASE WHEN substr(descriptor,21,1)=x'00' THEN dedupit_extent_descriptor(descriptor) ELSE descriptor END
           FROM file_extents;
         DROP TABLE file_extents;
         ALTER TABLE file_extents_compact RENAME TO file_extents;
         PRAGMA user_version=6;",
    )?;
    let bad: bool = tx.prepare("PRAGMA foreign_key_check")?.exists([])?;
    ensure!(!bad, "consolidation has foreign key violations");
    tx.commit()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::params;
    fn inline(payload: &[u8]) -> Vec<u8> {
        let mut raw = vec![0; 21];
        raw[..8].copy_from_slice(&42u64.to_le_bytes());
        raw[8..16].copy_from_slice(&(payload.len() as u64).to_le_bytes());
        raw.extend_from_slice(payload);
        raw
    }
    #[test]
    fn inline_identity_keeps_header_and_detects_changes_at_the_end_of_payload() {
        let mut raw = inline(&vec![7; 2000]);
        let compact = descriptor(&raw);
        assert_eq!(compact.len(), 53);
        assert_eq!(&compact[..21], &raw[..21]);
        *raw.last_mut().unwrap() ^= 1;
        assert_ne!(compact, descriptor(&raw));
        raw[16] = 1;
        assert_ne!(&descriptor(&raw)[..21], &compact[..21]);
        let mut regular = vec![9; 53];
        regular[20] = 1;
        assert_eq!(descriptor(&regular), regular);
        regular[20] = 2;
        assert_eq!(descriptor(&regular), regular);
    }
    fn legacy(c: &Connection) -> Vec<u8> {
        c.execute_batch("PRAGMA foreign_keys=ON;
          CREATE TABLE files(id INTEGER PRIMARY KEY);
          INSERT INTO files VALUES(1),(2),(3);
          CREATE TABLE ranges(id INTEGER PRIMARY KEY AUTOINCREMENT,file_id INTEGER REFERENCES files(id),kind INTEGER,offset INTEGER,length INTEGER,hash BLOB,pending INTEGER,UNIQUE(file_id,kind,offset));
          INSERT INTO ranges VALUES(41,1,0,0,4096,x'11',1),(55,1,1,0,4096,x'77',0);
          CREATE TABLE completed(target INTEGER PRIMARY KEY REFERENCES ranges(id) ON DELETE CASCADE,source INTEGER REFERENCES ranges(id) ON DELETE CASCADE);
          INSERT INTO completed VALUES(55,41);
          CREATE TRIGGER invalidate_source BEFORE DELETE ON ranges BEGIN
            UPDATE ranges SET pending=1 WHERE id IN (SELECT target FROM completed WHERE source=OLD.id);
          END;
          CREATE TABLE chunk_cache(file_id INTEGER REFERENCES files(id),offset INTEGER,length INTEGER,hash BLOB,nonzero INTEGER,PRIMARY KEY(file_id,offset));
          INSERT INTO chunk_cache VALUES(1,0,4096,x'11',1),(2,0,4096,x'22',1),(3,0,4096,x'33',0);
          CREATE TABLE extent_state(file_id INTEGER PRIMARY KEY REFERENCES files(id),incarnation BLOB);
          INSERT INTO extent_state VALUES(1,x'01'),(2,x'02'),(3,x'03');
          CREATE TRIGGER invalidate_extent_baseline AFTER DELETE ON chunk_cache BEGIN DELETE FROM extent_state WHERE file_id=OLD.file_id; END;
          CREATE TABLE file_extents(file_id INTEGER REFERENCES files(id),offset INTEGER,length INTEGER,descriptor BLOB,PRIMARY KEY(file_id,offset));
          CREATE TABLE crawls(id INTEGER PRIMARY KEY,cursor BLOB,low INTEGER,high INTEGER);
          INSERT INTO crawls VALUES(9,x'1234',7,99);
          CREATE TABLE discovery(crawl_id INTEGER REFERENCES crawls(id),ino INTEGER,directory INTEGER);
          INSERT INTO discovery VALUES(9,20,0);
          PRAGMA user_version=5;").unwrap();
        let raw = inline(&vec![17; 1800]);
        c.execute("INSERT INTO file_extents VALUES(1,0,1800,?1)", [&raw])
            .unwrap();
        let mut regular = vec![8; 53];
        regular[20] = 1;
        c.execute("INSERT INTO file_extents VALUES(2,0,4096,?1)", [&regular])
            .unwrap();
        raw
    }
    #[test]
    fn migration_reuses_candidate_rows_without_changing_jobs_or_baselines() {
        let c = Connection::open_in_memory().unwrap();
        let raw = legacy(&c);
        migrate(&c).unwrap();
        assert_eq!(
            c.query_row("SELECT COUNT(*) FROM chunk_cache_extra", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            2
        );
        assert_eq!(
            c.query_row("SELECT COUNT(*) FROM chunk_cache", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            3
        );
        assert_eq!(
            c.query_row(
                "SELECT COUNT(*) FROM chunk_cache_extra WHERE file_id=1",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            0
        );
        assert_eq!(
            c.query_row(
                "SELECT group_concat(id||':'||hex(hash)||':'||pending) FROM ranges ORDER BY id",
                [],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
            "41:11:1,55:77:0"
        );
        assert_eq!(
            c.query_row("SELECT source FROM completed WHERE target=55", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            41
        );
        assert_eq!(
            c.query_row("SELECT COUNT(*) FROM extent_state", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            3
        );
        assert_eq!(
            c.query_row(
                "SELECT hex(cursor)||':'||low||':'||high FROM crawls",
                [],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
            "1234:7:99"
        );
        assert_eq!(
            c.query_row("SELECT ino FROM discovery", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            20
        );
        assert_eq!(
            c.query_row(
                "SELECT descriptor FROM file_extents WHERE file_id=1",
                [],
                |r| r.get::<_, Vec<u8>>(0)
            )
            .unwrap(),
            descriptor(&raw)
        );
        c.execute("DELETE FROM chunk_cache WHERE file_id=1", [])
            .unwrap();
        assert_eq!(
            c.query_row(
                "SELECT COUNT(*) FROM extent_state WHERE file_id=1",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            0
        );
        assert_eq!(
            c.query_row("SELECT COUNT(*) FROM ranges WHERE id=41", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            0
        );
    }
    #[test]
    fn failures_after_cache_drop_roll_back_all_progress_and_allow_retry() {
        let c = Connection::open_in_memory().unwrap();
        let raw = legacy(&c);
        c.execute_batch("CREATE TABLE file_extents_compact(block_migration INTEGER)")
            .unwrap();
        assert!(migrate(&c).is_err());
        assert_eq!(
            c.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            5
        );
        assert_eq!(
            c.query_row(
                "SELECT type FROM sqlite_schema WHERE name='chunk_cache'",
                [],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
            "table"
        );
        assert_eq!(
            c.query_row(
                "SELECT descriptor FROM file_extents WHERE file_id=1",
                [],
                |r| r.get::<_, Vec<u8>>(0)
            )
            .unwrap(),
            raw
        );
        assert_eq!(
            c.query_row("SELECT COUNT(*) FROM extent_state", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            3
        );
        c.execute_batch("DROP TABLE file_extents_compact").unwrap();
        migrate(&c).unwrap();
    }
    #[test]
    fn inconsistent_copies_abort_without_discarding_either_hash() {
        let c = Connection::open_in_memory().unwrap();
        legacy(&c);
        c.execute(
            "UPDATE chunk_cache SET hash=?1 WHERE file_id=1",
            params![b"wrong".as_slice()],
        )
        .unwrap();
        assert!(migrate(&c).is_err());
        assert_eq!(
            c.query_row("SELECT hex(hash) FROM ranges WHERE id=41", [], |r| r
                .get::<_, String>(0))
                .unwrap(),
            "11"
        );
        assert_eq!(
            c.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            5
        );
    }
}
