//! Normalized, byte-preserving Unix paths. Migration retains all scan/job IDs.
use crate::index::SqlCache;
use anyhow::{ensure, Context, Result};
use rusqlite::{functions::FunctionFlags, Connection};
use std::{os::unix::ffi::OsStrExt, path::Path};

fn split(path: &[u8]) -> (&[u8], &[u8]) {
    let slash = path.iter().rposition(|b| *b == b'/').unwrap_or(0);
    (&path[..slash.max(1)], &path[slash + 1..])
}

pub(crate) fn functions(conn: &Connection) -> Result<()> {
    let flags = FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC;
    conn.create_scalar_function("dedupit_parent", 1, flags, |ctx| {
        let bytes: Vec<u8> = ctx.get(0)?;
        Ok(split(&bytes).0.to_vec())
    })?;
    conn.create_scalar_function("dedupit_name", 1, flags, |ctx| {
        let bytes: Vec<u8> = ctx.get(0)?;
        Ok(split(&bytes).1.to_vec())
    })?;
    Ok(())
}

pub(crate) fn directory_id(conn: &Connection, directory: &Path) -> Result<i64> {
    let bytes = directory.as_os_str().as_bytes();
    conn.execute_cached(
        "INSERT OR IGNORE INTO path_parents(path) VALUES (?1)",
        [bytes],
    )?;
    Ok(
        conn.query_row_cached("SELECT id FROM path_parents WHERE path=?1", [bytes], |r| {
            r.get(0)
        })?,
    )
}

pub(crate) fn intern<'a>(conn: &Connection, path: &'a Path) -> Result<(i64, &'a [u8])> {
    let parent = path.parent().context("file path has no parent")?;
    let name = path
        .file_name()
        .context("file path has no name")?
        .as_bytes();
    Ok((directory_id(conn, parent)?, name))
}

pub(crate) fn migrate(conn: &Connection) -> Result<()> {
    eprintln!("Migrating index paths; retaining hashes, file IDs, and scan cursors");
    // SQLite's documented table-rebuild procedure: disable foreign keys outside
    // the transaction and check every relationship before committing the swap.
    conn.execute_batch("PRAGMA foreign_keys=OFF;")?;
    let result = (|| -> Result<()> {
        let tx = conn.unchecked_transaction()?;
        tx.execute_batch(
            "CREATE TABLE path_parents(id INTEGER PRIMARY KEY,path BLOB NOT NULL UNIQUE);
             INSERT INTO path_parents(path) SELECT DISTINCT dedupit_parent(path) FROM files;
             INSERT OR IGNORE INTO path_parents(path) SELECT DISTINCT parent FROM btrfs_dirs;
             CREATE TABLE files_new (
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               parent_id INTEGER NOT NULL REFERENCES path_parents(id), name BLOB NOT NULL,
               dev INTEGER NOT NULL,ino INTEGER NOT NULL,fingerprint BLOB NOT NULL,
               fskey BLOB NOT NULL,alignment INTEGER NOT NULL);
             INSERT INTO files_new SELECT f.id,p.id,dedupit_name(f.path),f.dev,f.ino,f.fingerprint,f.fskey,f.alignment
               FROM files f JOIN path_parents p ON p.path=dedupit_parent(f.path);
             UPDATE sqlite_sequence SET seq=MAX(seq,COALESCE((SELECT seq FROM sqlite_sequence WHERE name='files'),0)) WHERE name='files_new';
             CREATE UNIQUE INDEX file_name ON files_new(parent_id,name);
             CREATE UNIQUE INDEX file_inode ON files_new(dev,ino);
             CREATE TABLE btrfs_files_new (
               file_id INTEGER PRIMARY KEY REFERENCES files(id) ON DELETE CASCADE,
               crawl_id INTEGER NOT NULL REFERENCES crawls(id) ON DELETE CASCADE,
               ino INTEGER NOT NULL,UNIQUE(crawl_id,ino));
             INSERT INTO btrfs_files_new SELECT file_id,crawl_id,ino FROM btrfs_files;
             CREATE TABLE btrfs_dirs_new (
               crawl_id INTEGER NOT NULL REFERENCES crawls(id) ON DELETE CASCADE,
               ino INTEGER NOT NULL,path BLOB NOT NULL,
               parent_id INTEGER NOT NULL REFERENCES path_parents(id),
               PRIMARY KEY(crawl_id,ino)) WITHOUT ROWID;
             INSERT INTO btrfs_dirs_new SELECT b.crawl_id,b.ino,b.path,p.id
               FROM btrfs_dirs b JOIN path_parents p ON p.path=b.parent;
             DROP TABLE btrfs_files;
             DROP TABLE btrfs_dirs;
             DROP TABLE files;
             ALTER TABLE files_new RENAME TO files;
             ALTER TABLE btrfs_files_new RENAME TO btrfs_files;
             ALTER TABLE btrfs_dirs_new RENAME TO btrfs_dirs;
             CREATE INDEX btrfs_dir_parent ON btrfs_dirs(crawl_id,parent_id);
             CREATE VIEW file_paths AS SELECT f.*,CAST(p.path || CASE WHEN p.path=x'2f' THEN x'' ELSE x'2f' END || f.name AS BLOB) AS path
               FROM files f JOIN path_parents p ON p.id=f.parent_id;
             PRAGMA user_version=5;",
        )?;
        let violations: u64 =
            tx.query_row("SELECT COUNT(*) FROM pragma_foreign_key_check", [], |r| {
                r.get(0)
            })?;
        ensure!(violations == 0, "path migration has foreign key violations");
        tx.commit()?;
        Ok(())
    })();
    conn.execute_batch("PRAGMA foreign_keys=ON;")?;
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::params;
    #[test]
    fn unix_path_bytes_are_preserved() {
        let c = Connection::open_in_memory().unwrap();
        functions(&c).unwrap();
        for path in [b"/a".as_slice(), b"/parent/\xff", b"/a\xff/b\x80"] {
            let (parent, name): (Vec<u8>, Vec<u8>) = c
                .query_row(
                    "SELECT dedupit_parent(?1),dedupit_name(?1)",
                    params![path],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .unwrap();
            let mut rebuilt = parent;
            if rebuilt != b"/" {
                rebuilt.push(b'/');
            }
            rebuilt.extend(name);
            assert_eq!(rebuilt, path);
        }
    }
}

#[cfg(test)]
mod migration_tests {
    use super::*;
    use rusqlite::params;
    fn legacy(c: &Connection) {
        functions(c).unwrap();
        c.execute_batch("PRAGMA foreign_keys=ON;
          CREATE TABLE files(id INTEGER PRIMARY KEY AUTOINCREMENT,path BLOB UNIQUE,dev INTEGER,ino INTEGER,fingerprint BLOB,fskey BLOB,alignment INTEGER,UNIQUE(dev,ino));
          CREATE TABLE crawls(id INTEGER PRIMARY KEY,cursor BLOB,low INTEGER,high INTEGER,completed INTEGER);
          CREATE TABLE btrfs_files(file_id INTEGER PRIMARY KEY REFERENCES files(id) ON DELETE CASCADE,crawl_id INTEGER REFERENCES crawls(id),ino INTEGER,parent BLOB,UNIQUE(crawl_id,ino));
          CREATE TABLE btrfs_dirs(crawl_id INTEGER REFERENCES crawls(id),ino INTEGER,path BLOB,parent BLOB,PRIMARY KEY(crawl_id,ino));
          CREATE TABLE ranges(id INTEGER PRIMARY KEY,file_id INTEGER REFERENCES files(id),hash BLOB,pending INTEGER);
          CREATE TABLE completed(target INTEGER PRIMARY KEY REFERENCES ranges(id),source INTEGER REFERENCES ranges(id));
          CREATE TABLE chunk_cache(file_id INTEGER REFERENCES files(id),offset INTEGER,hash BLOB);
          CREATE TABLE discovery(crawl_id INTEGER REFERENCES crawls(id),ino INTEGER,directory INTEGER);
          INSERT INTO crawls VALUES(9,x'abcdf0',123,456,0);
          INSERT INTO discovery VALUES(9,88,1);
          PRAGMA user_version=4;").unwrap();
        for (id, path) in [
            (7, b"/parent/\xff".as_slice()),
            (12, b"/parent/b".as_slice()),
        ] {
            c.execute(
                "INSERT INTO files VALUES(?1,?2,2,?1,x'123456',x'01',4096)",
                params![id, path],
            )
            .unwrap();
            c.execute(
                "INSERT INTO btrfs_files VALUES(?1,9,?1,?2)",
                params![id, b"/parent".as_slice()],
            )
            .unwrap();
            c.execute(
                "INSERT INTO ranges VALUES(?1,?1,x'cafeba',?2)",
                params![id, id % 2],
            )
            .unwrap();
            c.execute("INSERT INTO chunk_cache VALUES(?1,0,x'cafeba')", [id])
                .unwrap();
        }
        c.execute(
            "INSERT INTO btrfs_dirs VALUES(9,88,?1,?2)",
            params![b"/parent".as_slice(), b"/".as_slice()],
        )
        .unwrap();
        c.execute_batch("INSERT INTO completed VALUES(12,7); UPDATE sqlite_sequence SET seq=99 WHERE name='files';").unwrap();
    }
    #[test]
    fn migration_preserves_ids_hashes_jobs_cursors_and_non_utf8_paths() {
        let c = Connection::open_in_memory().unwrap();
        legacy(&c);
        migrate(&c).unwrap();
        let paths: Vec<(i64, Vec<u8>)> = c
            .prepare("SELECT id,path FROM file_paths ORDER BY id")
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(
            paths,
            vec![(7, b"/parent/\xff".to_vec()), (12, b"/parent/b".to_vec())]
        );
        assert_eq!(
            c.query_row("SELECT COUNT(*) FROM path_parents", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            2
        );
        assert_eq!(
            c.query_row(
                "SELECT seq FROM sqlite_sequence WHERE name='files'",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            99
        );
        assert_eq!(
            c.query_row(
                "SELECT hex(cursor)||':'||low||':'||high||':'||completed FROM crawls WHERE id=9",
                [],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
            "ABCDF0:123:456:0"
        );
        assert_eq!(
            c.query_row("SELECT ino FROM discovery WHERE crawl_id=9", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            88
        );
        assert_eq!(
            c.query_row("SELECT source FROM completed WHERE target=12", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            7
        );
        assert_eq!(
            c.query_row(
                "SELECT group_concat(hex(hash)||':'||pending) FROM ranges ORDER BY id",
                [],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
            "CAFEBA:1,CAFEBA:0"
        );
        assert_eq!(
            c.query_row("SELECT COUNT(*) FROM chunk_cache", [], |r| r
                .get::<_, i64>(0))
                .unwrap(),
            2
        );
        assert!(!c
            .prepare("PRAGMA foreign_key_check")
            .unwrap()
            .exists([])
            .unwrap());
        assert_eq!(
            c.query_row("PRAGMA foreign_keys", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            1
        );
    }
    #[test]
    fn failed_migration_rolls_back_and_can_be_retried() {
        let c = Connection::open_in_memory().unwrap();
        legacy(&c);
        c.execute_batch("CREATE TABLE files_new(block_migration INTEGER)")
            .unwrap();
        assert!(migrate(&c).is_err());
        assert_eq!(
            c.query_row("PRAGMA user_version", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            4
        );
        assert_eq!(
            c.query_row(
                "SELECT COUNT(*) FROM files WHERE path IS NOT NULL",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
            2
        );
        assert!(c
            .query_row("SELECT COUNT(*) FROM path_parents", [], |r| r
                .get::<_, i64>(0))
            .is_err());
        assert_eq!(
            c.query_row("PRAGMA foreign_keys", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            1
        );
        c.execute_batch("DROP TABLE files_new").unwrap();
        migrate(&c).unwrap();
    }
}
