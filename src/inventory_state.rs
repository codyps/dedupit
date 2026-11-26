//! Durable, derived sharing inventory. Scanner tables and cursors remain authoritative.
#[doc(hidden)]
pub use crate::index::SqlCache;
use anyhow::Result;
use rusqlite::{Connection, Transaction};

/// Install transactional invalidation alongside the generation-driven scanner.
/// This is additive: no scan cursor, range, hash, or pending job is changed.
pub fn initialize(conn: &Connection) -> Result<()> {
    let installed: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='inventory_progress')",
        [],
        |r| r.get(0),
    )?;
    if installed {
        return Ok(());
    }
    eprintln!("Preparing incremental sharing inventory; retaining scan progress");
    let tx = conn.unchecked_transaction()?;
    tx.execute_batch("CREATE TABLE inventory_progress(id INTEGER PRIMARY KEY CHECK(id=1),cursor INTEGER NOT NULL,ceiling INTEGER NOT NULL,checked INTEGER NOT NULL DEFAULT 0,checked_bytes INTEGER NOT NULL DEFAULT 0,hardlinks INTEGER,indexed INTEGER NOT NULL,pending INTEGER NOT NULL,dirty_files INTEGER NOT NULL DEFAULT 0,dirty_groups INTEGER NOT NULL DEFAULT 0);
      INSERT INTO inventory_progress(id,cursor,ceiling,indexed,pending) SELECT 1,0,coalesce(max(id),0),count(*),(SELECT count(*) FROM discovery) FROM files;
      CREATE TABLE inventory_dirty_groups(fskey BLOB NOT NULL,physical BLOB NOT NULL,PRIMARY KEY(fskey,physical)) WITHOUT ROWID;
      CREATE TABLE inventory_verified(file_id INTEGER PRIMARY KEY REFERENCES files(id) ON DELETE CASCADE,size INTEGER NOT NULL,links INTEGER NOT NULL,mapped INTEGER NOT NULL,referenced INTEGER NOT NULL,saved INTEGER NOT NULL);
      CREATE TABLE inventory_groups(fskey BLOB NOT NULL,physical BLOB NOT NULL,mapped INTEGER NOT NULL,referenced INTEGER NOT NULL,saved INTEGER NOT NULL,physical_saved INTEGER NOT NULL,covered INTEGER NOT NULL,PRIMARY KEY(fskey,physical)) WITHOUT ROWID;
      CREATE TABLE inventory_totals(id INTEGER PRIMARY KEY CHECK(id=1),files INTEGER NOT NULL DEFAULT 0,bytes INTEGER NOT NULL DEFAULT 0,mapped INTEGER NOT NULL DEFAULT 0,referenced INTEGER NOT NULL DEFAULT 0,saved INTEGER NOT NULL DEFAULT 0,physical_saved INTEGER NOT NULL DEFAULT 0,covered INTEGER NOT NULL DEFAULT 0);
      INSERT INTO inventory_totals(id) VALUES(1);
      CREATE INDEX inventory_physical ON file_extents(substr(descriptor,22,8)) WHERE substr(descriptor,21,1)=x'01';")?;
    // An expression index reuses canonical extent records; no second extent or
    // chunk cache is copied into the inventory.
    for (table, fields) in [
        (
            "inventory_verified",
            vec![
                ("files", "1"),
                ("bytes", "size"),
                ("mapped", "mapped"),
                ("referenced", "referenced"),
                ("saved", "saved"),
            ],
        ),
        (
            "inventory_groups",
            vec![
                ("mapped", "mapped"),
                ("referenced", "referenced"),
                ("saved", "saved"),
                ("physical_saved", "physical_saved"),
                ("covered", "covered"),
            ],
        ),
    ] {
        for (event, row, sign) in [("INSERT", "NEW", "+"), ("DELETE", "OLD", "-")] {
            let updates = fields
                .iter()
                .map(|(total, field)| {
                    let value = if *field == "1" {
                        "1".to_owned()
                    } else {
                        format!("{row}.{field}")
                    };
                    format!("{total}={total}{sign}{value}")
                })
                .collect::<Vec<_>>()
                .join(",");
            tx.execute_batch(&format!("CREATE TRIGGER {table}_{event} AFTER {event} ON {table} BEGIN UPDATE inventory_totals SET {updates} WHERE id=1; END;"))?;
        }
    }
    create_file_queue(&tx)?;
    for (table, field) in [("files", "indexed"), ("discovery", "pending")] {
        for (event, sign) in [("INSERT", "+"), ("DELETE", "-")] {
            tx.execute_batch(&format!("CREATE TRIGGER inventory_count_{table}_{event} AFTER {event} ON {table} BEGIN UPDATE inventory_progress SET {field}={field}{sign}1 WHERE id=1; END;"))?;
        }
    }
    tx.commit()?;
    Ok(())
}

// Rebuild only derived verification work. Existing extent maps, scan cursors,
// verified rows, and queued allocation recalculations remain usable.
pub(crate) fn repair_file_queue(tx: &Transaction<'_>) -> Result<()> {
    let installed: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE name='inventory_progress')",
        [],
        |r| r.get(0),
    )?;
    if !installed {
        return Ok(());
    }
    tx.execute_batch(
        "ALTER TABLE inventory_progress ADD COLUMN dirty_files INTEGER NOT NULL DEFAULT 0;
         ALTER TABLE inventory_progress ADD COLUMN dirty_groups INTEGER NOT NULL DEFAULT 0;
         DROP TABLE inventory_dirty;
         DELETE FROM inventory_groups WHERE (fskey,physical) IN
           (SELECT fskey,physical FROM inventory_dirty_groups);
         UPDATE inventory_progress SET cursor=0,ceiling=(SELECT coalesce(max(id),0) FROM files),
           dirty_groups=(SELECT count(*) FROM inventory_dirty_groups) WHERE id=1;",
    )?;
    create_file_queue(tx)
}

fn create_file_queue(tx: &Transaction<'_>) -> Result<()> {
    tx.execute_batch(
        "CREATE TABLE inventory_dirty(
           file_id INTEGER PRIMARY KEY REFERENCES files(id) ON DELETE CASCADE,
           revision INTEGER NOT NULL,retry_after INTEGER NOT NULL DEFAULT 0);
         CREATE INDEX inventory_dirty_ready ON inventory_dirty(retry_after);
         CREATE TRIGGER inventory_queue_old_groups BEFORE DELETE ON inventory_verified BEGIN
           INSERT INTO inventory_dirty_groups
             SELECT f.fskey,substr(e.descriptor,22,8)
             FROM file_extents e JOIN files f ON f.id=e.file_id
             WHERE e.file_id=OLD.file_id AND substr(e.descriptor,21,1)=x'01'
             ON CONFLICT(fskey,physical) DO NOTHING;
         END;
         CREATE TRIGGER inventory_invalidate_group AFTER INSERT ON inventory_dirty_groups BEGIN
           DELETE FROM inventory_groups WHERE fskey=NEW.fskey AND physical=NEW.physical;
           UPDATE inventory_progress SET dirty_groups=dirty_groups+1 WHERE id=1;
         END;
         CREATE TRIGGER inventory_count_dirty_group_delete AFTER DELETE ON inventory_dirty_groups BEGIN
           UPDATE inventory_progress SET dirty_groups=dirty_groups-1 WHERE id=1;
         END;
         CREATE TRIGGER inventory_count_dirty_insert AFTER INSERT ON inventory_dirty BEGIN
           UPDATE inventory_progress SET dirty_files=dirty_files+1 WHERE id=1;
         END;
         CREATE TRIGGER inventory_count_dirty_delete AFTER DELETE ON inventory_dirty BEGIN
           UPDATE inventory_progress SET dirty_files=dirty_files-1 WHERE id=1;
         END;",
    )?;
    // A parent file's BEFORE DELETE trigger captures its affected allocations.
    // Cascading child deletes run after the file row is gone and cannot queue it.
    for event in ["INSERT", "UPDATE"] {
        tx.execute_batch(&format!(
            "CREATE TRIGGER inventory_invalidate_{event} AFTER {event} ON inventory_dirty BEGIN
               DELETE FROM inventory_verified WHERE file_id=NEW.file_id;
             END;"
        ))?;
    }
    let dirty = |id: &str| {
        format!("INSERT INTO inventory_dirty(file_id,revision) SELECT id,1 FROM files WHERE id={id} ON CONFLICT(file_id) DO UPDATE SET revision=revision+1,retry_after=0;")
    };
    for (table, id) in [
        ("files", "id"),
        ("extent_state", "file_id"),
        ("btrfs_files", "file_id"),
        ("file_extents", "file_id"),
    ] {
        for (event, row) in [("INSERT", "NEW"), ("DELETE", "OLD"), ("UPDATE", "OLD")] {
            // BEFORE preserves old allocation keys across deletes/rewrites.
            // INSERT needs AFTER so the newly allocated key is visible.
            let timing = if event == "INSERT" { "AFTER" } else { "BEFORE" };
            let mut body = dirty(&format!("{row}.{id}"));
            if event == "UPDATE" {
                body.push_str(&dirty(&format!("NEW.{id}")));
            }
            tx.execute_batch(&format!("DROP TRIGGER IF EXISTS inventory_track_{table}_{event}; CREATE TRIGGER inventory_track_{table}_{event} {timing} {event} ON {table} BEGIN {body} END;"))?;
        }
    }
    for (event, row) in [("INSERT", "NEW"), ("DELETE", "OLD")] {
        tx.execute_batch(&format!("DROP TRIGGER IF EXISTS inventory_track_discovery_{event}; CREATE TRIGGER inventory_track_discovery_{event} AFTER {event} ON discovery BEGIN
          INSERT INTO inventory_dirty(file_id,revision) SELECT b.file_id,1 FROM btrfs_files b JOIN files f ON f.id=b.file_id WHERE b.crawl_id={row}.crawl_id AND b.ino={row}.ino
          ON CONFLICT(file_id) DO UPDATE SET revision=revision+1,retry_after=0; END;"))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upgrade_discards_deleted_file_work_but_retains_allocation_rechecks() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "CREATE TABLE files(id INTEGER PRIMARY KEY, fskey BLOB);
             CREATE TABLE extent_state(file_id INTEGER REFERENCES files ON DELETE CASCADE);
             CREATE TABLE btrfs_files(file_id INTEGER REFERENCES files ON DELETE CASCADE,crawl_id INTEGER,ino INTEGER);
             CREATE TABLE file_extents(file_id INTEGER REFERENCES files ON DELETE CASCADE,descriptor BLOB);
             CREATE TABLE discovery(crawl_id INTEGER,ino INTEGER);
             CREATE TABLE inventory_progress(id INTEGER PRIMARY KEY,cursor INTEGER,ceiling INTEGER);
             CREATE TABLE inventory_dirty(file_id INTEGER PRIMARY KEY,revision INTEGER,retry_after INTEGER);
             CREATE TABLE inventory_dirty_groups(fskey BLOB,physical BLOB,PRIMARY KEY(fskey,physical));
             CREATE TABLE inventory_groups(fskey BLOB,physical BLOB,PRIMARY KEY(fskey,physical));
             CREATE TABLE inventory_verified(file_id INTEGER PRIMARY KEY REFERENCES files ON DELETE CASCADE);
             INSERT INTO files VALUES(500,x'01'),(600,x'01');
             INSERT INTO extent_state VALUES(600);
             INSERT INTO inventory_progress VALUES(1,550,550);
             INSERT INTO inventory_verified VALUES(500),(600);
             INSERT INTO inventory_dirty VALUES(1,3,0),(2,3,0);
             INSERT INTO inventory_groups VALUES(x'01',x'11'),(x'01',x'22');
             INSERT INTO inventory_dirty_groups VALUES(x'01',x'11');
             PRAGMA user_version=7;",
        )
        .unwrap();

        crate::schema::migrate(&conn, 7).unwrap();
        let counts = || {
            conn.query_row(
                "SELECT dirty_files, (SELECT count(*) FROM inventory_dirty),
                  dirty_groups, (SELECT count(*) FROM inventory_dirty_groups) FROM inventory_progress",
                [],
                |r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        r.get::<_, i64>(1)?,
                        r.get::<_, i64>(2)?,
                        r.get::<_, i64>(3)?,
                    ))
                },
            )
            .unwrap()
        };
        assert_eq!(counts(), (0, 0, 1, 1));
        let resumed: (i64, i64, i64, i64) = conn
            .query_row(
                "SELECT cursor,ceiling,(SELECT count(*) FROM inventory_verified),
                  (SELECT count(*) FROM inventory_groups) FROM inventory_progress",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap();
        assert_eq!(resumed, (0, 600, 2, 1));
        assert!(conn
            .execute("INSERT INTO inventory_dirty VALUES(3,1,0)", [])
            .is_err());

        conn.execute_batch(
            "INSERT INTO file_extents VALUES
               (600,CAST(zeroblob(20) || x'01' || x'0102030405060708' AS BLOB));
             DELETE FROM inventory_dirty_groups WHERE physical=x'0102030405060708';
             INSERT INTO inventory_verified VALUES(600);
             INSERT INTO inventory_groups VALUES(x'01',x'0102030405060708');
             UPDATE files SET fskey=fskey WHERE id=600;",
        )
        .unwrap();
        assert_eq!(counts(), (1, 1, 2, 2));
        let groups: i64 = conn
            .query_row("SELECT count(*) FROM inventory_groups", [], |r| r.get(0))
            .unwrap();
        assert_eq!(groups, 1);
        conn.execute("DELETE FROM files WHERE id=600", []).unwrap();
        assert_eq!(counts(), (0, 0, 2, 2));
    }
}
