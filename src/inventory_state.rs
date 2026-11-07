//! Durable, derived sharing inventory. Scanner tables and cursors remain authoritative.
#[doc(hidden)]
pub use crate::index::SqlCache;
use anyhow::Result;
use rusqlite::Connection;

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
    tx.execute_batch("CREATE TABLE inventory_progress(id INTEGER PRIMARY KEY CHECK(id=1),cursor INTEGER NOT NULL,ceiling INTEGER NOT NULL,checked INTEGER NOT NULL DEFAULT 0,checked_bytes INTEGER NOT NULL DEFAULT 0,hardlinks INTEGER,indexed INTEGER NOT NULL,pending INTEGER NOT NULL);
      INSERT INTO inventory_progress(id,cursor,ceiling,indexed,pending) SELECT 1,0,coalesce(max(id),0),count(*),(SELECT count(*) FROM discovery) FROM files;
      CREATE TABLE inventory_dirty(file_id INTEGER PRIMARY KEY,revision INTEGER NOT NULL,retry_after INTEGER NOT NULL DEFAULT 0);
      CREATE INDEX inventory_dirty_ready ON inventory_dirty(retry_after);
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
    let invalidate = |id: &str| {
        format!("INSERT OR IGNORE INTO inventory_dirty_groups SELECT f.fskey,substr(e.descriptor,22,8) FROM file_extents e JOIN files f ON f.id=e.file_id WHERE e.file_id={id} AND substr(e.descriptor,21,1)=x'01'; DELETE FROM inventory_verified WHERE file_id={id};")
    };
    for event in ["INSERT", "UPDATE"] {
        tx.execute_batch(&format!("CREATE TRIGGER inventory_invalidate_{event} AFTER {event} ON inventory_dirty WHEN EXISTS(SELECT 1 FROM inventory_verified WHERE file_id=NEW.file_id) BEGIN {} END;", invalidate("NEW.file_id")))?;
    }
    let dirty = |id: &str| {
        format!("INSERT INTO inventory_dirty(file_id,revision) VALUES({id},1) ON CONFLICT(file_id) DO UPDATE SET revision=revision+1,retry_after=0;")
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
            tx.execute_batch(&format!("CREATE TRIGGER inventory_track_{table}_{event} {timing} {event} ON {table} BEGIN {body} END;"))?;
        }
    }
    for (event, row) in [("INSERT", "NEW"), ("DELETE", "OLD")] {
        tx.execute_batch(&format!("CREATE TRIGGER inventory_track_discovery_{event} AFTER {event} ON discovery BEGIN
          INSERT INTO inventory_dirty(file_id,revision) SELECT file_id,1 FROM btrfs_files WHERE crawl_id={row}.crawl_id AND ino={row}.ino
          ON CONFLICT(file_id) DO UPDATE SET revision=revision+1,retry_after=0; END;"))?;
    }
    for (table, field) in [("files", "indexed"), ("discovery", "pending")] {
        for (event, sign) in [("INSERT", "+"), ("DELETE", "-")] {
            tx.execute_batch(&format!("CREATE TRIGGER inventory_count_{table}_{event} AFTER {event} ON {table} BEGIN UPDATE inventory_progress SET {field}={field}{sign}1 WHERE id=1; END;"))?;
        }
    }
    tx.commit()?;
    Ok(())
}
