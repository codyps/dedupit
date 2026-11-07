//! SQLite storage policy and offline, atomic compaction.
use anyhow::{ensure, Context, Result};
use fs2::FileExt;
use rusqlite::{Connection, OpenFlags};
use std::{
    fs::{self, File, OpenOptions},
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
};

#[derive(Clone, Copy, Debug, Default, clap::ValueEnum)]
pub enum Storage {
    #[default]
    Cow,
    Nocow,
    Compressed,
    /// Compressed COW main database, uncompressed NOCOW WAL.
    Hybrid,
}
fn sidecar(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_os_string();
    name.push(suffix);
    name.into()
}

fn configure(file: &File, nocow: bool, compressed: bool) -> Result<()> {
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;
        let fd = file.as_raw_fd();
        let mut stat = std::mem::MaybeUninit::<libc::statfs>::uninit();
        // SAFETY: live file descriptor and writable statfs storage.
        ensure!(
            unsafe { libc::fstatfs(fd, stat.as_mut_ptr()) } == 0,
            "inspect index filesystem: {}",
            std::io::Error::last_os_error()
        );
        // SAFETY: fstatfs succeeded.
        if unsafe { stat.assume_init() }.f_type as u64 != 0x9123683e {
            return Ok(());
        }
        let mut flags: libc::c_long = 0;
        // SAFETY: FS_IOC_GETFLAGS writes flags to the supplied live buffer.
        ensure!(
            unsafe { libc::ioctl(fd, 0x80086601u64, &mut flags) } == 0,
            "read index flags: {}",
            std::io::Error::last_os_error()
        );
        if nocow && flags & 0x800000 == 0 {
            ensure!(
                file.metadata()?.len() == 0,
                "NOCOW requires an empty file; run dedupit compact with the service stopped"
            );
            flags = (flags & !(4 | 0x400)) | 0x800000;
            // SAFETY: FS_IOC_SETFLAGS reads flags from a live buffer.
            ensure!(
                unsafe { libc::ioctl(fd, 0x40086602u64, &flags) } == 0,
                "set index NOCOW: {}",
                std::io::Error::last_os_error()
            );
        }
        if compressed {
            ensure!(
                flags & 0x800000 == 0,
                "compression requires COW; run dedupit compact with the service stopped"
            );
            // SAFETY: fixed NUL-terminated attribute name and four-byte value.
            ensure!(
                unsafe {
                    libc::fsetxattr(
                        fd,
                        c"btrfs.compression".as_ptr(),
                        b"zstd".as_ptr().cast(),
                        4,
                        0,
                    )
                } == 0,
                "set index compression: {}",
                std::io::Error::last_os_error()
            );
        }
    }
    #[cfg(not(target_os = "linux"))]
    let _ = (file, nocow, compressed);
    Ok(())
}

pub(crate) fn prepare(database: &Path, main: &File, storage: Storage) -> Result<()> {
    configure(
        main,
        matches!(storage, Storage::Nocow),
        matches!(storage, Storage::Compressed | Storage::Hybrid),
    )?;
    if matches!(storage, Storage::Nocow | Storage::Hybrid) {
        let wal = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(sidecar(database, "-wal"))?;
        ensure!(wal.metadata()?.is_file(), "WAL is not a regular file");
        configure(&wal, true, false)?;
    }
    Ok(())
}

/// Rebuild and atomically replace an idle index, preserving every durable ID,
/// hash, scan cursor, pending job, and completed job. The original remains usable
/// until the fully validated and synced replacement is renamed over it.
pub fn compact(database: &Path, storage: Storage) -> Result<()> {
    let database = fs::canonicalize(database)?;
    let parent = database.parent().context("index has no parent")?;
    let service = parent.join("service.lock");
    let _service = if service.exists() {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(service)?;
        file.try_lock_exclusive()
            .context("stop the dedupit service before compaction")?;
        Some(file)
    } else {
        None
    };
    let lock = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(sidecar(&database, ".dedupit-lock"))?;
    lock.try_lock_exclusive()
        .context("index is already in use")?;
    let original = OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&database)?;
    original
        .try_lock_exclusive()
        .context("database inode is already in use")?;
    let conn = Connection::open(&database)?;
    let version: i64 = conn.query_row("PRAGMA user_version", [], |r| r.get(0))?;
    ensure!(
        (4..=6).contains(&version),
        "compact supports schema versions 4 through 6"
    );
    crate::paths::functions(&conn)?;
    conn.execute_batch(
        "PRAGMA synchronous=FULL; PRAGMA temp_store=FILE; PRAGMA cache_size=-16384;",
    )?;
    if version < 5 {
        crate::paths::migrate(&conn)?;
    }
    if version < 6 {
        crate::records::migrate(&conn)?;
    }
    let (busy, _, _): (i64, i64, i64) =
        conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |r| {
            Ok((r.get(0)?, r.get(1)?, r.get(2)?))
        })?;
    ensure!(
        busy == 0,
        "index has an active reader; stop it before compaction"
    );
    let target = sidecar(&database, ".compact");
    let output = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&target)
        .context("create compact output (remove an abandoned .compact file before retrying)")?;
    let result = (|| -> Result<()> {
        configure(
            &output,
            matches!(storage, Storage::Nocow),
            matches!(storage, Storage::Compressed | Storage::Hybrid),
        )?;
        eprintln!("Compacting index; original retained until replacement is validated");
        conn.execute(
            "VACUUM INTO ?1",
            [target
                .to_str()
                .context("SQLite compact output path must be UTF-8")?],
        )?;
        output.sync_all()?;
        eprintln!("Validating compacted index");
        let check = Connection::open_with_flags(&target, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        let integrity: String = check.query_row("PRAGMA integrity_check", [], |r| r.get(0))?;
        ensure!(
            integrity == "ok",
            "compact output integrity check failed: {integrity}"
        );
        ensure!(
            !check.prepare("PRAGMA foreign_key_check")?.exists([])?,
            "compact output has foreign key violations"
        );
        drop(check);
        drop(conn);
        ensure!(
            !sidecar(&database, "-wal").exists(),
            "index WAL remained after close; original was not replaced"
        );
        fs::rename(&target, &database)?;
        File::open(parent)?.sync_all()?;
        eprintln!("Index compaction complete; scan progress retained");
        Ok(())
    })();
    if result.is_err() {
        let _ = fs::remove_file(target);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn compaction_keeps_hashes_and_scan_progress_and_refuses_a_live_index() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("data");
        fs::create_dir(&root).unwrap();
        fs::write(root.join("a"), vec![42; 8192]).unwrap();
        fs::write(root.join("b"), vec![42; 8192]).unwrap();
        let options = crate::Options {
            database: dir.path().join("index.sqlite"),
            roots: vec![root],
            chunk_size: 4096,
            granularity: crate::Granularity::Both,
            verify: false,
            scanner: crate::Scanner::Portable,
            commit_batch_size: 64,
            storage: Storage::Cow,
        };
        let first = crate::run(&options, false).unwrap();
        assert_eq!(first.files_hashed, 2);
        let index = crate::index::Index::open(&options).unwrap();
        assert!(compact(&options.database, Storage::Cow).is_err());
        drop(index);
        let before = Connection::open(&options.database)
            .unwrap()
            .query_row(
                "SELECT group_concat(id||':'||file_id||':'||hex(hash)) FROM ranges",
                [],
                |r| r.get::<_, String>(0),
            )
            .unwrap();
        compact(&options.database, Storage::Cow).unwrap();
        let after = Connection::open(&options.database)
            .unwrap()
            .query_row(
                "SELECT group_concat(id||':'||file_id||':'||hex(hash)) FROM ranges",
                [],
                |r| r.get::<_, String>(0),
            )
            .unwrap();
        assert_eq!(before, after);
        let next = crate::run(&options, false).unwrap();
        assert_eq!(next.bytes_hashed, 0);
        assert_eq!(next.duplicate_files, first.duplicate_files);
        assert!(!sidecar(&options.database, ".compact").exists());
    }
}
