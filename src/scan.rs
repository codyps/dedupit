use crate::{
    backend,
    index::{path_bytes, Index, Interleaver, ScanPhase, SqlCache},
    Granularity, Options, Stats,
};
use anyhow::{ensure, Context, Result};
use rusqlite::{params, OptionalExtension};
use std::{
    fs::{self, File, Metadata, OpenOptions},
    io,
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::{
            ffi::OsStrExt,
            fs::{FileExt, MetadataExt, OpenOptionsExt},
        },
    },
    path::{Component, Path},
};
use walkdir::WalkDir;

pub(crate) fn fingerprint(meta: &Metadata) -> Vec<u8> {
    [
        meta.dev(),
        meta.ino(),
        meta.len(),
        meta.mtime() as u64,
        meta.mtime_nsec() as u64,
        meta.ctime() as u64,
        meta.ctime_nsec() as u64,
    ]
    .into_iter()
    .flat_map(u64::to_le_bytes)
    .collect()
}

/// Open without following symlinks in any path component. O_NONBLOCK prevents
/// a raced replacement by a FIFO from
/// hanging the process; fstat rejects all non-regular leaf objects.
pub(crate) fn open_regular(path: &Path, write: bool) -> io::Result<File> {
    if !path.is_absolute()
        || path
            .components()
            .any(|c| !matches!(c, Component::RootDir | Component::Normal(_)))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "expected a normalized absolute path",
        ));
    }
    #[cfg(target_os = "linux")]
    if let Some(file) = open_regular_linux(path, write)? {
        return Ok(file);
    }
    open_regular_components(path, write)
}

/// Resolve all components in one syscall with the same no-symlink guarantee as
/// the component walk. Older kernels, syscall filters and long paths retain
/// the portable walk. No directory-descriptor cache can hide renamed ancestors.
#[cfg(target_os = "linux")]
fn open_regular_linux(path: &Path, write: bool) -> io::Result<Option<File>> {
    let bytes = path.as_os_str().as_bytes();
    let mut name = [0; 4096];
    if bytes.len() >= name.len() {
        return Ok(None);
    }
    name[..bytes.len()].copy_from_slice(bytes);
    let name = std::ffi::CStr::from_bytes_with_nul(&name[..=bytes.len()])
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
    // SAFETY: open_how is a C struct of integers; all-zero is its required
    // initial state, including any padding/future fields.
    let mut how: libc::open_how = unsafe { std::mem::zeroed() };
    how.flags = ((if write { libc::O_RDWR } else { libc::O_RDONLY })
        | libc::O_CLOEXEC
        | libc::O_NOFOLLOW
        | libc::O_NONBLOCK
        | libc::O_NOATIME) as u64;
    how.resolve = libc::RESOLVE_NO_SYMLINKS;
    loop {
        // SAFETY: name is NUL-terminated; how is fully initialized with no
        // creation flags and its exact ABI size. AT_FDCWD ignores cwd for this
        // absolute path; NO_SYMLINKS applies to every component, not just leaf.
        let fd = unsafe {
            libc::syscall(
                libc::SYS_openat2,
                libc::AT_FDCWD,
                name.as_ptr(),
                &how as *const libc::open_how,
                std::mem::size_of::<libc::open_how>(),
            )
        };
        if fd >= 0 {
            // SAFETY: openat2 returned a new owned fd, representable as c_int.
            let file = unsafe { File::from_raw_fd(fd as libc::c_int) };
            if !file.metadata()?.is_file() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "not a regular file",
                ));
            }
            return Ok(Some(file));
        }
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::EPERM) && how.flags & libc::O_NOATIME as u64 != 0 {
            how.flags &= !(libc::O_NOATIME as u64);
            continue;
        }
        return if error.raw_os_error().is_some_and(|code| {
            [
                libc::ENOSYS,
                libc::EINVAL,
                libc::E2BIG,
                libc::EPERM,
                libc::ENAMETOOLONG,
            ]
            .contains(&code)
        }) {
            Ok(None)
        } else {
            Err(error)
        };
    }
}

fn open_regular_components(path: &Path, write: bool) -> io::Result<File> {
    let mut components = path.components().peekable();
    if !path.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "expected an absolute path",
        ));
    }
    let mut parent = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_CLOEXEC)
        .open("/")?;
    let mut inline_name = [0; 256];
    while let Some(component) = components.next() {
        let Component::Normal(name) = component else {
            if matches!(component, Component::RootDir) {
                continue;
            }
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "non-normal path component",
            ));
        };
        let bytes = name.as_bytes();
        let heap_name;
        // Most Unix names fit in NAME_MAX=255. Retain an allocating fallback
        // for filesystems with longer components, and reject interior NULs in
        // either case. Never follow a symlink to save path-opening work.
        let name = if bytes.len() < inline_name.len() {
            inline_name[..bytes.len()].copy_from_slice(bytes);
            inline_name[bytes.len()] = 0;
            std::ffi::CStr::from_bytes_with_nul(&inline_name[..=bytes.len()])
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?
        } else {
            heap_name = std::ffi::CString::new(bytes)?;
            &heap_name
        };
        let last = components.peek().is_none();
        let access = if last && write {
            libc::O_RDWR
        } else {
            libc::O_RDONLY
        };
        #[cfg(target_os = "linux")]
        let no_atime = if last { libc::O_NOATIME } else { 0 };
        #[cfg(not(target_os = "linux"))]
        let no_atime = 0;
        let flags = access
            | libc::O_NOFOLLOW
            | libc::O_CLOEXEC
            | libc::O_NONBLOCK
            | no_atime
            | if last { 0 } else { libc::O_DIRECTORY };
        // SAFETY: parent is a live directory fd; name is NUL-terminated; flags
        // do not include O_CREAT, so no variadic mode argument is needed.
        let mut fd = unsafe { libc::openat(parent.as_raw_fd(), name.as_ptr(), flags) };
        if fd < 0 && no_atime != 0 && io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
        {
            // O_NOATIME needs ownership/CAP_FOWNER. Retain normal read access
            // for callers lacking that optional permission.
            // SAFETY: same directory/name and non-creating flags as above.
            fd = unsafe { libc::openat(parent.as_raw_fd(), name.as_ptr(), flags & !no_atime) };
        }
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: openat returned an owned, previously unwrapped descriptor.
        parent = unsafe { File::from_raw_fd(fd) };
    }
    if !parent.metadata()?.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "not a regular file",
        ));
    }
    Ok(parent)
}

pub(crate) fn scan(
    index: &mut Index,
    options: &Options,
    stats: &mut Stats,
    sharing: &mut Interleaver<'_>,
) -> Result<()> {
    #[cfg(target_os = "linux")]
    if options.scanner != crate::Scanner::Portable
        && !options.verify
        && crate::btrfs_scan::scan(index, options, stats, sharing)?
    {
        return Ok(());
    }
    ensure!(
        options.scanner != crate::Scanner::Btrfs || options.verify,
        "btrfs generation discovery is unavailable on this platform or filesystem"
    );
    // A portable reconciliation invalidates native discovery positions. A later
    // native run must establish a fresh baseline, even if fingerprints match.
    index
        .conn
        .execute_batch("DELETE FROM crawls; DELETE FROM extent_state; DELETE FROM file_extents; DELETE FROM apply_mappings;")?;
    portable(index, options, stats, sharing)
}

fn portable(
    index: &mut Index,
    options: &Options,
    stats: &mut Stats,
    sharing: &mut Interleaver<'_>,
) -> Result<()> {
    index.scan_phase = ScanPhase::Portable;
    let mut traversal_complete = true;
    let excluded_ids: Vec<_> = index
        .excluded
        .iter()
        .filter_map(|path| fs::symlink_metadata(path).ok())
        .map(|meta| (meta.dev(), meta.ino()))
        .collect();
    sharing.between_batches(index, stats)?;
    for root in index.roots.clone() {
        for entry in WalkDir::new(&root).follow_links(false).max_open(32) {
            index.progress(stats)?;
            let entry = match entry {
                Ok(entry) => entry,
                Err(error) => {
                    stats.errors.push(error.to_string());
                    traversal_complete = false;
                    continue;
                }
            };
            if !entry.file_type().is_file() || index.excluded.iter().any(|p| p == entry.path()) {
                continue;
            }
            stats.files_seen += 1;
            index.begin_batch("portable")?;
            let bytes_before = stats.bytes_hashed;
            if let Err(error) = index_file(index, entry.path(), options, stats, &excluded_ids, None)
            {
                index
                    .check_batch()
                    .with_context(|| format!("fatal scan {:?}: {error:#}", entry.path()))?;
                stats
                    .errors
                    .push(format!("scan {:?}: {error:#}", entry.path()));
            }
            index.batch_step(stats.bytes_hashed.saturating_sub(bytes_before))?;
            sharing.between_batches(index, stats)?;
        }
    }
    index.flush_batch()?;
    // A traversal error must not make an unreadable subtree look deleted.
    if traversal_complete {
        stats.files_removed = index.conn.execute(
            "DELETE FROM files WHERE id NOT IN (SELECT file_id FROM seen)",
            [],
        )? as u64;
    }
    Ok(())
}

pub(crate) struct NativeView {
    pub incarnation: Vec<u8>,
    pub extents: Vec<Extent>,
    pub cow: bool,
    pub sharing_policy: u64,
    pub fingerprint: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Extent {
    pub offset: u64,
    pub length: u64,
    pub descriptor: Vec<u8>,
}

pub(crate) fn index_file(
    index: &mut Index,
    path: &Path,
    options: &Options,
    stats: &mut Stats,
    excluded_ids: &[(u64, u64)],
    native: Option<&NativeView>,
) -> Result<()> {
    let result = index_file_inner(index, path, options, stats, excluded_ids, native);
    if result.is_err() {
        index.discard_file(path).context("invalidate failed file")?;
    }
    result
}

// A standalone call owns its transaction. Within a batch, failure cleanup in
// index_file removes the entire failed file before any commit, so a long-lived
// nested savepoint (and its per-page rollback journal) is unnecessary.
enum FileUpdate<'a> {
    Standalone(rusqlite::Savepoint<'a>),
    Batched(&'a rusqlite::Connection),
}
impl<'a> FileUpdate<'a> {
    fn new(conn: &'a mut rusqlite::Connection) -> rusqlite::Result<Self> {
        if conn.is_autocommit() {
            Ok(Self::Standalone(conn.savepoint()?))
        } else {
            Ok(Self::Batched(conn))
        }
    }
    fn commit(self) -> rusqlite::Result<()> {
        match self {
            Self::Standalone(tx) => tx.commit(),
            Self::Batched(_) => Ok(()),
        }
    }
}
impl std::ops::Deref for FileUpdate<'_> {
    type Target = rusqlite::Connection;
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Standalone(tx) => tx,
            Self::Batched(conn) => conn,
        }
    }
}

fn index_file_inner(
    index: &mut Index,
    path: &Path,
    options: &Options,
    stats: &mut Stats,
    excluded_ids: &[(u64, u64)],
    native: Option<&NativeView>,
) -> Result<()> {
    // Check aliases by inode before opening anything. Even a read-only open /
    // close of SQLite's main or SHM file can discard its process-wide locks.
    let candidate = fs::symlink_metadata(path)?;
    if index.is_excluded(path, &candidate)
        || excluded_ids.contains(&(candidate.dev(), candidate.ino()))
    {
        return Ok(());
    }
    let file = open_regular(path, false)?;
    let before = file.metadata()?;
    if index.is_excluded(path, &before) || excluded_ids.contains(&(before.dev(), before.ino())) {
        index.defer_close(file);
        return Ok(());
    }

    ensure!(
        before.len() <= i64::MAX as u64,
        "file too large for the index"
    );
    let version = fingerprint(&before);
    if let Some(view) = native {
        ensure!(
            view.fingerprint == version,
            "file changed during extent lookup"
        );
    }
    let existing: Option<(i64, Vec<u8>, Vec<u8>, bool)> = index.conn.query_row_cached(
        "SELECT id,fingerprint,path,EXISTS(SELECT 1 FROM seen WHERE file_id=file_paths.id) FROM file_paths WHERE dev=?1 AND ino=?2",
        params![before.dev() as i64, before.ino() as i64],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?,r.get(3)?)),
    ).optional()?;
    if let (Some((id, ..)), Some(view)) = (&existing, native) {
        crate::index::save_sharing_policy(&index.conn, *id, view.sharing_policy)?;
    }
    if let Some((_, _, _, seen)) = existing.as_ref() {
        if *seen {
            stats.hardlinks_skipped += 1;
            return Ok(());
        }
    }
    let baseline: Option<Vec<u8>> = if let (Some((id, ..)), Some(_)) = (&existing, native) {
        index
            .conn
            .query_row_cached(
                "SELECT incarnation FROM extent_state WHERE file_id=?1",
                [id],
                |r| r.get(0),
            )
            .optional()?
    } else {
        None
    };
    let reusable =
        native.is_some_and(|view| !options.verify && baseline.as_ref() == Some(&view.incarnation));
    if let Some((id, previous, previous_path, _)) = existing.as_ref() {
        if !options.verify
            && version == *previous
            && path_bytes(path) == previous_path
            && (native.is_none() || reusable)
        {
            // TEMP storage avoids a durable update/fsync per unchanged file.
            index
                .conn
                .execute_cached("INSERT INTO seen VALUES (?1)", [id])?;
            if let Some(view) = native {
                let tx = FileUpdate::new(&mut index.conn)?;
                reconcile_provisional_mapping(&tx, *id, view)?;
                store_extents(&tx, *id, view)?;
                tx.commit()?;
            }
            stats.files_unchanged += 1;
            return Ok(());
        }
    }
    let tx = FileUpdate::new(&mut index.conn)?;
    let (parent, name) = crate::paths::intern(&tx, path)?;
    // A name may now refer to a different inode. Invalidate it atomically with
    // insertion/rename of the current inode's row.
    tx.execute_cached(
        "DELETE FROM files WHERE parent_id=?1 AND name=?2 AND (dev!=?3 OR ino!=?4)",
        params![parent, name, before.dev() as i64, before.ino() as i64],
    )?;
    if let Some((id, previous, _, _)) = existing.as_ref() {
        if !options.verify && version == *previous && native.is_none() {
            tx.execute_cached(
                "UPDATE files SET parent_id=?1,name=?2 WHERE id=?3",
                params![parent, name, id],
            )?;
            tx.execute_cached("INSERT INTO seen VALUES (?1)", [id])?;
            tx.commit()?;
            stats.files_unchanged += 1;
            return Ok(());
        }
    }
    let fs = backend::filesystem(&file)?;
    let old_size = existing
        .as_ref()
        .map(|(_, fp, ..)| u64::from_le_bytes(fp[16..24].try_into().unwrap()));
    let file_id = if let Some((id, _, _, _)) = existing {
        tx.execute_cached(
            "UPDATE files SET parent_id=?1,name=?2,fingerprint=?3,fskey=?4,alignment=?5 WHERE id=?6",
            params![parent, name, version, fs.key, fs.alignment, id],
        )?;
        id
    } else {
        tx.execute_cached("INSERT INTO files(parent_id,name,dev,ino,fingerprint,fskey,alignment) VALUES (?1,?2,?3,?4,?5,?6,?7)",
            params![parent, name, before.dev() as i64, before.ino() as i64, version, fs.key, fs.alignment])?;
        tx.last_insert_rowid()
    };
    if let Some(view) = native {
        crate::index::save_sharing_policy(&tx, file_id, view.sharing_policy)?;
    }
    tx.execute_cached("INSERT INTO seen VALUES (?1)", [file_id])?;
    // Provisional apply maps may outlive the filesystem transaction that
    // produced them. With changed contents they cannot certify clean chunks.
    // An unchanged fingerprint can still refresh them above without rehashing.
    let provisional = tx.query_row_cached(
        "SELECT EXISTS(SELECT 1 FROM apply_mappings WHERE file_id=?1)",
        [file_id],
        |r| r.get::<_, bool>(0),
    )?;
    let full = !reusable || provisional || native.is_none_or(|view| !view.cow);
    let dirty = if full {
        if before.len() == 0 {
            vec![]
        } else {
            vec![(0, before.len())]
        }
    } else {
        let mut statement = tx.prepare_cached(
            "SELECT offset,length,descriptor FROM file_extents WHERE file_id=?1 ORDER BY offset",
        )?;
        let old = statement
            .query_map([file_id], |r| {
                Ok(Extent {
                    offset: r.get(0)?,
                    length: r.get(1)?,
                    descriptor: r.get(2)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        dirty_ranges(
            &old,
            &native.unwrap().extents,
            old_size.unwrap_or(0),
            before.len(),
            options.chunk_size as u64,
        )
    };
    if full {
        tx.execute_cached("DELETE FROM ranges WHERE file_id=?1", [file_id])?;
        tx.execute_cached("DELETE FROM chunk_cache_extra WHERE file_id=?1", [file_id])?;
    } else {
        for (start, end) in &dirty {
            tx.execute_cached(
                "DELETE FROM ranges WHERE file_id=?1 AND kind=0 AND offset>=?2 AND offset<?3",
                params![file_id, start, end],
            )?;
            tx.execute_cached(
                "DELETE FROM chunk_cache_extra WHERE file_id=?1 AND offset>=?2 AND offset<?3",
                params![file_id, start, end],
            )?;
        }
        tx.execute_cached(
            "DELETE FROM ranges WHERE file_id=?1 AND kind=0 AND offset>=?2",
            params![file_id, before.len()],
        )?;
        tx.execute_cached(
            "DELETE FROM chunk_cache_extra WHERE file_id=?1 AND offset>=?2",
            params![file_id, before.len()],
        )?;
        stats.chunks_reused += tx.query_row_cached(
            "SELECT COUNT(*) FROM chunk_cache WHERE file_id=?1",
            [file_id],
            |r| r.get::<_, u64>(0),
        )?;
        if !dirty.is_empty() || old_size != Some(before.len()) {
            tx.execute_cached("DELETE FROM ranges WHERE file_id=?1 AND kind=1", [file_id])?;
        }
    }
    // One lazily grown scratch buffer per run, shared by portable and native
    // scans. Small/empty files and metadata-only changes need no full-size
    // allocation, and unchanged files never touch the buffer.
    let observer = index.observer.clone();
    let buffer = &mut index.buffer;
    if !dirty.is_empty() {
        let needed = before.len().min(options.chunk_size as u64) as usize;
        if buffer.len() < needed {
            buffer.reserve_exact(needed - buffer.len());
            buffer.resize(needed, 0);
        }
    }
    let mut sparse_supported = true;
    let mut insert = tx.prepare_cached(
        "INSERT INTO ranges(file_id,kind,offset,length,hash) VALUES (?1,?2,?3,?4,?5)",
    )?;
    let mut cache = tx.prepare_cached(
        "INSERT INTO chunk_cache_extra(file_id,offset,length,hash,nonzero) VALUES (?1,?2,?3,?4,?5)",
    )?;
    for (start, end) in &dirty {
        let mut offset = *start;
        while offset < *end {
            if let Some(observer) = &observer {
                observer.progress(stats)?;
            }
            if sparse_supported {
                match next_data(&file, offset) {
                    Ok(Some(data)) => {
                        let next = data / options.chunk_size as u64 * options.chunk_size as u64;
                        let next = next.min(*end);
                        if next > offset {
                            stats.hole_bytes_skipped += next - offset;
                            offset = next;
                        }
                    }
                    Ok(None) => {
                        stats.hole_bytes_skipped += *end - offset;
                        break;
                    }
                    Err(error)
                        if error.raw_os_error().is_some_and(|code| {
                            [libc::EINVAL, libc::ENOTSUP, libc::EOPNOTSUPP, libc::ENOSYS]
                                .contains(&code)
                        }) =>
                    {
                        sparse_supported = false;
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            if offset >= *end {
                break;
            }
            let length = (*end - offset).min(options.chunk_size as u64) as usize;
            file.read_exact_at(&mut buffer[..length], offset)
                .context("read chunk")?;
            stats.bytes_hashed += length as u64;
            let hash = blake3::hash(&buffer[..length]);
            let nonzero = buffer[..length].iter().any(|byte| *byte != 0);
            // Candidate ranges are also the canonical chunk cache. Retain a
            // separate row only for chunks which cannot be candidates.
            if nonzero && options.granularity != Granularity::File {
                insert.execute(params![
                    file_id,
                    0,
                    offset,
                    length as u64,
                    hash.as_bytes().as_slice()
                ])?;
            } else {
                cache.execute(params![
                    file_id,
                    offset,
                    length as u64,
                    hash.as_bytes().as_slice(),
                    nonzero
                ])?;
            }
            offset += length as u64;
        }
    }
    drop(cache);
    if options.granularity != Granularity::Chunk
        && before.len() != 0
        && (full || !dirty.is_empty() || old_size != Some(before.len()))
    {
        let mut whole = blake3::Hasher::new_derive_key("dedupit whole-file chunk tree v1");
        whole.update(&before.len().to_le_bytes());
        let mut statement=tx.prepare_cached("SELECT offset,length,hash FROM chunk_cache WHERE file_id=?1 AND nonzero=1 ORDER BY offset")?;
        let mut rows = statement.query([file_id])?;
        while let Some(row) = rows.next()? {
            whole.update(&row.get::<_, u64>(0)?.to_le_bytes());
            whole.update(&row.get::<_, u64>(1)?.to_le_bytes());
            whole.update(row.get_ref(2)?.as_blob()?);
        }
        insert.execute(params![
            file_id,
            1,
            0,
            before.len(),
            whole.finalize().as_bytes().as_slice()
        ])?;
    }
    drop(insert);
    if let Some(view) = native {
        store_extents(&tx, file_id, view)?;
    } else {
        tx.execute_cached("DELETE FROM extent_state WHERE file_id=?1", [file_id])?;
        tx.execute_cached("DELETE FROM file_extents WHERE file_id=?1", [file_id])?;
    }
    #[cfg(feature = "test-failpoints")]
    crate::failpoints::hit(&tx, "file:hashed")?;
    ensure!(
        fingerprint(&file.metadata()?) == version,
        "file changed while hashing; retry on next run"
    );
    // Do not commit the hash under a path that was swapped during the scan.
    let after_path = fs::symlink_metadata(path)?;
    ensure!(
        after_path.is_file()
            && after_path.dev() == before.dev()
            && after_path.ino() == before.ino(),
        "path changed while hashing"
    );
    tx.commit()?;
    if full || !dirty.is_empty() {
        stats.files_hashed += 1;
    } else {
        stats.files_unchanged += 1;
    }
    Ok(())
}

fn reconcile_provisional_mapping(
    tx: &rusqlite::Connection,
    file_id: i64,
    view: &NativeView,
) -> Result<()> {
    let provisional = tx.query_row_cached(
        "SELECT EXISTS(SELECT 1 FROM apply_mappings WHERE file_id=?1)",
        [file_id],
        |r| r.get::<_, bool>(0),
    )?;
    if !provisional {
        return Ok(());
    }
    let mut statement = tx.prepare_cached(
        "SELECT offset,length,descriptor FROM file_extents WHERE file_id=?1 ORDER BY offset",
    )?;
    let mut rows = statement.query([file_id])?;
    let mut same = true;
    for extent in &view.extents {
        let Some(row) = rows.next()? else {
            same = false;
            break;
        };
        if row.get::<_, u64>(0)? != extent.offset
            || row.get::<_, u64>(1)? != extent.length
            || row.get_ref(2)?.as_blob()? != extent.descriptor
        {
            same = false;
            break;
        }
    }
    same &= rows.next()?.is_none();
    drop(rows);
    drop(statement);
    if !same {
        // SQLite may have committed a completion before a power loss rolled
        // back btrfs sharing. Retain valid hashes but retry the uncertain work.
        tx.execute_cached(
            "DELETE FROM completed WHERE target IN (SELECT id FROM ranges WHERE file_id=?1)",
            [file_id],
        )?;
        tx.execute_cached("UPDATE ranges SET pending=1 WHERE file_id=?1", [file_id])?;
    }
    Ok(())
}

pub(crate) fn store_extents(
    tx: &rusqlite::Connection,
    file_id: i64,
    view: &NativeView,
) -> Result<()> {
    tx.execute_cached("DELETE FROM apply_mappings WHERE file_id=?1", [file_id])?;
    tx.execute_cached("DELETE FROM file_extents WHERE file_id=?1", [file_id])?;
    for extent in &view.extents {
        tx.execute_cached(
            "INSERT INTO file_extents VALUES (?1,?2,?3,?4)",
            params![file_id, extent.offset, extent.length, extent.descriptor],
        )?;
    }
    tx.execute_cached(
        "INSERT OR REPLACE INTO extent_state VALUES (?1,?2)",
        params![file_id, view.incarnation],
    )?;
    Ok(())
}

fn dirty_ranges(
    old: &[Extent],
    new: &[Extent],
    old_size: u64,
    size: u64,
    chunk: u64,
) -> Vec<(u64, u64)> {
    use std::collections::BTreeMap;
    let before: BTreeMap<_, _> = old.iter().map(|e| (e.offset, e)).collect();
    let after: BTreeMap<_, _> = new.iter().map(|e| (e.offset, e)).collect();
    // Compare logical mappings over their common subdivisions. A 4 KiB COW
    // write can split a much larger extent; unchanged left/right pieces should
    // retain their cached hashes despite the changed extent boundaries.
    let mut boundaries = vec![0, size];
    for extent in old.iter().chain(new) {
        boundaries.push(extent.offset.min(size));
        boundaries.push(extent.offset.saturating_add(extent.length).min(size));
    }
    boundaries.sort_unstable();
    boundaries.dedup();
    let mut intervals = Vec::new();
    for pair in boundaries.windows(2) {
        let at = pair[0];
        let a = before
            .range(..=at)
            .next_back()
            .map(|(_, e)| *e)
            .filter(|e| at < e.offset.saturating_add(e.length));
        let b = after
            .range(..=at)
            .next_back()
            .map(|(_, e)| *e)
            .filter(|e| at < e.offset.saturating_add(e.length));
        if mapping(a, at) != mapping(b, at) {
            intervals.push((at, pair[1]));
        }
    }
    if old_size != size {
        intervals.push((old_size.min(size) / chunk * chunk, old_size.max(size)));
    }
    let mut aligned: Vec<_> = intervals
        .into_iter()
        .filter_map(|(start, end)| {
            let start = start / chunk * chunk;
            let end = end.saturating_add(chunk - 1) / chunk * chunk;
            (start < size && end > start).then_some((start, end.min(size)))
        })
        .collect();
    aligned.sort_unstable();
    let mut result: Vec<(u64, u64)> = Vec::new();
    for (start, end) in aligned {
        if let Some(previous) = result.last_mut() {
            if start <= previous.1 {
                previous.1 = previous.1.max(end);
                continue;
            }
        }
        result.push((start, end));
    }
    result
}

fn mapping(extent: Option<&Extent>, at: u64) -> Option<(Vec<u8>, u64)> {
    let extent = extent?;
    let data = &extent.descriptor;
    if data.len() >= 53 && matches!(data[20], 1 | 2) {
        let disk = u64::from_le_bytes(data[21..29].try_into().unwrap());
        if data[20] == 2 || disk == 0 {
            return None;
        }
        let mut identity = data[..8].to_vec(); // allocation generation prevents address reuse
        identity.extend_from_slice(&data[16..37]);
        let offset = u64::from_le_bytes(data[37..45].try_into().unwrap());
        return Some((identity, offset.saturating_add(at - extent.offset)));
    }
    Some((data.clone(), at - extent.offset))
}

fn next_data(file: &File, offset: u64) -> io::Result<Option<u64>> {
    #[cfg(any(target_os = "linux", target_os = "macos", target_os = "freebsd"))]
    {
        // SAFETY: valid descriptor and offset bounded by the indexed file size.
        // lseek64 also handles large files on 32-bit Linux.
        #[cfg(target_os = "linux")]
        let result =
            unsafe { libc::lseek64(file.as_raw_fd(), offset as libc::off64_t, libc::SEEK_DATA) };
        #[cfg(not(target_os = "linux"))]
        let result = {
            let offset = libc::off_t::try_from(offset)
                .map_err(|_| io::Error::from_raw_os_error(libc::EOVERFLOW))?;
            // SAFETY: valid descriptor and checked, representable offset.
            unsafe { libc::lseek(file.as_raw_fd(), offset, libc::SEEK_DATA) }
        };
        if result >= 0 {
            return Ok(Some(result as u64));
        }
        let error = io::Error::last_os_error();
        if error.raw_os_error() == Some(libc::ENXIO) {
            return Ok(None);
        }
        Err(error)
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "freebsd")))]
    {
        let _ = (file, offset);
        Err(io::Error::from_raw_os_error(libc::ENOSYS))
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn excluded_sqlite_aliases_do_not_cancel_posix_locks() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        fs::create_dir(&data).unwrap();
        let options = Options {
            database: dir.path().join("index.sqlite"),
            roots: vec![data.clone()],
            chunk_size: 4096,
            granularity: Granularity::Both,
            verify: false,
            scanner: crate::Scanner::Portable,
            commit_batch_size: 64,
            storage: crate::storage::Storage::Cow,
        };
        let mut index = Index::open(&options).unwrap();
        let shm = dir.path().join("index.sqlite-shm");
        crate::lock_tests::assert_sqlite_locks(&options.database, &shm);
        let ids = index
            .excluded
            .iter()
            .filter_map(|p| fs::symlink_metadata(p).ok())
            .map(|m| (m.dev(), m.ino()))
            .collect::<Vec<_>>();
        for (name, source) in [("database-alias", &options.database), ("shm-alias", &shm)] {
            let alias = data.join(name);
            fs::hard_link(source, &alias).unwrap();
            index_file(
                &mut index,
                &alias,
                &options,
                &mut Stats::default(),
                &ids,
                None,
            )
            .unwrap();
        }
        crate::lock_tests::assert_sqlite_locks(&options.database, &shm);
    }

    #[test]
    fn compact_inline_maps_reuse_unchanged_chunks_but_detect_payload_edits() {
        let mut raw = vec![0; 21];
        raw.extend_from_slice(&vec![7; 1800]);
        let old = Extent {
            offset: 0,
            length: 1800,
            descriptor: crate::records::descriptor(&raw),
        };
        let same = Extent {
            offset: 0,
            length: 1800,
            descriptor: crate::records::descriptor(&raw),
        };
        assert!(
            super::dirty_ranges(std::slice::from_ref(&old), &[same], 1800, 1800, 4096).is_empty()
        );
        *raw.last_mut().unwrap() = 8;
        let changed = Extent {
            offset: 0,
            length: 1800,
            descriptor: crate::records::descriptor(&raw),
        };
        assert_eq!(
            super::dirty_ranges(&[old], &[changed], 1800, 1800, 4096),
            vec![(0, 1800)]
        );
    }

    use super::*;
    use std::os::unix::{ffi::OsStringExt, fs::symlink};

    #[test]
    fn guarded_opens_preserve_identity_and_reject_symlinks_and_special_files() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().canonicalize().unwrap();
        let directory = root.join("directory");
        fs::create_dir(&directory).unwrap();
        let leaf = directory.join(std::ffi::OsString::from_vec(vec![b'x', 0xff]));
        fs::write(&leaf, b"contents").unwrap();
        let long = directory.join("x".repeat(255));
        fs::write(&long, b"long name").unwrap();
        let leaf_link = directory.join("leaf-link");
        symlink(&leaf, &leaf_link).unwrap();
        let parent_link = root.join("parent-link");
        symlink(&directory, &parent_link).unwrap();
        let fifo = directory.join("fifo");
        let fifo_name = std::ffi::CString::new(fifo.as_os_str().as_bytes()).unwrap();
        // SAFETY: live NUL-terminated path and valid permissions, no pointer retention.
        assert_eq!(unsafe { libc::mkfifo(fifo_name.as_ptr(), 0o600) }, 0);

        // Exercise the portable fallback directly even on kernels with openat2.
        for open in [open_regular, open_regular_components] {
            for write in [false, true] {
                for path in [&leaf, &long] {
                    let file = open(path, write).unwrap();
                    assert_eq!(
                        file.metadata().unwrap().ino(),
                        fs::metadata(path).unwrap().ino()
                    );
                }
                for path in [
                    &directory,
                    &fifo,
                    &leaf_link,
                    &parent_link.join(leaf.file_name().unwrap()),
                ] {
                    assert!(open(path, write).is_err(), "opened {path:?}");
                }
            }
            assert!(open(
                &directory
                    .join("../directory")
                    .join(leaf.file_name().unwrap()),
                false
            )
            .is_err());
            assert!(open(Path::new("relative"), false).is_err());
            assert!(open(&directory.join("nul\0name"), false).is_err());
        }
    }
}
