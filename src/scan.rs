use crate::{
    backend,
    index::{path_bytes, Index},
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

/// Open each path component relative to an already opened directory, without
/// following symlinks. O_NONBLOCK prevents a raced replacement by a FIFO from
/// hanging the process; fstat rejects all non-regular leaf objects.
pub(crate) fn open_regular(path: &Path, write: bool) -> io::Result<File> {
    let components: Vec<_> = path.components().collect();
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
    for (i, component) in components.iter().enumerate() {
        let Component::Normal(name) = component else {
            if matches!(component, Component::RootDir) {
                continue;
            }
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "non-normal path component",
            ));
        };
        let name = std::ffi::CString::new(name.as_bytes())?;
        let last = i == components.len() - 1;
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

pub(crate) fn scan(index: &mut Index, options: &Options, stats: &mut Stats) -> Result<()> {
    #[cfg(target_os = "linux")]
    if options.scanner != crate::Scanner::Portable
        && !options.verify
        && crate::btrfs_scan::scan(index, options, stats)?
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
        .execute_batch("DELETE FROM crawls; DELETE FROM extent_state; DELETE FROM file_extents;")?;
    portable(index, options, stats)
}

fn portable(index: &mut Index, options: &Options, stats: &mut Stats) -> Result<()> {
    let mut traversal_complete = true;
    let excluded_ids: Vec<_> = index
        .excluded
        .iter()
        .filter_map(|path| fs::symlink_metadata(path).ok())
        .map(|meta| (meta.dev(), meta.ino()))
        .collect();
    for root in index.roots.clone() {
        for entry in WalkDir::new(&root).follow_links(false).max_open(32) {
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
            if let Err(error) = index_file(index, entry.path(), options, stats, &excluded_ids, None)
            {
                // Never leave old hashes usable after a failed scan of a path.
                index.conn.execute(
                    "DELETE FROM files WHERE path=?1",
                    [path_bytes(entry.path())],
                )?;
                stats
                    .errors
                    .push(format!("scan {:?}: {error:#}", entry.path()));
            }
        }
    }
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
    let file = open_regular(path, false)?;
    let before = file.metadata()?;
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
    // Exclude hardlinks to the index, WAL and lock as well as their paths.
    if excluded_ids.contains(&(before.dev(), before.ino())) {
        return Ok(());
    }
    let existing: Option<(i64, Vec<u8>, Vec<u8>, bool)> = index.conn.query_row(
        "SELECT id,fingerprint,path,EXISTS(SELECT 1 FROM seen WHERE file_id=files.id) FROM files WHERE dev=?1 AND ino=?2",
        params![before.dev() as i64, before.ino() as i64],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?,r.get(3)?)),
    ).optional()?;
    if let Some((_, _, _, seen)) = existing.as_ref() {
        if *seen {
            stats.hardlinks_skipped += 1;
            return Ok(());
        }
    }
    let baseline: Option<Vec<u8>> = if let Some((id, ..)) = &existing {
        index
            .conn
            .query_row(
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
            index.conn.execute("INSERT INTO seen VALUES (?1)", [id])?;
            if let Some(view) = native {
                let tx = index.conn.transaction()?;
                store_extents(&tx, *id, view)?;
                tx.commit()?;
            }
            stats.files_unchanged += 1;
            return Ok(());
        }
    }
    let tx = index.conn.transaction()?;
    // A name may now refer to a different inode. Invalidate it atomically with
    // insertion/rename of the current inode's row.
    tx.execute(
        "DELETE FROM files WHERE path=?1 AND (dev!=?2 OR ino!=?3)",
        params![path_bytes(path), before.dev() as i64, before.ino() as i64],
    )?;
    if let Some((id, previous, _, _)) = existing.as_ref() {
        if !options.verify && version == *previous && native.is_none() {
            tx.execute(
                "UPDATE files SET path=?1 WHERE id=?2",
                params![path_bytes(path), id],
            )?;
            tx.execute("INSERT INTO seen VALUES (?1)", [id])?;
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
        tx.execute(
            "UPDATE files SET path=?1,fingerprint=?2,fskey=?3,alignment=?4 WHERE id=?5",
            params![path_bytes(path), version, fs.key, fs.alignment, id],
        )?;
        id
    } else {
        tx.execute("INSERT INTO files(path,dev,ino,fingerprint,fskey,alignment) VALUES (?1,?2,?3,?4,?5,?6)",
            params![path_bytes(path), before.dev() as i64, before.ino() as i64, version, fs.key, fs.alignment])?;
        tx.last_insert_rowid()
    };
    tx.execute("INSERT INTO seen VALUES (?1)", [file_id])?;
    let full = !reusable || native.is_none_or(|view| !view.cow);
    let dirty = if full {
        if before.len() == 0 {
            vec![]
        } else {
            vec![(0, before.len())]
        }
    } else {
        let mut statement = tx.prepare(
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
        tx.execute("DELETE FROM ranges WHERE file_id=?1", [file_id])?;
        tx.execute("DELETE FROM chunk_cache WHERE file_id=?1", [file_id])?;
    } else {
        for (start, end) in &dirty {
            tx.execute(
                "DELETE FROM ranges WHERE file_id=?1 AND kind=0 AND offset>=?2 AND offset<?3",
                params![file_id, start, end],
            )?;
            tx.execute(
                "DELETE FROM chunk_cache WHERE file_id=?1 AND offset>=?2 AND offset<?3",
                params![file_id, start, end],
            )?;
        }
        tx.execute(
            "DELETE FROM ranges WHERE file_id=?1 AND kind=0 AND offset>=?2",
            params![file_id, before.len()],
        )?;
        tx.execute(
            "DELETE FROM chunk_cache WHERE file_id=?1 AND offset>=?2",
            params![file_id, before.len()],
        )?;
        stats.chunks_reused += tx.query_row(
            "SELECT COUNT(*) FROM chunk_cache WHERE file_id=?1",
            [file_id],
            |r| r.get::<_, u64>(0),
        )?;
        if !dirty.is_empty() || old_size != Some(before.len()) {
            tx.execute("DELETE FROM ranges WHERE file_id=?1 AND kind=1", [file_id])?;
        }
    }
    let mut buffer = vec![0; options.chunk_size];
    let mut sparse_supported = true;
    let mut insert = tx.prepare_cached(
        "INSERT INTO ranges(file_id,kind,offset,length,hash) VALUES (?1,?2,?3,?4,?5)",
    )?;
    let mut cache = tx.prepare_cached(
        "INSERT INTO chunk_cache(file_id,offset,length,hash,nonzero) VALUES (?1,?2,?3,?4,?5)",
    )?;
    for (start, end) in &dirty {
        let mut offset = *start;
        while offset < *end {
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
            cache.execute(params![
                file_id,
                offset,
                length as u64,
                hash.as_bytes().as_slice(),
                nonzero
            ])?;
            if options.granularity != Granularity::File {
                insert.execute(params![
                    file_id,
                    0,
                    offset,
                    length as u64,
                    hash.as_bytes().as_slice()
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
        let mut statement=tx.prepare("SELECT offset,length,hash FROM chunk_cache WHERE file_id=?1 AND nonzero=1 ORDER BY offset")?;
        let mut rows = statement.query([file_id])?;
        while let Some(row) = rows.next()? {
            whole.update(&row.get::<_, u64>(0)?.to_le_bytes());
            whole.update(&row.get::<_, u64>(1)?.to_le_bytes());
            whole.update(&row.get::<_, Vec<u8>>(2)?);
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
        tx.execute("DELETE FROM extent_state WHERE file_id=?1", [file_id])?;
        tx.execute("DELETE FROM file_extents WHERE file_id=?1", [file_id])?;
    }
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

fn store_extents(tx: &rusqlite::Transaction<'_>, file_id: i64, view: &NativeView) -> Result<()> {
    tx.execute("DELETE FROM file_extents WHERE file_id=?1", [file_id])?;
    for extent in &view.extents {
        tx.execute(
            "INSERT INTO file_extents VALUES (?1,?2,?3,?4)",
            params![file_id, extent.offset, extent.length, extent.descriptor],
        )?;
    }
    tx.execute(
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
