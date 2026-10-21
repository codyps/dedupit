//! Persistent generation windows and a durable inode work queue. Discovery
//! commits its cursor and queue together; processing acknowledges only after
//! stable hashing or a confirmed deletion/out-of-scope observation.
use crate::{
    btrfs_io::{self as io, Inode, Item, Key},
    index::{decode_path, path_bytes, Index, SqlCache},
    scan::{self, Extent, NativeView},
    Options, Scanner, Stats,
};
use anyhow::{ensure, Context, Result};
use rusqlite::{params, OptionalExtension};
use std::{
    collections::{BTreeSet, HashSet},
    fs::{self, File, OpenOptions},
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
};
use walkdir::WalkDir;

struct Crawl {
    id: i64,
    path: PathBuf,
    file: File,
}

/// Injectable metadata source keeps the crash/ordering state machine testable
/// without privileges. Production always uses the btrfs ioctl implementation.
trait Source {
    fn boundary(&mut self, file: &File) -> Result<u64>;
    fn page(&mut self, file: &File, key: Key, low: u64) -> Result<Vec<Item>>;
    fn paths(&mut self, file: &File, ino: u64) -> Result<Vec<PathBuf>>;
    fn inode(&mut self, file: &File, ino: u64) -> Result<Option<Inode>>;
    fn extents(&mut self, file: &File, ino: u64) -> Result<Vec<Extent>>;
    fn same_tree(&mut self, a: &File, b: &File) -> Result<bool>;
}
struct Native;

/// Capture actual kernel mappings using the already-open destination. Metadata
/// probe failures merely retain ordinary discovery; SQL failures abort the batch.
/// Fingerprints are guards, never refreshed to accept a concurrent writer.
pub(crate) fn refresh_apply_mapping(
    index: &mut Index,
    file_id: i64,
    file: &File,
    fingerprint: &[u8],
    stats: &mut Stats,
) -> Result<()> {
    let baseline: Option<(Vec<u8>, i64)> = index.conn.query_row_cached(
        "SELECT e.incarnation,b.crawl_id FROM extent_state e JOIN btrfs_files b ON b.file_id=e.file_id WHERE e.file_id=?1",
        [file_id], |r| Ok((r.get(0)?, r.get(1)?)),
    ).optional()?;
    let Some((incarnation, crawl_id)) = baseline else {
        return Ok(());
    };
    let capture = || -> Result<NativeView> {
        let before = file.metadata()?;
        ensure!(
            scan::fingerprint(&before) == fingerprint,
            "destination changed"
        );
        let item = io::inode_item(file, before.ino())?.context("inode disappeared")?;
        let inode = Inode::parse(&item)?;
        ensure!(
            inode.regular() && inode.generation.to_le_bytes().as_slice() == incarnation,
            "destination incarnation changed"
        );
        let extents = io::extents(file, before.ino())?
            .into_iter()
            .map(|e| Extent {
                offset: e.offset,
                length: e.length,
                descriptor: e.descriptor,
            })
            .collect();
        ensure!(
            io::inode_item(file, before.ino())?.as_ref() == Some(&item)
                && scan::fingerprint(&file.metadata()?) == fingerprint,
            "destination changed during mapping capture"
        );
        Ok(NativeView {
            incarnation,
            extents,
            cow: inode.cow(),
            fingerprint: fingerprint.to_vec(),
        })
    };
    let Ok(view) = capture() else {
        return Ok(());
    };
    // Register the durability barrier before staging any provisional cache.
    if let std::collections::hash_map::Entry::Vacant(entry) = index.mapping_sync.entry(crawl_id) {
        entry.insert(file.try_clone()?);
    }
    scan::store_extents(&index.conn, file_id, &view)?;
    index
        .conn
        .execute_cached("INSERT INTO apply_mappings VALUES (?1)", [file_id])?;
    index.test_hook("apply:mapping-staged")?;
    stats.btrfs_mapping_updates += 1;
    Ok(())
}

/// SQLite batches can commit provisional maps before btrfs commits. Only this
/// final, ordered barrier makes those maps eligible to suppress discovery.
/// Read delayed inode updates after committing btrfs. Ordinary discovery queues
/// every unrelated change before advancing its cursor/watermark. Confirmation
/// and cursor acknowledgement share those same bounded SQLite batches.
pub(crate) fn confirm_apply_mappings(index: &mut Index, stats: &mut Stats) -> Result<()> {
    if index.mapping_sync.is_empty() {
        return Ok(());
    }
    index.test_hook("apply:before-mapping-sync")?;
    for (id, file) in std::mem::take(&mut index.mapping_sync) {
        let high = io::committed(&file).context("commit btrfs before confirming apply mappings")?;
        index.test_hook("apply:mappings-synced")?;
        let path =
            index
                .conn
                .query_row_cached("SELECT path FROM crawls WHERE id=?1", [id], |r| {
                    Ok(decode_path(r.get(0)?))
                })?;
        discover_inner(
            index,
            &Crawl { id, file, path },
            &mut Native,
            stats,
            Some(high),
        )?;
    }
    index.test_hook("apply:after-mapping-confirm")?;
    Ok(())
}
impl Source for Native {
    fn boundary(&mut self, file: &File) -> Result<u64> {
        io::committed(file)
    }
    fn page(&mut self, file: &File, key: Key, low: u64) -> Result<Vec<Item>> {
        io::search(file, 0, key, Key::LAST, low)
    }
    fn paths(&mut self, file: &File, ino: u64) -> Result<Vec<PathBuf>> {
        io::paths(file, ino)
    }
    fn inode(&mut self, file: &File, ino: u64) -> Result<Option<Inode>> {
        io::inode(file, ino)
    }
    fn extents(&mut self, file: &File, ino: u64) -> Result<Vec<Extent>> {
        Ok(io::extents(file, ino)?
            .into_iter()
            .map(|e| Extent {
                offset: e.offset,
                length: e.length,
                descriptor: e.descriptor,
            })
            .collect())
    }
    fn same_tree(&mut self, a: &File, b: &File) -> Result<bool> {
        Ok(io::tree_id(a)? == io::tree_id(b)?)
    }
}

pub(crate) fn scan(index: &mut Index, options: &Options, stats: &mut Stats) -> Result<bool> {
    let prepared = prepare(index);
    let contexts=match prepared {
        Ok(contexts)=>contexts,
        Err(error) if options.scanner==Scanner::Auto=>{
            stats.scanner_fallbacks.push(format!("using portable discovery: {error:#}"));
            return Ok(false);
        }
        Err(error)=>return Err(error.context("btrfs discovery requires accessible subvolume roots and CAP_SYS_ADMIN/CAP_DAC_READ_SEARCH; use --scanner portable to walk directories")),
    };
    let mut ids = HashSet::new();
    let mut crawls = Vec::new();
    for (path, file, identity) in contexts {
        index.conn.execute_cached(
            "INSERT OR IGNORE INTO crawls(identity,path) VALUES (?1,?2)",
            params![identity, path_bytes(&path)],
        )?;
        let id: i64 = index.conn.query_row_cached(
            "SELECT id FROM crawls WHERE identity=?1",
            [identity],
            |r| r.get(0),
        )?;
        ids.insert(id);
        crawls.push(Crawl { id, path, file });
    }
    // Missing subvolumes or changed UUIDs invalidate their old observations.
    let stale: Vec<i64> = index
        .conn
        .prepare_cached("SELECT id FROM crawls")?
        .query_map([], |r| r.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    for id in stale.into_iter().filter(|id| !ids.contains(id)) {
        let tx = index.conn.savepoint()?;
        stats.files_removed += tx.execute_cached(
            "DELETE FROM files WHERE id IN (SELECT file_id FROM btrfs_files WHERE crawl_id=?1)",
            [id],
        )? as u64;
        tx.execute_cached("DELETE FROM crawls WHERE id=?1", [id])?;
        tx.commit()?;
    }
    let mut source = Native;
    for crawl in crawls {
        stats.btrfs_subvolumes += 1;
        discover(index, &crawl, &mut source, stats)?;
        process(index, &crawl, options, &mut source, stats)?;
    }
    // Old portable observations which never mapped to an accessible inode are
    // removed only after successful native enumeration of every context.
    if stats.errors.is_empty() {
        stats.files_removed += index.conn.execute_cached(
            "DELETE FROM files WHERE id NOT IN (SELECT file_id FROM btrfs_files)",
            [],
        )? as u64;
    }
    Ok(true)
}

type Prepared = Vec<(PathBuf, File, Vec<u8>)>;
fn prepare(index: &Index) -> Result<Prepared> {
    reject_nested_mounts(&index.roots)?;
    let mut bases = BTreeSet::new();
    for root in &index.roots {
        let mut base = if root.is_dir() {
            root.clone()
        } else {
            root.parent().context("file root has no parent")?.to_owned()
        };
        let fd = open_directory(&base)?;
        ensure!(
            crate::backend::filesystem(&fd)?.key.starts_with(b"btrfs:"),
            "{root:?} is not on btrfs"
        );
        while fs::symlink_metadata(&base)?.ino() != 256 {
            let parent = base.parent().context("subvolume root is not reachable")?;
            ensure!(
                fs::symlink_metadata(parent)?.dev() == fs::symlink_metadata(&base)?.dev(),
                "subvolume root is hidden by a bind mount"
            );
            base = parent.to_owned();
        }
        bases.insert(base);
    }
    let mut result = Vec::new();
    let mut visited = HashSet::new();
    while let Some(path) = bases.pop_first() {
        if !visited.insert(path.clone()) {
            continue;
        }
        let file = open_directory(&path)?;
        ensure!(
            file.metadata()?.ino() == 256,
            "subvolume root changed while preparing discovery"
        );
        // Probe before modifying persistent state. Auto can fall back safely
        // when the filesystem or invoking user cannot access these ioctls.
        // An empty/nonexistent key range probes authorization without fetching
        // an old metadata page on every otherwise-idle invocation.
        io::search(&file, 0, Key::FIRST, Key::FIRST, 0)?;
        io::probe_paths(&file)?;
        for child in io::children(&file)? {
            let child = path.join(child);
            if relevant(&child, &index.roots) {
                bases.insert(child);
            }
        }
        let mut identity = io::identity(&file)?;
        identity.extend_from_slice(path_bytes(&path));
        result.push((path, file, identity));
    }
    Ok(result)
}

fn open_directory(path: &Path) -> Result<File> {
    Ok(OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_DIRECTORY | libc::O_CLOEXEC)
        .open(path)?)
}

fn reject_nested_mounts(roots: &[PathBuf]) -> Result<()> {
    // A metadata tree cannot enumerate files supplied by an overlaid mount.
    // Conservatively use the portable walk for such scopes.
    let mountinfo = fs::read("/proc/self/mountinfo")?;
    for line in mountinfo.split(|b| *b == b'\n') {
        let Some(raw) = line.split(|b| *b == b' ').nth(4) else {
            continue;
        };
        let mut decoded = Vec::new();
        let mut at = 0;
        while at < raw.len() {
            if raw[at] == b'\\'
                && at + 3 < raw.len()
                && raw[at + 1..at + 4]
                    .iter()
                    .all(|b| (b'0'..=b'7').contains(b))
            {
                let value = (raw[at + 1] - b'0') as u16 * 64
                    + (raw[at + 2] - b'0') as u16 * 8
                    + (raw[at + 3] - b'0') as u16;
                ensure!(value <= 255, "invalid mountinfo escape");
                decoded.push(value as u8);
                at += 4;
            } else {
                decoded.push(raw[at]);
                at += 1;
            }
        }
        let mount = decode_path(decoded);
        ensure!(
            !roots
                .iter()
                .any(|root| mount != *root && mount.starts_with(root)),
            "nested mount {mount:?} requires portable discovery"
        );
    }
    Ok(())
}

fn discover(
    index: &mut Index,
    crawl: &Crawl,
    source: &mut impl Source,
    stats: &mut Stats,
) -> Result<()> {
    discover_inner(index, crawl, source, stats, None)
}

fn discover_inner(
    index: &mut Index,
    crawl: &Crawl,
    source: &mut impl Source,
    stats: &mut Stats,
    confirmation: Option<u64>,
) -> Result<()> {
    let now = match confirmation {
        Some(high) => high,
        None => source.boundary(&crawl.file)?,
    };
    let (completed, high): (u64, Option<u64>) = index.conn.query_row_cached(
        "SELECT completed,high FROM crawls WHERE id=?1",
        [crawl.id],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    if now < completed || high.is_some_and(|high| now < high) {
        // Restored/rolled-back filesystem: a previous extent baseline cannot
        // certify data on the new timeline, even if an inode number repeats.
        let tx = index.conn.savepoint()?;
        tx.execute_cached(
            "DELETE FROM files WHERE id IN (SELECT file_id FROM btrfs_files WHERE crawl_id=?1)",
            [crawl.id],
        )?;
        tx.execute_cached("DELETE FROM discovery WHERE crawl_id=?1", [crawl.id])?;
        tx.execute_cached("DELETE FROM btrfs_dirs WHERE crawl_id=?1", [crawl.id])?;
        tx.execute_cached("UPDATE crawls SET completed=0,low=NULL,high=NULL,cursor=NULL,enumerated=0,initialized=0 WHERE id=?1",[crawl.id])?;
        tx.commit()?;
    }
    let (completed, high): (u64, Option<u64>) = index.conn.query_row_cached(
        "SELECT completed,high FROM crawls WHERE id=?1",
        [crawl.id],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    if high.is_none() {
        if completed == now {
            return Ok(());
        }
        index.conn.execute_cached(
            "UPDATE crawls SET low=?2,high=?3,cursor=?4,enumerated=0 WHERE id=?1",
            params![
                crawl.id,
                if completed == 0 { 0 } else { completed + 1 },
                now,
                Key::FIRST.encode()
            ],
        )?;
    }
    loop {
        index.begin_batch(if confirmation.is_some() {
            "mapping-confirm"
        } else {
            "discovery"
        })?;
        let (low, high, cursor): (u64, u64, Vec<u8>) = index.conn.query_row_cached(
            "SELECT low,high,cursor FROM crawls WHERE id=?1",
            [crawl.id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;
        let key = Key::decode(&cursor)?;
        let page = source.page(&crawl.file, key, low)?;
        stats.btrfs_search_pages += 1;
        let tx = index.conn.savepoint()?;
        for item in &page {
            if item.key.kind != io::INODE {
                continue;
            }
            let inode = Inode::parse(&item.data)?;
            if inode.transid < low
                || inode.transid > high
                || !(inode.regular() || inode.directory())
            {
                continue;
            }
            if confirmation.is_some() && inode.regular() {
                let cached: Option<(i64, Vec<u8>, Vec<u8>)> = tx
                    .query_row_cached(
                        "SELECT a.file_id,f.fingerprint,e.incarnation FROM btrfs_files b
                     JOIN apply_mappings a ON a.file_id=b.file_id JOIN files f ON f.id=b.file_id
                     JOIN extent_state e ON e.file_id=b.file_id
                     WHERE b.crawl_id=?1 AND b.ino=?2",
                        params![crawl.id, item.key.ino],
                        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                    )
                    .optional()?;
                if let Some((file_id, fingerprint, incarnation)) = cached {
                    if inode.generation.to_le_bytes().as_slice() == incarnation
                        && io::matches_fingerprint(&item.data, &fingerprint)?
                    {
                        tx.execute_cached(
                            "DELETE FROM apply_mappings WHERE file_id=?1",
                            [file_id],
                        )?;
                        #[cfg(feature = "test-failpoints")]
                        crate::failpoints::hit(&tx, "apply:mappings-confirmed")?;
                        // This exact committed window is acknowledged with the
                        // marker deletion. Future changes have a later transid;
                        // existing queue entries are deliberately left intact.
                        stats.btrfs_events_reused += 1;
                        continue;
                    }
                }
            }
            tx.execute_cached("INSERT INTO discovery VALUES (?1,?2,?3) ON CONFLICT(crawl_id,ino) DO UPDATE SET directory=excluded.directory",params![crawl.id,item.key.ino,inode.directory()])?;
            stats.discovery_jobs += 1;
        }
        if page.is_empty() || page.last().unwrap().key.next().is_none() {
            // Advancing the generation is safe because ALL discovered work is
            // durable, including jobs that fail processing and need later retry.
            tx.execute_cached("UPDATE crawls SET completed=high,low=NULL,high=NULL,cursor=NULL,enumerated=1 WHERE id=?1",[crawl.id])?;
            tx.commit()?;
            index.batch_step(page.iter().map(|item| item.data.len() as u64).sum())?;
            index.flush_batch()?;
            if high < now {
                // Finish a resumed older window, then catch up through the
                // boundary captured at this invocation's start. Do not chase
                // an unbounded stream of concurrent writers.
                index.conn.execute_cached(
                    "UPDATE crawls SET low=?2,high=?3,cursor=?4,enumerated=0 WHERE id=?1",
                    params![crawl.id, high + 1, now, Key::FIRST.encode()],
                )?;
                continue;
            }
            return Ok(());
        }
        let next = page.last().unwrap().key.next().unwrap();
        ensure!(next > key, "btrfs cursor did not advance");
        tx.execute_cached(
            "UPDATE crawls SET cursor=?2 WHERE id=?1",
            params![crawl.id, next.encode()],
        )?;
        tx.commit()?;
        index.batch_step(page.iter().map(|item| item.data.len() as u64).sum())?;
    }
}

fn process(
    index: &mut Index,
    crawl: &Crawl,
    options: &Options,
    source: &mut impl Source,
    stats: &mut Stats,
) -> Result<()> {
    // Directory jobs discover deletions, scope changes and renamed descendants.
    // Process each failing job at most once per invocation.
    index.conn.execute_batch("CREATE TEMP TABLE IF NOT EXISTS attempted (ino INTEGER PRIMARY KEY); DELETE FROM attempted;")?;
    let initial = !index.conn.query_row_cached(
        "SELECT initialized FROM crawls WHERE id=?1",
        [crawl.id],
        |r| r.get::<_, bool>(0),
    )?;
    for directory in [true, false] {
        index.conn.execute_cached("DELETE FROM attempted", [])?;
        let mut lower = i64::MIN;
        loop {
            let Some(ino) = next_queued(&index.conn, crawl.id, directory, &mut lower)? else {
                break;
            };
            index
                .conn
                .execute_cached("INSERT INTO attempted VALUES (?1)", [ino])?;
            index.begin_batch(if directory {
                "native-directory"
            } else {
                "native-file"
            })?;
            let bytes_before = stats.bytes_hashed;
            // Directory reconciliation can make partial namespace edits, so it
            // uses a savepoint. Failed file jobs explicitly discard their file
            // records; avoiding a long-lived nested savepoint keeps dense
            // hashing from journaling every modified index page twice.
            if directory {
                index.conn.execute_batch("SAVEPOINT discovery_job")?;
            }
            let result = if directory {
                directory_job(index, crawl, ino as u64, source, initial)
            } else {
                file_job(index, crawl, ino as u64, options, source, stats)
            };
            if directory && result.is_err() {
                index.conn.execute_batch("ROLLBACK TO discovery_job")?;
            }
            if directory {
                index.conn.execute_batch("RELEASE discovery_job")?;
            }
            if let Err(error) = result {
                index
                    .check_batch()
                    .with_context(|| format!("fatal native job: {error:#}"))?;
                stats
                    .errors
                    .push(format!("btrfs {:?} inode {ino}: {error:#}", crawl.path));
                // Keep the queue entry; invalidate its hash baseline so a retry
                // cannot trust data from an unstable or inaccessible file.
                if !directory {
                    index.conn.execute_cached("DELETE FROM files WHERE id IN (SELECT file_id FROM btrfs_files WHERE crawl_id=?1 AND ino=?2)",params![crawl.id,ino])?;
                }
            } else {
                index.conn.execute_cached(
                    "DELETE FROM discovery WHERE crawl_id=?1 AND ino=?2",
                    params![crawl.id, ino],
                )?;
            }
            index.batch_step(stats.bytes_hashed.saturating_sub(bytes_before))?;
        }
        if directory {
            // Failed bootstrap directory jobs remain queued. Subsequent cycles
            // must nevertheless treat newly included subtrees as new scope.
            index.conn.execute_cached(
                "UPDATE crawls SET initialized=1 WHERE id=?1 AND initialized=0",
                [crawl.id],
            )?;
        }
        index.flush_batch()?;
    }
    Ok(())
}

const NEXT_QUEUED: &str = "SELECT ino FROM discovery WHERE crawl_id=?1 AND directory=?2
    AND ino>=?3 AND ino NOT IN (SELECT ino FROM attempted) ORDER BY ino LIMIT 1";

fn next_queued(
    conn: &rusqlite::Connection,
    crawl: i64,
    directory: bool,
    lower: &mut i64,
) -> Result<Option<i64>> {
    loop {
        let ino = conn
            .query_row_cached(NEXT_QUEUED, params![crawl, directory, *lower], |r| {
                r.get::<_, i64>(0)
            })
            .optional()?;
        if let Some(ino) = ino {
            // Use SQLite's signed bounds without overflowing at MAX.
            // attempted excludes MAX if advancing wraps to MIN.
            *lower = ino.checked_add(1).unwrap_or(i64::MIN);
            return Ok(Some(ino));
        }
        if *lower == i64::MIN {
            return Ok(None);
        }
        // A directory job may enqueue a lower-numbered inode. Revisit that
        // prefix after the forward pass; attempted prevents retry loops and
        // keeps failing jobs from being visited more than once this invocation.
        *lower = i64::MIN;
    }
}

fn in_scope(path: &Path, roots: &[PathBuf]) -> bool {
    roots.iter().any(|root| path.starts_with(root))
}
fn relevant(path: &Path, roots: &[PathBuf]) -> bool {
    in_scope(path, roots) || roots.iter().any(|root| root.starts_with(path))
}

fn resolved(crawl: &Crawl, ino: u64, source: &mut impl Source) -> Result<Vec<PathBuf>> {
    Ok(source
        .paths(&crawl.file, ino)?
        .into_iter()
        .map(|relative| crawl.path.join(relative))
        .collect())
}

fn file_job(
    index: &mut Index,
    crawl: &Crawl,
    ino: u64,
    options: &Options,
    source: &mut impl Source,
    stats: &mut Stats,
) -> Result<()> {
    let paths = resolved(crawl, ino, source)?;
    let Some(path) = paths.into_iter().find(|path| in_scope(path, &index.roots)) else {
        stats.files_removed+=index.conn.execute_cached("DELETE FROM files WHERE id IN (SELECT file_id FROM btrfs_files WHERE crawl_id=?1 AND ino=?2)",params![crawl.id,ino])? as u64;
        return Ok(());
    };
    let result = file_job_at(index, crawl, ino, options, source, stats, &path);
    if result.is_err() {
        index
            .discard_file(&path)
            .context("invalidate failed native observation")?;
    }
    result
}

fn file_job_at(
    index: &mut Index,
    crawl: &Crawl,
    ino: u64,
    options: &Options,
    source: &mut impl Source,
    stats: &mut Stats,
    path: &Path,
) -> Result<()> {
    let file = scan::open_regular(path, false)?;
    let before = file.metadata()?;
    ensure!(
        before.ino() == ino && source.same_tree(&file, &crawl.file)?,
        "resolved path changed identity"
    );
    let fingerprint = scan::fingerprint(&before);
    file.sync_all()
        .context("flush file before comparing extent maps")?;
    let inode = source
        .inode(&crawl.file, ino)?
        .context("inode disappeared")?;
    ensure!(inode.regular(), "inode is no longer a regular file");
    let extents = source.extents(&crawl.file, ino)?;
    ensure!(
        scan::fingerprint(&file.metadata()?) == fingerprint,
        "file changed during extent enumeration"
    );
    let view = NativeView {
        incarnation: inode.generation.to_le_bytes().to_vec(),
        extents,
        cow: inode.cow(),
        fingerprint,
    };
    let excluded = index
        .excluded
        .iter()
        .filter_map(|p| fs::symlink_metadata(p).ok())
        .map(|m| (m.dev(), m.ino()))
        .collect::<Vec<_>>();
    stats.files_seen += 1;
    scan::index_file(index, path, options, stats, &excluded, Some(&view))?;
    let file_id: Option<i64> = index
        .conn
        .query_row_cached(
            "SELECT id FROM files WHERE dev=?1 AND ino=?2",
            params![before.dev() as i64, ino],
            |r| r.get(0),
        )
        .optional()?;
    if let Some(file_id) = file_id {
        index.conn.execute_cached("INSERT INTO btrfs_files VALUES (?1,?2,?3,?4) ON CONFLICT(file_id) DO UPDATE SET crawl_id=excluded.crawl_id,ino=excluded.ino,parent=excluded.parent",params![file_id,crawl.id,ino,path_bytes(path.parent().unwrap())])?;
    }
    Ok(())
}

fn directory_job(
    index: &mut Index,
    crawl: &Crawl,
    ino: u64,
    source: &mut impl Source,
    initial: bool,
) -> Result<()> {
    let old: Option<Vec<u8>> = index
        .conn
        .query_row_cached(
            "SELECT path FROM btrfs_dirs WHERE crawl_id=?1 AND ino=?2",
            params![crawl.id, ino],
            |r| r.get(0),
        )
        .optional()?;
    let old = old.map(decode_path);
    let path = resolved(crawl, ino, source)?.into_iter().next();
    // Directory renames do not update descendant inode timestamps. Queue known
    // descendants and walk only a newly included/moved subtree to discover old
    // inodes which were previously outside the requested scope.
    if old != path {
        if let Some(old) = &old {
            queue_prefix(index, crawl.id, old)?;
            move_directories(index, crawl.id, old, path.as_deref())?;
        }
        if !initial && old.as_ref().is_none_or(|old| !in_scope(old, &index.roots)) {
            if let Some(path) = path.as_ref().filter(|path| in_scope(path, &index.roots)) {
                let device = fs::symlink_metadata(path)?.dev();
                for entry in WalkDir::new(path)
                    .follow_links(false)
                    .same_file_system(true)
                {
                    let entry = entry?;
                    if !entry.file_type().is_file() && !entry.file_type().is_dir() {
                        continue;
                    }
                    let metadata = entry.metadata()?;
                    // WalkDir yields subvolume boundary entries even when it
                    // does not descend. Their inode numbers belong to another
                    // tree and must be handled by that subvolume's crawl.
                    if metadata.dev() != device {
                        continue;
                    }
                    if entry.file_type().is_file() {
                        queue(index, crawl.id, metadata.ino(), false)?;
                    } else if entry.file_type().is_dir() && entry.path() != path {
                        let child = metadata.ino();
                        // Old descendant directories may never appear in this
                        // generation window. Remember their locations so later
                        // deletion can queue every formerly indexed descendant.
                        // The ancestor walk has already enumerated this tree;
                        // seeding unknown locations avoids walking it again for
                        // each child job. Preserve known locations as rename
                        // evidence and queue validation of every child.
                        index.conn.execute_cached(
                            "INSERT OR IGNORE INTO btrfs_dirs VALUES (?1,?2,?3,?4)",
                            params![
                                crawl.id,
                                child,
                                path_bytes(entry.path()),
                                path_bytes(entry.path().parent().unwrap())
                            ],
                        )?;
                        queue(index, crawl.id, child, true)?;
                    }
                }
            }
        }
    }
    let Some(path) = path.filter(|path| relevant(path, &index.roots)) else {
        index.conn.execute_cached(
            "DELETE FROM btrfs_dirs WHERE crawl_id=?1 AND ino=?2",
            params![crawl.id, ino],
        )?;
        return Ok(());
    };
    let meta = fs::symlink_metadata(&path)?;
    ensure!(
        meta.is_dir() && meta.ino() == ino,
        "directory path changed identity"
    );
    // Deleted inodes have no tree item. A changed parent directory is the
    // evidence for looking up its former children and pruning deleted names.
    // These cursors read only files/btrfs_dirs while queue writes discovery.
    // Streaming avoids retaining a second copy of a wide directory's paths.
    let mut files = index.conn.prepare_cached("SELECT b.ino,f.path FROM btrfs_files b JOIN files f ON f.id=b.file_id WHERE b.crawl_id=?1 AND b.parent=?2")?;
    let mut files = files.query(params![crawl.id, path_bytes(&path)])?;
    let mut names = None;
    while let Some(row) = files.next()? {
        // Initial discovery has no indexed children. Do not enumerate and
        // retain all names merely to compare against an empty baseline.
        if names.is_none() {
            names = Some(
                fs::read_dir(&path)?
                    .map(|entry| entry.map(|e| e.file_name()))
                    .collect::<std::io::Result<HashSet<_>>>()?,
            );
        }
        let child: u64 = row.get(0)?;
        let bytes = row.get(1)?;
        let child_path = decode_path(bytes);
        if child_path
            .file_name()
            .is_none_or(|name| !names.as_ref().unwrap().contains(name))
        {
            queue(index, crawl.id, child, false)?;
        }
    }
    let mut dirs = index.conn.prepare_cached(
        "SELECT ino,path FROM btrfs_dirs WHERE crawl_id=?1 AND parent=?2 AND ino!=?3",
    )?;
    let dirs = dirs.query_map(params![crawl.id, path_bytes(&path), ino], |r| {
        Ok((r.get(0)?, r.get(1)?))
    })?;
    for entry in dirs {
        let (child, bytes): (u64, Vec<u8>) = entry?;
        let child_path = decode_path(bytes);
        let matches = fs::symlink_metadata(&child_path)
            .is_ok_and(|meta| meta.is_dir() && meta.ino() == child);
        if !matches {
            queue(index, crawl.id, child, true)?;
        }
    }
    index.conn.execute_cached("INSERT INTO btrfs_dirs VALUES (?1,?2,?3,?4) ON CONFLICT(crawl_id,ino) DO UPDATE SET path=excluded.path,parent=excluded.parent",params![crawl.id,ino,path_bytes(&path),path_bytes(path.parent().unwrap_or(&path))])?;
    Ok(())
}

fn move_directories(index: &mut Index, crawl: i64, old: &Path, new: Option<&Path>) -> Result<()> {
    let (prefix, end) = prefix_bounds(old);
    let dirs: Vec<(u64, Vec<u8>)> = index
        .conn
        .prepare_cached(
            "SELECT ino,path FROM btrfs_dirs WHERE crawl_id=?1 AND path>=?2 AND path<?3",
        )?
        .query_map(params![crawl, prefix, end], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect::<rusqlite::Result<_>>()?;
    let tx = index.conn.savepoint()?;
    for (ino, bytes) in dirs {
        if let Some(new) = new {
            let previous = decode_path(bytes);
            let path = new.join(previous.strip_prefix(old)?);
            tx.execute_cached(
                "UPDATE btrfs_dirs SET path=?3,parent=?4 WHERE crawl_id=?1 AND ino=?2",
                params![
                    crawl,
                    ino,
                    path_bytes(&path),
                    path_bytes(path.parent().unwrap())
                ],
            )?;
        } else {
            tx.execute_cached(
                "DELETE FROM btrfs_dirs WHERE crawl_id=?1 AND ino=?2",
                params![crawl, ino],
            )?;
        }
    }
    tx.commit()?;
    Ok(())
}

fn queue(index: &Index, crawl: i64, ino: u64, directory: bool) -> Result<()> {
    index.conn.execute_cached("INSERT INTO discovery VALUES (?1,?2,?3) ON CONFLICT(crawl_id,ino) DO UPDATE SET directory=excluded.directory",params![crawl,ino,directory])?;
    Ok(())
}

fn queue_prefix(index: &Index, crawl: i64, path: &Path) -> Result<()> {
    let (prefix, end) = prefix_bounds(path);
    index.conn.execute_cached("INSERT OR IGNORE INTO discovery SELECT b.crawl_id,b.ino,0 FROM btrfs_files b JOIN files f ON f.id=b.file_id WHERE b.crawl_id=?1 AND f.path>=?2 AND f.path<?3",params![crawl,prefix,end])?;
    Ok(())
}

fn prefix_bounds(path: &Path) -> (Vec<u8>, Vec<u8>) {
    let mut prefix = path_bytes(path).to_vec();
    if !prefix.ends_with(b"/") {
        prefix.push(b'/');
    }
    let mut end = prefix.clone();
    *end.last_mut().unwrap() += 1;
    (prefix, end)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Granularity;
    use std::{collections::BTreeMap, os::unix::fs::FileExt};

    struct Node {
        path: PathBuf,
        transid: u64,
        generation: u64,
        fingerprint: Vec<u8>,
        flags: u64,
        directory: bool,
        extents: Vec<Extent>,
    }
    struct Fake {
        base: PathBuf,
        high: u64,
        nodes: BTreeMap<u64, Node>,
        calls: Vec<Key>,
        fail_at: Option<usize>,
        fail_file: bool,
    }
    impl Fake {
        fn new(base: PathBuf) -> Self {
            Self {
                base,
                high: 1,
                nodes: BTreeMap::new(),
                calls: vec![],
                fail_at: None,
                fail_file: false,
            }
        }
        fn add(&mut self, path: &str, extents: Vec<Extent>) -> u64 {
            let meta = fs::metadata(self.base.join(path)).unwrap();
            let ino = meta.ino();
            self.nodes.insert(
                ino,
                Node {
                    path: path.into(),
                    transid: self.high,
                    generation: 1,
                    fingerprint: scan::fingerprint(&meta),
                    flags: 0,
                    directory: meta.is_dir(),
                    extents,
                },
            );
            ino
        }
        fn payload(node: &Node) -> Vec<u8> {
            let mut data = vec![0; 160];
            data[..8].copy_from_slice(&node.generation.to_le_bytes());
            data[8..16].copy_from_slice(&node.transid.to_le_bytes());
            data[52..56].copy_from_slice(
                &(if node.directory {
                    libc::S_IFDIR
                } else {
                    libc::S_IFREG
                })
                .to_le_bytes(),
            );
            data[64..72].copy_from_slice(&node.flags.to_le_bytes());
            for (start, field, length) in [
                (16, 16, 8),
                (136, 24, 8),
                (144, 32, 4),
                (124, 40, 8),
                (132, 48, 4),
            ] {
                data[start..start + length]
                    .copy_from_slice(&node.fingerprint[field..field + length]);
            }
            data
        }
    }
    impl Source for Fake {
        fn boundary(&mut self, _: &File) -> Result<u64> {
            Ok(self.high)
        }
        fn page(&mut self, _: &File, key: Key, low: u64) -> Result<Vec<Item>> {
            self.calls.push(key);
            if self.fail_at == Some(self.calls.len()) {
                anyhow::bail!("injected discovery interruption");
            }
            // One item per page exercises persisted key pagination. Return old
            // items from newer leaves too: only item transid selects work.
            Ok(self
                .nodes
                .iter()
                .filter_map(|(ino, node)| {
                    let k = Key {
                        ino: *ino,
                        kind: io::INODE,
                        offset: 0,
                    };
                    (k >= key && self.high >= low).then(|| Item {
                        key: k,
                        data: Self::payload(node),
                    })
                })
                .take(1)
                .collect())
        }
        fn paths(&mut self, _: &File, ino: u64) -> Result<Vec<PathBuf>> {
            Ok(self
                .nodes
                .get(&ino)
                .map(|node| vec![node.path.clone()])
                .unwrap_or_default())
        }
        fn inode(&mut self, _: &File, ino: u64) -> Result<Option<Inode>> {
            self.nodes
                .get(&ino)
                .map(|node| Inode::parse(&Self::payload(node)))
                .transpose()
        }
        fn extents(&mut self, _: &File, ino: u64) -> Result<Vec<Extent>> {
            ensure!(!self.fail_file, "injected file processing failure");
            Ok(self.nodes[&ino].extents.clone())
        }
        fn same_tree(&mut self, _: &File, _: &File) -> Result<bool> {
            Ok(true)
        }
    }
    fn extent(offset: u64, length: u64, disk: u64, within: u64, generation: u64) -> Extent {
        let mut descriptor = vec![0; 53];
        descriptor[..8].copy_from_slice(&generation.to_le_bytes());
        descriptor[8..16].copy_from_slice(&12288u64.to_le_bytes());
        descriptor[20] = 1;
        descriptor[21..29].copy_from_slice(&disk.to_le_bytes());
        descriptor[29..37].copy_from_slice(&12288u64.to_le_bytes());
        descriptor[37..45].copy_from_slice(&within.to_le_bytes());
        descriptor[45..53].copy_from_slice(&length.to_le_bytes());
        Extent {
            offset,
            length,
            descriptor,
        }
    }
    fn setup(granularity: Granularity) -> (tempfile::TempDir, Options, Fake, u64) {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("data");
        fs::create_dir(&base).unwrap();
        fs::write(base.join("f"), vec![1; 12288]).unwrap();
        let options = Options {
            database: dir.path().join("index.sqlite"),
            roots: vec![base.clone()],
            chunk_size: 4096,
            granularity,
            verify: false,
            scanner: Scanner::Btrfs,
            commit_batch_size: 64,
        };
        let mut source = Fake::new(base);
        source.add("", vec![]);
        let ino = source.add("f", vec![extent(0, 12288, 100, 0, 1)]);
        (dir, options, source, ino)
    }
    fn open(options: &Options, source: &Fake) -> (Index, Crawl) {
        let index = Index::open(options).unwrap();
        index
            .conn
            .execute(
                "INSERT OR IGNORE INTO crawls(identity,path) VALUES (x'0102',?1)",
                [path_bytes(&source.base)],
            )
            .unwrap();
        let id = index
            .conn
            .query_row("SELECT id FROM crawls WHERE identity=x'0102'", [], |r| {
                r.get(0)
            })
            .unwrap();
        let crawl = Crawl {
            id,
            path: source.base.clone(),
            file: File::open(&source.base).unwrap(),
        };
        (index, crawl)
    }
    fn cycle(options: &Options, source: &mut Fake) -> Result<Stats> {
        let (mut index, crawl) = open(options, source);
        let mut stats = Stats::default();
        discover(&mut index, &crawl, source, &mut stats)?;
        process(&mut index, &crawl, options, source, &mut stats)?;
        Ok(stats)
    }
    fn whole(options: &Options) -> Vec<u8> {
        rusqlite::Connection::open(&options.database)
            .unwrap()
            .query_row("SELECT hash FROM ranges WHERE kind=1", [], |r| r.get(0))
            .unwrap()
    }
    fn assert_fresh_hash(options: &Options) {
        let mut fresh = options.clone();
        fresh.database.set_file_name("fresh.sqlite");
        fresh.scanner = Scanner::Portable;
        fresh.verify = true;
        crate::run(&fresh, false).unwrap();
        assert_eq!(whole(options), whole(&fresh));
    }

    #[test]
    fn confirmation_requires_a_committed_window_and_matching_fingerprint_and_preserves_queued_jobs()
    {
        for (confirm, changed, queued) in [
            (true, false, false),
            (true, true, false),
            (false, false, false),
            (true, false, true),
        ] {
            let (_dir, options, mut source, ino) = setup(Granularity::Both);
            cycle(&options, &mut source).unwrap();
            source.high += 1;
            source.nodes.get_mut(&ino).unwrap().transid = source.high;
            let (mut index, crawl) = open(&options, &source);
            index
                .conn
                .execute(
                    "INSERT INTO apply_mappings SELECT file_id FROM btrfs_files WHERE ino=?1",
                    [ino],
                )
                .unwrap();
            if queued {
                index
                    .conn
                    .execute(
                        "INSERT INTO discovery VALUES (?1,?2,0)",
                        params![crawl.id, ino],
                    )
                    .unwrap();
            }
            if changed {
                fs::write(source.base.join("f"), vec![42; 12288]).unwrap();
                source.nodes.get_mut(&ino).unwrap().fingerprint =
                    scan::fingerprint(&fs::metadata(source.base.join("f")).unwrap());
            }
            let mut stats = Stats::default();
            let boundary = confirm.then_some(source.high);
            discover_inner(&mut index, &crawl, &mut source, &mut stats, boundary).unwrap();
            process(&mut index, &crawl, &options, &mut source, &mut stats).unwrap();
            assert_eq!(
                stats.files_seen,
                u64::from(!confirm || changed || queued),
                "{stats:?}"
            );
            assert_eq!(stats.btrfs_events_reused, u64::from(confirm && !changed));
            assert_eq!(stats.bytes_hashed, if changed { 12288 } else { 0 });
            drop(index);
            assert_fresh_hash(&options);
        }
    }

    #[test]
    fn unconfirmed_maps_force_full_hash_after_a_write_even_if_maps_look_unchanged() {
        let (_dir, options, mut source, ino) = setup(Granularity::Both);
        cycle(&options, &mut source).unwrap();
        let (index, _) = open(&options, &source);
        index
            .conn
            .execute(
                "INSERT INTO apply_mappings SELECT file_id FROM btrfs_files WHERE ino=?1",
                [ino],
            )
            .unwrap();
        drop(index);
        // Model a rolled-back/reused physical mapping: it now holds different
        // bytes despite matching the speculative descriptors. No generation
        // event is needed: startup must queue the provisional observation.
        fs::write(source.base.join("f"), vec![42; 12288]).unwrap();
        let stats = cycle(&options, &mut source).unwrap();
        assert_eq!(stats.bytes_hashed, 12288, "{stats:?}");
        assert_eq!(stats.chunks_reused, 0);
        assert_fresh_hash(&options);
    }

    #[test]
    fn queue_seeks_past_failed_jobs_without_quadratic_rescans() {
        use rusqlite::StatementStatus;
        let (_dir, options, source, _) = setup(Granularity::Both);
        let (index, crawl) = open(&options, &source);
        index
            .conn
            .execute_batch("CREATE TEMP TABLE attempted (ino INTEGER PRIMARY KEY); BEGIN;")
            .unwrap();
        // Retain every job, simulating persistent failures. Interleave file
        // jobs to ensure selecting directories uses the kind/ino index too.
        let count = 20_000;
        for ino in 0..count {
            queue(&index, crawl.id, ino * 2, false).unwrap();
            queue(&index, crawl.id, ino * 2 + 1, true).unwrap();
        }
        index.conn.execute_batch("COMMIT").unwrap();
        let mut lower = i64::MIN;
        for ino in 0..count {
            assert_eq!(
                next_queued(&index.conn, crawl.id, true, &mut lower).unwrap(),
                Some((ino * 2 + 1) as i64)
            );
            index
                .conn
                .execute_cached("INSERT INTO attempted VALUES (?1)", [(ino * 2 + 1) as i64])
                .unwrap();
        }
        assert_eq!(
            next_queued(&index.conn, crawl.id, true, &mut lower).unwrap(),
            None
        );
        let statement = index.conn.prepare_cached(NEXT_QUEUED).unwrap();
        // Count actual SQLite VM work rather than timing a fast machine. This
        // includes the final wrap over failed jobs; growth must stay linear.
        assert!(statement.get_status(StatementStatus::VmStep) < count as i32 * 100);
        assert_eq!(statement.get_status(StatementStatus::Sort), 0);
        assert_eq!(statement.get_status(StatementStatus::FullscanStep), 0);
    }

    #[test]
    fn queue_wraps_for_new_earlier_jobs_and_retries_failures_on_restart() {
        let (_dir, options, source, _) = setup(Granularity::Both);
        let (index, crawl) = open(&options, &source);
        index
            .conn
            .execute_batch("CREATE TEMP TABLE attempted (ino INTEGER PRIMARY KEY);")
            .unwrap();
        for ino in [5, 10] {
            queue(&index, crawl.id, ino, true).unwrap();
        }
        let mut lower = i64::MIN;
        assert_eq!(
            next_queued(&index.conn, crawl.id, true, &mut lower).unwrap(),
            Some(5)
        );
        index
            .conn
            .execute("INSERT INTO attempted VALUES (5)", [])
            .unwrap();
        queue(&index, crawl.id, 2, true).unwrap();
        assert_eq!(
            next_queued(&index.conn, crawl.id, true, &mut lower).unwrap(),
            Some(10)
        );
        index
            .conn
            .execute("DELETE FROM discovery WHERE ino=10", [])
            .unwrap();
        assert_eq!(
            next_queued(&index.conn, crawl.id, true, &mut lower).unwrap(),
            Some(2)
        );
        index
            .conn
            .execute("DELETE FROM discovery WHERE ino=2", [])
            .unwrap();
        assert_eq!(
            next_queued(&index.conn, crawl.id, true, &mut lower).unwrap(),
            None
        );
        index.conn.execute("DELETE FROM attempted", []).unwrap();
        assert_eq!(
            next_queued(&index.conn, crawl.id, true, &mut lower).unwrap(),
            Some(5)
        );
        index.conn.execute("DELETE FROM discovery", []).unwrap();
        index.conn.execute("DELETE FROM attempted", []).unwrap();
        for ino in [i64::MIN, i64::MAX] {
            index
                .conn
                .execute(
                    "INSERT INTO discovery VALUES (?1,?2,1)",
                    params![crawl.id, ino],
                )
                .unwrap();
        }
        lower = i64::MIN;
        for ino in [i64::MIN, i64::MAX] {
            assert_eq!(
                next_queued(&index.conn, crawl.id, true, &mut lower).unwrap(),
                Some(ino)
            );
            index
                .conn
                .execute("INSERT INTO attempted VALUES (?1)", [ino])
                .unwrap();
        }
        assert_eq!(
            next_queued(&index.conn, crawl.id, true, &mut lower).unwrap(),
            None
        );
    }

    #[test]
    fn resumes_metadata_cursor_and_durable_queue_after_restart() {
        let (_dir, mut options, mut source, _) = setup(Granularity::Both);
        options.commit_batch_size = 1;
        source.fail_at = Some(2);
        assert!(cycle(&options, &mut source).is_err());
        let (index, _) = open(&options, &source);
        let saved: Vec<u8> = index
            .conn
            .query_row("SELECT cursor FROM crawls", [], |r| r.get(0))
            .unwrap();
        assert!(Key::decode(&saved).unwrap() > Key::FIRST);
        let queued: u64 = index
            .conn
            .query_row("SELECT COUNT(*) FROM discovery", [], |r| r.get(0))
            .unwrap();
        assert_eq!(queued, 1);
        drop(index);
        source.fail_at = None;
        source.calls.clear();
        let resumed = cycle(&options, &mut source).unwrap();
        assert_eq!(source.calls[0], Key::decode(&saved).unwrap());
        assert_eq!(resumed.bytes_hashed, 12288);
        source.calls.clear();
        let idle = cycle(&options, &mut source).unwrap();
        assert!(source.calls.is_empty());
        assert_eq!(idle.files_seen, 0);
        assert_eq!(idle.bytes_hashed, 0);
    }

    #[test]
    fn uncommitted_metadata_pages_replay_with_their_queue_after_restart() {
        let (_dir, options, mut source, _) = setup(Granularity::Both);
        source.fail_at = Some(2);
        assert!(cycle(&options, &mut source).is_err());
        let (index, _) = open(&options, &source);
        let saved: Vec<u8> = index
            .conn
            .query_row("SELECT cursor FROM crawls", [], |r| r.get(0))
            .unwrap();
        assert_eq!(Key::decode(&saved).unwrap(), Key::FIRST);
        let queued: u64 = index
            .conn
            .query_row("SELECT COUNT(*) FROM discovery", [], |r| r.get(0))
            .unwrap();
        assert_eq!(queued, 0);
        drop(index);
        source.fail_at = None;
        source.calls.clear();
        let resumed = cycle(&options, &mut source).unwrap();
        assert_eq!(source.calls[0], Key::FIRST);
        assert_eq!(resumed.bytes_hashed, 12288);
        assert_fresh_hash(&options);
    }

    #[test]
    fn resumed_window_catches_up_with_changes_made_during_downtime() {
        let (_dir, options, mut source, _) = setup(Granularity::Both);
        source.fail_at = Some(2);
        assert!(cycle(&options, &mut source).is_err());
        source.fail_at = None;
        source.high = 2;
        fs::write(source.base.join("new"), vec![2; 4096]).unwrap();
        source.add("new", vec![extent(0, 4096, 200, 0, 2)]);
        let result = cycle(&options, &mut source).unwrap();
        assert!(result.errors.is_empty(), "{result:?}");
        assert_eq!(result.bytes_hashed, 16384);
        let (index, _) = open(&options, &source);
        assert_eq!(
            index
                .conn
                .query_row("SELECT completed FROM crawls", [], |r| r.get::<_, u64>(0))
                .unwrap(),
            2
        );
        assert_eq!(
            index
                .conn
                .query_row("SELECT COUNT(*) FROM discovery", [], |r| r.get::<_, u64>(0))
                .unwrap(),
            0
        );
    }

    #[test]
    fn small_cow_write_reuses_other_chunks_and_preserves_whole_file_hash() {
        for granularity in [Granularity::File, Granularity::Both] {
            let (_dir, options, mut source, ino) = setup(granularity);
            cycle(&options, &mut source).unwrap();
            let writer = OpenOptions::new()
                .write(true)
                .open(source.base.join("f"))
                .unwrap();
            writer.write_all_at(&vec![2; 4096], 4096).unwrap();
            source.high = 2;
            let node = source.nodes.get_mut(&ino).unwrap();
            node.transid = 2;
            node.extents = vec![
                extent(0, 4096, 100, 0, 1),
                extent(4096, 4096, 200, 0, 2),
                extent(8192, 4096, 100, 8192, 1),
            ];
            let result = cycle(&options, &mut source).unwrap();
            assert!(result.errors.is_empty(), "{result:?}");
            assert_eq!(result.bytes_hashed, 4096);
            assert_eq!(result.chunks_reused, 2);
            assert_eq!(result.files_seen, 1);
            assert_fresh_hash(&options);
        }
    }

    #[test]
    fn truncated_eof_and_sparse_extension_update_cached_fingerprints() {
        let (_dir, options, mut source, ino) = setup(Granularity::Both);
        cycle(&options, &mut source).unwrap();
        OpenOptions::new()
            .write(true)
            .open(source.base.join("f"))
            .unwrap()
            .set_len(5000)
            .unwrap();
        source.high = 2;
        let node = source.nodes.get_mut(&ino).unwrap();
        node.transid = 2;
        node.extents = vec![extent(0, 8192, 100, 0, 1)];
        let result = cycle(&options, &mut source).unwrap();
        assert!(result.errors.is_empty(), "{result:?}");
        assert_eq!(result.bytes_hashed, 904);
        assert_eq!(result.chunks_reused, 1);
        assert_fresh_hash(&options);
        OpenOptions::new()
            .write(true)
            .open(source.base.join("f"))
            .unwrap()
            .set_len(1024 * 1024)
            .unwrap();
        source.high = 3;
        source.nodes.get_mut(&ino).unwrap().transid = 3;
        let result = cycle(&options, &mut source).unwrap();
        assert!(result.errors.is_empty(), "{result:?}");
        assert!(result.bytes_hashed < 16384);
        assert_fresh_hash(&options);
    }

    #[test]
    fn failed_jobs_retry_without_repeating_discovery_and_ack_replay_reads_no_data() {
        let (_dir, options, mut source, ino) = setup(Granularity::File);
        source.fail_file = true;
        let failed = cycle(&options, &mut source).unwrap();
        assert!(!failed.errors.is_empty());
        source.fail_file = false;
        source.calls.clear();
        let (mut index, crawl) = open(&options, &source);
        let mut stats = Stats::default();
        file_job(&mut index, &crawl, ino, &options, &mut source, &mut stats).unwrap();
        assert_eq!(stats.bytes_hashed, 12288);
        drop(index); // simulate crash before queue acknowledgement
        let replay = cycle(&options, &mut source).unwrap();
        assert!(replay.errors.is_empty(), "{replay:?}");
        assert!(source.calls.is_empty());
        assert_eq!(replay.bytes_hashed, 0);
        let (index, _) = open(&options, &source);
        assert_eq!(
            index
                .conn
                .query_row("SELECT COUNT(*) FROM discovery", [], |r| r.get::<_, u64>(0))
                .unwrap(),
            0
        );
    }

    #[test]
    fn newer_leaves_do_not_make_old_items_new_and_future_items_wait_for_next_window() {
        let (_dir, options, mut source, ino) = setup(Granularity::Both);
        source.nodes.get_mut(&ino).unwrap().transid = 2;
        let first = cycle(&options, &mut source).unwrap();
        assert_eq!(first.bytes_hashed, 0);
        source.high = 2;
        let next = cycle(&options, &mut source).unwrap();
        assert_eq!(next.bytes_hashed, 12288);
        source.high = 3;
        let old = cycle(&options, &mut source).unwrap();
        assert_eq!(old.files_seen, 0);
        assert_eq!(old.discovery_jobs, 0);
    }

    #[test]
    fn nocow_changes_and_new_inode_incarnations_force_full_rehash() {
        for incarnation in [false, true] {
            let (_dir, options, mut source, ino) = setup(Granularity::Both);
            cycle(&options, &mut source).unwrap();
            fs::write(source.base.join("f"), vec![2; 12288]).unwrap();
            source.high = 2;
            let node = source.nodes.get_mut(&ino).unwrap();
            node.transid = 2;
            if incarnation {
                node.generation = 2;
            } else {
                node.flags = 2;
            }
            let result = cycle(&options, &mut source).unwrap();
            assert_eq!(result.bytes_hashed, 12288);
            assert_fresh_hash(&options);
        }
    }

    #[test]
    fn renamed_directory_then_deleted_child_reconciles_descendants() {
        let (_dir, options, mut source, ino) = setup(Granularity::Both);
        fs::create_dir_all(source.base.join("a/child")).unwrap();
        fs::rename(source.base.join("f"), source.base.join("a/child/f")).unwrap();
        source.nodes.get_mut(&ino).unwrap().path = "a/child/f".into();
        let a = source.add("a", vec![]);
        let child = source.add("a/child", vec![]);
        cycle(&options, &mut source).unwrap();
        fs::rename(source.base.join("a"), source.base.join("b")).unwrap();
        source.high = 2;
        source.nodes.get_mut(&a).unwrap().transid = 2;
        source.nodes.get_mut(&a).unwrap().path = "b".into();
        source.nodes.get_mut(&child).unwrap().path = "b/child".into();
        source.nodes.get_mut(&ino).unwrap().path = "b/child/f".into();
        let renamed = cycle(&options, &mut source).unwrap();
        assert!(renamed.errors.is_empty(), "{renamed:?}");
        assert_eq!(renamed.bytes_hashed, 0);
        let (index, _) = open(&options, &source);
        let path: Vec<u8> = index
            .conn
            .query_row("SELECT path FROM files", [], |r| r.get(0))
            .unwrap();
        assert_eq!(decode_path(path), source.base.join("b/child/f"));
        drop(index);
        fs::remove_dir_all(source.base.join("b/child")).unwrap();
        source.high = 3;
        source.nodes.remove(&child);
        source.nodes.remove(&ino);
        source.nodes.get_mut(&a).unwrap().transid = 3;
        let removed = cycle(&options, &mut source).unwrap();
        assert!(removed.errors.is_empty(), "{removed:?}");
        assert_eq!(removed.files_removed, 1);
    }

    #[test]
    fn moved_into_scope_discovers_old_descendant_inodes() {
        let (dir, options, mut source, ino) = setup(Granularity::Both);
        // Make the metadata tree cover a larger subvolume than the input scope.
        source.base = dir.path().to_owned();
        fs::create_dir(dir.path().join("outside")).unwrap();
        fs::rename(options.roots[0].join("f"), dir.path().join("outside/f")).unwrap();
        for node in source.nodes.values_mut() {
            node.path = Path::new("data").join(&node.path);
        }
        source.nodes.get_mut(&ino).unwrap().path = "outside/f".into();
        source.add("", vec![]);
        let outside = source.add("outside", vec![]);
        let first = cycle(&options, &mut source).unwrap();
        assert_eq!(first.files_seen, 0);
        fs::rename(dir.path().join("outside"), options.roots[0].join("inside")).unwrap();
        source.high = 2;
        source.nodes.get_mut(&outside).unwrap().path = "data/inside".into();
        source.nodes.get_mut(&outside).unwrap().transid = 2;
        source.nodes.get_mut(&ino).unwrap().path = "data/inside/f".into();
        let moved = cycle(&options, &mut source).unwrap();
        assert!(moved.errors.is_empty(), "{moved:?}");
        assert_eq!(moved.bytes_hashed, 12288);
    }

    #[test]
    fn moved_in_old_directories_track_later_deleted_descendants() {
        let (dir, options, mut source, ino) = setup(Granularity::Both);
        source.base = dir.path().to_owned();
        fs::create_dir_all(dir.path().join("outside/child/grandchild")).unwrap();
        fs::rename(
            options.roots[0].join("f"),
            dir.path().join("outside/child/grandchild/f"),
        )
        .unwrap();
        for node in source.nodes.values_mut() {
            node.path = Path::new("data").join(&node.path);
        }
        source.nodes.get_mut(&ino).unwrap().path = "outside/child/grandchild/f".into();
        source.add("", vec![]);
        let outside = source.add("outside", vec![]);
        let child = source.add("outside/child", vec![]);
        let grandchild = source.add("outside/child/grandchild", vec![]);
        assert_eq!(cycle(&options, &mut source).unwrap().files_seen, 0);
        fs::rename(dir.path().join("outside"), options.roots[0].join("inside")).unwrap();
        source.high = 2;
        for id in [outside, child, grandchild, ino] {
            let node = source.nodes.get_mut(&id).unwrap();
            node.path = Path::new("data/inside").join(node.path.strip_prefix("outside").unwrap());
        }
        source.nodes.get_mut(&outside).unwrap().path = "data/inside".into();
        source.nodes.get_mut(&outside).unwrap().transid = 2;
        let moved = cycle(&options, &mut source).unwrap();
        assert!(moved.errors.is_empty(), "{moved:?}");
        assert_eq!(moved.bytes_hashed, 12288);
        fs::remove_dir_all(options.roots[0].join("inside/child")).unwrap();
        source.high = 3;
        source.nodes.get_mut(&outside).unwrap().transid = 3;
        for id in [child, grandchild, ino] {
            source.nodes.remove(&id);
        }
        let removed = cycle(&options, &mut source).unwrap();
        assert!(removed.errors.is_empty(), "{removed:?}");
        assert_eq!(removed.files_removed, 1);
    }

    #[test]
    fn generation_rollback_discards_cached_baselines() {
        let (_dir, options, mut source, _) = setup(Granularity::Both);
        source.high = 4;
        cycle(&options, &mut source).unwrap();
        source.high = 2;
        let result = cycle(&options, &mut source).unwrap();
        assert!(result.errors.is_empty(), "{result:?}");
        assert_eq!(result.bytes_hashed, 12288);
    }

    #[test]
    fn stale_apply_path_requeues_native_identity_without_another_inode_event() {
        struct Mock;
        impl crate::Backend for Mock {
            fn dedupe(
                &mut self,
                _: &File,
                _: &File,
                range: crate::Range,
                _: u64,
            ) -> std::io::Result<crate::Outcome> {
                Ok(crate::Outcome::Shared(range.len))
            }
        }
        let (_dir, options, mut source, _) = setup(Granularity::File);
        fs::write(source.base.join("b"), vec![1; 12288]).unwrap();
        source.add("b", vec![extent(0, 12288, 200, 0, 1)]);
        cycle(&options, &mut source).unwrap();
        let (mut index, _) = open(&options, &source);
        let (ino, bytes): (u64, Vec<u8>) = index
            .conn
            .query_row("SELECT ino,path FROM files ORDER BY id LIMIT 1", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .unwrap();
        fs::rename(decode_path(bytes), source.base.join("renamed")).unwrap();
        source.nodes.get_mut(&ino).unwrap().path = "renamed".into();
        let mut stats = Stats::default();
        index.apply(&mut Mock, &mut stats).unwrap();
        assert!(!stats.errors.is_empty());
        drop(index);
        source.calls.clear();
        let retry = cycle(&options, &mut source).unwrap();
        assert!(retry.errors.is_empty(), "{retry:?}");
        assert!(source.calls.is_empty());
        assert_eq!(retry.bytes_hashed, 12288);
        let (index, _) = open(&options, &source);
        assert_eq!(
            index
                .conn
                .query_row("SELECT COUNT(*) FROM files", [], |r| r.get::<_, u64>(0))
                .unwrap(),
            2
        );
    }
}
