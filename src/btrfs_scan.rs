//! Persistent generation windows and a durable inode work queue. Discovery
//! commits its cursor and queue together; processing acknowledges only after
//! stable hashing or a confirmed deletion/out-of-scope observation.
use crate::{
    btrfs_io::{self as io, Inode, Item, Key},
    index::{decode_path, path_bytes, Index},
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
        index.conn.execute(
            "INSERT OR IGNORE INTO crawls(identity,path) VALUES (?1,?2)",
            params![identity, path_bytes(&path)],
        )?;
        let id: i64 =
            index
                .conn
                .query_row("SELECT id FROM crawls WHERE identity=?1", [identity], |r| {
                    r.get(0)
                })?;
        ids.insert(id);
        crawls.push(Crawl { id, path, file });
    }
    // Missing subvolumes or changed UUIDs invalidate their old observations.
    let stale: Vec<i64> = index
        .conn
        .prepare("SELECT id FROM crawls")?
        .query_map([], |r| r.get(0))?
        .collect::<rusqlite::Result<_>>()?;
    for id in stale.into_iter().filter(|id| !ids.contains(id)) {
        let tx = index.conn.transaction()?;
        stats.files_removed += tx.execute(
            "DELETE FROM files WHERE id IN (SELECT file_id FROM btrfs_files WHERE crawl_id=?1)",
            [id],
        )? as u64;
        tx.execute("DELETE FROM crawls WHERE id=?1", [id])?;
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
        stats.files_removed += index.conn.execute(
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
    let now = source.boundary(&crawl.file)?;
    let (completed, high): (u64, Option<u64>) = index.conn.query_row(
        "SELECT completed,high FROM crawls WHERE id=?1",
        [crawl.id],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    if now < completed || high.is_some_and(|high| now < high) {
        // Restored/rolled-back filesystem: a previous extent baseline cannot
        // certify data on the new timeline, even if an inode number repeats.
        let tx = index.conn.transaction()?;
        tx.execute(
            "DELETE FROM files WHERE id IN (SELECT file_id FROM btrfs_files WHERE crawl_id=?1)",
            [crawl.id],
        )?;
        tx.execute("DELETE FROM discovery WHERE crawl_id=?1", [crawl.id])?;
        tx.execute("DELETE FROM btrfs_dirs WHERE crawl_id=?1", [crawl.id])?;
        tx.execute("UPDATE crawls SET completed=0,low=NULL,high=NULL,cursor=NULL,enumerated=0,initialized=0 WHERE id=?1",[crawl.id])?;
        tx.commit()?;
    }
    let (completed, high): (u64, Option<u64>) = index.conn.query_row(
        "SELECT completed,high FROM crawls WHERE id=?1",
        [crawl.id],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    if high.is_none() {
        if completed == now {
            return Ok(());
        }
        index.conn.execute(
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
        let (low, high, cursor): (u64, u64, Vec<u8>) = index.conn.query_row(
            "SELECT low,high,cursor FROM crawls WHERE id=?1",
            [crawl.id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;
        let key = Key::decode(&cursor)?;
        let page = source.page(&crawl.file, key, low)?;
        stats.btrfs_search_pages += 1;
        let tx = index.conn.transaction()?;
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
            tx.execute("INSERT INTO discovery VALUES (?1,?2,?3) ON CONFLICT(crawl_id,ino) DO UPDATE SET directory=excluded.directory",params![crawl.id,item.key.ino,inode.directory()])?;
            stats.discovery_jobs += 1;
        }
        if page.is_empty() || page.last().unwrap().key.next().is_none() {
            // Advancing the generation is safe because ALL discovered work is
            // durable, including jobs that fail processing and need later retry.
            tx.execute("UPDATE crawls SET completed=high,low=NULL,high=NULL,cursor=NULL,enumerated=1 WHERE id=?1",[crawl.id])?;
            tx.commit()?;
            if high < now {
                // Finish a resumed older window, then catch up through the
                // boundary captured at this invocation's start. Do not chase
                // an unbounded stream of concurrent writers.
                index.conn.execute(
                    "UPDATE crawls SET low=?2,high=?3,cursor=?4,enumerated=0 WHERE id=?1",
                    params![crawl.id, high + 1, now, Key::FIRST.encode()],
                )?;
                continue;
            }
            return Ok(());
        }
        let next = page.last().unwrap().key.next().unwrap();
        ensure!(next > key, "btrfs cursor did not advance");
        tx.execute(
            "UPDATE crawls SET cursor=?2 WHERE id=?1",
            params![crawl.id, next.encode()],
        )?;
        tx.commit()?;
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
    let initial = !index.conn.query_row(
        "SELECT initialized FROM crawls WHERE id=?1",
        [crawl.id],
        |r| r.get::<_, bool>(0),
    )?;
    for directory in [true, false] {
        index.conn.execute("DELETE FROM attempted", [])?;
        loop {
            let ino:Option<i64>=index.conn.query_row("SELECT ino FROM discovery WHERE crawl_id=?1 AND directory=?2 AND ino NOT IN (SELECT ino FROM attempted) ORDER BY ino LIMIT 1",params![crawl.id,directory],|r|r.get(0)).optional()?;
            let Some(ino) = ino else { break };
            index
                .conn
                .execute("INSERT INTO attempted VALUES (?1)", [ino])?;
            let result = if directory {
                directory_job(index, crawl, ino as u64, source, initial)
            } else {
                file_job(index, crawl, ino as u64, options, source, stats)
            };
            if let Err(error) = result {
                stats
                    .errors
                    .push(format!("btrfs {:?} inode {ino}: {error:#}", crawl.path));
                // Keep the queue entry; invalidate its hash baseline so a retry
                // cannot trust data from an unstable or inaccessible file.
                if !directory {
                    index.conn.execute("DELETE FROM files WHERE id IN (SELECT file_id FROM btrfs_files WHERE crawl_id=?1 AND ino=?2)",params![crawl.id,ino])?;
                }
            } else {
                index.conn.execute(
                    "DELETE FROM discovery WHERE crawl_id=?1 AND ino=?2",
                    params![crawl.id, ino],
                )?;
            }
        }
        if directory {
            // Failed bootstrap directory jobs remain queued. Subsequent cycles
            // must nevertheless treat newly included subtrees as new scope.
            index.conn.execute(
                "UPDATE crawls SET initialized=1 WHERE id=?1 AND initialized=0",
                [crawl.id],
            )?;
        }
    }
    Ok(())
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
        stats.files_removed+=index.conn.execute("DELETE FROM files WHERE id IN (SELECT file_id FROM btrfs_files WHERE crawl_id=?1 AND ino=?2)",params![crawl.id,ino])? as u64;
        return Ok(());
    };
    let file = scan::open_regular(&path, false)?;
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
    scan::index_file(index, &path, options, stats, &excluded, Some(&view))?;
    let file_id: Option<i64> = index
        .conn
        .query_row(
            "SELECT id FROM files WHERE dev=?1 AND ino=?2",
            params![before.dev() as i64, ino],
            |r| r.get(0),
        )
        .optional()?;
    if let Some(file_id) = file_id {
        index.conn.execute("INSERT INTO btrfs_files VALUES (?1,?2,?3,?4) ON CONFLICT(file_id) DO UPDATE SET crawl_id=excluded.crawl_id,ino=excluded.ino,parent=excluded.parent",params![file_id,crawl.id,ino,path_bytes(path.parent().unwrap())])?;
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
        .query_row(
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
                        index.conn.execute(
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
        index.conn.execute(
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
    let names = fs::read_dir(&path)?
        .map(|entry| entry.map(|e| e.file_name()))
        .collect::<std::io::Result<HashSet<_>>>()?;
    // Deleted inodes have no tree item. A changed parent directory is the
    // evidence for looking up its former children and pruning deleted names.
    let files:Vec<(u64,Vec<u8>)>=index.conn.prepare("SELECT b.ino,f.path FROM btrfs_files b JOIN files f ON f.id=b.file_id WHERE b.crawl_id=?1 AND b.parent=?2")?.query_map(params![crawl.id,path_bytes(&path)],|r|Ok((r.get(0)?,r.get(1)?)))?.collect::<rusqlite::Result<_>>()?;
    for (child, bytes) in files {
        let child_path = decode_path(bytes);
        if child_path
            .file_name()
            .is_none_or(|name| !names.contains(name))
        {
            queue(index, crawl.id, child, false)?;
        }
    }
    let dirs: Vec<(u64, Vec<u8>)> = index
        .conn
        .prepare("SELECT ino,path FROM btrfs_dirs WHERE crawl_id=?1 AND parent=?2 AND ino!=?3")?
        .query_map(params![crawl.id, path_bytes(&path), ino], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })?
        .collect::<rusqlite::Result<_>>()?;
    for (child, bytes) in dirs {
        let child_path = decode_path(bytes);
        let matches = fs::symlink_metadata(&child_path)
            .is_ok_and(|meta| meta.is_dir() && meta.ino() == child);
        if !matches {
            queue(index, crawl.id, child, true)?;
        }
    }
    index.conn.execute("INSERT INTO btrfs_dirs VALUES (?1,?2,?3,?4) ON CONFLICT(crawl_id,ino) DO UPDATE SET path=excluded.path,parent=excluded.parent",params![crawl.id,ino,path_bytes(&path),path_bytes(path.parent().unwrap_or(&path))])?;
    Ok(())
}

fn move_directories(index: &mut Index, crawl: i64, old: &Path, new: Option<&Path>) -> Result<()> {
    let (prefix, end) = prefix_bounds(old);
    let dirs: Vec<(u64, Vec<u8>)> = index
        .conn
        .prepare("SELECT ino,path FROM btrfs_dirs WHERE crawl_id=?1 AND path>=?2 AND path<?3")?
        .query_map(params![crawl, prefix, end], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect::<rusqlite::Result<_>>()?;
    let tx = index.conn.transaction()?;
    for (ino, bytes) in dirs {
        if let Some(new) = new {
            let previous = decode_path(bytes);
            let path = new.join(previous.strip_prefix(old)?);
            tx.execute(
                "UPDATE btrfs_dirs SET path=?3,parent=?4 WHERE crawl_id=?1 AND ino=?2",
                params![
                    crawl,
                    ino,
                    path_bytes(&path),
                    path_bytes(path.parent().unwrap())
                ],
            )?;
        } else {
            tx.execute(
                "DELETE FROM btrfs_dirs WHERE crawl_id=?1 AND ino=?2",
                params![crawl, ino],
            )?;
        }
    }
    tx.commit()?;
    Ok(())
}

fn queue(index: &Index, crawl: i64, ino: u64, directory: bool) -> Result<()> {
    index.conn.execute("INSERT INTO discovery VALUES (?1,?2,?3) ON CONFLICT(crawl_id,ino) DO UPDATE SET directory=excluded.directory",params![crawl,ino,directory])?;
    Ok(())
}

fn queue_prefix(index: &Index, crawl: i64, path: &Path) -> Result<()> {
    let (prefix, end) = prefix_bounds(path);
    index.conn.execute("INSERT OR IGNORE INTO discovery SELECT b.crawl_id,b.ino,0 FROM btrfs_files b JOIN files f ON f.id=b.file_id WHERE b.crawl_id=?1 AND f.path>=?2 AND f.path<?3",params![crawl,prefix,end])?;
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
    fn resumes_metadata_cursor_and_durable_queue_after_restart() {
        let (_dir, options, mut source, _) = setup(Granularity::Both);
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
