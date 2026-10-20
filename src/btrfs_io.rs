//! Read-only btrfs metadata interfaces. Disk payloads are little endian; search
//! headers use native endian. Never apply the search upper generation to leaves:
//! a newer COW leaf can still contain older, unprocessed inode items.
use anyhow::{bail, ensure, Context, Result};
use std::{
    fs::File,
    io,
    os::{fd::AsRawFd, unix::ffi::OsStrExt},
    path::{Component, Path, PathBuf},
};

pub const INODE: u32 = 1;
const EXTENT: u32 = 108;
const ROOT_REF: u32 = 156;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Key {
    pub ino: u64,
    pub kind: u32,
    pub offset: u64,
}

impl Key {
    pub const FIRST: Self = Self {
        ino: 256,
        kind: 0,
        offset: 0,
    };
    pub const LAST: Self = Self {
        ino: u64::MAX,
        kind: 255,
        offset: u64::MAX,
    };
    pub fn next(self) -> Option<Self> {
        if self.offset < u64::MAX {
            Some(Self {
                offset: self.offset + 1,
                ..self
            })
        } else if self.kind < 255 {
            Some(Self {
                kind: self.kind + 1,
                offset: 0,
                ..self
            })
        } else {
            self.ino.checked_add(1).map(|ino| Self {
                ino,
                kind: 0,
                offset: 0,
            })
        }
    }
    pub fn encode(self) -> Vec<u8> {
        [
            self.ino.to_le_bytes().as_slice(),
            self.kind.to_le_bytes().as_slice(),
            self.offset.to_le_bytes().as_slice(),
        ]
        .concat()
    }
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        ensure!(bytes.len() == 20, "invalid persisted tree cursor");
        Ok(Self {
            ino: le64(bytes, 0)?,
            kind: le32(bytes, 8)?,
            offset: le64(bytes, 12)?,
        })
    }
}

#[derive(Debug)]
pub struct Item {
    pub key: Key,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Extent {
    pub offset: u64,
    pub length: u64,
    pub descriptor: Vec<u8>,
}

pub struct Inode {
    pub generation: u64,
    pub transid: u64,
    pub mode: u32,
    pub flags: u64,
}
impl Inode {
    pub fn parse(data: &[u8]) -> Result<Self> {
        ensure!(data.len() >= 160, "short btrfs inode item");
        Ok(Self {
            generation: le64(data, 0)?,
            transid: le64(data, 8)?,
            mode: le32(data, 52)?,
            flags: le64(data, 64)?,
        })
    }
    pub fn directory(&self) -> bool {
        self.mode & libc::S_IFMT == libc::S_IFDIR
    }
    pub fn regular(&self) -> bool {
        self.mode & libc::S_IFMT == libc::S_IFREG
    }
    pub fn cow(&self) -> bool {
        self.flags & (1 | 2) == 0
    } // NODATASUM / NODATACOW
}

#[repr(C)]
#[derive(Default)]
struct SearchKey {
    tree: u64,
    min_ino: u64,
    max_ino: u64,
    min_offset: u64,
    max_offset: u64,
    min_transid: u64,
    max_transid: u64,
    min_type: u32,
    max_type: u32,
    count: u32,
    unused: u32,
    reserved: [u64; 4],
}
#[repr(C)]
struct SearchHeader {
    key: SearchKey,
    size: u64,
}
nix::ioctl_readwrite!(search_ioctl, 0x94, 17, SearchHeader);

pub fn search(file: &File, tree: u64, start: Key, end: Key, min_transid: u64) -> Result<Vec<Item>> {
    let mut size: usize = 64 * 1024;
    loop {
        // u64 allocation supplies alignment for the header and flexible buffer.
        let mut storage = vec![0u64; (112 + size).div_ceil(8)];
        let pointer = storage.as_mut_ptr().cast::<SearchHeader>();
        let header = SearchHeader {
            key: SearchKey {
                tree,
                min_ino: start.ino,
                max_ino: end.ino,
                min_offset: start.offset,
                max_offset: end.offset,
                min_transid,
                max_transid: u64::MAX,
                min_type: start.kind,
                max_type: end.kind,
                count: 1024,
                ..Default::default()
            },
            size: size as u64,
        };
        // SAFETY: aligned owned storage includes the complete header and buffer.
        unsafe { pointer.write(header) };
        // SAFETY: live fd, initialized header and writable flexible output.
        match unsafe { search_ioctl(file.as_raw_fd(), pointer) } {
            Ok(_) => {
                // SAFETY: ioctl completed, storage remains alive and exclusive.
                let count = unsafe { (*pointer).key.count } as usize;
                ensure!(count <= 1024, "invalid btrfs search item count");
                // SAFETY: allocated and initialized byte buffer of this size.
                let bytes = unsafe {
                    std::slice::from_raw_parts(storage.as_ptr().cast::<u8>().add(112), size)
                };
                return parse_page(bytes, count, start, end);
            }
            Err(nix::errno::Errno::EOVERFLOW) => {
                // SAFETY: kernel writes required size on EOVERFLOW.
                let required = unsafe { (*pointer).size } as usize;
                ensure!(
                    required > size && required <= 16 * 1024 * 1024,
                    "invalid btrfs search buffer size"
                );
                size = required;
            }
            Err(error) => return Err(io::Error::from_raw_os_error(error as i32).into()),
        }
    }
}

fn parse_page(bytes: &[u8], count: usize, start: Key, end: Key) -> Result<Vec<Item>> {
    let mut at = 0usize;
    let mut result = Vec::with_capacity(count);
    let mut previous = None;
    for _ in 0..count {
        let header = bytes
            .get(at..at.checked_add(32).context("search offset overflow")?)
            .context("truncated search header")?;
        let key = Key {
            ino: u64::from_ne_bytes(header[8..16].try_into()?),
            offset: u64::from_ne_bytes(header[16..24].try_into()?),
            kind: u32::from_ne_bytes(header[24..28].try_into()?),
        };
        ensure!(
            key >= start && key <= end && previous.is_none_or(|p| key > p),
            "unordered or out-of-range btrfs search results"
        );
        let len = u32::from_ne_bytes(header[28..32].try_into()?) as usize;
        at += 32;
        let finish = at.checked_add(len).context("search length overflow")?;
        result.push(Item {
            key,
            data: bytes
                .get(at..finish)
                .context("truncated search item")?
                .to_vec(),
        });
        previous = Some(key);
        at = finish;
    }
    Ok(result)
}

pub fn inode(file: &File, ino: u64) -> Result<Option<Inode>> {
    let key = Key {
        ino,
        kind: INODE,
        offset: 0,
    };
    search(file, 0, key, key, 0)?
        .first()
        .map(|item| Inode::parse(&item.data))
        .transpose()
}

pub fn extents(file: &File, ino: u64) -> Result<Vec<Extent>> {
    let mut key = Key {
        ino,
        kind: EXTENT,
        offset: 0,
    };
    let end = Key {
        offset: u64::MAX,
        ..key
    };
    let mut result = Vec::new();
    loop {
        let page = search(file, 0, key, end, 0)?;
        if page.is_empty() {
            return Ok(result);
        }
        for item in &page {
            ensure!(item.data.len() >= 21, "short btrfs file extent");
            let length = match item.data[20] {
                0 => le64(&item.data, 8)?, // inline, potentially compressed
                1 | 2 => le64(&item.data, 45)?,
                _ => bail!("unknown btrfs extent type"),
            };
            ensure!(
                length > 0 && item.key.offset.checked_add(length).is_some(),
                "invalid extent length"
            );
            result.push(Extent {
                offset: item.key.offset,
                length,
                descriptor: item.data.clone(),
            });
        }
        let Some(next) = page.last().unwrap().key.next().filter(|next| *next <= end) else {
            return Ok(result);
        };
        key = next;
    }
}

#[repr(C)]
struct Lookup {
    tree: u64,
    ino: u64,
    name: [u8; 4080],
}
nix::ioctl_readwrite!(lookup_ioctl, 0x94, 18, Lookup);

pub fn lookup(file: &File, tree: u64, ino: u64) -> Result<PathBuf> {
    let mut arg = Lookup {
        tree,
        ino,
        name: [0; 4080],
    };
    // SAFETY: valid fd and fully initialized C-layout argument.
    unsafe { lookup_ioctl(file.as_raw_fd(), &mut arg) }.map_err(io::Error::from)?;
    relative(
        &arg.name[..arg
            .name
            .iter()
            .position(|b| *b == 0)
            .context("unterminated inode path")?],
    )
}

pub fn tree_id(file: &File) -> Result<u64> {
    let mut arg = Lookup {
        tree: 0,
        ino: 256,
        name: [0; 4080],
    };
    // SAFETY: valid fd and fully initialized C-layout argument.
    unsafe { lookup_ioctl(file.as_raw_fd(), &mut arg) }.map_err(io::Error::from)?;
    Ok(arg.tree)
}

#[repr(C)]
struct PathArgs {
    ino: u64,
    size: u64,
    reserved: [u64; 4],
    pointer: u64,
}
nix::ioctl_readwrite!(paths_ioctl, 0x94, 35, PathArgs);

pub fn paths(file: &File, ino: u64) -> Result<Vec<PathBuf>> {
    if ino == 256 {
        return Ok(vec![PathBuf::new()]);
    }
    raw_paths(file, ino)
}

pub fn probe_paths(file: &File) -> Result<()> {
    let mut buffer = [0u64; 512];
    let mut args = PathArgs {
        ino: 256,
        size: 4096,
        reserved: [0; 4],
        pointer: buffer.as_mut_ptr() as u64,
    };
    // SAFETY: initialized UAPI argument and live referenced output storage.
    // Root inode paths can contain the special '..' reference; the probe only
    // checks authorization, so it deliberately does not interpret that path.
    match unsafe { paths_ioctl(file.as_raw_fd(), &mut args) } {
        Ok(_) | Err(nix::errno::Errno::ENOENT) => Ok(()),
        Err(error) => Err(io::Error::from(error).into()),
    }
}

fn raw_paths(file: &File, ino: u64) -> Result<Vec<PathBuf>> {
    let mut buffer = [0u64; 512];
    let mut args = PathArgs {
        ino,
        size: 4096,
        reserved: [0; 4],
        pointer: buffer.as_mut_ptr() as u64,
    };
    // SAFETY: args and its separately referenced 4 KiB output stay alive.
    match unsafe { paths_ioctl(file.as_raw_fd(), &mut args) } {
        Ok(_) => {}
        Err(nix::errno::Errno::ENOENT) => return Ok(vec![]),
        Err(error) => return Err(io::Error::from(error).into()),
    }
    // SAFETY: viewing fully initialized storage as bytes is valid.
    let bytes = unsafe { std::slice::from_raw_parts(buffer.as_ptr().cast::<u8>(), 4096) };
    let missing = u32::from_ne_bytes(bytes[4..8].try_into()?);
    let count = u32::from_ne_bytes(bytes[8..12].try_into()?) as usize;
    let missed = u32::from_ne_bytes(bytes[12..16].try_into()?);
    ensure!(
        missing == 0 && missed == 0,
        "inode path buffer overflow; use --scanner portable for this hardlink set"
    );
    ensure!(count <= (4096 - 16) / 8, "invalid inode path count");
    let mut result = Vec::with_capacity(count);
    for i in 0..count {
        let offset = u64::from_ne_bytes(bytes[16 + i * 8..24 + i * 8].try_into()?);
        let start = usize::try_from(offset)?
            .checked_add(16)
            .context("path offset overflow")?;
        ensure!(start >= 16 + count * 8, "inode path overlaps pointer array");
        let value = bytes.get(start..).context("inode path outside buffer")?;
        let end = value
            .iter()
            .position(|b| *b == 0)
            .context("unterminated inode path")?;
        result.push(relative(&value[..end])?);
    }
    Ok(result)
}

fn relative(bytes: &[u8]) -> Result<PathBuf> {
    let path = Path::new(std::ffi::OsStr::from_bytes(bytes));
    ensure!(
        path.components().all(|c| matches!(c, Component::Normal(_))),
        "unsafe path returned by btrfs"
    );
    Ok(path.to_owned())
}

#[repr(C)]
struct SubvolInfo {
    tree: u64,
    name: [u8; 256],
    parent: u64,
    dir: u64,
    generation: u64,
    flags: u64,
    uuid: [u8; 16],
    rest: [u8; 192],
}
nix::ioctl_read!(subvol_ioctl, 0x94, 60, SubvolInfo);
pub fn identity(file: &File) -> Result<Vec<u8>> {
    // SAFETY: output structure contains only integer fields.
    let mut info: SubvolInfo = unsafe { std::mem::zeroed() };
    // SAFETY: correctly sized writable output, live fd.
    unsafe { subvol_ioctl(file.as_raw_fd(), &mut info) }.map_err(io::Error::from)?;
    let mut identity = crate::backend::filesystem(file)?.key;
    identity.extend_from_slice(&info.tree.to_le_bytes());
    identity.extend_from_slice(&info.uuid);
    Ok(identity)
}

pub fn children(file: &File) -> Result<Vec<PathBuf>> {
    let parent = tree_id(file)?;
    let mut key = Key {
        ino: parent,
        kind: ROOT_REF,
        offset: 0,
    };
    let end = Key {
        offset: u64::MAX,
        ..key
    };
    let mut result = Vec::new();
    loop {
        let page = search(file, 1, key, end, 0)?;
        if page.is_empty() {
            return Ok(result);
        }
        for item in &page {
            let dir = le64(&item.data, 0)?;
            let length = u16::from_le_bytes(
                item.data
                    .get(16..18)
                    .context("short root reference")?
                    .try_into()?,
            ) as usize;
            let name = relative(
                item.data
                    .get(18..18 + length)
                    .context("short subvolume name")?,
            )?;
            ensure!(name.components().count() == 1, "invalid subvolume name");
            result.push(lookup(file, parent, dir)?.join(name));
        }
        let Some(next) = page.last().unwrap().key.next().filter(|next| *next <= end) else {
            return Ok(result);
        };
        key = next;
    }
}

nix::ioctl_none!(sync_ioctl, 0x94, 8);
nix::ioctl_read!(start_sync_ioctl, 0x94, 24, u64);
nix::ioctl_write_ptr!(wait_sync_ioctl, 0x94, 22, u64);
pub fn committed(file: &File) -> Result<u64> {
    // Flush delayed allocation before taking a committed transaction boundary.
    // SAFETY: valid filesystem fd; no pointer argument.
    unsafe { sync_ioctl(file.as_raw_fd()) }.map_err(io::Error::from)?;
    let mut transid = 0;
    // SAFETY: initialized writable transaction ID and live fd.
    unsafe { start_sync_ioctl(file.as_raw_fd(), &mut transid) }.map_err(io::Error::from)?;
    // SAFETY: initialized transaction ID and live fd.
    unsafe { wait_sync_ioctl(file.as_raw_fd(), &transid) }.map_err(io::Error::from)?;
    ensure!(
        transid <= i64::MAX as u64,
        "transaction ID exceeds SQLite integer range"
    );
    Ok(transid)
}

fn le64(data: &[u8], at: usize) -> Result<u64> {
    Ok(u64::from_le_bytes(
        data.get(at..at + 8)
            .context("short btrfs item")?
            .try_into()?,
    ))
}
fn le32(data: &[u8], at: usize) -> Result<u32> {
    Ok(u32::from_le_bytes(
        data.get(at..at + 4)
            .context("short btrfs item")?
            .try_into()?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn abi_and_cursors() {
        assert_eq!(std::mem::size_of::<SearchKey>(), 104);
        assert_eq!(std::mem::size_of::<SearchHeader>(), 112);
        assert_eq!(std::mem::size_of::<Lookup>(), 4096);
        assert_eq!(std::mem::size_of::<PathArgs>(), 56);
        assert_eq!(std::mem::size_of::<SubvolInfo>(), 504);
        assert_eq!(Key::decode(&Key::FIRST.encode()).unwrap(), Key::FIRST);
        assert!(Key::LAST.next().is_none());
        assert_eq!(
            Key {
                ino: 256,
                kind: 1,
                offset: u64::MAX
            }
            .next()
            .unwrap(),
            Key {
                ino: 256,
                kind: 2,
                offset: 0
            }
        );
    }
    #[test]
    fn reject_bad_pages_and_paths() {
        assert!(parse_page(&[0; 31], 1, Key::FIRST, Key::LAST).is_err());
        assert!(relative(b"../escape").is_err());
        assert!(relative(b"/absolute").is_err());
        assert!(relative(b"ok/path").is_ok());
        let mut page = [0u8; 32];
        page[8..16].copy_from_slice(&256u64.to_ne_bytes());
        page[28..32].copy_from_slice(&100u32.to_ne_bytes());
        assert!(parse_page(&page, 1, Key::FIRST, Key::LAST).is_err());
    }
}
