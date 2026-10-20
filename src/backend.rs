//! Backends must compare bytes atomically with sharing. A hash alone is never
//! sufficient authorization to replace file contents.
use std::{fs::File, io};

#[derive(Clone, Copy, Debug)]
pub struct Range {
    pub offset: u64,
    pub len: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// May be shorter than requested; the caller must continue the remainder.
    Shared(u64),
    AlreadyShared(u64),
    Differs,
    Unsupported,
}

pub trait Backend {
    /// Implementors must preserve bytes and independent write semantics,
    /// including in the presence of concurrent writes and hash collisions.
    fn dedupe(
        &mut self,
        source: &File,
        destination: &File,
        range: Range,
        destination_offset: u64,
    ) -> io::Result<Outcome>;
}

pub struct NativeBackend;

impl Backend for NativeBackend {
    fn dedupe(
        &mut self,
        source: &File,
        destination: &File,
        range: Range,
        destination_offset: u64,
    ) -> io::Result<Outcome> {
        #[cfg(target_os = "linux")]
        return linux::dedupe(source, destination, range, destination_offset);
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (source, destination, range, destination_offset);
            Ok(Outcome::Unsupported)
        }
    }
}

pub(crate) struct Filesystem {
    pub key: Vec<u8>,
    pub alignment: u64,
}

pub(crate) fn filesystem(file: &File) -> io::Result<Filesystem> {
    use std::os::unix::fs::MetadataExt;
    #[cfg(target_os = "linux")]
    if let Some(info) = linux::btrfs_info(file)? {
        return Ok(info);
    }
    // Device IDs group ordinary Unix filesystems. Btrfs needs its FS UUID
    // instead: each subvolume can expose a different st_dev.
    Ok(Filesystem {
        key: file.metadata()?.dev().to_le_bytes().to_vec(),
        alignment: filesystem_alignment(file)?,
    })
}

fn filesystem_alignment(file: &File) -> io::Result<u64> {
    use std::os::fd::AsRawFd;
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::zeroed();
    // SAFETY: stat points to writable statvfs storage; file owns a live fd.
    if unsafe { libc::fstatvfs(file.as_raw_fd(), stat.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: fstatvfs initialized the output on success.
    let stat = unsafe { stat.assume_init() };
    // fsblkcnt_t is 32-bit on some Unix targets and 64-bit on others.
    #[allow(clippy::unnecessary_cast)]
    let alignment = stat.f_frsize as u64;
    Ok(alignment.max(1))
}

#[cfg(target_os = "linux")]
mod linux {
    use super::*;
    use std::os::fd::AsRawFd;

    // UAPI layouts from linux/fs.h, linux/fiemap.h and linux/btrfs.h.
    // Request numbers are computed by nix for the target architecture.
    #[repr(C)]
    #[derive(Default)]
    struct DedupeHeader {
        source_offset: u64,
        source_length: u64,
        destination_count: u16,
        reserved1: u16,
        reserved2: u32,
    }

    #[repr(C)]
    #[derive(Default)]
    struct DedupeInfo {
        destination_fd: i64,
        destination_offset: u64,
        bytes_deduped: u64,
        status: i32,
        reserved: u32,
    }

    #[repr(C)]
    #[derive(Default)]
    struct DedupeRequest {
        header: DedupeHeader,
        info: DedupeInfo,
    }

    nix::ioctl_readwrite!(dedupe_ioctl, 0x94, 54, DedupeHeader);

    #[repr(C)]
    struct BtrfsInfo {
        max_id: u64,
        num_devices: u64,
        fsid: [u8; 16],
        nodesize: u32,
        sectorsize: u32,
        rest: [u8; 984],
    }

    nix::ioctl_read!(btrfs_info_ioctl, 0x94, 31, BtrfsInfo);

    pub(super) fn btrfs_info(file: &File) -> io::Result<Option<Filesystem>> {
        let mut stat = std::mem::MaybeUninit::<libc::statfs>::zeroed();
        // SAFETY: a live fd and writable statfs output; read only on success.
        if unsafe { libc::fstatfs(file.as_raw_fd(), stat.as_mut_ptr()) } != 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: fstatfs initialized the structure on success.
        if unsafe { stat.assume_init() }.f_type as libc::c_ulong != 0x9123683e {
            // Do not require unrelated filesystems (notably FUSE/network
            // implementations) to recognize a btrfs-specific ioctl.
            return Ok(None);
        }
        // SAFETY: BtrfsInfo is a C output structure consisting solely of integers.
        let mut info: BtrfsInfo = unsafe { std::mem::zeroed() };
        // SAFETY: correctly sized, initialized, writable output and valid fd.
        match unsafe { btrfs_info_ioctl(file.as_raw_fd(), &mut info) } {
            Ok(_) if info.sectorsize > 0 => {
                let mut key = b"btrfs:".to_vec();
                key.extend_from_slice(&info.fsid);
                Ok(Some(Filesystem {
                    key,
                    alignment: info.sectorsize.into(),
                }))
            }
            Ok(_) => Err(io::Error::other("btrfs returned a zero sector size")),
            Err(nix::errno::Errno::ENOTTY | nix::errno::Errno::EOPNOTSUPP) => Ok(None),
            Err(error) => Err(io::Error::from_raw_os_error(error as i32)),
        }
    }

    #[repr(C)]
    #[derive(Default)]
    struct FiemapHeader {
        start: u64,
        length: u64,
        flags: u32,
        mapped_extents: u32,
        extent_count: u32,
        reserved: u32,
    }

    #[repr(C)]
    #[derive(Default)]
    struct Extent {
        logical: u64,
        physical: u64,
        length: u64,
        reserved64: [u64; 2],
        flags: u32,
        reserved: [u32; 3],
    }

    #[repr(C)]
    #[derive(Default)]
    struct FiemapRequest {
        header: FiemapHeader,
        extent: Extent,
    }

    nix::ioctl_readwrite!(fiemap_ioctl, b'f', 11, FiemapHeader);

    fn physical(file: &File, offset: u64, len: u64) -> Option<u64> {
        let mut request = FiemapRequest {
            header: FiemapHeader {
                start: offset,
                length: len,
                extent_count: 1,
                ..Default::default()
            },
            ..Default::default()
        };
        // SAFETY: repr(C) buffer includes space for the single requested extent.
        let pointer = std::ptr::addr_of_mut!(request).cast::<FiemapHeader>();
        unsafe { fiemap_ioctl(file.as_raw_fd(), pointer) }.ok()?;
        let extent = request.extent;
        // Only ordinary, fully mapped extents can be compared this way.
        const UNSAFE_MAPPING: u32 = 0x2 | 0x4 | 0x8 | 0x80 | 0x100 | 0x200 | 0x400 | 0x800;
        if request.header.mapped_extents != 1
            || extent.flags & UNSAFE_MAPPING != 0
            || extent.physical == 0
            || extent.logical > offset
            || extent.logical.checked_add(extent.length)? < offset.checked_add(len)?
        {
            return None;
        }
        extent.physical.checked_add(offset - extent.logical)
    }

    pub(super) fn dedupe(
        source: &File,
        destination: &File,
        range: Range,
        destination_offset: u64,
    ) -> io::Result<Outcome> {
        if range.len == 0 || range.len > 16 * 1024 * 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid dedupe length",
            ));
        }
        // FIEMAP is only an optimization. The engine has already checked that
        // the files belong to the same filesystem. Encoded extents fall through
        // to the kernel. A race can only cause redundant or missed work here.
        use std::os::unix::fs::MetadataExt;
        if source.metadata()?.dev() == destination.metadata()?.dev() {
            if let (Some(a), Some(b)) = (
                physical(source, range.offset, range.len),
                physical(destination, destination_offset, range.len),
            ) {
                if a == b {
                    return Ok(Outcome::AlreadyShared(range.len));
                }
            }
        }
        let mut request = DedupeRequest {
            header: DedupeHeader {
                source_offset: range.offset,
                source_length: range.len,
                destination_count: 1,
                ..Default::default()
            },
            info: DedupeInfo {
                destination_fd: destination.as_raw_fd().into(),
                destination_offset,
                ..Default::default()
            },
        };
        loop {
            // SAFETY: the initialized repr(C) request includes one destination,
            // fits within a page, and both file descriptors stay alive.
            let pointer = std::ptr::addr_of_mut!(request).cast::<DedupeHeader>();
            match unsafe { dedupe_ioctl(source.as_raw_fd(), pointer) } {
                Ok(_) => break,
                Err(nix::errno::Errno::EINTR) => continue,
                Err(nix::errno::Errno::ENOTTY | nix::errno::Errno::EOPNOTSUPP) => {
                    return Ok(Outcome::Unsupported);
                }
                Err(error) => return Err(io::Error::from_raw_os_error(error as i32)),
            }
        }
        match request.info.status {
            0 => Ok(Outcome::Shared(request.info.bytes_deduped)),
            1 => Ok(Outcome::Differs),
            status if status == -libc::EOPNOTSUPP || status == -libc::ENOTTY => {
                Ok(Outcome::Unsupported)
            }
            status if status < 0 => Err(io::Error::from_raw_os_error(-status)),
            _ => Err(io::Error::other("unknown kernel dedupe status")),
        }
    }

    #[test]
    fn kernel_abi_layouts() {
        assert_eq!(std::mem::size_of::<DedupeHeader>(), 24);
        assert_eq!(std::mem::size_of::<DedupeInfo>(), 32);
        assert_eq!(std::mem::size_of::<DedupeRequest>(), 56);
        assert_eq!(std::mem::offset_of!(DedupeRequest, info), 24);
        assert_eq!(std::mem::size_of::<FiemapHeader>(), 32);
        assert_eq!(std::mem::size_of::<Extent>(), 56);
        assert_eq!(std::mem::offset_of!(FiemapRequest, extent), 32);
        assert_eq!(std::mem::size_of::<BtrfsInfo>(), 1024);
    }
}
