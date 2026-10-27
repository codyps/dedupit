//! Metadata-only access to subvolume roots hidden by bind mounts. Detached
//! mounts never enter a namespace and disappear with their last descriptor.
use anyhow::{ensure, Context, Result};
use std::{
    ffi::{CStr, CString},
    fs::{self, File, OpenOptions},
    io,
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::{fs::FileTypeExt, fs::MetadataExt, fs::OpenOptionsExt},
    },
    path::PathBuf,
};

pub(super) struct Mount {
    id: u64,
    pub point: PathBuf,
    source: PathBuf,
    btrfs: bool,
}

pub(super) fn mounts() -> Result<Vec<Mount>> {
    parse_mounts(&fs::read("/proc/self/mountinfo")?)
}

fn unescape(raw: &[u8]) -> Result<PathBuf> {
    let mut bytes = Vec::new();
    let mut at = 0;
    while at < raw.len() {
        if raw[at] == b'\\' {
            let digits = raw.get(at + 1..at + 4).context("short mountinfo escape")?;
            ensure!(
                digits.iter().all(|b| (b'0'..=b'7').contains(b)),
                "invalid mountinfo escape"
            );
            let value = (digits[0] - b'0') as u16 * 64
                + (digits[1] - b'0') as u16 * 8
                + (digits[2] - b'0') as u16;
            ensure!(value > 0 && value <= 255, "invalid mountinfo escape");
            bytes.push(value as u8);
            at += 4;
        } else {
            ensure!(raw[at] != 0, "NUL in mountinfo");
            bytes.push(raw[at]);
            at += 1;
        }
    }
    Ok(crate::index::decode_path(bytes))
}

fn parse_mounts(bytes: &[u8]) -> Result<Vec<Mount>> {
    bytes
        .split(|b| *b == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| {
            let fields: Vec<_> = line.split(|b| *b == b' ').collect();
            let separator = fields
                .iter()
                .position(|field| *field == b"-")
                .context("missing mountinfo separator")?;
            ensure!(
                separator >= 6 && fields.len() >= separator + 4,
                "short mountinfo record"
            );
            let point = unescape(fields[4])?;
            ensure!(point.is_absolute(), "relative mount point");
            Ok(Mount {
                id: std::str::from_utf8(fields[0])?.parse()?,
                point,
                source: unescape(fields[separator + 2])?,
                btrfs: fields[separator + 1] == b"btrfs",
            })
        })
        .collect()
}

/// Use the opened directory's mount ID, not pathname prefix matching (which
/// cannot distinguish stacked mounts). Pin and validate an already-mounted
/// device before giving its descriptor to the kernel; never run mount helpers.
pub(super) fn open_root(anchor: &File, mounts: &[Mount]) -> Result<File> {
    // Fail before opening devices when metadata access itself is unavailable.
    crate::btrfs_io::search(
        anchor,
        0,
        crate::btrfs_io::Key::FIRST,
        crate::btrfs_io::Key::FIRST,
        0,
    )?;
    let fdinfo = fs::read_to_string(format!("/proc/self/fdinfo/{}", anchor.as_raw_fd()))?;
    let mount_id: u64 = fdinfo
        .lines()
        .find_map(|line| line.strip_prefix("mnt_id:"))
        .context("directory has no mount ID")?
        .trim()
        .parse()?;
    let mount = mounts
        .iter()
        .find(|mount| mount.id == mount_id && mount.btrfs)
        .context("btrfs mount changed during discovery")?;
    let expected = crate::backend::filesystem(anchor)?.key;
    let fsid = expected
        .strip_prefix(b"btrfs:")
        .filter(|id| id.len() == 16)
        .context("not a btrfs filesystem")?;
    let uuid = fsid
        .iter()
        .enumerate()
        .map(|(i, byte)| {
            format!(
                "{}{byte:02x}",
                if [4, 6, 8, 10].contains(&i) { "-" } else { "" }
            )
        })
        .collect::<String>();
    ensure!(
        mount.source.is_absolute(),
        "mounted btrfs device path is not absolute"
    );
    let device = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NONBLOCK | libc::O_NOFOLLOW)
        .open(fs::canonicalize(&mount.source).context("resolve mounted btrfs device")?)?;
    let metadata = device.metadata()?;
    ensure!(
        metadata.file_type().is_block_device(),
        "btrfs source is not a block device"
    );
    let number = format!(
        "{}:{}",
        libc::major(metadata.rdev()),
        libc::minor(metadata.rdev())
    );
    let devices = fs::read_dir(format!("/sys/fs/btrfs/{uuid}/devices"))
        .context("validate device membership of mounted btrfs filesystem")?;
    let mut member = false;
    for entry in devices {
        if fs::read_to_string(entry?.path().join("dev"))?.trim() == number {
            member = true;
            break;
        }
    }
    ensure!(
        member,
        "btrfs source device does not belong to the requested filesystem"
    );
    let tree = crate::btrfs_io::tree_id(anchor)?;
    let root =
        detached_root(&device, tree).context("mount hidden btrfs root for metadata access")?;
    ensure!(
        root.metadata()?.ino() == 256
            && crate::backend::filesystem(&root)?.key == expected
            && crate::btrfs_io::identity(&root)? == crate::btrfs_io::identity(anchor)?,
        "mounted btrfs root does not match the requested subvolume"
    );
    Ok(root)
}

fn owned_fd(result: libc::c_long) -> io::Result<File> {
    if result < 0 {
        Err(io::Error::last_os_error())
    } else {
        // SAFETY: a successful fd-creating syscall returned a new owned fd.
        Ok(unsafe { File::from_raw_fd(result as libc::c_int) })
    }
}

fn configure(
    fd: &File,
    command: libc::c_uint,
    key: Option<&CStr>,
    value: Option<&CStr>,
) -> io::Result<()> {
    // SAFETY: live context fd, valid command, and live optional C strings.
    let result = unsafe {
        libc::syscall(
            libc::SYS_fsconfig,
            fd.as_raw_fd(),
            command,
            key.map_or(std::ptr::null(), CStr::as_ptr),
            value.map_or(std::ptr::null(), CStr::as_ptr),
            0,
        )
    };
    if result < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn detached_root(device: &File, tree: u64) -> Result<File> {
    // SAFETY: static NUL-terminated filesystem name and valid CLOEXEC flag.
    let context = owned_fd(unsafe {
        libc::syscall(libc::SYS_fsopen, c"btrfs".as_ptr(), libc::FSOPEN_CLOEXEC)
    })?;
    let source = CString::new(format!("/proc/self/fd/{}", device.as_raw_fd()))?;
    let subvolume = CString::new(tree.to_string())?;
    configure(
        &context,
        libc::FSCONFIG_SET_STRING,
        Some(c"source"),
        Some(&source),
    )?;
    configure(
        &context,
        libc::FSCONFIG_SET_STRING,
        Some(c"subvolid"),
        Some(&subvolume),
    )?;
    configure(&context, libc::FSCONFIG_CMD_CREATE, None, None)?;
    let attributes = libc::MOUNT_ATTR_RDONLY
        | libc::MOUNT_ATTR_NOSUID
        | libc::MOUNT_ATTR_NODEV
        | libc::MOUNT_ATTR_NOEXEC;
    // SAFETY: configured live context, valid flags. This mount is never attached
    // with move_mount: closing its descriptors also cleans up after errors/panic.
    let mount = owned_fd(unsafe {
        libc::syscall(
            libc::SYS_fsmount,
            context.as_raw_fd(),
            libc::FSMOUNT_CLOEXEC,
            attributes as libc::c_uint,
        )
    })?;
    // fsmount returns O_PATH; reopen relative to that descriptor for ioctls.
    // SAFETY: live directory fd, static name and non-creating directory flags.
    owned_fd(unsafe {
        libc::openat(
            mount.as_raw_fd(),
            c".".as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
        ) as libc::c_long
    })
    .context("open detached metadata root")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{os::unix::ffi::OsStrExt, path::Path};

    #[test]
    fn mountinfo_handles_optional_fields_and_non_utf8_paths() {
        let mounts = parse_mounts(b"41 1 0:27 /prefix /a\\040b\\134\\377 rw shared:2 - btrfs /dev/mapper/root\\040disk rw\n").unwrap();
        assert_eq!(mounts.len(), 1);
        assert_eq!(mounts[0].id, 41);
        assert_eq!(mounts[0].point.as_os_str().as_bytes(), b"/a b\\\xff");
        assert_eq!(mounts[0].source, Path::new("/dev/mapper/root disk"));
        assert!(mounts[0].btrfs);
    }

    #[test]
    fn malformed_mountinfo_is_rejected() {
        for point in [
            b"relative".as_slice(),
            b"/bad\\777",
            b"/bad\\000",
            b"/bad\\04",
            b"/bad\\xyz",
        ] {
            let mut line = b"41 1 0:27 / ".to_vec();
            line.extend_from_slice(point);
            line.extend_from_slice(b" rw - btrfs /dev/loop0 rw");
            assert!(parse_mounts(&line).is_err(), "{point:?}");
        }
        assert!(parse_mounts(b"41 1 0:27 / / rw").is_err());
    }

    #[test]
    fn detached_root_is_restricted_cloexec_and_absent_from_mount_table() {
        let dir = tempfile::tempdir().unwrap();
        let anchor = File::open(dir.path()).unwrap();
        if !crate::backend::filesystem(&anchor)
            .unwrap()
            .key
            .starts_with(b"btrfs:")
        {
            eprintln!("SKIP: detached mount test needs btrfs");
            return;
        }
        let root = match open_root(&anchor, &mounts().unwrap()) {
            Ok(root) => root,
            Err(error)
                if error.chain().any(|cause| {
                    cause.downcast_ref::<io::Error>().is_some_and(|error| {
                        matches!(error.raw_os_error(), Some(libc::EPERM | libc::EACCES))
                    })
                }) =>
            {
                eprintln!("SKIP: detached mount test needs native mount capabilities");
                return;
            }
            Err(error) => panic!("{error:#}"),
        };
        assert_eq!(root.metadata().unwrap().ino(), 256);
        let mut info = std::mem::MaybeUninit::<libc::statvfs>::zeroed();
        // SAFETY: live directory fd and writable output, read only on success.
        assert_eq!(
            unsafe { libc::fstatvfs(root.as_raw_fd(), info.as_mut_ptr()) },
            0
        );
        // SAFETY: successful fstatvfs initialized the output.
        let flags = unsafe { info.assume_init() }.f_flag;
        for flag in [
            libc::ST_RDONLY,
            libc::ST_NOSUID,
            libc::ST_NODEV,
            libc::ST_NOEXEC,
        ] {
            assert_ne!(flags & flag, 0);
        }
        // SAFETY: F_GETFD only reads descriptor flags.
        assert_ne!(
            unsafe { libc::fcntl(root.as_raw_fd(), libc::F_GETFD) } & libc::FD_CLOEXEC,
            0
        );
        let fdinfo = fs::read_to_string(format!("/proc/self/fdinfo/{}", root.as_raw_fd())).unwrap();
        let id: u64 = fdinfo
            .lines()
            .find_map(|line| line.strip_prefix("mnt_id:"))
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        assert!(mounts().unwrap().iter().all(|mount| mount.id != id));
    }
}
