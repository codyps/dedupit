//! Subprocess probes: POSIX locks cannot be inspected reliably from their owner.
use std::{fs::OpenOptions, os::fd::AsRawFd, path::Path, process::Command};

pub(crate) fn assert_sqlite_locks(database: &Path, shm: &Path) {
    let result = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "lock_tests::posix_lock_probe_child",
            "--nocapture",
        ])
        .env("DEDUPIT_PROBE_DATABASE", database)
        .env("DEDUPIT_PROBE_SHM", shm)
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "SQLite locks were lost: {} {}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
}
#[test]
fn posix_lock_probe_child() {
    let Some(database) = std::env::var_os("DEDUPIT_PROBE_DATABASE") else {
        return;
    };
    let shm = std::env::var_os("DEDUPIT_PROBE_SHM").unwrap();
    for path in [database, shm] {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        // SAFETY: flock is a plain C struct; the initialized fields request a
        // nonblocking whole-file POSIX write lock in this separate process.
        let mut lock: libc::flock = unsafe { std::mem::zeroed() };
        lock.l_type = libc::F_WRLCK as _;
        lock.l_whence = libc::SEEK_SET as _;
        let result = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_SETLK, &lock) };
        let err = std::io::Error::last_os_error().raw_os_error();
        assert!(
            result == -1 && matches!(err, Some(libc::EACCES) | Some(libc::EAGAIN)),
            "SQLite file unexpectedly unlocked: {:?}",
            Path::new(&path).file_name()
        );
    }
}
