//! Test-only process stop protocol. Production builds do not include this module.
use anyhow::{bail, ensure, Context, Result};
use rusqlite::Connection;
use std::{
    fs::OpenOptions,
    io::Write,
    sync::atomic::{AtomicU64, Ordering},
};

pub(crate) fn hit(conn: &Connection, event: &str) -> Result<()> {
    if std::env::var("DEDUPIT_TEST_FAILPOINT").as_deref() != Ok(event) {
        return Ok(());
    }
    static HITS: AtomicU64 = AtomicU64::new(0);
    let target: u64 = std::env::var("DEDUPIT_TEST_HIT")
        .unwrap_or_else(|_| "1".into())
        .parse()?;
    ensure!(target > 0, "test failpoint hit must be positive");
    if HITS.fetch_add(1, Ordering::Relaxed) + 1 != target {
        return Ok(());
    }
    if std::env::var("DEDUPIT_TEST_ACTION").as_deref() == Ok("error") {
        bail!("injected test failure at {event}");
    }
    // Force dirty pages into the WAL at the actual stop boundary, independent
    // of record size. The crash test must cover spilled, uncommitted pages.
    if !conn.is_autocommit() && std::env::var_os("DEDUPIT_TEST_SMALL_CACHE").is_some() {
        conn.cache_flush()?;
    }
    let mut marker = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(std::env::var_os("DEDUPIT_TEST_READY").context("missing test failpoint marker")?)?;
    let count = |table: &str| -> Result<u64> {
        Ok(
            conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })?,
        )
    };
    serde_json::to_writer(
        &mut marker,
        &serde_json::json!({
            "event": event, "pid": std::process::id(), "autocommit": conn.is_autocommit(),
            "files": count("files")?, "ranges": count("ranges")?,
            "completed": count("completed")?, "discovery": count("discovery")?,
            "apply_mappings": count("apply_mappings")?,
            "unconfirmed": count("apply_mappings")?,
        }),
    )?;
    marker.write_all(b"\n")?;
    marker.sync_all()?;
    // SAFETY: SIGSTOP targets only this opted-in test child. The test parent
    // verifies the stopped state and delivers SIGKILL; no host privilege needed.
    unsafe {
        libc::raise(libc::SIGSTOP);
    }
    if std::env::var("DEDUPIT_TEST_ACTION").as_deref() == Ok("pause") {
        return Ok(());
    }
    bail!("test failpoint unexpectedly resumed")
}
