use serde_json::Value;
use std::{fs, os::unix::ffi::OsStringExt, process::Command};

#[test]
fn cli_lists_non_utf8_paths_applies_and_restarts_incrementally() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    fs::create_dir(&data).unwrap();
    let bytes = vec![42; 128 * 1024];
    fs::write(data.join("a"), &bytes).unwrap();
    fs::write(data.join(std::ffi::OsString::from_vec(vec![0xff])), &bytes).unwrap();
    let database = dir.path().join("index.sqlite");
    let invoke = |extra: &[&str]| {
        let output = Command::new(env!("CARGO_BIN_EXE_dedupit"))
            .arg("--database")
            .arg(&database)
            .arg("--json")
            .args(extra)
            .arg(&data)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout).unwrap()
    };
    let listing = invoke(&["--list"]);
    let records: Vec<Value> = listing
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(records.len(), 3); // One chunk, one whole file, and statistics.
    for record in &records[..2] {
        assert_eq!(record["type"], "duplicate");
        assert!(record["source_path_bytes"].is_array());
        assert!(record["destination_path_bytes"].is_array());
        assert_eq!(record["length"], 128 * 1024);
    }
    assert_eq!(records[2]["stats"]["files_hashed"], 2);
    let applied: Value = serde_json::from_str(&invoke(&["--apply"])).unwrap();
    assert_eq!(applied["bytes_hashed"], 0);
    assert!(
        applied["jobs_completed"].as_u64().unwrap() > 0
            || applied["unsupported"].as_u64().unwrap() > 0
    );
    let restarted: Value = serde_json::from_str(&invoke(&["--apply"])).unwrap();
    assert_eq!(restarted["bytes_hashed"], 0);
    if applied["jobs_completed"].as_u64().unwrap() > 0 {
        assert_eq!(restarted["jobs_attempted"], 0);
    }
    assert_eq!(fs::read(data.join("a")).unwrap(), bytes);
    assert_eq!(
        fs::read(data.join(std::ffi::OsString::from_vec(vec![0xff]))).unwrap(),
        bytes
    );
}
