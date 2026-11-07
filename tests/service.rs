use serde_json::Value;
use std::{
    fs,
    path::Path,
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

struct Service(Child);
impl Service {
    fn stop(&mut self) {
        // SAFETY: pid belongs to the child managed by this test.
        unsafe {
            libc::kill(self.0.id() as libc::pid_t, libc::SIGTERM);
        }
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(status) = self.0.try_wait().unwrap() {
                assert!(status.success(), "service failed: {status}");
                return;
            }
            assert!(Instant::now() < deadline, "service ignored SIGTERM");
            thread::sleep(Duration::from_millis(20));
        }
    }
}
impl Drop for Service {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn launch(state: &Path, data: &Path, minimum: &str, interval: &str) -> Service {
    Service(
        Command::new(env!("CARGO_BIN_EXE_dedupit"))
            .args(["service", "--state-directory"])
            .arg(state)
            .args([
                "--scanner",
                "portable",
                "--minimum-free-bytes",
                minimum,
                "--interval",
                interval,
            ])
            .arg(data)
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    )
}
fn wait_status(state: &Path, predicate: impl Fn(&Value) -> bool) -> Value {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if let Ok(bytes) = fs::read(state.join("filesystem-status.json")) {
            let value: Value = serde_json::from_slice(&bytes).expect("atomic complete JSON");
            if predicate(&value) {
                return value;
            }
        }
        assert!(
            Instant::now() < deadline,
            "service did not reach expected state"
        );
        thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn service_repeats_records_statistics_and_resumes_index_and_totals_after_sigterm() {
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("state");
    let data = dir.path().join("data");
    fs::create_dir(&data).unwrap();
    fs::write(data.join("a"), vec![42; 8192]).unwrap();
    fs::write(data.join("b"), vec![42; 8192]).unwrap();
    let mut service = launch(&state, &data, "0", "1");
    let first = wait_status(&state, |v| v["totals"]["completed_runs"] == 1);
    assert_eq!(first["progress"]["files_hashed"], 2);
    fs::write(data.join("c"), vec![43; 4096]).unwrap();
    wait_status(&state, |v| {
        v["totals"]["completed_runs"].as_u64().unwrap_or(0) >= 2
    });
    service.stop();
    let before: Value =
        serde_json::from_slice(&fs::read(state.join("filesystem-status.json")).unwrap()).unwrap();
    assert_eq!(before["state"], "stopped");
    assert_eq!(before["totals"]["files_hashed"], 3);
    assert_eq!(before["totals"]["bytes_hashed"], 20480);
    let mut restarted = launch(&state, &data, "0", "3600");
    let after = wait_status(&state, |v| {
        v["totals"]["completed_runs"].as_u64().unwrap_or(0)
            > before["totals"]["completed_runs"].as_u64().unwrap()
    });
    assert_eq!(after["progress"]["files_hashed"], 0);
    assert_eq!(after["totals"]["files_hashed"], 3);
    assert_eq!(after["activity"]["files_hashed"], 3);
    assert_eq!(after["activity"]["bytes_hashed"], 20480);
    assert!(
        after["activity"]["scans_started"].as_u64().unwrap()
            > before["activity"]["scans_started"].as_u64().unwrap()
    );
    let output = Command::new(env!("CARGO_BIN_EXE_dedupit"))
        .args(["stats", "--json", "--state-directory"])
        .arg(&state)
        .output()
        .unwrap();
    assert!(output.status.success());
    let stats: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(stats["last_result"]["files_seen"], 3);
    assert_eq!(stats["status"]["totals"]["bytes_hashed"], 20480);
    let human = Command::new(env!("CARGO_BIN_EXE_dedupit"))
        .args(["stats", "--state-directory"])
        .arg(&state)
        .output()
        .unwrap();
    assert!(human.status.success());
    assert!(String::from_utf8(human.stdout)
        .unwrap()
        .contains("Recorded work"));
    restarted.stop();
}

#[test]
fn service_pauses_before_opening_index_preserves_report_and_enforces_single_owner() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let state = dir.path().join("state");
    fs::create_dir(&data).unwrap();
    fs::create_dir(&state).unwrap();
    fs::write(
        state.join("filesystem-result.json"),
        r#"{"files_seen":123}"#,
    )
    .unwrap();
    let mut service = launch(&state, &data, "18446744073709551615", "3600");
    wait_status(&state, |v| v["state"] == "paused_for_space");
    assert!(!state.join("filesystem.sqlite").exists());
    assert_eq!(
        fs::read_to_string(state.join("filesystem-result.json")).unwrap(),
        r#"{"files_seen":123}"#
    );
    let second = Command::new(env!("CARGO_BIN_EXE_dedupit"))
        .args(["service", "--state-directory"])
        .arg(&state)
        .arg(&data)
        .output()
        .unwrap();
    assert!(!second.status.success());
    assert!(String::from_utf8(second.stderr)
        .unwrap()
        .contains("another dedupit service"));
    service.stop();
}

#[test]
fn stats_without_reports_and_service_without_roots_fail_clearly() {
    let dir = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_dedupit"))
        .args(["stats", "--state-directory"])
        .arg(dir.path())
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8(output.stderr)
        .unwrap()
        .contains("no recorded statistics"));
    assert!(!Command::new(env!("CARGO_BIN_EXE_dedupit"))
        .arg("service")
        .output()
        .unwrap()
        .status
        .success());
}

#[test]
fn normal_stats_reads_small_summaries_and_only_explicit_errors_reads_the_log() {
    let directory = tempfile::tempdir().unwrap();
    let status = serde_json::json!({"state":"running", "progress": {
        "files_seen":123, "errors":["sample error"], "errors_total":1000000
    }, "totals":{}, "activity":{}});
    fs::write(
        directory.path().join("filesystem-status.json"),
        status.to_string(),
    )
    .unwrap();
    fs::write(
        directory.path().join("filesystem-result-summary.json"),
        r#"{"files_seen":456}"#,
    )
    .unwrap();
    // A full diagnostic report is deliberately unreadable JSON. Normal stats
    // must not touch it when its compact completed-run summary is available.
    fs::write(
        directory.path().join("filesystem-result.json"),
        b"large diagnostic archive; do not parse",
    )
    .unwrap();
    fs::write(
        directory.path().join("filesystem-errors.jsonl"),
        b"not JSON; normal stats must not read this either",
    )
    .unwrap();
    let invoke = |flags: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_dedupit"))
            .args(["stats", "--state-directory"])
            .arg(directory.path())
            .args(flags)
            .output()
            .unwrap()
    };
    let human = invoke(&[]);
    assert!(
        human.status.success(),
        "{}",
        String::from_utf8_lossy(&human.stderr)
    );
    assert!(String::from_utf8(human.stdout)
        .unwrap()
        .contains("1,000,000"));
    let json = invoke(&["--json"]);
    assert!(json.status.success());
    let json: Value = serde_json::from_slice(&json.stdout).unwrap();
    assert_eq!(json["status"]["progress"]["errors_total"], 1000000);
    assert_eq!(json["last_result"]["files_seen"], 456);
    assert!(!invoke(&["--errors"]).status.success());
    fs::write(
        directory.path().join("filesystem-errors.jsonl"),
        b"\"first detail\"\n\"second detail\"\n",
    )
    .unwrap();
    let details = invoke(&["--errors"]);
    assert!(details.status.success());
    assert!(String::from_utf8(details.stdout)
        .unwrap()
        .contains("second detail"));
}
