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

fn history_stats(state: &Path, args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_dedupit"))
        .args(["stats", "--state-directory"])
        .arg(state)
        .args(args)
        .output()
        .unwrap()
}

fn history_json(state: &Path, args: &[&str]) -> Value {
    let output = history_stats(state, args);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

#[test]
fn history_preserves_the_legacy_result_and_new_run_stats() {
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("state");
    let data = dir.path().join("data");
    fs::create_dir(&data).unwrap();
    fs::create_dir(&state).unwrap();
    fs::write(data.join("file"), b"new file").unwrap();
    fs::write(
        state.join("filesystem-result.json"),
        r#"{"files_seen":77,"errors":["old error one","old error two"]}"#,
    )
    .unwrap();

    let mut daemon = launch(&state, &data, "0", "3600");
    wait_status(&state, |s| s["state"] == "completed");
    daemon.stop();

    let runs = history_json(&state, &["--runs", "--json"]);
    let runs = runs.as_array().unwrap();
    assert_eq!(runs.len(), 2);
    assert_eq!(runs[0]["run_id"], 1);
    assert_eq!(runs[0]["state"], "completed");
    assert_eq!(runs[1]["run_id"], 0);
    assert_eq!(runs[1]["state"], "finished_with_errors");
    assert!(runs[1]["started_at"].is_null());
    let listing = history_stats(&state, &["--runs"]);
    assert!(listing.status.success());
    assert!(String::from_utf8(listing.stdout)
        .unwrap()
        .contains("Completed with file errors"));

    let legacy = history_json(&state, &["--run", "0", "--json"]);
    assert_eq!(legacy["progress"]["files_seen"], 77);
    let report = history_stats(&state, &["--run", "0", "--errors"]);
    assert!(report.status.success());
    let text = String::from_utf8(report.stdout).unwrap();
    assert!(text.contains("File errors from run 0"));
    assert!(text.contains("old error one"));
    assert!(text.contains("old error two"));
    assert!(!history_stats(&state, &["--run", "99"]).status.success());
}

#[test]
fn history_recovers_an_abandoned_run_and_keeps_its_error_log() {
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("state");
    let data = dir.path().join("data");
    let abandoned = state.join("runs/41");
    fs::create_dir(&data).unwrap();
    fs::create_dir_all(&abandoned).unwrap();
    fs::write(
        state.join("filesystem-status.json"),
        r#"{"run_id":41,"state":"running","started_at":10,"updated_at":11,
        "progress":{"errors":["previous error"],"errors_total":2}}"#,
    )
    .unwrap();
    let old_log = abandoned.join("errors.jsonl");
    fs::write(
        &old_log,
        b"\"previous error\"\n\"previous detail\"\n\"partial",
    )
    .unwrap();
    fs::hard_link(&old_log, state.join("filesystem-errors.jsonl")).unwrap();

    let mut daemon = launch(&state, &data, "18446744073709551615", "3600");
    wait_status(&state, |s| s["state"] == "paused_for_space");
    daemon.stop();

    let runs = history_json(&state, &["--runs", "--json"]);
    assert_eq!(runs[0]["run_id"], 42);
    assert_eq!(runs[0]["state"], "paused_for_space");
    assert_eq!(runs[1]["run_id"], 41);
    assert_eq!(runs[1]["state"], "interrupted");
    assert!(runs[1]["finished_at"].is_null());
    let errors = history_stats(&state, &["--run", "41", "--errors"]);
    assert!(errors.status.success());
    let errors = String::from_utf8(errors.stdout).unwrap();
    assert!(errors.contains("previous detail"));
    assert!(!errors.contains("partial"));
    let current = history_stats(&state, &["--run", "42", "--errors"]);
    assert!(current.status.success());
    assert!(!String::from_utf8(current.stdout)
        .unwrap()
        .contains("previous detail"));
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
    let human = String::from_utf8(human.stdout).unwrap();
    assert!(human.contains("1,000,000"));
    assert!(human.contains("File errors from the latest scan"));
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
    let details = String::from_utf8(details.stdout).unwrap();
    assert!(details.contains("File errors from the latest scan"));
    assert!(details.contains("second detail"));

    fs::write(
        directory.path().join("filesystem-status.json"),
        r#"{"state":"running","progress":{"files_seen":123}}"#,
    )
    .unwrap();
    fs::write(
        directory.path().join("filesystem-result-summary.json"),
        r#"{"errors":["older error preview"],"errors_total":2}"#,
    )
    .unwrap();
    fs::write(
        directory.path().join("filesystem-result.json"),
        r#"{"errors":["older error preview","older error detail"]}"#,
    )
    .unwrap();

    let human = invoke(&[]);
    assert!(human.status.success());
    let human = String::from_utf8(human.stdout).unwrap();
    assert!(human.contains("File errors from the last completed scan"));
    assert!(human.contains("older error preview"));
    assert!(!human.contains("older error detail"));

    let details = invoke(&["--errors"]);
    assert!(details.status.success());
    let details = String::from_utf8(details.stdout).unwrap();
    assert!(details.contains("File errors from the last completed scan"));
    assert!(details.contains("older error detail"));
    assert!(!details.contains("second detail"));
}

#[test]
fn stats_pairs_savings_with_measured_files_and_shows_newer_queues() {
    let directory = tempfile::tempdir().unwrap();
    fs::write(
        directory.path().join("filesystem-status.json"),
        r#"{"state":"running","progress":{"bytes_deduped":1024,"estimated_bytes_saved":512,"savings_measured_bytes":1024}}"#,
    )
    .unwrap();
    let inventory = serde_json::json!({
        "state": "Bootstrapping incremental inventory",
        "estimate": {
            "as_of": 1,
            "verified_files": 658,
            "indexed_files": 1000,
            "checked_file_bytes": 8192,
            "physical_covered_bytes": 4096,
            "referenced_data_bytes": 8192,
            "physical_estimate_bytes": 2048,
            "saved_uncompressed_bytes": 4096,
            "pending_jobs": 987,
        },
        "refresh": {
            "as_of": 2,
            "incremental": true,
            "verified_files": 100,
            "indexed_files": 2000,
            "pending_jobs": 765,
            "pending_files": 321,
            "pending_groups": 12,
        }
    });
    fs::write(
        directory.path().join("sharing-inventory.json"),
        inventory.to_string(),
    )
    .unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_dedupit"))
        .args(["stats", "--state-directory"])
        .arg(directory.path())
        .output()
        .unwrap();
    assert!(output.status.success());
    let text = String::from_utf8(output.stdout).unwrap();
    let normalized = text.split_whitespace().collect::<Vec<_>>().join(" ");
    for expected in [
        "On disk (estimated) 2.00 KiB",
        "Savings from filesystem sharing",
        "Based on 4.00 KiB of 8.00 KiB referenced by the measured files.",
        "Before compression 4.00 KiB",
        "Files measured 658 of 1,000 indexed (65.8%)",
        "Measured file sizes 8.00 KiB",
        "Files and directories to scan 765",
        "Files to check for sharing 321",
        "Disk allocations to recalculate 12",
        "Checking files already in the index.",
        "Logical bytes shared 1.00 KiB",
        "Dedupit scan activity",
        "Disk space freed (est.) 512 B",
    ] {
        assert!(
            normalized.contains(expected),
            "{expected:?} missing from {text}"
        );
    }
    assert!(!normalized.contains("Files and directories to scan 987"));
}
