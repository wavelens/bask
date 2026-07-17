/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use std::path::Path;
use std::time::Duration;

use bask_sandbox::{ExecRequest, Isolation, Limits, SandboxSpec, spawn};

fn local_spec() -> SandboxSpec {
    SandboxSpec {
        isolation: Isolation::Local,
        ..SandboxSpec::default()
    }
}

#[tokio::test]
async fn runs_command_and_captures_stdout_and_exit() {
    let sb = spawn(&local_spec()).await.unwrap();
    let out = sb
        .exec(ExecRequest::new(vec![
            "sh".into(),
            "-c".into(),
            "printf hi; exit 3".into(),
        ]))
        .await
        .unwrap();
    assert_eq!(out.exit_code, 3);
    assert_eq!(out.stdout, b"hi");
    sb.teardown().await.unwrap();
}

#[tokio::test]
async fn times_out_to_124() {
    let mut spec = local_spec();
    spec.limits = Limits {
        timeout: Some(Duration::from_millis(100)),
        ..Limits::default()
    };
    let sb = spawn(&spec).await.unwrap();
    let out = sb
        .exec(ExecRequest::new(vec![
            "sh".into(),
            "-c".into(),
            "sleep 5".into(),
        ]))
        .await
        .unwrap();
    assert_eq!(out.exit_code, 124);
}

#[tokio::test]
async fn timeout_kills_child() {
    let marker = std::env::temp_dir().join(format!("bask_kill_marker_{}", std::process::id()));
    if marker.exists() {
        std::fs::remove_file(&marker).unwrap();
    }

    let mut spec = local_spec();
    spec.limits = Limits {
        timeout: Some(Duration::from_millis(100)),
        ..Limits::default()
    };
    let sb = spawn(&spec).await.unwrap();
    let out = sb
        .exec(ExecRequest::new(vec![
            "sh".into(),
            "-c".into(),
            format!("sleep 1; printf x > {}", marker.display()),
        ]))
        .await
        .unwrap();
    assert_eq!(out.exit_code, 124);

    tokio::time::sleep(Duration::from_millis(1500)).await;

    let survived = marker.exists();
    if marker.exists() {
        std::fs::remove_file(&marker).unwrap();
    }
    assert!(!survived, "child process was not killed on timeout");
}

#[tokio::test]
async fn truncates_output() {
    let mut spec = local_spec();
    spec.limits = Limits {
        max_output_bytes: Some(4),
        ..Limits::default()
    };
    let sb = spawn(&spec).await.unwrap();
    let out = sb
        .exec(ExecRequest::new(vec![
            "sh".into(),
            "-c".into(),
            "printf 0123456789".into(),
        ]))
        .await
        .unwrap();
    assert_eq!(out.stdout, b"0123");
    assert!(out.truncated);
}

#[tokio::test]
async fn writes_and_reads_files_under_workdir() {
    let sb = spawn(&local_spec()).await.unwrap();
    sb.write_file(Path::new("a/b.txt"), b"payload")
        .await
        .unwrap();
    let got = sb.read_file(Path::new("a/b.txt")).await.unwrap();
    assert_eq!(got, b"payload");
}
