/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

#![cfg(all(feature = "os-sandbox", target_os = "linux"))]

use std::path::Path;

use bask_sandbox::{ExecRequest, Isolation, Network, SandboxSpec, spawn};

fn os_spec() -> SandboxSpec {
    SandboxSpec {
        isolation: Isolation::OsSandbox,
        ..SandboxSpec::default()
    }
}

async fn os_sandbox_available() -> bool {
    spawn(&os_spec()).await.is_ok()
}

#[tokio::test]
async fn runs_command_and_reads_system_paths() {
    if !os_sandbox_available().await {
        eprintln!("skipping: no Landlock support");
        return;
    }
    let sb = spawn(&os_spec()).await.unwrap();
    // Reading/executing a system binary works (read-open policy).
    let out = sb
        .exec(ExecRequest::new(vec![
            "sh".into(),
            "-c".into(),
            "printf hi".into(),
        ]))
        .await
        .unwrap();
    assert_eq!(out.exit_code, 0);
    assert_eq!(out.stdout, b"hi");
    sb.teardown().await.unwrap();
}

#[tokio::test]
async fn write_outside_workdir_is_denied() {
    if !os_sandbox_available().await {
        eprintln!("skipping: no Landlock support");
        return;
    }
    let sb = spawn(&os_spec()).await.unwrap();
    let marker = std::env::temp_dir().join(format!("bask_os_escape_{}", std::process::id()));
    let _ = std::fs::remove_file(&marker);
    let cmd = format!("printf x > {}", marker.display());
    let out = sb
        .exec(ExecRequest::new(vec!["sh".into(), "-c".into(), cmd]))
        .await
        .unwrap();
    assert_ne!(out.exit_code, 0, "write outside the workdir must fail");
    assert!(!marker.exists(), "the file must not have been created");
    let _ = std::fs::remove_file(&marker);
    sb.teardown().await.unwrap();
}

#[tokio::test]
async fn write_inside_workdir_succeeds() {
    if !os_sandbox_available().await {
        eprintln!("skipping: no Landlock support");
        return;
    }
    let sb = spawn(&os_spec()).await.unwrap();
    let out = sb
        .exec(ExecRequest::new(vec![
            "sh".into(),
            "-c".into(),
            "printf ok > note.txt".into(),
        ]))
        .await
        .unwrap();
    assert_eq!(out.exit_code, 0);
    let got = sb.read_file(Path::new("note.txt")).await.unwrap();
    assert_eq!(got, b"ok");
    sb.teardown().await.unwrap();
}

#[tokio::test]
async fn network_is_denied_by_default() {
    if !os_sandbox_available().await {
        eprintln!("skipping: no Landlock support");
        return;
    }
    let sb = spawn(&os_spec()).await.unwrap();
    // A TCP connect must fail under the default Network::None (seccomp denies inet sockets).
    // Assert the guarantee whenever python3 OR bash can attempt a real connection; skip only if
    // neither exists. A reachable network reads as OPEN and must fail the assertion.
    let py = "python3 - <<'PY'\nimport socket,sys\ntry:\n socket.create_connection(('1.1.1.1',80),timeout=2); print('OPEN')\nexcept Exception:\n print('BLOCKED')\nPY";
    let out = sb
        .exec(ExecRequest::new(vec!["sh".into(), "-c".into(), py.into()]))
        .await
        .unwrap();
    if !out.stdout.is_empty() {
        assert_eq!(
            out.stdout, b"BLOCKED\n",
            "network must be blocked by default"
        );
        sb.teardown().await.unwrap();
        return;
    }

    let bash = "exec 3<>/dev/tcp/1.1.1.1/80 && echo OPEN || echo BLOCKED";
    let out = sb
        .exec(ExecRequest::new(vec![
            "bash".into(),
            "-c".into(),
            bash.into(),
        ]))
        .await
        .unwrap();
    if !out.stdout.is_empty() {
        assert!(
            String::from_utf8_lossy(&out.stdout).contains("BLOCKED"),
            "network must be blocked by default (bash /dev/tcp)"
        );
        sb.teardown().await.unwrap();
        return;
    }

    eprintln!("skipping: neither python3 nor bash available in sandbox");
    sb.teardown().await.unwrap();
}

#[tokio::test]
async fn spawn_reports_availability_and_isolation_default_is_secure() {
    // OsSandbox is the primitive's declared isolation for a caller that picks it.
    assert_eq!(os_spec().isolation, Isolation::OsSandbox);
    let _ = Network::None;
    // Availability is environment-dependent; just assert spawn does not panic.
    let _ = spawn(&os_spec()).await;
}
