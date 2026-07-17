/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

#![cfg(feature = "container")]

use std::path::Path;
use std::time::Duration;

use bask_sandbox::{ExecRequest, Isolation, SandboxSpec, spawn};

fn container_spec() -> SandboxSpec {
    SandboxSpec {
        isolation: Isolation::Container,
        image: Some("busybox:latest".into()),
        ..SandboxSpec::default()
    }
}

/// Reachability probe: a client can always be constructed, so we must actually
/// ping the daemon (bounded by a short timeout) to know a runtime is listening.
async fn runtime_available() -> bool {
    let Ok(docker) = bollard::Docker::connect_with_local_defaults() else {
        return false;
    };
    matches!(
        tokio::time::timeout(Duration::from_secs(2), docker.ping()).await,
        Ok(Ok(_))
    )
}

#[tokio::test]
async fn runs_command_in_container() {
    if !runtime_available().await {
        eprintln!("skipping: no container runtime");
        return;
    }
    let sb = spawn(&container_spec()).await.unwrap();
    let out = sb
        .exec(ExecRequest::new(vec![
            "sh".into(),
            "-c".into(),
            "printf hi; exit 2".into(),
        ]))
        .await
        .unwrap();
    assert_eq!(out.exit_code, 2);
    assert_eq!(out.stdout, b"hi");
    sb.teardown().await.unwrap();
}

#[tokio::test]
async fn writes_and_reads_files_in_container() {
    if !runtime_available().await {
        eprintln!("skipping: no container runtime");
        return;
    }
    let sb = spawn(&container_spec()).await.unwrap();
    sb.write_file(Path::new("note.txt"), b"payload")
        .await
        .unwrap();
    let got = sb.read_file(Path::new("note.txt")).await.unwrap();
    assert_eq!(got, b"payload");
    sb.teardown().await.unwrap();
}
