/*
 * SPDX-FileCopyrightText: 2026 Wavelens GmbH <info@wavelens.io>
 *
 * SPDX-License-Identifier: MIT OR Apache-2.0
 */

use std::os::unix::io::OwnedFd;
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use landlock::{
    ABI, Access, AccessFs, AccessNet, CompatLevel, Compatible, PathBeneath, PathFd, Ruleset,
    RulesetAttr, RulesetCreatedAttr, RulesetStatus,
};
use tempfile::TempDir;

use crate::exec_common::{PreExecHook, Subprocess};
use crate::spec::{ExecRequest, ExecResult, Network, SandboxSpec};
use crate::{Error, Result, Sandbox};

const ABI_TARGET: ABI = ABI::V5;

/// A daemonless OS-level backend: each command runs under a Landlock ruleset (write-confined to the
/// workdir, read+exec broad) plus a seccomp inet-socket denial when network is off.
pub(crate) struct OsSandbox {
    root: TempDir,
    max_output_bytes: Option<usize>,
    default_timeout: Option<std::time::Duration>,
    env: Vec<(String, String)>,
    allow_network: bool,
}

/// Probe the running kernel for Landlock without restricting anything: `create()` only yields a
/// real ruleset fd when the kernel supports at least one handled access, so its presence is a
/// side-effect-free support signal. The definitive guarantee is still the child-side enforcement
/// check in `build_confinement`.
fn landlock_supported() -> bool {
    let Ok(created) = Ruleset::default()
        .set_compatibility(CompatLevel::BestEffort)
        .handle_access(AccessFs::from_all(ABI_TARGET))
        .and_then(|r| r.create())
    else {
        return false;
    };
    Option::<OwnedFd>::from(created).is_some()
}

impl OsSandbox {
    pub(crate) async fn spawn(spec: &SandboxSpec) -> Result<Self> {
        if !landlock_supported() {
            return Err(Error::Unavailable(
                "os-sandbox",
                "kernel lacks Landlock support (needs Linux 5.13+)".to_string(),
            ));
        }
        let root = TempDir::new()?;
        for seed in &spec.seed_files {
            let target = resolve(root.path(), &seed.path);
            if let Some(parent) = target.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            tokio::fs::write(&target, &seed.contents).await?;
        }
        Ok(OsSandbox {
            root,
            max_output_bytes: spec.limits.max_output_bytes,
            default_timeout: spec.limits.timeout,
            env: spec
                .env
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            allow_network: !matches!(spec.network, Network::None),
        })
    }

    fn subprocess(&self) -> Subprocess {
        Subprocess {
            root: self.root.path().to_path_buf(),
            env: self.env.clone(),
            max_output_bytes: self.max_output_bytes,
            default_timeout: self.default_timeout,
        }
    }
}

fn resolve(root: &Path, path: &Path) -> PathBuf {
    root.join(path.strip_prefix("/").unwrap_or(path))
}

/// Build the per-exec confinement hook in the PARENT (all allocation and fd opening happens here).
/// The returned closure runs in the forked child and performs only syscalls: `restrict_self` (which
/// also sets NO_NEW_PRIVS) then `apply_filter`.
fn build_confinement(workdir: PathBuf, allow_network: bool) -> Result<PreExecHook> {
    let read_exec = AccessFs::Execute | AccessFs::ReadFile | AccessFs::ReadDir;

    let mut ruleset = Ruleset::default()
        .set_compatibility(CompatLevel::BestEffort)
        .handle_access(AccessFs::from_all(ABI_TARGET))
        .map_err(map_landlock)?;
    if !allow_network {
        ruleset = ruleset
            .handle_access(AccessNet::from_all(ABI_TARGET))
            .map_err(map_landlock)?;
    }
    let created = ruleset
        .create()
        .map_err(map_landlock)?
        .add_rule(PathBeneath::new(
            PathFd::new("/").map_err(map_landlock)?,
            read_exec,
        ))
        .map_err(map_landlock)?
        .add_rule(PathBeneath::new(
            PathFd::new(&workdir).map_err(map_landlock)?,
            AccessFs::from_all(ABI_TARGET),
        ))
        .map_err(map_landlock)?;

    let bpf = if allow_network {
        None
    } else {
        Some(build_socket_deny_filter()?)
    };

    let mut created = Some(created);
    Ok(Box::new(move || {
        let rs = created.take().expect("pre_exec hook called once");
        let status = rs
            .restrict_self()
            .map_err(|e| std::io::Error::other(format!("landlock: {e}")))?;
        if status.ruleset == RulesetStatus::NotEnforced {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "landlock not enforced; refusing to run unconfined",
            ));
        }
        if let Some(bpf) = &bpf {
            seccompiler::apply_filter(bpf)
                .map_err(|e| std::io::Error::other(format!("seccomp: {e}")))?;
        }
        Ok(())
    }))
}

/// A seccomp program that fails `socket(AF_INET|AF_INET6, ...)` with EACCES and allows everything
/// else (so AF_UNIX and local IPC keep working). Compiled in the parent; applied in the child.
fn build_socket_deny_filter() -> Result<seccompiler::BpfProgram> {
    use seccompiler::{
        SeccompAction, SeccompCmpArgLen, SeccompCmpOp, SeccompCondition, SeccompFilter, SeccompRule,
    };
    use std::collections::BTreeMap;

    let deny_domain = |domain: i64| -> Result<SeccompRule> {
        let c = SeccompCondition::new(0, SeccompCmpArgLen::Dword, SeccompCmpOp::Eq, domain as u64)
            .map_err(map_seccomp)?;
        SeccompRule::new(vec![c]).map_err(map_seccomp)
    };

    let mut rules: BTreeMap<i64, Vec<SeccompRule>> = BTreeMap::new();
    rules.insert(
        libc::SYS_socket,
        vec![
            deny_domain(libc::AF_INET as i64)?,
            deny_domain(libc::AF_INET6 as i64)?,
        ],
    );

    let filter = SeccompFilter::new(
        rules,
        SeccompAction::Allow,
        SeccompAction::Errno(libc::EACCES as u32),
        target_arch()?,
    )
    .map_err(map_seccomp)?;
    filter.try_into().map_err(map_seccomp)
}

fn target_arch() -> Result<seccompiler::TargetArch> {
    seccompiler::TargetArch::try_from(std::env::consts::ARCH).map_err(map_seccomp)
}

fn map_landlock<E: std::fmt::Display>(e: E) -> Error {
    Error::Exec(format!("landlock setup: {e}"))
}
fn map_seccomp<E: std::fmt::Display>(e: E) -> Error {
    Error::Exec(format!("seccomp setup: {e}"))
}

#[async_trait]
impl Sandbox for OsSandbox {
    async fn exec(&self, req: ExecRequest) -> Result<ExecResult> {
        let workdir = self.root.path().to_path_buf();
        let allow_network = self.allow_network;
        let factory = move || build_confinement(workdir.clone(), allow_network);
        self.subprocess().run(req, Some(&factory)).await
    }

    async fn write_file(&self, path: &Path, contents: &[u8]) -> Result<()> {
        let target = resolve(self.root.path(), path);
        if let Some(parent) = target.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        tokio::fs::write(&target, contents).await?;
        Ok(())
    }

    async fn read_file(&self, path: &Path) -> Result<Vec<u8>> {
        Ok(tokio::fs::read(resolve(self.root.path(), path)).await?)
    }

    async fn teardown(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}
