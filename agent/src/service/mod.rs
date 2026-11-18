//! Init-system service manager for the official sing-box installation.
//!
//! Mirrors Go `service_manager.go` + `systemd.go` + `openrc.go`: detect
//! systemd vs OpenRC, whitelist actions, and expose Active/Enabled/Prepare
//! plus PID/start-time evidence used by ApplyConfig.

mod openrc;
mod systemd;

pub use openrc::{
    ensure_openrc_logging, ensure_openrc_singbox_service, openrc_log_path,
    openrc_log_path_from_config, openrc_singbox_service_script, process_args_load_managed_config,
    split_nul_terminated, OpenRcServiceManager, OPENRC_MANAGED_MARKER, OPENRC_SINGBOX_CONF,
    OPENRC_SINGBOX_INIT, OPENRC_SINGBOX_LOG,
};
pub use systemd::{parse_systemd_timestamp, SystemdServiceManager, SYSTEMD_UNIT_PATH};

use std::path::Path;
use std::time::SystemTime;

use anyhow::{anyhow, bail, Result};

use crate::exec::{run, RunResult};

const OPENRC_START_STOP_DAEMON: &str = "/sbin/start-stop-daemon";

/// Init system kind reported in apply/rollback messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceKind {
    Systemd,
    OpenRc,
}

impl ServiceKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Systemd => "systemd",
            Self::OpenRc => "openrc",
        }
    }
}

impl std::fmt::Display for ServiceKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Whitelist shared by systemd and OpenRC (status is handled separately).
const ALLOWED_SERVICE_ACTIONS: &[&str] = &[
    "start", "stop", "restart", "reload", "enable", "disable",
];

fn is_allowed_service_action(action: &str) -> bool {
    ALLOWED_SERVICE_ACTIONS.contains(&action)
}

/// Host init-system adapter used by ApplyConfig and command handlers.
#[derive(Debug)]
pub enum ServiceManager {
    Systemd(SystemdServiceManager),
    OpenRc(OpenRcServiceManager),
}

impl ServiceManager {
    pub fn kind(&self) -> ServiceKind {
        match self {
            Self::Systemd(_) => ServiceKind::Systemd,
            Self::OpenRc(_) => ServiceKind::OpenRc,
        }
    }

    pub async fn prepare(&self) -> Result<String> {
        match self {
            Self::Systemd(m) => m.prepare().await,
            Self::OpenRc(m) => m.prepare().await,
        }
    }

    pub async fn action(&self, action: &str) -> Result<String> {
        match self {
            Self::Systemd(m) => m.action(action).await,
            Self::OpenRc(m) => m.action(action).await,
        }
    }

    pub async fn set_enabled(&self, enabled: bool) -> Result<String> {
        match self {
            Self::Systemd(m) => m.set_enabled(enabled).await,
            Self::OpenRc(m) => m.set_enabled(enabled).await,
        }
    }

    pub async fn active(&self) -> bool {
        match self {
            Self::Systemd(m) => m.active().await,
            Self::OpenRc(m) => m.active().await,
        }
    }

    pub async fn enabled(&self) -> bool {
        match self {
            Self::Systemd(m) => m.enabled().await,
            Self::OpenRc(m) => m.enabled().await,
        }
    }

    pub async fn status(&self) -> String {
        match self {
            Self::Systemd(m) => m.status().await,
            Self::OpenRc(m) => m.status().await,
        }
    }

    pub async fn main_pid(&self) -> Result<i32> {
        match self {
            Self::Systemd(m) => m.main_pid().await,
            Self::OpenRc(m) => m.main_pid().await,
        }
    }

    pub async fn started_at(&self, pid: i32) -> Result<SystemTime> {
        match self {
            Self::Systemd(m) => m.started_at(pid).await,
            Self::OpenRc(m) => m.started_at(pid).await,
        }
    }

    /// OpenRC waits for process-clock uncertainty after writing config; systemd is a no-op.
    pub async fn wait_for_config_start(&self) -> Result<()> {
        match self {
            Self::Systemd(_) => Ok(()),
            Self::OpenRc(m) => m.wait_for_config_start().await,
        }
    }

    pub fn log_command(&self, lines: i32, follow: bool) -> (String, Vec<String>) {
        match self {
            Self::Systemd(m) => m.log_command(lines, follow),
            Self::OpenRc(m) => m.log_command(lines, follow),
        }
    }
}

/// Detects the host init system and constructs the matching manager.
pub async fn current_service_manager() -> Result<ServiceManager> {
    let kind = detect_service_kind(
        path_exists("/run/systemd/system"),
        path_exists("/sbin/openrc-run"),
        command_exists,
    )?;
    match kind {
        ServiceKind::Systemd => Ok(ServiceManager::Systemd(SystemdServiceManager::new())),
        ServiceKind::OpenRc => {
            let clock = openrc::read_process_clock_at("/proc", || SystemTime::now())?;
            Ok(ServiceManager::OpenRc(OpenRcServiceManager::new(clock)))
        }
    }
}

pub fn detect_service_kind(
    systemd_runtime: bool,
    openrc_runner: bool,
    exists: impl Fn(&str) -> bool,
) -> Result<ServiceKind> {
    if systemd_runtime && exists("systemctl") {
        return Ok(ServiceKind::Systemd);
    }
    if openrc_runner && exists("rc-service") && exists("rc-update") {
        return Ok(ServiceKind::OpenRc);
    }
    bail!("unsupported init system: sing-box-panel-agent requires systemd or OpenRC");
}

pub fn path_exists(path: &str) -> bool {
    Path::new(path).exists()
}

pub fn command_exists(name: &str) -> bool {
    which_in_path(name).is_some()
}

fn which_in_path(name: &str) -> Option<std::path::PathBuf> {
    if name.contains('/') {
        let p = Path::new(name);
        return if p.is_file() {
            Some(p.to_path_buf())
        } else {
            None
        };
    }
    let path = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&path) {
        let candidate = dir.join(name);
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}

/// Runs a whitelisted service action through the host init system.
pub async fn service_action(action: &str) -> Result<String> {
    let manager = current_service_manager().await?;
    if action == "status" {
        return Ok(manager.status().await);
    }
    if !is_allowed_service_action(action) {
        bail!("disallowed service action {action:?}");
    }
    manager.action(action).await
}

/// Reports whether sing-box is active under the host init system.
pub async fn service_active() -> bool {
    match current_service_manager().await {
        Ok(m) => m.active().await,
        Err(_) => false,
    }
}

/// Reports whether sing-box is enabled at boot.
pub async fn service_enabled() -> bool {
    match current_service_manager().await {
        Ok(m) => m.enabled().await,
        Err(_) => false,
    }
}

/// Detached OpenRC helper used by self-update / uninstall (Go `launchOpenRCDetachedShell`).
pub async fn launch_openrc_detached_shell(script: &str, pid_file: &str) -> Result<()> {
    let result = run(
        OPENRC_START_STOP_DAEMON,
        &[
            "--start",
            "--background",
            "--make-pidfile",
            "--pidfile",
            pid_file,
            "--exec",
            "/bin/sh",
            "--",
            script,
        ],
    )
    .await
    .map_err(|e| anyhow!("start detached OpenRC task: {e}"))?;
    if !result.success() {
        bail!(
            "start detached OpenRC task: command failed: {}",
            result.output
        );
    }
    Ok(())
}

/// Shared helper: run a command and map non-zero exit to Err with output retained.
pub(crate) async fn run_ok(name: &str, args: &[&str]) -> Result<String> {
    let result = run(name, args).await?;
    result_to_string(result)
}

pub(crate) fn result_to_string(result: RunResult) -> Result<String> {
    if result.success() {
        Ok(result.output)
    } else {
        Err(anyhow!(
            "command failed (status {}): {}",
            result.status,
            result.output
        ))
    }
}

/// Like [`run_ok`] but always returns the output string alongside the error.
pub(crate) async fn run_with_output(name: &str, args: &[&str]) -> Result<String, (String, anyhow::Error)> {
    match run(name, args).await {
        Ok(result) => {
            if result.success() {
                Ok(result.output)
            } else {
                let out = result.output.clone();
                Err((
                    out.clone(),
                    anyhow!("command failed (status {}): {}", result.status, out),
                ))
            }
        }
        Err(e) => Err((String::new(), e)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_prefers_systemd() {
        let kind = detect_service_kind(true, true, |_| true).unwrap();
        assert_eq!(kind, ServiceKind::Systemd);
    }

    #[test]
    fn detect_openrc() {
        let kind = detect_service_kind(false, true, |n| n == "rc-service" || n == "rc-update")
            .unwrap();
        assert_eq!(kind, ServiceKind::OpenRc);
    }

    #[test]
    fn detect_missing_commands() {
        assert!(detect_service_kind(false, true, |_| false).is_err());
    }

    #[test]
    fn whitelist() {
        assert!(is_allowed_service_action("restart"));
        assert!(!is_allowed_service_action("status"));
        assert!(!is_allowed_service_action("mask"));
    }
}
