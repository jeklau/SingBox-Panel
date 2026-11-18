//! Detached Agent self-uninstall (Go `selfuninstall.go`).

use anyhow::{anyhow, Result};

use crate::atomic_file::write_atomic_file;
use crate::exec::run;
use crate::service::{
    current_service_manager, launch_openrc_detached_shell, ServiceKind,
};

const AGENT_UNINSTALL_UNIT: &str = "/etc/systemd/system/singbox-panel-agent-uninstall.service";
const AGENT_UNINSTALL_SCRIPT: &str = "/run/singbox-panel-agent-uninstall.sh";
const AGENT_UNINSTALL_UNIT_NAME: &str = "singbox-panel-agent-uninstall.service";
const AGENT_UNINSTALL_PID_FILE: &str = "/run/singbox-panel-agent-uninstall.pid";

const SELF_UNINSTALL_SCRIPT: &str = r#"#!/bin/sh
sleep 5
if [ -d /run/systemd/system ] && command -v systemctl >/dev/null 2>&1; then
  systemctl disable --now singbox-panel-agent.service >/dev/null 2>&1 || true
else
  rc-service singbox-panel-agent stop >/dev/null 2>&1 || true
  rc-update del singbox-panel-agent default >/dev/null 2>&1 || true
fi
rm -f /usr/local/bin/singbox-panel-agent
rm -rf /etc/singbox-panel-agent
rm -f /etc/systemd/system/singbox-panel-agent.service
rm -f /etc/systemd/system/singbox-panel-agent-uninstall.service
rm -f /etc/init.d/singbox-panel-agent
rm -f /run/singbox-panel-agent.pid
if [ -d /run/systemd/system ] && command -v systemctl >/dev/null 2>&1; then
  systemctl daemon-reload >/dev/null 2>&1 || true
fi
rm -f /run/singbox-panel-agent-uninstall.sh
rm -f /run/singbox-panel-agent-uninstall.pid
"#;

const SELF_UNINSTALL_UNIT: &str = r#"[Unit]
Description=Remove SingBox Panel Agent

[Service]
Type=oneshot
ExecStart=/bin/sh /run/singbox-panel-agent-uninstall.sh
"#;

/// Creates a detached cleanup process. It waits briefly so the command
/// acknowledgement reaches the panel before stopping this Agent, then removes
/// the binary, configuration and host service definition.
pub async fn schedule_self_uninstall() -> Result<String> {
    let manager = current_service_manager().await?;
    write_atomic_file(
        AGENT_UNINSTALL_SCRIPT,
        SELF_UNINSTALL_SCRIPT.as_bytes(),
        0o700,
    )
    .map_err(|e| anyhow!("prepare Agent cleanup script: {e}"))?;

    match manager.kind() {
        ServiceKind::Systemd => {
            if let Err(e) =
                write_atomic_file(AGENT_UNINSTALL_UNIT, SELF_UNINSTALL_UNIT.as_bytes(), 0o644)
            {
                let _ = std::fs::remove_file(AGENT_UNINSTALL_SCRIPT);
                return Err(anyhow!("prepare Agent cleanup service: {e}"));
            }
            match manager.prepare().await {
                Ok(_) => {}
                Err(e) => {
                    cleanup_agent_uninstall_files();
                    return Err(anyhow!("reload systemd for Agent cleanup: {e}"));
                }
            }
            match run(
                "systemctl",
                &["--no-block", "start", AGENT_UNINSTALL_UNIT_NAME],
            )
            .await
            {
                Ok(r) if r.success() => {}
                Ok(r) => {
                    cleanup_agent_uninstall_files();
                    let _ = manager.prepare().await;
                    return Err(anyhow!(
                        "start Agent cleanup: {}: {}",
                        r.status,
                        r.output
                    ));
                }
                Err(e) => {
                    cleanup_agent_uninstall_files();
                    let _ = manager.prepare().await;
                    return Err(anyhow!("start Agent cleanup: {e}"));
                }
            }
        }
        ServiceKind::OpenRc => {
            if let Err(e) =
                launch_openrc_detached_shell(AGENT_UNINSTALL_SCRIPT, AGENT_UNINSTALL_PID_FILE).await
            {
                cleanup_agent_uninstall_files();
                return Err(e);
            }
        }
    }
    Ok("Agent、配置文件和开机自启服务将在数秒后删除".into())
}

fn cleanup_agent_uninstall_files() {
    let _ = std::fs::remove_file(AGENT_UNINSTALL_UNIT);
    let _ = std::fs::remove_file(AGENT_UNINSTALL_SCRIPT);
    let _ = std::fs::remove_file(AGENT_UNINSTALL_PID_FILE);
}

/// Script body (for tests).
pub fn self_uninstall_script() -> &'static str {
    SELF_UNINSTALL_SCRIPT
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn self_uninstall_script_cleans_agent_but_keeps_singbox() {
        let script = SELF_UNINSTALL_SCRIPT;
        for path in [
            "/usr/local/bin/singbox-panel-agent",
            "/etc/singbox-panel-agent",
            "/etc/systemd/system/singbox-panel-agent.service",
            "/etc/systemd/system/singbox-panel-agent-uninstall.service",
            "/etc/init.d/singbox-panel-agent",
            "rc-update del singbox-panel-agent default",
        ] {
            assert!(script.contains(path), "cleanup script does not remove {path}");
        }
        assert!(!script.contains("/usr/bin/sing-box"));
        assert!(!script.contains("/etc/sing-box"));
        assert!(!script.contains("sing-box.service"));
    }

    #[test]
    fn self_uninstall_script_has_valid_shell_syntax() {
        let status = std::process::Command::new("sh")
            .arg("-n")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .and_then(|mut child| {
                use std::io::Write;
                child
                    .stdin
                    .as_mut()
                    .unwrap()
                    .write_all(SELF_UNINSTALL_SCRIPT.as_bytes())?;
                child.wait_with_output()
            })
            .expect("run sh -n");
        assert!(
            status.status.success(),
            "self-uninstall shell syntax: {}",
            String::from_utf8_lossy(&status.stderr)
        );
    }

}
