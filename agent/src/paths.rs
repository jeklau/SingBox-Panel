//! Official sing-box and agent filesystem paths used by the Go agent.

use std::path::{Path, PathBuf};

/// Official sing-box binary install location.
pub const SINGBOX_BINARY: &str = "/usr/bin/sing-box";

/// Official sing-box config directory.
pub const CONFIG_DIR: &str = "/etc/sing-box";

/// Official sing-box config file.
pub const CONFIG_FILE: &str = "/etc/sing-box/config.json";

/// Official sing-box service / unit name.
pub const SERVICE_NAME: &str = "sing-box";

/// Temporary config written before `sing-box check` and atomic rename.
pub const TMP_CONFIG_FILE: &str = "/etc/sing-box/.config.json.tmp";

/// Backup of the previous managed config (rollback target).
pub const BAK_CONFIG_FILE: &str = "/etc/sing-box/config.json.bak";

/// First-seen original config preserved before panel adoption.
pub const ORIG_CONFIG_FILE: &str = "/etc/sing-box/config.json.orig";

/// Maximum accepted config.json size (16 MiB), matching Go `maxConfigSize`.
pub const MAX_CONFIG_SIZE: u64 = 16 << 20;

/// Where the install script places the agent binary.
pub const AGENT_BINARY: &str = "/usr/local/bin/singbox-panel-agent";

/// Previous agent binary retained for self-update rollback.
pub const AGENT_PREVIOUS_BINARY: &str = "/usr/local/bin/singbox-panel-agent.prev";

/// Agent systemd/OpenRC service name.
pub const AGENT_SERVICE_NAME: &str = "singbox-panel-agent";

/// Default agent KEY=VALUE config file.
pub const AGENT_CONF_FILE: &str = "/etc/singbox-panel-agent/agent.conf";

/// Agent config directory.
pub const AGENT_CONF_DIR: &str = "/etc/singbox-panel-agent";

/// Readiness marker written after successful WS registration.
pub const AGENT_READY_FILE: &str = "/run/singbox-panel-agent.ready";

/// Expected SHA-256 of a pending self-update binary.
pub const AGENT_EXPECTED_SHA_FILE: &str = "/run/singbox-panel-agent.expected-sha256";

/// Transient systemd unit used during self-update.
pub const AGENT_UPGRADE_UNIT: &str = "/etc/systemd/system/singbox-panel-agent-upgrade.service";

/// Self-update helper script.
pub const AGENT_UPGRADE_SCRIPT: &str = "/run/singbox-panel-agent-upgrade.sh";

/// Upgrade unit name (systemctl).
pub const AGENT_UPGRADE_UNIT_NAME: &str = "singbox-panel-agent-upgrade.service";

/// PID file for the upgrade helper.
pub const AGENT_UPGRADE_PID_FILE: &str = "/run/singbox-panel-agent-upgrade.pid";

/// Durable self-update status file.
pub const AGENT_UPDATE_STATUS: &str = "/var/lib/singbox-panel-agent/update-status";

/// Agent state directory under /var/lib.
pub const AGENT_STATE_DIR: &str = "/var/lib/singbox-panel-agent";

/// HTTP response header carrying the panel-published agent SHA-256.
pub const AGENT_CHECKSUM_HEADER: &str = "X-SingBox-Panel-SHA256";

/// Maximum accepted agent binary download size (128 MiB).
pub const MAX_AGENT_BINARY_SIZE: u64 = 128 << 20;

/// Candidate locations for an existing sing-box binary (detection order).
pub const SINGBOX_BINARY_CANDIDATES: &[&str] = &[
    SINGBOX_BINARY,
    "/usr/local/bin/sing-box",
    "/usr/sbin/sing-box",
    "/usr/local/sbin/sing-box",
    "/opt/sing-box/sing-box",
];

/// Resolves the sing-box executable path.
///
/// Prefers the official install location, then common manual install paths,
/// then `PATH`, and finally falls back to [`SINGBOX_BINARY`] (the install target).
pub fn singbox_binary() -> PathBuf {
    for p in SINGBOX_BINARY_CANDIDATES {
        let path = Path::new(p);
        if path.is_file() {
            return path.to_path_buf();
        }
    }
    if let Some(found) = which_in_path("sing-box") {
        return found;
    }
    PathBuf::from(SINGBOX_BINARY)
}

fn which_in_path(name: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&path) {
        let candidate = dir.join(name);
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}
