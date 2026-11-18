//! Agent self-update with SHA-256 verification and upgrade watchdog
//! (Go `selfupdate.go`).

use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use futures_util::StreamExt;
use sha2::{Digest, Sha256};

use crate::atomic_file::write_atomic_file;
use crate::exec::run;
use crate::install::first_line;
use crate::paths::{
    AGENT_BINARY, AGENT_CHECKSUM_HEADER, AGENT_EXPECTED_SHA_FILE, AGENT_PREVIOUS_BINARY,
    AGENT_READY_FILE, AGENT_UPGRADE_PID_FILE, AGENT_UPGRADE_SCRIPT, AGENT_UPGRADE_UNIT,
    AGENT_UPGRADE_UNIT_NAME, MAX_AGENT_BINARY_SIZE,
};
use crate::service::{
    current_service_manager, launch_openrc_detached_shell, ServiceKind,
};
use crate::sysinfo::go_arch;

const AGENT_UPGRADE_SCRIPT_BODY: &str = r#"#!/bin/sh
sleep 3
READY=/run/singbox-panel-agent.ready
EXPECTED=/run/singbox-panel-agent.expected-sha256
BIN=/usr/local/bin/singbox-panel-agent
PREV=/usr/local/bin/singbox-panel-agent.prev
STATUS=/var/lib/singbox-panel-agent/update-status
PIDFILE=/run/singbox-panel-agent-upgrade.pid

if [ -d /run/systemd/system ] && command -v systemctl >/dev/null 2>&1; then
  SERVICE_MANAGER=systemd
else
  SERVICE_MANAGER=openrc
fi

service_active() {
  if [ "$SERVICE_MANAGER" = systemd ]; then
    systemctl is-active --quiet singbox-panel-agent.service
  else
    rc-service singbox-panel-agent status >/dev/null 2>&1
  fi
}

service_restart() {
  if [ "$SERVICE_MANAGER" = systemd ]; then
    systemctl daemon-reload >/dev/null 2>&1 || true
    systemctl restart singbox-panel-agent.service
  else
    rc-service singbox-panel-agent restart
  fi
}

service_stop() {
  if [ "$SERVICE_MANAGER" = systemd ]; then
    systemctl stop singbox-panel-agent.service >/dev/null 2>&1 || true
  else
    rc-service singbox-panel-agent stop >/dev/null 2>&1 || true
  fi
}

mkdir -p /var/lib/singbox-panel-agent
rm -f "$READY"
service_restart >/dev/null 2>&1 || true

i=0
while [ "$i" -lt 30 ]; do
  expected_sha=$(cat "$EXPECTED" 2>/dev/null || true)
  ready_sha=$(sed -n '2p' "$READY" 2>/dev/null || true)
  if service_active && [ -n "$expected_sha" ] && [ "$ready_sha" = "$expected_sha" ]; then
    printf 'ok\n' > "$STATUS"
    rm -f "$PREV" "$EXPECTED"
    rm -f /etc/systemd/system/singbox-panel-agent-upgrade.service
    rm -f /run/singbox-panel-agent-upgrade.sh "$PIDFILE"
    if [ "$SERVICE_MANAGER" = systemd ]; then systemctl daemon-reload >/dev/null 2>&1 || true; fi
    exit 0
  fi
  i=$((i + 1))
  sleep 2
done

printf 'rollback\n' > "$STATUS"
service_stop
if [ -x "$PREV" ]; then
	if mv -f "$PREV" "$BIN" && chmod 755 "$BIN" && service_restart >/dev/null 2>&1; then
	  printf 'rollback-ok\n' > "$STATUS"
	else
	  printf 'rollback-failed\n' > "$STATUS"
	fi
else
  printf 'rollback-failed-no-previous-binary\n' > "$STATUS"
fi
rm -f /etc/systemd/system/singbox-panel-agent-upgrade.service
rm -f /run/singbox-panel-agent-upgrade.sh "$EXPECTED" "$PIDFILE"
if [ "$SERVICE_MANAGER" = systemd ]; then systemctl daemon-reload >/dev/null 2>&1 || true; fi
"#;

const AGENT_UPGRADE_UNIT_BODY: &str = r#"[Unit]
Description=Upgrade SingBox Panel Agent with automatic rollback

[Service]
Type=oneshot
ExecStart=/bin/sh /run/singbox-panel-agent-upgrade.sh
"#;

/// Converts the panel's websocket URL into an http(s) base URL.
pub fn http_base(panel_url: &str) -> String {
    let mut s = panel_url.trim().to_string();
    if let Some(stripped) = s.strip_suffix("/api/agent/ws") {
        s = stripped.to_string();
    }
    s = s.trim_end_matches('/').to_string();
    if let Some(rest) = s.strip_prefix("wss://") {
        return format!("https://{rest}");
    }
    if let Some(rest) = s.strip_prefix("ws://") {
        return format!("http://{rest}");
    }
    s
}

/// Downloads and validates the Agent currently served by the panel, atomically
/// stages it beside a previous binary, then starts a separate upgrade watchdog.
pub async fn self_update(panel_url: &str, token: &str, insecure: bool) -> Result<String> {
    let base = http_base(panel_url);
    if base.is_empty() {
        bail!("panel URL 未知，无法升级");
    }
    let arch = go_arch();
    let url = format!(
        "{}/api/agent/download?arch={}",
        base,
        urlencoding_simple(&arch)
    );

    let mut builder = reqwest::Client::builder().timeout(Duration::from_secs(180));
    if insecure {
        builder = builder.danger_accept_invalid_certs(true);
    }
    let client = builder.build().context("build http client")?;

    let resp = client
        .get(&url)
        .header(reqwest::header::AUTHORIZATION, format!("Bearer {token}"))
        .send()
        .await
        .map_err(|e| anyhow!("下载 Agent 失败: {e}"))?;

    if !resp.status().is_success() {
        bail!("下载 Agent 失败: HTTP {}", resp.status().as_u16());
    }

    let expected = parse_agent_checksum(
        resp.headers()
            .get(AGENT_CHECKSUM_HEADER)
            .and_then(|v| v.to_str().ok())
            .unwrap_or(""),
    )
    .map_err(|e| anyhow!("Agent 校验信息无效: {e}"))?;

    let dir = Path::new(AGENT_BINARY)
        .parent()
        .unwrap_or_else(|| Path::new("/usr/local/bin"));
    let mut tmp = tempfile::Builder::new()
        .prefix(".singbox-panel-agent-")
        .tempfile_in(dir)
        .map_err(|e| anyhow!("创建临时文件失败: {e}"))?;

    let mut hasher = Sha256::new();
    let mut n: u64 = 0;
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| anyhow!("写入 Agent 失败: {e}"))?;
        n += chunk.len() as u64;
        if n > MAX_AGENT_BINARY_SIZE {
            bail!("下载的 Agent 超过大小限制 ({n} 字节)");
        }
        hasher.update(&chunk);
        tmp.write_all(&chunk)
            .map_err(|e| anyhow!("写入 Agent 失败: {e}"))?;
    }
    tmp.flush().map_err(|e| anyhow!("写入 Agent 失败: {e}"))?;
    tmp.as_file()
        .sync_all()
        .map_err(|e| anyhow!("写入 Agent 失败: {e}"))?;

    let actual = hex::encode(hasher.finalize());
    if actual != expected {
        bail!("Agent SHA256 不匹配");
    }

    tmp.as_file()
        .set_permissions(std::fs::Permissions::from_mode(0o755))
        .map_err(|e| anyhow!("设置 Agent 权限失败: {e}"))?;

    // Keep the temp path; Drop of TempPath would otherwise remove it on error paths
    // after we take ownership below.
    let tmp_path = tmp.into_temp_path();
    let version = match validate_agent_binary(&tmp_path).await {
        Ok(v) => v,
        Err(e) => {
            let _ = std::fs::remove_file(&tmp_path);
            return Err(e);
        }
    };

    if let Ok(current) = file_sha256(AGENT_BINARY) {
        if current == actual {
            let _ = std::fs::remove_file(&tmp_path);
            return Ok(format!("Agent 已是最新版本（{version}）"));
        }
    }

    if !Path::new(AGENT_BINARY).exists() {
        let _ = std::fs::remove_file(&tmp_path);
        bail!("当前 Agent 二进制不存在");
    }

    let _ = std::fs::remove_file(AGENT_PREVIOUS_BINARY);
    if let Err(e) = std::fs::rename(AGENT_BINARY, AGENT_PREVIOUS_BINARY) {
        let _ = std::fs::remove_file(&tmp_path);
        bail!("备份旧 Agent 失败: {e}");
    }

    if let Err(e) = std::fs::rename(&tmp_path, AGENT_BINARY) {
        let _ = tmp_path.close();
        if let Err(rollback_err) = restore_previous_agent() {
            return Err(anyhow!(
                "替换 Agent 失败: {e}；恢复旧 Agent 也失败: {rollback_err}"
            ));
        }
        bail!("替换 Agent 失败: {e}");
    }
    // rename moved the file; prevent TempPath Drop from unlinking the stale path.
    std::mem::forget(tmp_path);

    if let Err(e) = write_atomic_file(AGENT_EXPECTED_SHA_FILE, format!("{actual}\n").as_bytes(), 0o644)
    {
        let _ = std::fs::remove_file(AGENT_EXPECTED_SHA_FILE);
        if let Err(rollback_err) = restore_previous_agent() {
            return Err(anyhow!(
                "写入 Agent 就绪校验失败: {e}；恢复旧 Agent 也失败: {rollback_err}"
            ));
        }
        return Err(anyhow!("写入 Agent 就绪校验失败，已恢复旧 Agent: {e}"));
    }

    if let Err(e) = schedule_agent_upgrade().await {
        let _ = std::fs::remove_file(AGENT_EXPECTED_SHA_FILE);
        if let Err(rollback_err) = restore_previous_agent() {
            return Err(anyhow!(
                "启动升级守护失败: {e}；恢复旧 Agent 失败: {rollback_err}"
            ));
        }
        return Err(anyhow!("启动升级守护失败，已恢复旧 Agent: {e}"));
    }

    Ok(format!(
        "新 Agent 已校验（{version}，{n} 字节），将重启并在连接失败时自动回滚"
    ))
}

fn urlencoding_simple(s: &str) -> String {
    // GOARCH values are alphanumeric; keep a conservative escape.
    url::form_urlencoded::byte_serialize(s.as_bytes()).collect()
}

pub fn parse_agent_checksum(raw: &str) -> Result<String> {
    let raw = raw.trim().to_ascii_lowercase();
    let bytes = hex::decode(&raw).map_err(|_| anyhow!("缺少有效的 SHA256 响应头"))?;
    if bytes.len() != 32 {
        bail!("缺少有效的 SHA256 响应头");
    }
    Ok(raw)
}

pub fn file_sha256(path: impl AsRef<Path>) -> Result<String> {
    let mut f = std::fs::File::open(path.as_ref())?;
    let mut hasher = Sha256::new();
    std::io::copy(&mut f, &mut hasher)?;
    Ok(hex::encode(hasher.finalize()))
}

pub async fn validate_agent_binary(path: impl AsRef<Path>) -> Result<String> {
    let path_str = path.as_ref().to_string_lossy().into_owned();
    let result = tokio::time::timeout(Duration::from_secs(10), run(&path_str, &["--version"]))
        .await
        .map_err(|_| anyhow!("新 Agent 无法执行版本检查: timed out"))?
        .map_err(|e| anyhow!("新 Agent 无法执行版本检查: {e}"))?;
    if !result.success() {
        bail!(
            "新 Agent 无法执行版本检查: status {}: {}",
            result.status,
            result.output
        );
    }
    let out = result.output;
    let trimmed = out.trim();
    if !trimmed.starts_with("singbox-panel-agent ") {
        bail!("新 Agent 版本输出异常: {:?}", first_line(&out));
    }
    let version = first_line(&out)
        .trim()
        .strip_prefix("singbox-panel-agent ")
        .unwrap_or("")
        .trim()
        .to_string();
    Ok(version)
}

async fn schedule_agent_upgrade() -> Result<()> {
    let manager = current_service_manager().await?;
    write_atomic_file(AGENT_UPGRADE_SCRIPT, AGENT_UPGRADE_SCRIPT_BODY.as_bytes(), 0o700)
        .context("prepare Agent upgrade script")?;
    match manager.kind() {
        ServiceKind::Systemd => {
            if let Err(e) =
                write_atomic_file(AGENT_UPGRADE_UNIT, AGENT_UPGRADE_UNIT_BODY.as_bytes(), 0o644)
            {
                let _ = std::fs::remove_file(AGENT_UPGRADE_SCRIPT);
                return Err(anyhow!("prepare Agent upgrade service: {e}"));
            }
            if let Err(e) = manager.prepare().await {
                cleanup_agent_upgrade_files();
                return Err(anyhow!("reload systemd for Agent upgrade: {e}"));
            }
            match run(
                "systemctl",
                &["--no-block", "start", AGENT_UPGRADE_UNIT_NAME],
            )
            .await
            {
                Ok(r) if r.success() => Ok(()),
                Ok(r) => {
                    cleanup_agent_upgrade_files();
                    let _ = manager.prepare().await;
                    Err(anyhow!(
                        "start Agent upgrade service: {}: {}",
                        r.status,
                        r.output
                    ))
                }
                Err(e) => {
                    cleanup_agent_upgrade_files();
                    let _ = manager.prepare().await;
                    Err(anyhow!("start Agent upgrade service: {e}"))
                }
            }
        }
        ServiceKind::OpenRc => {
            if let Err(e) =
                launch_openrc_detached_shell(AGENT_UPGRADE_SCRIPT, AGENT_UPGRADE_PID_FILE).await
            {
                cleanup_agent_upgrade_files();
                return Err(e);
            }
            Ok(())
        }
    }
}

fn cleanup_agent_upgrade_files() {
    let _ = std::fs::remove_file(AGENT_UPGRADE_UNIT);
    let _ = std::fs::remove_file(AGENT_UPGRADE_SCRIPT);
    let _ = std::fs::remove_file(AGENT_UPGRADE_PID_FILE);
}

fn restore_previous_agent() -> Result<()> {
    restore_file_from_previous(AGENT_PREVIOUS_BINARY, AGENT_BINARY)
}

pub fn restore_file_from_previous(previous: &str, live: &str) -> Result<()> {
    std::fs::rename(previous, live).with_context(|| format!("restore {previous} -> {live}"))
}

/// Called only after the WebSocket handshake and registration write succeed.
pub fn mark_agent_ready(version: &str) -> Result<()> {
    let executable = current_executable_path()?;
    let checksum =
        file_sha256(&executable).map_err(|e| anyhow!("hash running Agent: {e}"))?;
    write_atomic_file(
        AGENT_READY_FILE,
        ready_marker(version, &checksum).as_bytes(),
        0o644,
    )
}

pub fn ready_marker(version: &str, checksum: &str) -> String {
    format!("{version}\n{checksum}\n")
}

fn current_executable_path() -> Result<std::path::PathBuf> {
    // Opening /proc/self/exe follows the kernel's reference to the running
    // inode. This matters during an upgrade: the pathname may already point to
    // the new binary while the old process is still running from a deleted inode.
    if Path::new("/proc/self/exe").exists() {
        return Ok(std::path::PathBuf::from("/proc/self/exe"));
    }
    std::env::current_exe().context("current_exe")
}

/// Exposed for tests / scripting checks.
pub fn upgrade_script_body() -> &'static str {
    AGENT_UPGRADE_SCRIPT_BODY
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn parse_agent_checksum_accepts_hex() {
        let sum = Sha256::digest(b"agent");
        let want = hex::encode(sum);
        let got = parse_agent_checksum(&format!("  {} \n", want.to_ascii_uppercase())).unwrap();
        assert_eq!(got, want);
        for bad in ["", "abc", &"z".repeat(64), &"0".repeat(62)] {
            assert!(parse_agent_checksum(bad).is_err());
        }
    }

    #[tokio::test]
    async fn validate_agent_binary_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("agent");
        std::fs::write(
            &path,
            b"#!/bin/sh\nprintf 'singbox-panel-agent test-build\\n'\n",
        )
        .unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        let version = validate_agent_binary(&path).await.unwrap();
        assert_eq!(version, "test-build");
    }

    #[test]
    fn upgrade_watchdog_keeps_singbox_untouched() {
        let body = AGENT_UPGRADE_SCRIPT_BODY;
        for required in [
            AGENT_PREVIOUS_BINARY,
            AGENT_READY_FILE,
            AGENT_EXPECTED_SHA_FILE,
            "systemctl restart singbox-panel-agent.service",
            "rc-service singbox-panel-agent restart",
        ] {
            assert!(body.contains(required), "missing {required}");
        }
        assert!(!body.contains("/etc/sing-box"));
        assert!(!body.contains("sing-box.service"));
        assert!(!body.contains(r#"rm -f "$BIN""#));
        for required in ["ready_sha", "expected_sha", "rollback-ok", "rollback-failed"] {
            assert!(body.contains(required), "missing {required}");
        }
    }

    #[test]
    fn ready_marker_contains_version_and_checksum() {
        assert_eq!(ready_marker("v1.0.2", "abc123"), "v1.0.2\nabc123\n");
    }

    #[test]
    fn restore_file_from_previous_replaces() {
        let dir = tempfile::tempdir().unwrap();
        let live = dir.path().join("agent");
        let previous = dir.path().join("agent.prev");
        std::fs::write(&live, b"new").unwrap();
        std::fs::write(&previous, b"old").unwrap();
        restore_file_from_previous(
            previous.to_str().unwrap(),
            live.to_str().unwrap(),
        )
        .unwrap();
        assert_eq!(std::fs::read_to_string(&live).unwrap(), "old");
        assert!(!previous.exists());
    }

    #[test]
    fn http_base_converts_ws() {
        assert_eq!(
            http_base("wss://panel.example.com/api/agent/ws"),
            "https://panel.example.com"
        );
        assert_eq!(
            http_base("ws://localhost:8080/api/agent/ws"),
            "http://localhost:8080"
        );
        assert_eq!(
            http_base("https://panel.example.com/"),
            "https://panel.example.com"
        );
    }

    #[test]
    fn go_arch_names_for_download() {
        let a = go_arch();
        assert!(a == "amd64" || a == "arm64" || !a.is_empty());
    }

    #[tokio::test]
    async fn self_update_authenticates_download() {
        use std::sync::{Arc, Mutex};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;
        use tokio::sync::oneshot;

        let seen = Arc::new(Mutex::new(String::new()));
        let seen2 = Arc::clone(&seen);
        let (ready_tx, ready_rx) = oneshot::channel::<()>();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = Vec::new();
            let mut tmp = [0u8; 1024];
            // Read until end of headers.
            loop {
                let n = sock.read(&mut tmp).await.unwrap_or(0);
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&tmp[..n]);
                if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
            }
            let req = String::from_utf8_lossy(&buf);
            for line in req.lines() {
                if let Some((k, v)) = line.split_once(':') {
                    if k.eq_ignore_ascii_case("authorization") {
                        *seen2.lock().unwrap() = v.trim().to_string();
                    }
                }
            }
            let _ = sock
                .write_all(b"HTTP/1.1 401 Unauthorized\r\nConnection: close\r\nContent-Length: 0\r\n\r\n")
                .await;
            let _ = sock.shutdown().await;
            let _ = ready_tx.send(());
        });

        let token = "0123456789abcdef0123456789abcdef";
        let url = format!("http://{addr}");
        let err = self_update(&url, token, false).await.unwrap_err();
        assert!(
            err.to_string().contains("401") || err.to_string().contains("下载"),
            "unexpected error: {err:#}"
        );
        let _ = tokio::time::timeout(Duration::from_secs(2), ready_rx).await;
        let auth = seen.lock().unwrap().clone();
        assert_eq!(auth, format!("Bearer {token}"));
        // Arch query must use Go names (amd64/arm64), not Rust x86_64/aarch64.
        assert!(
            go_arch() == "amd64" || go_arch() == "arm64" || !go_arch().is_empty()
        );
    }
}
