//! Official sing-box install and version detection (Go `install.go`).

use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};
use regex::Regex;
use tempfile::Builder;

use crate::exec::run;
use crate::paths::{singbox_binary, SINGBOX_BINARY};
use crate::protocol::{CHANNEL_BETA, CHANNEL_STABLE, METHOD_APT, METHOD_DNF};
use crate::service::{current_service_manager, ServiceManager};

/// Installs the official sing-box using method (`script` | `apt` | `dnf`) and
/// channel (`stable` | `beta`). Only invokes upstream installer/repositories.
pub async fn install_singbox(channel: &str, version: &str, method: &str) -> Result<String> {
    let mut channel = channel;
    if channel.is_empty() {
        channel = CHANNEL_STABLE;
    }
    if channel != CHANNEL_STABLE && channel != CHANNEL_BETA {
        bail!("unknown sing-box channel {channel:?}");
    }
    let version_re = version_regex();
    if !version.is_empty() && !version_re.is_match(version) {
        bail!("invalid version {version:?}");
    }
    if !version.is_empty() && (method == METHOD_APT || method == METHOD_DNF) {
        bail!("specific sing-box versions require the script installation method");
    }

    let mut output = match method {
        "apt" => install_apt(channel).await?,
        "dnf" => install_dnf(channel).await?,
        "script" | "" => install_script(channel, version).await?,
        other => bail!("unknown install method {other:?}"),
    };

    let (installed, actual_version) = detect_version().await;
    if !installed || !version_re.is_match(&actual_version) {
        if !output.is_empty() {
            output.push('\n');
        }
        bail!("sing-box installation did not produce a working binary with a valid version");
    }
    if !version.is_empty() && actual_version != version {
        bail!("sing-box version mismatch: requested {version}, installed {actual_version}");
    }
    prepare_singbox_service()
        .await
        .map_err(|e| anyhow!("prepare sing-box service: {e}"))?;
    Ok(output)
}

fn version_regex() -> Regex {
    Regex::new(r"^[0-9][0-9A-Za-z.\-]*$").expect("version regex")
}

pub async fn prepare_singbox_service() -> Result<()> {
    let manager = current_service_manager().await?;
    prepare_singbox_service_with_manager(&manager).await
}

pub async fn prepare_singbox_service_with_manager(manager: &ServiceManager) -> Result<()> {
    match manager.prepare().await {
        Ok(_) => {}
        Err(e) => bail!("prepare service: {e}"),
    }
    if !manager.enabled().await {
        match manager.set_enabled(true).await {
            Ok(_) => {}
            Err(e) => bail!("enable service at boot: {e}"),
        }
    }
    Ok(())
}

async fn install_script(channel: &str, version: &str) -> Result<String> {
    let tmp = Builder::new()
        .prefix("singbox-install-")
        .suffix(".sh")
        .tempfile()
        .context("create installer file")?;
    // Keep the file around after close; delete ourselves.
    let (file, path) = tmp.keep().context("persist installer temp")?;
    drop(file);

    let mut output = String::new();
    let path_str = path.to_string_lossy().into_owned();
    let dl = run(
        "curl",
        &[
            "-fsSL",
            "--output",
            &path_str,
            "https://sing-box.app/install.sh",
        ],
    )
    .await
    .context("download sing-box installer")?;
    if !dl.success() {
        let _ = std::fs::remove_file(&path);
        bail!("download sing-box installer: {}", dl.output);
    }
    if !dl.output.is_empty() {
        output.push_str(&dl.output);
    }

    let meta = match std::fs::metadata(&path) {
        Ok(m) => m,
        Err(e) => {
            let _ = std::fs::remove_file(&path);
            return Err(e).context("read sing-box installer");
        }
    };
    if meta.len() == 0 {
        let _ = std::fs::remove_file(&path);
        bail!("downloaded sing-box installer is empty");
    }

    let mut cmd_args = vec!["sh".to_string(), path_str.clone()];
    if channel == CHANNEL_BETA {
        cmd_args.push("--beta".to_string());
    }
    if !version.is_empty() {
        cmd_args.push("--version".to_string());
        cmd_args.push(version.to_string());
    }
    let arg_refs: Vec<&str> = cmd_args.iter().map(|s| s.as_str()).collect();
    // run("sh", args) — first element of cmd_args is the binary name for display;
    // pass path and flags as args.
    let installed = run(arg_refs[0], &arg_refs[1..]).await;
    let _ = std::fs::remove_file(&path);
    let installed = installed.context("run sing-box installer")?;
    if !installed.output.is_empty() {
        if !output.is_empty() {
            output.push('\n');
        }
        output.push_str(&installed.output);
    }
    if !installed.success() {
        bail!("run sing-box installer: {}", installed.output);
    }
    Ok(output)
}

fn pkg_name(channel: &str) -> &'static str {
    if channel == CHANNEL_BETA {
        "sing-box-beta"
    } else {
        "sing-box"
    }
}

async fn install_apt(channel: &str) -> Result<String> {
    let script = format!(
        r#"set -e
mkdir -p /etc/apt/keyrings
curl -fsSL https://sing-box.app/gpg.key -o /etc/apt/keyrings/sagernet.asc
chmod a+r /etc/apt/keyrings/sagernet.asc
printf '%s\n' 'Types: deb' 'URIs: https://deb.sagernet.org/' 'Suites: *' 'Components: *' 'Enabled: yes' 'Signed-By: /etc/apt/keyrings/sagernet.asc' > /etc/apt/sources.list.d/sagernet.sources
apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install -y {}"#,
        pkg_name(channel)
    );
    let result = run("sh", &["-c", &script]).await?;
    if result.success() {
        Ok(result.output)
    } else {
        Err(anyhow!("{}", result.output))
    }
}

async fn install_dnf(channel: &str) -> Result<String> {
    let script = format!(
        r#"set -e
dnf config-manager addrepo --from-repofile=https://sing-box.app/sing-box.repo 2>/dev/null || dnf config-manager --add-repo https://sing-box.app/sing-box.repo
dnf install -y {}"#,
        pkg_name(channel)
    );
    let result = run("sh", &["-c", &script]).await?;
    if result.success() {
        Ok(result.output)
    } else {
        Err(anyhow!("{}", result.output))
    }
}

/// Reports whether the sing-box binary is present and its version string.
pub async fn detect_version() -> (bool, String) {
    let bin = singbox_binary();
    if !bin.is_file() {
        return (false, String::new());
    }
    let bin_str = bin.to_string_lossy();
    if let Ok(out) = run(bin_str.as_ref(), &["version", "-n"]).await {
        if out.success() && !out.output.is_empty() {
            return (true, first_line(&out.output));
        }
    }
    // Fallback for builds without `-n`: parse `sing-box version <X> ...`.
    if let Ok(out) = run(bin_str.as_ref(), &["version"]).await {
        if out.success() {
            let line = first_line(&out.output);
            let fields: Vec<&str> = line.split_whitespace().collect();
            if fields.len() >= 3 && fields[0] == "sing-box" && fields[1] == "version" {
                return (true, fields[2].to_string());
            }
            return (true, line);
        }
    }
    (true, String::new())
}

/// Returns the resolved binary path, or an error if absent.
pub fn detect_version_installed() -> Result<PathBuf> {
    let bin = singbox_binary();
    if !bin.is_file() {
        bail!("sing-box not installed at {SINGBOX_BINARY}");
    }
    Ok(bin)
}

pub fn first_line(s: &str) -> String {
    match s.find('\n') {
        Some(i) => s[..i].trim().to_string(),
        None => s.trim().to_string(),
    }
}

/// Re-export for callers that want the same helper as Go `singboxBinary`.
pub fn resolve_singbox_binary() -> PathBuf {
    singbox_binary()
}

#[allow(dead_code)]
fn path_is_file(p: &Path) -> bool {
    p.is_file()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_regex_accepts() {
        let re = version_regex();
        assert!(re.is_match("1.12.0"));
        assert!(re.is_match("1.12.0-beta.1"));
        assert!(!re.is_match("../etc/passwd"));
        assert!(!re.is_match(""));
    }

    #[test]
    fn first_line_trims() {
        assert_eq!(first_line("1.2.3\nextra"), "1.2.3");
    }
}
