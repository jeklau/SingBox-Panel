//! OpenRC-backed [`crate::service::ServiceManager`] implementation.

use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use anyhow::{anyhow, bail, Context, Result};
use tokio::time::sleep;

use crate::atomic_file::write_atomic_file;
use crate::paths::{singbox_binary, CONFIG_DIR, CONFIG_FILE, SERVICE_NAME};

use super::{run_ok, run_with_output};

pub const OPENRC_SINGBOX_INIT: &str = "/etc/init.d/sing-box";
pub const OPENRC_SINGBOX_CONF: &str = "/etc/conf.d/sing-box";
pub const OPENRC_SINGBOX_LOG: &str = "/var/log/sing-box.log";
pub const OPENRC_MANAGED_MARKER: &str = "Managed by SingBox Panel";

const PROCESS_CLOCK_TICK: Duration = Duration::from_millis(10);

#[derive(Debug, Clone)]
pub struct ProcessStartClock {
    pub boot_lower: SystemTime,
    pub uncertainty: Duration,
}

#[derive(Debug)]
pub struct OpenRcServiceManager {
    runlevel_root: PathBuf,
    clock: ProcessStartClock,
}

impl OpenRcServiceManager {
    pub fn new(clock: ProcessStartClock) -> Self {
        Self {
            runlevel_root: PathBuf::from("/etc/runlevels/default"),
            clock,
        }
    }

    #[cfg(test)]
    pub fn with_runlevel_root(mut self, root: impl Into<PathBuf>) -> Self {
        self.runlevel_root = root.into();
        self
    }

    pub async fn prepare(&self) -> Result<String> {
        let bin = singbox_binary();
        ensure_openrc_singbox_service(bin.to_str().unwrap_or("/usr/bin/sing-box"))?;
        ensure_openrc_logging()?;
        Ok(String::new())
    }

    pub async fn action(&self, action: &str) -> Result<String> {
        if action == "enable" || action == "disable" {
            return self.set_enabled(action == "enable").await;
        }
        if action == "start" || action == "restart" {
            if let Err(e) = self.prepare().await {
                return Err(e);
            }
        }
        match run_with_output("rc-service", &[SERVICE_NAME, action]).await {
            Ok(out) => Ok(out),
            Err((out, err)) => Err(anyhow!("{err}: {out}")),
        }
    }

    pub async fn set_enabled(&self, enabled: bool) -> Result<String> {
        let already = self.runlevel_path().exists();
        if enabled == already {
            return Ok(String::new());
        }
        let action = if enabled { "add" } else { "del" };
        match run_with_output("rc-update", &[action, SERVICE_NAME, "default"]).await {
            Ok(out) => Ok(out),
            Err((out, err)) => Err(anyhow!("{err}: {out}")),
        }
    }

    pub async fn active(&self) -> bool {
        run_ok("rc-service", &[SERVICE_NAME, "status"]).await.is_ok()
    }

    pub async fn enabled(&self) -> bool {
        self.runlevel_path().exists()
    }

    fn runlevel_path(&self) -> PathBuf {
        self.runlevel_root.join(SERVICE_NAME)
    }

    pub async fn status(&self) -> String {
        match crate::exec::run("rc-service", &[SERVICE_NAME, "status"]).await {
            Ok(r) => r.output,
            Err(e) => e.to_string(),
        }
    }

    pub async fn main_pid(&self) -> Result<i32> {
        find_managed_singbox_pid_at("/proc")
    }

    pub async fn started_at(&self, pid: i32) -> Result<SystemTime> {
        read_process_start_time_at("/proc", pid, self.clock.boot_lower)
    }

    pub async fn wait_for_config_start(&self) -> Result<()> {
        // Go rejects a zero bootLower or non-positive uncertainty.
        if self.clock.uncertainty.is_zero() {
            bail!("OpenRC process clock is unavailable");
        }
        sleep(self.clock.uncertainty).await;
        Ok(())
    }

    pub fn log_command(&self, lines: i32, follow: bool) -> (String, Vec<String>) {
        let mut args = vec!["-n".to_string(), lines.to_string()];
        if follow {
            args.push("-f".to_string());
        }
        args.push(openrc_log_path());
        ("tail".to_string(), args)
    }
}

pub fn openrc_log_path() -> String {
    match std::fs::read_to_string(OPENRC_SINGBOX_CONF) {
        Ok(raw) => openrc_log_path_from_config(&raw),
        Err(_) => OPENRC_SINGBOX_LOG.to_string(),
    }
}

pub fn openrc_log_path_from_config(raw: &str) -> String {
    for key in ["output_log", "error_log"] {
        if let Some(value) = openrc_log_setting(raw, key) {
            if Path::new(&value).is_absolute() {
                return value;
            }
        }
    }
    OPENRC_SINGBOX_LOG.to_string()
}

pub fn ensure_openrc_singbox_service(binary: &str) -> Result<()> {
    match std::fs::metadata(OPENRC_SINGBOX_INIT) {
        Ok(_) => return Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e).context("inspect OpenRC sing-box service"),
    }
    if let Some(parent) = Path::new(OPENRC_SINGBOX_INIT).parent() {
        std::fs::create_dir_all(parent).context("create OpenRC service directory")?;
    }
    let script = openrc_singbox_service_script(binary);
    write_atomic_file(OPENRC_SINGBOX_INIT, script.as_bytes(), 0o755)
        .context("write OpenRC sing-box service")?;
    Ok(())
}

pub fn openrc_singbox_service_script(binary: &str) -> String {
    format!(
        r#"#!/sbin/openrc-run
# {marker}. This service is removed only when it still carries this marker.
description="sing-box service"
supervisor="supervise-daemon"
command={binary:?}
command_args="run --disable-color -C /etc/sing-box"
respawn_delay=5
respawn_max=0

depend() {{
	after net dns
}}

start_pre() {{
	"$command" check -C /etc/sing-box
}}
"#,
        marker = OPENRC_MANAGED_MARKER,
        binary = binary,
    )
}

pub fn ensure_openrc_logging() -> Result<()> {
    let raw = match std::fs::read(OPENRC_SINGBOX_CONF) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(e) => return Err(e).context("read OpenRC sing-box configuration"),
    };
    let updated = append_openrc_logging(&raw);
    if updated == raw {
        return Ok(());
    }
    if let Some(parent) = Path::new(OPENRC_SINGBOX_CONF).parent() {
        std::fs::create_dir_all(parent).context("create OpenRC configuration directory")?;
    }
    let mode = match std::fs::metadata(OPENRC_SINGBOX_CONF) {
        Ok(m) => {
            use std::os::unix::fs::PermissionsExt;
            m.permissions().mode() & 0o777
        }
        Err(_) => 0o644,
    };
    write_atomic_file(OPENRC_SINGBOX_CONF, &updated, mode)
        .context("configure OpenRC sing-box logs")?;
    Ok(())
}

fn append_openrc_logging(raw: &[u8]) -> Vec<u8> {
    let data = String::from_utf8_lossy(raw);
    let missing_output = !openrc_log_setting_present(&data, "output_log");
    let missing_error = !openrc_log_setting_present(&data, "error_log");
    if !missing_output && !missing_error {
        return raw.to_vec();
    }
    let mut b = String::new();
    b.push_str(&data);
    if !data.is_empty() && !data.ends_with('\n') {
        b.push('\n');
    }
    b.push_str("# BEGIN ");
    b.push_str(OPENRC_MANAGED_MARKER);
    b.push_str(" log forwarding\n");
    if missing_output {
        b.push_str("output_log=");
        b.push_str(OPENRC_SINGBOX_LOG);
        b.push('\n');
    }
    if missing_error {
        b.push_str("error_log=");
        b.push_str(OPENRC_SINGBOX_LOG);
        b.push('\n');
    }
    b.push_str("# END ");
    b.push_str(OPENRC_MANAGED_MARKER);
    b.push_str(" log forwarding\n");
    b.into_bytes()
}

fn openrc_log_setting_present(raw: &str, key: &str) -> bool {
    openrc_log_setting(raw, key).is_some()
}

fn openrc_log_setting(raw: &str, key: &str) -> Option<String> {
    for line in raw.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let line = line.strip_prefix("export ").unwrap_or(line);
        if !line.starts_with(key) {
            continue;
        }
        let remainder = line[key.len()..].trim_start();
        let Some(remainder) = remainder.strip_prefix('=') else {
            continue;
        };
        let mut value = remainder.trim();
        if value.len() >= 2
            && ((value.starts_with('"') && value.ends_with('"'))
                || (value.starts_with('\'') && value.ends_with('\'')))
        {
            value = &value[1..value.len() - 1];
        }
        return Some(value.to_string());
    }
    None
}

pub fn find_managed_singbox_pid_at(proc_root: &str) -> Result<i32> {
    let entries = std::fs::read_dir(proc_root).context("scan processes for OpenRC sing-box")?;
    let mut matches = Vec::new();
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let Ok(pid) = name.parse::<i32>() else {
            continue;
        };
        if pid <= 0 {
            continue;
        }
        let cmdline_path = Path::new(proc_root).join(name.as_ref()).join("cmdline");
        let raw = match std::fs::read(&cmdline_path) {
            Ok(b) if !b.is_empty() => b,
            _ => continue,
        };
        let args = split_nul_terminated(&raw);
        if args.is_empty() {
            continue;
        }
        let base = Path::new(&args[0])
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("");
        if base != "sing-box" || !process_args_load_managed_config(&args) {
            continue;
        }
        if process_is_zombie_at(proc_root, pid) {
            continue;
        }
        matches.push(pid);
    }
    match matches.len() {
        0 => bail!("OpenRC reports sing-box active but no managed sing-box process was found"),
        1 => Ok(matches[0]),
        _ => bail!("multiple managed sing-box processes found: {matches:?}"),
    }
}

fn process_is_zombie_at(proc_root: &str, pid: i32) -> bool {
    let raw = match std::fs::read_to_string(Path::new(proc_root).join(pid.to_string()).join("stat"))
    {
        Ok(s) => s,
        Err(_) => return false,
    };
    matches!(parse_process_state(&raw), Ok(s) if s == "Z")
}

pub fn split_nul_terminated(raw: &[u8]) -> Vec<String> {
    String::from_utf8_lossy(raw)
        .split('\0')
        .filter(|p| !p.is_empty())
        .map(|s| s.to_string())
        .collect()
}

/// Whether process argv loads the managed config file or directory.
pub fn process_args_load_managed_config(args: &[String]) -> bool {
    for (i, arg) in args.iter().enumerate() {
        match arg.as_str() {
            "-c" | "--config" => {
                if let Some(next) = args.get(i + 1) {
                    if canonicalize_clean(next) == CONFIG_FILE {
                        return true;
                    }
                }
            }
            "-C" | "--config-directory" => {
                if let Some(next) = args.get(i + 1) {
                    if canonicalize_clean(next) == CONFIG_DIR {
                        return true;
                    }
                }
            }
            _ => {}
        }
        for prefix in ["-c=", "--config="] {
            if let Some(rest) = arg.strip_prefix(prefix) {
                if canonicalize_clean(rest) == CONFIG_FILE {
                    return true;
                }
            }
        }
        for prefix in ["-C=", "--config-directory="] {
            if let Some(rest) = arg.strip_prefix(prefix) {
                if canonicalize_clean(rest) == CONFIG_DIR {
                    return true;
                }
            }
        }
    }
    false
}

fn canonicalize_clean(path: &str) -> String {
    // filepath.Clean equivalent without requiring the path to exist.
    let p = Path::new(path);
    let mut out = PathBuf::new();
    for c in p.components() {
        match c {
            std::path::Component::ParentDir => {
                out.pop();
            }
            std::path::Component::CurDir => {}
            other => out.push(other.as_os_str()),
        }
    }
    if out.as_os_str().is_empty() {
        ".".to_string()
    } else {
        out.to_string_lossy().into_owned()
    }
}

pub fn read_process_clock_at(
    proc_root: &str,
    mut now: impl FnMut() -> SystemTime,
) -> Result<ProcessStartClock> {
    let before = now();
    let raw = std::fs::read_to_string(Path::new(proc_root).join("uptime"))
        .context("read system uptime")?;
    let after = now();
    let fields: Vec<&str> = raw.split_whitespace().collect();
    if fields.is_empty() {
        bail!("system uptime is missing");
    }
    let uptime_secs: f64 = fields[0]
        .parse()
        .map_err(|_| anyhow!("invalid system uptime clock sample"))?;
    if uptime_secs < 0.0 || after < before {
        bail!("invalid system uptime clock sample");
    }
    let uptime = Duration::from_secs_f64(uptime_secs);
    let boot_lower = before
        .checked_sub(uptime + PROCESS_CLOCK_TICK)
        .ok_or_else(|| anyhow!("invalid system uptime clock sample"))?;
    let uncertainty = after
        .duration_since(before)
        .unwrap_or_default()
        + PROCESS_CLOCK_TICK
        + PROCESS_CLOCK_TICK;
    Ok(ProcessStartClock {
        boot_lower,
        uncertainty,
    })
}

pub fn read_process_start_time_at(
    proc_root: &str,
    pid: i32,
    boot_lower: SystemTime,
) -> Result<SystemTime> {
    let stat = std::fs::read_to_string(Path::new(proc_root).join(pid.to_string()).join("stat"))
        .context("read process stat")?;
    let ticks = parse_process_start_ticks(&stat)?;
    // Linux USER_HZ is fixed at 100 → 10ms ticks.
    let tick_nanos = PROCESS_CLOCK_TICK.as_nanos();
    let total_nanos = (ticks as u128)
        .checked_mul(tick_nanos)
        .ok_or_else(|| anyhow!("process start time exceeds duration range"))?;
    if total_nanos > i64::MAX as u128 {
        bail!("process start time exceeds duration range");
    }
    Ok(boot_lower + Duration::from_nanos(total_nanos as u64))
}

fn parse_process_start_ticks(raw: &str) -> Result<u64> {
    let close = raw
        .rfind(')')
        .ok_or_else(|| anyhow!("invalid process stat format"))?;
    if close + 1 >= raw.len() {
        bail!("invalid process stat format");
    }
    let fields: Vec<&str> = raw[close + 1..].split_whitespace().collect();
    // fields[0] is field 3 (state); start time is field 22 → index 19 after ')'.
    if fields.len() <= 19 {
        bail!("process stat is missing start time");
    }
    fields[19]
        .parse()
        .with_context(|| format!("invalid process start time {:?}", fields[19]))
}

fn parse_process_state(raw: &str) -> Result<String> {
    let close = raw
        .rfind(')')
        .ok_or_else(|| anyhow!("invalid process stat format"))?;
    let fields: Vec<&str> = raw[close + 1..].split_whitespace().collect();
    if fields.is_empty() {
        bail!("process stat is missing state");
    }
    Ok(fields[0].to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_script_contains_marker() {
        let s = openrc_singbox_service_script("/usr/bin/sing-box");
        assert!(s.contains(OPENRC_MANAGED_MARKER));
        assert!(s.contains("/usr/bin/sing-box"));
    }

    #[test]
    fn log_path_from_config() {
        let raw = "output_log=\"/tmp/custom.log\"\n";
        assert_eq!(openrc_log_path_from_config(raw), "/tmp/custom.log");
    }

    #[test]
    fn append_logging_idempotent() {
        let once = append_openrc_logging(b"");
        let twice = append_openrc_logging(&once);
        assert_eq!(once, twice);
        assert!(String::from_utf8_lossy(&once).contains(OPENRC_MANAGED_MARKER));
    }

    #[test]
    fn process_args_detect_config_dir() {
        let args = vec![
            "sing-box".into(),
            "run".into(),
            "-C".into(),
            "/etc/sing-box".into(),
        ];
        assert!(process_args_load_managed_config(&args));
    }
}
