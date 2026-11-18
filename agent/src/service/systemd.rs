//! systemd-backed [`crate::service::ServiceManager`] implementation.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, Context, Result};

use crate::paths::SERVICE_NAME;

use super::{run_ok, run_with_output};

/// D-Bus object path for the sing-box.service unit (`sing-box` → `sing_2dbox_2eservice`).
pub const SYSTEMD_UNIT_PATH: &str = "/org/freedesktop/systemd1/unit/sing_2dbox_2eservice";

#[derive(Debug, Default)]
pub struct SystemdServiceManager;

impl SystemdServiceManager {
    pub fn new() -> Self {
        Self
    }

    pub async fn prepare(&self) -> Result<String> {
        run_ok("systemctl", &["daemon-reload"]).await
    }

    pub async fn action(&self, action: &str) -> Result<String> {
        if action == "start" || action == "restart" {
            // Pick up a unit file rewritten by an install/upgrade.
            let _ = self.prepare().await;
        }
        match run_with_output("systemctl", &[action, SERVICE_NAME]).await {
            Ok(out) => Ok(out),
            Err((out, err)) => Err(anyhow!("{err}: {out}")),
        }
    }

    pub async fn set_enabled(&self, enabled: bool) -> Result<String> {
        let action = if enabled { "enable" } else { "disable" };
        match run_with_output("systemctl", &[action, SERVICE_NAME]).await {
            Ok(out) => Ok(out),
            Err((out, err)) => Err(anyhow!("{err}: {out}")),
        }
    }

    pub async fn active(&self) -> bool {
        match run_ok("systemctl", &["is-active", SERVICE_NAME]).await {
            Ok(out) => out.trim() == "active",
            Err(_) => false,
        }
    }

    pub async fn enabled(&self) -> bool {
        match run_ok("systemctl", &["is-enabled", SERVICE_NAME]).await {
            Ok(out) => {
                let s = out.trim();
                s == "enabled" || s == "enabled-runtime" || s == "alias"
            }
            Err(_) => false,
        }
    }

    pub async fn status(&self) -> String {
        match crate::exec::run("systemctl", &["status", SERVICE_NAME, "--no-pager", "-l"]).await {
            Ok(r) => r.output,
            Err(e) => e.to_string(),
        }
    }

    pub async fn main_pid(&self) -> Result<i32> {
        let out = match run_with_output(
            "systemctl",
            &["show", SERVICE_NAME, "--property=MainPID", "--value"],
        )
        .await
        {
            Ok(o) => o,
            Err((o, e)) => bail!("systemctl show MainPID: {e}: {o}"),
        };
        let trimmed = out.trim();
        trimmed
            .parse::<i32>()
            .with_context(|| format!("invalid MainPID {trimmed:?}"))
    }

    pub async fn started_at(&self, _pid: i32) -> Result<SystemTime> {
        let out = match run_with_output(
            "busctl",
            &[
                "get-property",
                "org.freedesktop.systemd1",
                SYSTEMD_UNIT_PATH,
                "org.freedesktop.systemd1.Service",
                "ExecMainStartTimestamp",
            ],
        )
        .await
        {
            Ok(o) => o,
            Err((o, e)) => bail!("busctl get ExecMainStartTimestamp: {e}: {o}"),
        };
        parse_systemd_timestamp(&out)
    }

    pub fn log_command(&self, lines: i32, follow: bool) -> (String, Vec<String>) {
        let mut args = vec![
            "-u".to_string(),
            SERVICE_NAME.to_string(),
            "-n".to_string(),
            lines.to_string(),
        ];
        if follow {
            args.push("-f".to_string());
        }
        args.push("--no-pager".to_string());
        ("journalctl".to_string(), args)
    }
}

/// Parses `busctl` output of the form `t <microseconds>`.
pub fn parse_systemd_timestamp(out: &str) -> Result<SystemTime> {
    let fields: Vec<&str> = out.split_whitespace().collect();
    if fields.len() != 2 || fields[0] != "t" {
        bail!("invalid ExecMainStartTimestamp {out:?}");
    }
    let micros: u64 = fields[1]
        .parse()
        .map_err(|_| anyhow!("invalid ExecMainStartTimestamp {out:?}"))?;
    if micros == 0 {
        bail!("invalid ExecMainStartTimestamp {out:?}");
    }
    // Reject values that would overflow Duration from micros.
    let max_micros = (i64::MAX as u128 / 1_000) as u64;
    if micros > max_micros {
        bail!("invalid ExecMainStartTimestamp {out:?}");
    }
    Ok(UNIX_EPOCH + Duration::from_micros(micros))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_busctl_timestamp() {
        let t = parse_systemd_timestamp("t 1787285641404504").unwrap();
        let micros = t.duration_since(UNIX_EPOCH).unwrap().as_micros();
        assert_eq!(micros, 1787285641404504);
    }

    #[test]
    fn rejects_bad_timestamp() {
        assert!(parse_systemd_timestamp("s 1").is_err());
        assert!(parse_systemd_timestamp("t 0").is_err());
    }
}
