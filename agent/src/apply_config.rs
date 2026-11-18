//! Atomic config apply with validation, restart evidence, and rollback
//! (Go `applyconfig.go`).

use std::fs::{self, File};
use std::io::{self, Read};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use anyhow::{anyhow, bail, Context, Result};
use sha2::{Digest, Sha256};
use tokio::time::Instant;

use crate::config_read::read_config_file;
use crate::exec::run;
use crate::install::detect_version_installed;
use crate::paths::{
    BAK_CONFIG_FILE, CONFIG_DIR, CONFIG_FILE, MAX_CONFIG_SIZE, ORIG_CONFIG_FILE, SINGBOX_BINARY,
    TMP_CONFIG_FILE,
};
use crate::service::{
    current_service_manager, process_args_load_managed_config, split_nul_terminated, ServiceManager,
};

#[derive(Debug, Clone, Copy)]
struct ServiceState {
    active: bool,
    enabled: bool,
}

#[derive(Debug, Clone)]
struct ServiceConfigEvidence {
    pid: i32,
    started_at: SystemTime,
}

#[derive(Debug)]
struct MovedFile {
    from: PathBuf,
    to: PathBuf,
}

#[derive(Debug, Default)]
struct StrayMove {
    backup_dir: Option<PathBuf>,
    files: Vec<MovedFile>,
}

impl StrayMove {
    fn restore(&self) -> Result<()> {
        let mut errs: Vec<anyhow::Error> = Vec::new();
        for f in self.files.iter().rev() {
            if let Err(e) = fs::rename(&f.to, &f.from) {
                errs.push(anyhow!(
                    "restore {}: {e}",
                    f.from.file_name().and_then(|s| s.to_str()).unwrap_or("?")
                ));
            }
        }
        if let Some(dir) = &self.backup_dir {
            if let Err(e) = fs::remove_dir(dir) {
                if e.kind() != io::ErrorKind::NotFound {
                    errs.push(anyhow!("remove rollback directory: {e}"));
                }
            }
        }
        if errs.is_empty() {
            Ok(())
        } else {
            Err(anyhow!(
                "{}",
                errs.iter().map(|e| e.to_string()).collect::<Vec<_>>().join("; ")
            ))
        }
    }
}

/// Validates panel-generated config, atomically installs it, and restarts the
/// service. On failure rolls back config.json and every JSON file moved aside.
pub async fn apply_config(config_bytes: &[u8]) -> Result<String> {
    let config_bytes = prepare_config_bytes(config_bytes)?;
    let manager = current_service_manager().await?;
    let bin = detect_version_installed().map_err(|_| {
        anyhow!("sing-box not installed at {SINGBOX_BINARY}")
    })?;
    fs::create_dir_all(CONFIG_DIR).with_context(|| format!("mkdir {CONFIG_DIR}"))?;
    secure_known_config_files().context("secure existing config files")?;
    preserve_original().context("preserve original config")?;

    let expected_hash = Sha256::digest(&config_bytes);
    let expected_hash: [u8; 32] = expected_hash.into();

    // Skip no-op restart when byte-identical config is already proven loaded.
    if let Ok(existing) = read_config_file() {
        if existing == config_bytes && manager.active().await {
            if current_service_config_evidence(&manager, &expected_hash)
                .await
                .is_ok()
            {
                return Ok("config unchanged; running sing-box verified".into());
            }
        }
    }

    // 1) write temp
    fs::write(TMP_CONFIG_FILE, &config_bytes).context("write temp config")?;
    let _ = fs::set_permissions(TMP_CONFIG_FILE, fs::Permissions::from_mode(0o600));

    // 2) validate
    let check = run(
        bin.to_str().unwrap_or(SINGBOX_BINARY),
        &["check", "-c", TMP_CONFIG_FILE],
    )
    .await
    .context("sing-box check")?;
    if !check.success() {
        let _ = fs::remove_file(TMP_CONFIG_FILE);
        if let Some(detail) = check_failure_detail(&check.output) {
            // Match Go: return check output alongside a concise error.
            return Err(anyhow!("sing-box check failed: {detail}")
                .context(format!("sing-box output: {}", check.output)));
        }
        bail!("sing-box check failed: {}", check.output);
    }

    let previous_state = ServiceState {
        active: manager.active().await,
        enabled: manager.enabled().await,
    };

    // 3) move stray JSON
    let moved = match move_stray_configs_at(CONFIG_DIR) {
        Ok(m) => m,
        Err(e) => {
            let _ = fs::remove_file(TMP_CONFIG_FILE);
            return Err(e).context("clean config dir");
        }
    };

    // 4) backup + atomic rename
    let mut had_old = false;
    match fs::metadata(CONFIG_FILE) {
        Ok(_) => {
            if let Err(e) = copy_file(CONFIG_FILE, BAK_CONFIG_FILE) {
                let _ = fs::remove_file(TMP_CONFIG_FILE);
                if let Err(re) = moved.restore() {
                    return Err(anyhow!("backup config: {e}; restore moved configs: {re}"));
                }
                return Err(e).context("backup config");
            }
            had_old = true;
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => {
            let _ = fs::remove_file(TMP_CONFIG_FILE);
            if let Err(re) = moved.restore() {
                return Err(anyhow!(
                    "inspect current config: {e}; restore moved configs: {re}"
                ));
            }
            return Err(e).context("inspect current config");
        }
    }

    if let Err(e) = fs::rename(TMP_CONFIG_FILE, CONFIG_FILE) {
        let _ = fs::remove_file(TMP_CONFIG_FILE);
        if let Err(re) = moved.restore() {
            return Err(anyhow!("install config: {e}; restore moved configs: {re}"));
        }
        return Err(e).context("install config");
    }
    if let Err(e) = fs::set_permissions(CONFIG_FILE, fs::Permissions::from_mode(0o600)) {
        if let Err(re) = rollback_apply(&manager, had_old, &moved, previous_state).await {
            return Err(anyhow!("set config permissions: {e}; rollback failed: {re}"));
        }
        return Err(e).context("set config permissions");
    }

    // 5) prepare + restart/start
    let mut apply_err: Option<anyhow::Error> = None;
    if let Err(e) = manager.prepare().await {
        apply_err = Some(anyhow!(
            "prepare {} service: {e}",
            manager.kind()
        ));
    }
    if apply_err.is_none() {
        if let Err(e) = manager.wait_for_config_start().await {
            apply_err = Some(anyhow!("wait for process clock boundary: {e}"));
        }
    }
    if apply_err.is_none() {
        let action = config_apply_service_action(previous_state);
        if let Err(e) = manager.action(action).await {
            apply_err = Some(anyhow!("{} {action}: {e}", manager.kind()));
        }
    }

    // 6) evidence
    if apply_err.is_none() {
        if let Err(e) = wait_service_stable(&manager, Duration::from_secs(3), &expected_hash).await
        {
            apply_err = Some(e);
        }
    }
    if let Some(err) = apply_err {
        if let Err(re) = rollback_apply(&manager, had_old, &moved, previous_state).await {
            return Err(anyhow!("{err}; rollback failed: {re}"));
        }
        return Err(err);
    }
    Ok("config applied and running sing-box verified".into())
}

/// Formats config bytes the same way for hashing, check, and install.
pub fn prepare_config_bytes(raw: &[u8]) -> Result<Vec<u8>> {
    if raw.is_empty() {
        bail!("config is empty");
    }
    if raw.len() as u64 > MAX_CONFIG_SIZE {
        bail!("config exceeds {MAX_CONFIG_SIZE}-byte limit");
    }
    let formatted = indent_config_json(raw);
    if formatted.len() as u64 > MAX_CONFIG_SIZE {
        bail!(
            "indented config exceeds {MAX_CONFIG_SIZE}-byte limit (input {} bytes, formatted {} bytes)",
            raw.len(),
            formatted.len()
        );
    }
    Ok(formatted)
}

/// Lexical JSON indent that preserves key order and number/string literals
/// (matches Go `json.Indent`). Ill-formed input is returned unchanged.
pub fn indent_config_json(raw: &[u8]) -> Vec<u8> {
    let trimmed = trim_right_ws(raw);
    // Reject non-JSON before the lexical pretty-printer, which would otherwise
    // treat a bare token like `not-json` as a literal value.
    if serde_json::from_slice::<serde_json::Value>(trimmed).is_err() {
        return raw.to_vec();
    }
    match pretty_indent_json(trimmed) {
        Ok(mut buf) => {
            buf.push(b'\n');
            buf
        }
        Err(()) => raw.to_vec(),
    }
}

fn trim_right_ws(raw: &[u8]) -> &[u8] {
    let mut end = raw.len();
    while end > 0 {
        match raw[end - 1] {
            b' ' | b'\t' | b'\r' | b'\n' => end -= 1,
            _ => break,
        }
    }
    &raw[..end]
}

fn pretty_indent_json(input: &[u8]) -> Result<Vec<u8>, ()> {
    let mut out = Vec::with_capacity(input.len() + input.len() / 8 + 16);
    let mut i = 0;
    let mut depth: usize = 0;
    let mut begin_line = true;

    while i < input.len() {
        let c = input[i];
        match c {
            b' ' | b'\t' | b'\n' | b'\r' => {
                i += 1;
                continue;
            }
            b'"' => {
                if begin_line {
                    write_indent(&mut out, depth);
                    begin_line = false;
                }
                let start = i;
                i += 1;
                while i < input.len() {
                    match input[i] {
                        b'\\' => {
                            i += 1;
                            if i < input.len() {
                                i += 1;
                            }
                        }
                        b'"' => {
                            i += 1;
                            break;
                        }
                        _ => i += 1,
                    }
                }
                out.extend_from_slice(&input[start..i]);
            }
            b'{' | b'[' => {
                if begin_line {
                    write_indent(&mut out, depth);
                }
                out.push(c);
                i += 1;
                // peek next significant char for empty container
                let mut j = i;
                while j < input.len() && matches!(input[j], b' ' | b'\t' | b'\n' | b'\r') {
                    j += 1;
                }
                if j < input.len() && ((c == b'{' && input[j] == b'}') || (c == b'[' && input[j] == b']'))
                {
                    out.push(input[j]);
                    i = j + 1;
                    begin_line = false;
                } else {
                    depth += 1;
                    out.push(b'\n');
                    begin_line = true;
                }
            }
            b'}' | b']' => {
                if depth == 0 {
                    return Err(());
                }
                depth -= 1;
                if !begin_line {
                    out.push(b'\n');
                }
                write_indent(&mut out, depth);
                out.push(c);
                i += 1;
                begin_line = false;
            }
            b':' => {
                out.push(b':');
                out.push(b' ');
                i += 1;
                begin_line = false;
            }
            b',' => {
                out.push(b',');
                out.push(b'\n');
                i += 1;
                begin_line = true;
            }
            _ => {
                // number, true/false/null, or other literal — copy until delimiter
                if begin_line {
                    write_indent(&mut out, depth);
                    begin_line = false;
                }
                let start = i;
                while i < input.len() {
                    match input[i] {
                        b' ' | b'\t' | b'\n' | b'\r' | b',' | b']' | b'}' | b'{' | b'[' | b':'
                        | b'"' => break,
                        _ => i += 1,
                    }
                }
                if i == start {
                    return Err(());
                }
                out.extend_from_slice(&input[start..i]);
            }
        }
    }
    if depth != 0 {
        return Err(());
    }
    Ok(out)
}

fn write_indent(out: &mut Vec<u8>, depth: usize) {
    for _ in 0..depth {
        out.extend_from_slice(b"  ");
    }
}

fn config_apply_service_action(previous: ServiceState) -> &'static str {
    if previous.active {
        "restart"
    } else {
        "start"
    }
}

async fn wait_service_stable(
    manager: &ServiceManager,
    stable_for: Duration,
    expected_hash: &[u8; 32],
) -> Result<()> {
    let deadline = Instant::now() + stable_for;
    let mut applied: Option<ServiceConfigEvidence> = None;
    let mut ticker = tokio::time::interval(Duration::from_millis(250));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        match current_service_config_evidence(manager, expected_hash).await {
            Ok(current) => {
                if let Some(ref a) = applied {
                    if current.pid != a.pid || current.started_at != a.started_at {
                        bail!(
                            "sing-box process changed while verifying config apply (pid {} -> {})",
                            a.pid,
                            current.pid
                        );
                    }
                } else {
                    applied = Some(current);
                }
            }
            Err(_) => {}
        }

        if Instant::now() >= deadline {
            let final_ev = current_service_config_evidence(manager, expected_hash).await?;
            let Some(ref a) = applied else {
                bail!("sing-box service never became stable during config verification");
            };
            if final_ev.pid != a.pid || final_ev.started_at != a.started_at {
                bail!(
                    "sing-box process changed at the end of config verification (pid {} -> {})",
                    a.pid,
                    final_ev.pid
                );
            }
            return Ok(());
        }
        ticker.tick().await;
    }
}

async fn current_service_config_evidence(
    manager: &ServiceManager,
    expected_hash: &[u8; 32],
) -> Result<ServiceConfigEvidence> {
    if !manager.active().await {
        bail!("sing-box service is not active after apply");
    }
    let pid = manager
        .main_pid()
        .await
        .context("read sing-box main pid after apply")?;
    if pid <= 0 {
        bail!("sing-box service is active but has no main process");
    }
    let started_at = manager
        .started_at(pid)
        .await
        .context("read sing-box start time after apply")?;
    verify_managed_config_file(CONFIG_FILE, expected_hash, started_at)?;
    verify_process_loads_managed_config(pid)?;
    Ok(ServiceConfigEvidence { pid, started_at })
}

fn verify_managed_config_file(
    path: &str,
    expected_hash: &[u8; 32],
    service_started_at: SystemTime,
) -> Result<()> {
    let mut f = File::open(path).context("open applied config")?;
    let before_info = f.metadata().context("stat opened config before hashing")?;
    let mut hasher = Sha256::new();
    let mut limited = f.by_ref().take(MAX_CONFIG_SIZE + 1);
    let n = io::copy(&mut limited, &mut hasher).context("hash applied config")?;
    if n > MAX_CONFIG_SIZE {
        bail!("applied config exceeds {MAX_CONFIG_SIZE}-byte limit");
    }
    let actual: [u8; 32] = hasher.finalize().into();
    if &actual != expected_hash {
        bail!(
            "applied config hash mismatch: got {}, want {}",
            hex::encode(actual),
            hex::encode(expected_hash)
        );
    }
    let opened_info = f.metadata().context("stat opened config")?;
    if before_info.len() != n
        || opened_info.len() != n
        || before_info.modified().ok() != opened_info.modified().ok()
    {
        bail!("applied config changed while it was being hashed");
    }
    let path_info = fs::metadata(path).context("stat applied config")?;
    if !same_file(&opened_info, &path_info)
        || path_info.len() != n
        || path_info.modified().ok() != opened_info.modified().ok()
    {
        bail!("applied config changed while it was being verified");
    }
    let mtime = path_info.modified().context("config mtime")?;
    if service_started_at <= mtime {
        bail!(
            "sing-box started at {}, not after config update at {}",
            format_time(service_started_at),
            format_time(mtime)
        );
    }
    Ok(())
}

fn format_time(t: SystemTime) -> String {
    match t.duration_since(SystemTime::UNIX_EPOCH) {
        Ok(d) => {
            let secs = d.as_secs() as i64;
            let nanos = d.subsec_nanos();
            chrono::DateTime::from_timestamp(secs, nanos)
                .map(|dt| dt.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true))
                .unwrap_or_else(|| format!("{secs}.{nanos}"))
        }
        Err(_) => "unknown".into(),
    }
}

fn same_file(a: &fs::Metadata, b: &fs::Metadata) -> bool {
    a.dev() == b.dev() && a.ino() == b.ino()
}

fn verify_process_loads_managed_config(pid: i32) -> Result<()> {
    let raw = fs::read(format!("/proc/{pid}/cmdline")).context("read sing-box process arguments")?;
    let args = split_nul_terminated(&raw);
    if process_args_load_managed_config(&args) {
        return Ok(());
    }
    bail!("sing-box process does not load {CONFIG_FILE} or {CONFIG_DIR}");
}

async fn rollback_apply(
    manager: &ServiceManager,
    had_old: bool,
    moved: &StrayMove,
    previous: ServiceState,
) -> Result<()> {
    let mut errs: Vec<String> = Vec::new();
    if had_old {
        if let Err(e) = copy_file(BAK_CONFIG_FILE, CONFIG_FILE) {
            errs.push(format!("restore config.json: {e}"));
        }
    } else if let Err(e) = fs::remove_file(CONFIG_FILE) {
        if e.kind() != io::ErrorKind::NotFound {
            errs.push(format!("remove new config.json: {e}"));
        }
    }
    if let Err(e) = moved.restore() {
        errs.push(e.to_string());
    }

    let rollback = async {
        if let Err(e) = manager.set_enabled(previous.enabled).await {
            errs.push(format!("restore service enablement: {e}"));
        }
        let action = if previous.active { "restart" } else { "stop" };
        if let Err(e) = manager.action(action).await {
            errs.push(format!("restore service state with {action}: {e}"));
        }
    };
    match tokio::time::timeout(Duration::from_secs(30), rollback).await {
        Ok(()) => {}
        Err(_) => errs.push("rollback service timed out".into()),
    }

    if errs.is_empty() {
        Ok(())
    } else {
        Err(anyhow!("{}", errs.join("; ")))
    }
}

fn move_stray_configs_at(config_dir: &str) -> Result<StrayMove> {
    let entries = fs::read_dir(config_dir)?;
    let mut moved = StrayMove::default();
    for entry in entries.flatten() {
        let ft = entry.file_type()?;
        if ft.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if !name.ends_with(".json") || name == "config.json" {
            continue;
        }
        if moved.backup_dir.is_none() {
            let disabled = Path::new(config_dir).join("disabled");
            fs::create_dir_all(&disabled)?;
            let dir = tempfile::Builder::new()
                .prefix("apply-")
                .tempdir_in(&disabled)?;
            // Persist the directory beyond TempDir Drop by keeping the path.
            let path = dir.keep();
            moved.backup_dir = Some(path);
        }
        let backup_dir = moved.backup_dir.as_ref().unwrap();
        let from = Path::new(config_dir).join(name.as_ref());
        let to = backup_dir.join(name.as_ref());
        if let Err(e) = fs::rename(&from, &to) {
            let _ = moved.restore();
            bail!("move {name} into disabled directory: {e}");
        }
        moved.files.push(MovedFile {
            from: from.clone(),
            to: to.clone(),
        });
        if let Err(e) = fs::set_permissions(&to, fs::Permissions::from_mode(0o600)) {
            let _ = moved.restore();
            bail!("secure moved config {name}: {e}");
        }
    }
    Ok(moved)
}

fn copy_file(src: &str, dst: &str) -> Result<()> {
    let mut src_file = File::open(src)?;
    let dir = Path::new(dst).parent().unwrap_or_else(|| Path::new("."));
    let mut tmp = tempfile::Builder::new()
        .prefix(".singbox-panel-copy-")
        .tempfile_in(dir)?;
    io::copy(&mut src_file, tmp.as_file_mut())?;
    tmp.as_file()
        .set_permissions(fs::Permissions::from_mode(0o600))?;
    tmp.as_file().sync_all()?;
    let tmp_path = tmp.into_temp_path();
    fs::rename(&tmp_path, dst)?;
    let _ = tmp_path.keep();
    Ok(())
}

fn secure_known_config_files() -> Result<()> {
    for path in [CONFIG_FILE, TMP_CONFIG_FILE, BAK_CONFIG_FILE, ORIG_CONFIG_FILE] {
        if let Err(e) = fs::set_permissions(path, fs::Permissions::from_mode(0o600)) {
            if e.kind() != io::ErrorKind::NotFound {
                bail!("chmod {path}: {e}");
            }
        }
    }
    Ok(())
}

fn preserve_original() -> Result<()> {
    match fs::metadata(ORIG_CONFIG_FILE) {
        Ok(_) => return Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    match fs::metadata(CONFIG_FILE) {
        Ok(_) => copy_file_once(CONFIG_FILE, ORIG_CONFIG_FILE),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}

fn copy_file_once(src: &str, dst: &str) -> Result<()> {
    match fs::metadata(dst) {
        Ok(_) => return Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => {}
        Err(e) => return Err(e.into()),
    }
    let tmp = format!("{dst}.new");
    let _ = fs::remove_file(&tmp);
    copy_file(src, &tmp)?;
    let link_result = fs::hard_link(&tmp, dst);
    let _ = fs::remove_file(&tmp);
    match link_result {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => Ok(()),
        Err(e) => Err(e.into()),
    }
}

fn check_failure_detail(out: &str) -> Option<String> {
    let out = out.trim();
    if out.is_empty() {
        return None;
    }
    let mut msg = String::new();
    for line in out.lines().rev() {
        let s = line.trim();
        if !s.is_empty() {
            msg = s.to_string();
            break;
        }
    }
    if let Some(i) = msg.find("] ") {
        if i > 0 && i < 16 {
            msg = msg[i + 2..].trim().to_string();
        }
    }
    let chars: Vec<char> = msg.chars().collect();
    if chars.len() > 300 {
        msg = chars[..300].iter().collect::<String>() + "…";
    }
    Some(msg)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn indent_preserves_key_order_and_is_idempotent() {
        let raw = br#"{"z":1,"a":2}"#;
        let once = indent_config_json(raw);
        let twice = indent_config_json(&once);
        assert_eq!(once, twice);
        let s = String::from_utf8_lossy(&once);
        assert!(s.find("\"z\"").unwrap() < s.find("\"a\"").unwrap());
        assert!(s.ends_with('\n'));
    }

    #[test]
    fn indent_leaves_invalid_unchanged() {
        let raw = b"not-json";
        assert_eq!(indent_config_json(raw), raw);
    }

    #[test]
    fn check_detail_strips_fatal_prefix() {
        let d = check_failure_detail("FATAL[0000] missing certificate").unwrap();
        assert_eq!(d, "missing certificate");
    }
}
