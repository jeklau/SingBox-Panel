//! Atomic file write: temp + chmod + rename in the same directory.

use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;

use anyhow::{Context, Result};

/// Prevents a partial configuration, service definition or cleanup script from
/// being observed if the disk fills or the Agent is interrupted while preparing it.
///
/// Mirrors Go `writeAtomicFile`: create temp in the same dir, chmod, write, close, rename.
pub fn write_atomic_file(path: impl AsRef<Path>, data: &[u8], mode: u32) -> Result<()> {
    let path = path.as_ref();
    let dir = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));

    let mut tmp = tempfile::Builder::new()
        .prefix(".singbox-panel-atomic-")
        .tempfile_in(dir)
        .with_context(|| format!("create temp in {}", dir.display()))?;

    tmp.as_file()
        .set_permissions(std::fs::Permissions::from_mode(mode))
        .context("chmod temp")?;

    tmp.write_all(data).context("write temp")?;
    tmp.flush().context("flush temp")?;

    let tmp_path = tmp.into_temp_path();
    // into_temp_path keeps the file; rename then persist. On failure Drop removes it.
    match std::fs::rename(&tmp_path, path) {
        Ok(()) => {
            tmp_path.keep().ok();
            Ok(())
        }
        Err(e) => Err(e).with_context(|| {
            format!("rename {} -> {}", tmp_path.display(), path.display())
        }),
    }
}

/// Async wrapper around [`write_atomic_file`] for tokio callers.
pub async fn write_atomic_file_async(
    path: impl AsRef<Path> + Send + 'static,
    data: Vec<u8>,
    mode: u32,
) -> Result<()> {
    tokio::task::spawn_blocking(move || write_atomic_file(path, &data, mode))
        .await
        .context("join atomic write")?
}
