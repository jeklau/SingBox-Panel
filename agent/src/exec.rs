//! Command runner matching Go `exec.go`: new process group, kill on cancel,
//! 1 MiB capped combined output, and a WaitDelay-equivalent grace period.

use std::io;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, Context};
use nix::sys::signal::{kill, Signal};
use nix::unistd::Pid;
use tokio::io::AsyncReadExt;
use tokio::process::Command;

const MAX_COMMAND_OUTPUT_BYTES: usize = 1 << 20;
const WAIT_DELAY: Duration = Duration::from_secs(2);

/// Captures combined stdout/stderr with a hard 1 MiB cap.
#[derive(Default)]
struct CappedOutput {
    buf: Vec<u8>,
    truncated: bool,
}

impl CappedOutput {
    fn write(&mut self, p: &[u8]) {
        let remaining = MAX_COMMAND_OUTPUT_BYTES.saturating_sub(self.buf.len());
        if remaining == 0 {
            self.truncated = true;
            return;
        }
        if p.len() > remaining {
            self.buf.extend_from_slice(&p[..remaining]);
            self.truncated = true;
        } else {
            self.buf.extend_from_slice(p);
        }
    }

    fn into_string(self) -> String {
        let mut out = String::from_utf8_lossy(&self.buf).into_owned();
        if self.truncated {
            out.push_str("\n[command output truncated]");
        }
        out
    }
}

/// Result of a completed command (even when the exit status is non-zero).
#[derive(Debug)]
pub struct RunResult {
    pub output: String,
    pub status: std::process::ExitStatus,
}

impl RunResult {
    pub fn success(&self) -> bool {
        self.status.success()
    }
}

/// Executes a command and returns trimmed combined output.
///
/// Behaviour mirrors Go `run`:
/// - child is placed in a new process group (`Setpgid`)
/// - on cancel, the whole group is SIGKILL'd
/// - after kill, wait up to 2s (WaitDelay)
/// - stdout+stderr are capped at 1 MiB with a truncation marker
pub async fn run(name: &str, args: &[&str]) -> anyhow::Result<RunResult> {
    run_cancellable(None, name, args).await
}

/// Like [`run`], but aborts (process-group SIGKILL) when `cancel` completes.
pub async fn run_cancellable(
    cancel: Option<tokio::sync::oneshot::Receiver<()>>,
    name: &str,
    args: &[&str],
) -> anyhow::Result<RunResult> {
    let mut cmd = Command::new(name);
    cmd.args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);

    // Match Go SysProcAttr{Setpgid: true}.
    unsafe {
        cmd.pre_exec(|| {
            if libc::setpgid(0, 0) != 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }

    let mut child = cmd
        .spawn()
        .with_context(|| format!("spawn {name}"))?;

    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    let capped = Arc::new(Mutex::new(CappedOutput::default()));

    let cap_out = Arc::clone(&capped);
    let out_task = tokio::spawn(async move {
        if let Some(mut s) = stdout {
            let mut buf = [0u8; 8192];
            loop {
                match s.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => {
                        if let Ok(mut g) = cap_out.lock() {
                            g.write(&buf[..n]);
                        }
                    }
                    Err(_) => break,
                }
            }
        }
    });
    let cap_err = Arc::clone(&capped);
    let err_task = tokio::spawn(async move {
        if let Some(mut s) = stderr {
            let mut buf = [0u8; 8192];
            loop {
                match s.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => {
                        if let Ok(mut g) = cap_err.lock() {
                            g.write(&buf[..n]);
                        }
                    }
                    Err(_) => break,
                }
            }
        }
    });

    let wait_fut = child.wait();
    let status = if let Some(mut cancel_rx) = cancel {
        tokio::select! {
            res = wait_fut => {
                res.with_context(|| format!("wait {name}"))?
            }
            _ = &mut cancel_rx => {
                kill_process_group(&child);
                match tokio::time::timeout(WAIT_DELAY, child.wait()).await {
                    Ok(Ok(st)) => st,
                    Ok(Err(e)) => return Err(anyhow!("wait after kill {name}: {e}")),
                    Err(_) => {
                        kill_process_group(&child);
                        child.wait().await.with_context(|| format!("final wait {name}"))?
                    }
                }
            }
        }
    } else {
        wait_fut.await.with_context(|| format!("wait {name}"))?
    };

    let _ = out_task.await;
    let _ = err_task.await;

    let output = capped
        .lock()
        .map(|mut g| std::mem::take(&mut *g).into_string())
        .unwrap_or_default();
    let output = output.trim().to_string();

    Ok(RunResult { output, status })
}

/// Executes `/bin/sh -c <script>` (official install pipes).
pub async fn run_shell(script: &str) -> anyhow::Result<RunResult> {
    run("sh", &["-c", script]).await
}

/// Like [`run_shell`] with cancellation.
pub async fn run_shell_cancellable(
    cancel: Option<tokio::sync::oneshot::Receiver<()>>,
    script: &str,
) -> anyhow::Result<RunResult> {
    run_cancellable(cancel, "sh", &["-c", script]).await
}

fn kill_process_group(child: &tokio::process::Child) {
    if let Some(id) = child.id() {
        let pid = Pid::from_raw(id as i32);
        // Negative PID = process group, matching Go syscall.Kill(-pid, SIGKILL).
        let _ = kill(Pid::from_raw(-pid.as_raw()), Signal::SIGKILL);
    }
}

/// Helper used by callers that want Go-style checked success.
pub async fn run_checked(name: &str, args: &[&str]) -> Result<String, anyhow::Error> {
    let result = run(name, args).await?;
    if result.success() {
        Ok(result.output)
    } else {
        Err(anyhow!(
            "command `{name}` failed (status {}): {}",
            result.status,
            result.output
        ))
    }
}
