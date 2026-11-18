//! sing-box log retrieval and live streaming (Go `logs.go` + `logstream.go`).

use std::process::Stdio;
use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::Mutex;

use crate::service::current_service_manager;

/// Caps how much history the panel can pull in one request.
pub const MAX_LOG_LINES: i32 = 1000;

/// Returns the most recent sing-box service logs from this node.
pub async fn read_logs(lines: i32) -> Result<String> {
    let mut lines = lines;
    if lines <= 0 {
        lines = 200;
    }
    if lines > MAX_LOG_LINES {
        lines = MAX_LOG_LINES;
    }

    let manager = current_service_manager().await?;
    let (command, args) = manager.log_command(lines, false);
    let arg_refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();

    let out = match tokio::time::timeout(
        std::time::Duration::from_secs(20),
        crate::exec::run(&command, &arg_refs),
    )
    .await
    {
        Ok(Ok(r)) => r.output,
        Ok(Err(e)) => e.to_string(),
        Err(_) => String::new(),
    };

    if !out.trim().is_empty() && !out.contains("-- No entries --") {
        return Ok(out);
    }

    let status_out = tokio::time::timeout(std::time::Duration::from_secs(20), manager.status())
        .await
        .unwrap_or_default();

    if !status_out.trim().is_empty() {
        return Ok(status_out);
    }
    if !out.trim().is_empty() {
        return Ok(out);
    }
    Ok("(暂无日志，请确认 sing-box 服务已在节点上安装并运行)".into())
}

struct LogStreamerState {
    cancel: Option<tokio::sync::watch::Sender<bool>>,
    running: bool,
    generation: u64,
}

/// Manages one continuous native log follower per Agent session.
///
/// Construct once and share via [`Arc`] so follower tasks can clear state on exit.
pub struct LogStreamer {
    operation: Mutex<()>,
    state: Arc<Mutex<LogStreamerState>>,
}

impl LogStreamer {
    pub fn new() -> Self {
        Self {
            operation: Mutex::new(()),
            state: Arc::new(Mutex::new(LogStreamerState {
                cancel: None,
                running: false,
                generation: 0,
            })),
        }
    }

    /// Begins streaming sing-box logs. Replaces any previous stream.
    /// Each line is delivered to `on_line` in real time.
    pub async fn start<F>(&self, lines: i32, on_line: F) -> Result<()>
    where
        F: Fn(String) + Send + Sync + 'static,
    {
        let _op = self.operation.lock().await;

        let generation = {
            let mut st = self.state.lock().await;
            st.generation += 1;
            let generation = st.generation;
            if let Some(tx) = st.cancel.take() {
                let _ = tx.send(true);
            }
            st.running = false;
            generation
        };

        let mut lines = lines;
        if lines <= 0 {
            lines = 200;
        }
        if lines > MAX_LOG_LINES {
            lines = MAX_LOG_LINES;
        }

        let manager = current_service_manager().await?;
        let (command, args) = manager.log_command(lines, true);

        let (cancel_tx, mut cancel_rx) = tokio::sync::watch::channel(false);

        let mut cmd = Command::new(&command);
        cmd.args(&args)
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);

        let mut child = cmd.spawn().with_context(|| format!("spawn {command}"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| anyhow::anyhow!("missing stdout pipe"))?;

        {
            let mut st = self.state.lock().await;
            st.cancel = Some(cancel_tx);
            st.running = true;
        }

        let state = Arc::clone(&self.state);
        tokio::spawn(async move {
            let reader = BufReader::new(stdout);
            let mut lines_iter = reader.lines();
            loop {
                tokio::select! {
                    _ = cancel_rx.changed() => {
                        if *cancel_rx.borrow() {
                            break;
                        }
                    }
                    line = lines_iter.next_line() => {
                        match line {
                            Ok(Some(text)) => on_line(text),
                            Ok(None) => break,
                            Err(_) => break,
                        }
                    }
                }
            }
            let _ = child.kill().await;
            let _ = child.wait().await;
            let mut st = state.lock().await;
            if st.generation == generation {
                st.running = false;
                st.cancel = None;
            }
        });

        Ok(())
    }

    /// Stops the active log stream, if any.
    pub async fn stop(&self) {
        let _op = self.operation.lock().await;
        let mut st = self.state.lock().await;
        st.generation += 1;
        if let Some(tx) = st.cancel.take() {
            let _ = tx.send(true);
        }
        st.running = false;
    }

    pub async fn is_running(&self) -> bool {
        self.state.lock().await.running
    }
}

impl Default for LogStreamer {
    fn default() -> Self {
        Self::new()
    }
}
