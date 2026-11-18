//! Agent orchestrator: wires command handlers, traffic, and the WS client
//! (Go `agent.go`).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;
use tracing::{info, warn};

use crate::apply_config::apply_config;
use crate::client::Client;
use crate::config_read::read_config;
use crate::install::{detect_version, install_singbox};
use crate::logs::LogStreamer;
use crate::paths::{CONFIG_FILE, SINGBOX_BINARY};
use crate::protocol::{
    ApplyConfigCmd, CommandResultEvt, ConfigData, Envelope, GetLogsCmd, HeartbeatEvt,
    InstallSingboxCmd, LogEvt, LogsData, MessageType, ProgressEvt, RegisterEvt, ServiceActionCmd,
    StatusData, StreamLogsCmd, TestOutboundCmd, TrafficEvt,
};
use crate::self_uninstall::schedule_self_uninstall;
use crate::self_update::{mark_agent_ready, self_update};
use crate::service::{service_action, service_active, service_enabled};
use crate::sysinfo::{collect_sys_info, detect_public_ipv4};
use crate::test_outbound::test_outbound;
use crate::traffic::{StatsStream, TrafficSampler};
use crate::uninstall_singbox::uninstall_singbox;

/// Runtime configuration for the agent daemon.
#[derive(Debug, Clone)]
pub struct Config {
    pub panel_url: String,
    pub token: String,
    pub insecure: bool,
    pub agent_version: String,
}

/// Serializes mutating operations (install/apply/update/uninstall). Waiting
/// observes the command timeout so one stuck package manager cannot queue
/// later operations forever.
fn ops_gate() -> &'static Semaphore {
    static GATE: std::sync::OnceLock<Semaphore> = std::sync::OnceLock::new();
    GATE.get_or_init(|| Semaphore::new(1))
}

fn command_mutates(t: &MessageType) -> bool {
    matches!(
        t,
        MessageType::InstallSingbox
            | MessageType::ApplyConfig
            | MessageType::ServiceAction
            | MessageType::UpdateAgent
            | MessageType::UninstallSingbox
            | MessageType::UninstallAgent
    )
}

/// Deliberately shorter than the matching Panel HTTP timeout, leaving enough
/// time to send a failure result before the Panel gives up.
fn command_timeout(t: &MessageType) -> Duration {
    match t {
        MessageType::InstallSingbox => Duration::from_secs(5 * 60 + 30),
        MessageType::ApplyConfig => Duration::from_secs(50),
        MessageType::ServiceAction => Duration::from_secs(25),
        MessageType::UpdateAgent => Duration::from_secs(2 * 60 + 30),
        MessageType::UninstallSingbox => Duration::from_secs(2 * 60),
        MessageType::UninstallAgent => Duration::from_secs(30),
        MessageType::GetLogs => Duration::from_secs(25),
        MessageType::TestOutbound => Duration::from_secs(15),
        MessageType::GetStatus | MessageType::GetConfig => Duration::from_secs(12),
        MessageType::StreamLogs => Duration::from_secs(10),
        _ => Duration::from_secs(15),
    }
}

fn cmd_result(id: String, out: String, err: Option<anyhow::Error>) -> CommandResultEvt {
    match err {
        Some(e) => CommandResultEvt {
            id,
            ok: false,
            error: e.to_string(),
            output: out,
            data: None,
        },
        None => CommandResultEvt {
            id,
            ok: true,
            error: String::new(),
            output: out,
            data: None,
        },
    }
}

fn decode_result(id: String, err: anyhow::Error) -> CommandResultEvt {
    cmd_result(
        id,
        String::new(),
        Some(anyhow::anyhow!("decode command payload: {err}")),
    )
}

fn decode_payload<T: for<'de> serde::Deserialize<'de>>(env: &Envelope) -> anyhow::Result<T> {
    match &env.payload {
        Some(raw) => Ok(serde_json::from_str(raw.get())?),
        None => Ok(serde_json::from_str("{}")?),
    }
}

fn result_data(id: String, data: impl serde::Serialize) -> CommandResultEvt {
    let raw = serde_json::to_string(&data)
        .ok()
        .and_then(|s| serde_json::value::RawValue::from_string(s).ok());
    CommandResultEvt {
        id,
        ok: true,
        error: String::new(),
        output: String::new(),
        data: raw,
    }
}

/// Starts the agent: traffic loops, WS client, command dispatch until shutdown.
pub async fn run(cfg: Config, shutdown: tokio::sync::watch::Receiver<bool>) {
    let client = Client::new(&cfg.panel_url, &cfg.token, cfg.insecure);
    let traffic = Arc::new(TrafficSampler::new());
    let log_stream = Arc::new(LogStreamer::new());
    let traffic_report_in_flight = Arc::new(AtomicBool::new(false));

    let agent_version = cfg.agent_version.clone();
    let panel_url = cfg.panel_url.clone();
    let insecure = cfg.insecure;

    {
        let traffic_run = Arc::clone(&traffic);
        let shutdown_run = shutdown.clone();
        tokio::spawn(async move {
            traffic_run.run(shutdown_run).await;
        });
    }
    {
        let stats = StatsStream::new(Arc::clone(&traffic));
        let shutdown_run = shutdown.clone();
        tokio::spawn(async move {
            stats.run(shutdown_run).await;
        });
    }
    {
        let client_tr = Arc::clone(&client);
        let traffic_tr = Arc::clone(&traffic);
        let in_flight = Arc::clone(&traffic_report_in_flight);
        let mut shutdown_tr = shutdown.clone();
        tokio::spawn(async move {
            traffic_report_loop(client_tr, traffic_tr, in_flight, &mut shutdown_tr).await;
        });
    }
    {
        let log_stream_exit = Arc::clone(&log_stream);
        let mut shutdown_exit = shutdown.clone();
        tokio::spawn(async move {
            let _ = shutdown_exit.changed().await;
            if *shutdown_exit.borrow() {
                log_stream_exit.stop().await;
            }
        });
    }

    {
        let mut cbs = client.callbacks.write().expect("callbacks lock");

        let version_reg = agent_version.clone();
        let panel_reg = panel_url.clone();
        cbs.register = Some(Arc::new(move || {
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async {
                    let (installed, ver) =
                        tokio::time::timeout(Duration::from_secs(10), detect_version())
                            .await
                            .unwrap_or((false, String::new()));
                    let si = tokio::time::timeout(Duration::from_secs(10), collect_sys_info())
                        .await
                        .unwrap_or_default();
                    let public_ip =
                        tokio::time::timeout(Duration::from_secs(10), detect_public_ipv4())
                            .await
                            .unwrap_or_default();
                    RegisterEvt {
                        agent_version: version_reg.clone(),
                        hostname: si.hostname,
                        os: si.os,
                        arch: si.arch,
                        kernel: si.kernel,
                        public_ip,
                        singbox_installed: installed
                            || std::path::Path::new(SINGBOX_BINARY).exists(),
                        singbox_version: ver,
                        panel_url: panel_reg.clone(),
                    }
                })
            })
        }));

        let traffic_hb = Arc::clone(&traffic);
        cbs.heartbeat = Some(Arc::new(move || {
            let traffic_hb = Arc::clone(&traffic_hb);
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(async move {
                    let si = tokio::time::timeout(Duration::from_secs(10), collect_sys_info())
                        .await
                        .unwrap_or_default();
                    let (_, ver) =
                        tokio::time::timeout(Duration::from_secs(10), detect_version())
                            .await
                            .unwrap_or((false, String::new()));
                    let active = tokio::time::timeout(Duration::from_secs(10), service_active())
                        .await
                        .unwrap_or(false);
                    HeartbeatEvt {
                        ts: chrono::Utc::now().timestamp(),
                        load1: si.load1,
                        mem_used: si.mem_used,
                        mem_total: si.mem_total,
                        uptime: si.uptime,
                        singbox_active: active,
                        singbox_version: ver,
                        traffic: traffic_hb.summary_snapshot(),
                    }
                })
            })
        }));

        let traffic_ack = Arc::clone(&traffic);
        cbs.on_traffic_ack = Some(Arc::new(move |ack| {
            traffic_ack.acknowledge_report(ack);
        }));

        let version_ready = agent_version.clone();
        cbs.on_connected = Some(Arc::new(move || {
            if let Err(e) = mark_agent_ready(&version_ready) {
                warn!("agent: write readiness marker: {e:#}");
            } else {
                info!("agent: session registered with panel");
            }
        }));

        let log_disc = Arc::clone(&log_stream);
        cbs.on_disconnected = Some(Arc::new(move || {
            let log_disc = Arc::clone(&log_disc);
            tokio::spawn(async move {
                log_disc.stop().await;
            });
        }));

        let client_cmd = Arc::clone(&client);
        let log_cmd = Arc::clone(&log_stream);
        let version_cmd = agent_version.clone();
        let panel_cmd = panel_url.clone();
        cbs.on_command = Some(Arc::new(move |session, env| {
            let client = Arc::clone(&client_cmd);
            let log_stream = Arc::clone(&log_cmd);
            let agent_version = version_cmd.clone();
            let panel_url = panel_cmd.clone();
            Box::pin(async move {
                on_command(
                    session,
                    env,
                    client,
                    log_stream,
                    agent_version,
                    panel_url,
                    insecure,
                )
                .await
            })
        }));
    }

    client.run(shutdown).await;
}

async fn traffic_report_loop(
    client: Arc<Client>,
    traffic: Arc<TrafficSampler>,
    in_flight: Arc<AtomicBool>,
    shutdown: &mut tokio::sync::watch::Receiver<bool>,
) {
    let mut ticker = tokio::time::interval(Duration::from_secs(3));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Match Go NewTicker: first fire after 3s.
    ticker.tick().await;
    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    return;
                }
            }
            _ = ticker.tick() => {
                report_traffic(&client, &traffic, &in_flight).await;
            }
        }
    }
}

async fn report_traffic(
    client: &Client,
    traffic: &TrafficSampler,
    in_flight: &Arc<AtomicBool>,
) {
    if in_flight
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        return;
    }
    let snap = traffic.accounting_snapshot();
    let flag = Arc::clone(in_flight);
    let evt = TrafficEvt {
        traffic: Some(snap),
    };
    let sent = client
        .send_event_with_complete(MessageType::Traffic, &evt, move |_written| {
            // A socket receipt only releases the queue slot. The batch is retained
            // until a matching traffic_ack confirms database commit on the panel.
            flag.store(false, Ordering::SeqCst);
        })
        .await;
    if !sent {
        in_flight.store(false, Ordering::SeqCst);
    }
}

async fn on_command(
    session: u64,
    env: Envelope,
    client: Arc<Client>,
    log_stream: Arc<LogStreamer>,
    agent_version: String,
    panel_url: String,
    insecure: bool,
) -> CommandResultEvt {
    let timeout = command_timeout(&env.type_);
    let id = env.id.clone();
    let result = tokio::time::timeout(
        timeout,
        dispatch_with_gate(
            session,
            env,
            client,
            log_stream,
            agent_version,
            panel_url,
            insecure,
        ),
    )
    .await;

    match result {
        Ok(r) => r,
        Err(_) => CommandResultEvt {
            id,
            ok: false,
            error: "deadline exceeded".into(),
            output: String::new(),
            data: None,
        },
    }
}

async fn dispatch_with_gate(
    session: u64,
    env: Envelope,
    client: Arc<Client>,
    log_stream: Arc<LogStreamer>,
    agent_version: String,
    panel_url: String,
    insecure: bool,
) -> CommandResultEvt {
    let id = env.id.clone();
    if command_mutates(&env.type_) {
        let permit = match ops_gate().acquire().await {
            Ok(p) => p,
            Err(_) => {
                return cmd_result(
                    id,
                    String::new(),
                    Some(anyhow::anyhow!("wait for previous operation: cancelled")),
                );
            }
        };
        let result = dispatch_command(
            session,
            env,
            client,
            log_stream,
            agent_version,
            panel_url,
            insecure,
        )
        .await;
        drop(permit);
        result
    } else {
        dispatch_command(
            session,
            env,
            client,
            log_stream,
            agent_version,
            panel_url,
            insecure,
        )
        .await
    }
}

async fn dispatch_command(
    session: u64,
    env: Envelope,
    client: Arc<Client>,
    log_stream: Arc<LogStreamer>,
    agent_version: String,
    panel_url: String,
    insecure: bool,
) -> CommandResultEvt {
    match env.type_ {
        MessageType::InstallSingbox => {
            let c: InstallSingboxCmd = match decode_payload(&env) {
                Ok(c) => c,
                Err(e) => return decode_result(env.id, e),
            };
            client
                .send_event_for_session(
                    session,
                    MessageType::Progress,
                    &env.id,
                    &ProgressEvt {
                        id: env.id.clone(),
                        line: "开始安装 sing-box...".into(),
                        stream: "stdout".into(),
                    },
                )
                .await;
            match install_singbox(&c.channel, &c.version, &c.method).await {
                Ok(out) => cmd_result(env.id, out, None),
                Err(e) => {
                    client
                        .send_event_for_session(
                            session,
                            MessageType::Progress,
                            &env.id,
                            &ProgressEvt {
                                id: env.id.clone(),
                                line: e.to_string(),
                                stream: "stderr".into(),
                            },
                        )
                        .await;
                    cmd_result(env.id, String::new(), Some(e))
                }
            }
        }
        MessageType::ApplyConfig => {
            let c: ApplyConfigCmd = match decode_payload(&env) {
                Ok(c) => c,
                Err(e) => return decode_result(env.id, e),
            };
            match apply_config(c.config.get().as_bytes()).await {
                Ok(out) => cmd_result(env.id, out, None),
                Err(e) => cmd_result(env.id, String::new(), Some(e)),
            }
        }
        MessageType::ServiceAction => {
            let c: ServiceActionCmd = match decode_payload(&env) {
                Ok(c) => c,
                Err(e) => return decode_result(env.id, e),
            };
            match service_action(&c.action).await {
                Ok(out) => cmd_result(env.id, out, None),
                Err(e) => cmd_result(env.id, String::new(), Some(e)),
            }
        }
        MessageType::TestOutbound => {
            let c: TestOutboundCmd = match decode_payload(&env) {
                Ok(c) => c,
                Err(e) => return decode_result(env.id, e),
            };
            let data = test_outbound(c).await;
            result_data(env.id, data)
        }
        MessageType::GetLogs => {
            let c: GetLogsCmd = match decode_payload(&env) {
                Ok(c) => c,
                Err(e) => return decode_result(env.id, e),
            };
            match crate::logs::read_logs(c.lines).await {
                Ok(text) => result_data(env.id, LogsData { text }),
                Err(e) => cmd_result(env.id, String::new(), Some(e)),
            }
        }
        MessageType::StreamLogs => {
            let c: StreamLogsCmd = match decode_payload(&env) {
                Ok(c) => c,
                Err(e) => return decode_result(env.id, e),
            };
            if c.enable {
                let client_line = Arc::clone(&client);
                let cmd_id = env.id.clone();
                match log_stream
                    .start(c.lines, move |line| {
                        let client_line = Arc::clone(&client_line);
                        let cmd_id = cmd_id.clone();
                        tokio::spawn(async move {
                            client_line
                                .send_event_for_session(
                                    session,
                                    MessageType::Log,
                                    &cmd_id,
                                    &LogEvt {
                                        level: "info".into(),
                                        msg: line,
                                    },
                                )
                                .await;
                        });
                    })
                    .await
                {
                    Ok(()) => CommandResultEvt {
                        id: env.id,
                        ok: true,
                        error: String::new(),
                        output: "log streaming started".into(),
                        data: None,
                    },
                    Err(e) => cmd_result(env.id, String::new(), Some(e)),
                }
            } else {
                log_stream.stop().await;
                CommandResultEvt {
                    id: env.id,
                    ok: true,
                    error: String::new(),
                    output: "log streaming stopped".into(),
                    data: None,
                }
            }
        }
        MessageType::UpdateAgent => {
            client
                .send_event_for_session(
                    session,
                    MessageType::Progress,
                    &env.id,
                    &ProgressEvt {
                        id: env.id.clone(),
                        line: "开始更新 Agent...".into(),
                        stream: "stdout".into(),
                    },
                )
                .await;
            let token = client.token();
            match self_update(&panel_url, &token, insecure).await {
                Ok(out) => cmd_result(env.id, out, None),
                Err(e) => {
                    client
                        .send_event_for_session(
                            session,
                            MessageType::Progress,
                            &env.id,
                            &ProgressEvt {
                                id: env.id.clone(),
                                line: e.to_string(),
                                stream: "stderr".into(),
                            },
                        )
                        .await;
                    cmd_result(env.id, String::new(), Some(e))
                }
            }
        }
        MessageType::UninstallSingbox => match uninstall_singbox().await {
            Ok(out) => cmd_result(env.id, out, None),
            Err(e) => cmd_result(env.id, String::new(), Some(e)),
        },
        MessageType::UninstallAgent => match schedule_self_uninstall().await {
            Ok(out) => cmd_result(env.id, out, None),
            Err(e) => cmd_result(env.id, String::new(), Some(e)),
        },
        MessageType::GetStatus => {
            let (installed, ver) = detect_version().await;
            let si = collect_sys_info().await;
            let (_, briefs, has_cfg) = read_config();
            let public_ip = detect_public_ipv4().await;
            let data = StatusData {
                agent_version,
                singbox_installed: installed,
                singbox_version: ver,
                service_active: service_active().await,
                service_enabled: service_enabled().await,
                has_config: has_cfg,
                inbounds: briefs,
                hostname: si.hostname,
                os: si.os,
                arch: si.arch,
                kernel: si.kernel,
                public_ip,
                load1: si.load1,
                mem_used: si.mem_used,
                mem_total: si.mem_total,
                uptime: si.uptime,
            };
            result_data(env.id, data)
        }
        MessageType::GetConfig => {
            let (raw, briefs, exists) = read_config();
            let data = ConfigData {
                path: CONFIG_FILE.into(),
                exists,
                raw: String::from_utf8_lossy(&raw).into_owned(),
                inbounds: briefs,
            };
            result_data(env.id, data)
        }
        other => CommandResultEvt {
            id: env.id,
            ok: false,
            error: format!("unknown command {}", other.as_str()),
            output: String::new(),
            data: None,
        },
    }
}
