//! Agent reverse WebSocket client (panel-compatible with Go `client.go`).

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use rand::Rng;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex};
use tokio_rustls::TlsConnector;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{client_async_with_config, MaybeTlsStream, WebSocketStream};
use tracing::{info, warn};
use url::Url;

use crate::protocol::{self, Envelope, MessageType, TrafficAck};

const WRITE_WAIT: Duration = Duration::from_secs(10);
const PONG_WAIT: Duration = Duration::from_secs(60);
const PING_PERIOD: Duration = Duration::from_secs((60 * 9) / 10);
const STABLE_CONNECTION_TIME: Duration = Duration::from_secs(30);
const MIN_RECONNECT_BACKOFF: Duration = Duration::from_secs(1);
const MAX_RECONNECT_BACKOFF: Duration = Duration::from_secs(30);
const MAX_AGENT_MESSAGE_SIZE: usize = 16 << 20;
const SEND_QUEUE_CAP: usize = 256;
const DEFAULT_HEARTBEAT: Duration = Duration::from_secs(10);

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
type WsSink = SplitSink<WsStream, Message>;
type WsReader = SplitStream<WsStream>;

struct SessionEnvelope {
    session: u64,
    env: Envelope,
    on_complete: Option<Box<dyn FnOnce(bool) + Send>>,
}

impl SessionEnvelope {
    fn complete(self, written: bool) {
        if let Some(cb) = self.on_complete {
            cb(written);
        }
    }
}

/// Callbacks wired by the Agent orchestrator.
#[derive(Default)]
pub struct ClientCallbacks {
    pub register: Option<Arc<dyn Fn() -> protocol::RegisterEvt + Send + Sync>>,
    pub heartbeat: Option<Arc<dyn Fn() -> protocol::HeartbeatEvt + Send + Sync>>,
    pub on_command: Option<
        Arc<
            dyn Fn(u64, Envelope) -> std::pin::Pin<
                    Box<dyn std::future::Future<Output = protocol::CommandResultEvt> + Send>,
                > + Send
                + Sync,
        >,
    >,
    pub on_traffic_ack: Option<Arc<dyn Fn(TrafficAck) + Send + Sync>>,
    pub on_connected: Option<Arc<dyn Fn() + Send + Sync>>,
    pub on_disconnected: Option<Arc<dyn Fn() + Send + Sync>>,
}

/// Reverse WebSocket connection to the panel.
pub struct Client {
    ws_url: String,
    insecure: bool,
    token: Arc<std::sync::RwLock<String>>,
    heartbeat_interval: Duration,
    pub callbacks: Arc<std::sync::RwLock<ClientCallbacks>>,
    send_tx: mpsc::Sender<SessionEnvelope>,
    /// Shared receiver; taken by the active writer and returned on disconnect.
    send_rx: Mutex<Option<mpsc::Receiver<SessionEnvelope>>>,
    next_session: AtomicU64,
    active_session: Arc<AtomicU64>,
    event_mu: Mutex<()>,
}

impl Client {
    /// Builds a client. `panel_url` may be a base origin or a full ws URL;
    /// `/api/agent/ws` is appended when absent.
    pub fn new(panel_url: &str, token: &str, insecure: bool) -> Arc<Self> {
        let (tx, rx) = mpsc::channel(SEND_QUEUE_CAP);
        Arc::new(Self {
            ws_url: derive_ws_url(panel_url),
            insecure,
            token: Arc::new(std::sync::RwLock::new(token.to_string())),
            heartbeat_interval: DEFAULT_HEARTBEAT,
            callbacks: Arc::new(std::sync::RwLock::new(ClientCallbacks::default())),
            send_tx: tx,
            send_rx: Mutex::new(Some(rx)),
            next_session: AtomicU64::new(0),
            active_session: Arc::new(AtomicU64::new(0)),
            event_mu: Mutex::new(()),
        })
    }

    pub fn token(&self) -> String {
        self.token.read().map(|t| t.clone()).unwrap_or_default()
    }

    pub fn set_token(&self, token: impl Into<String>) {
        if let Ok(mut g) = self.token.write() {
            *g = token.into();
        }
    }

    pub fn active_session(&self) -> u64 {
        self.active_session.load(Ordering::SeqCst)
    }

    /// Non-blocking spontaneous event. Returns false if dropped / no session.
    pub async fn send_event(&self, t: MessageType, payload: &impl serde::Serialize) -> bool {
        self.send_event_inner(t, payload, None).await
    }

    /// Like [`send_event`], but invokes `on_complete(written)` after the writer
    /// either flushes the frame or drops it. Used by the traffic report gate.
    pub async fn send_event_with_complete(
        &self,
        t: MessageType,
        payload: &impl serde::Serialize,
        on_complete: impl FnOnce(bool) + Send + 'static,
    ) -> bool {
        self.send_event_inner(t, payload, Some(Box::new(on_complete)))
            .await
    }

    async fn send_event_inner(
        &self,
        t: MessageType,
        payload: &impl serde::Serialize,
        on_complete: Option<Box<dyn FnOnce(bool) + Send>>,
    ) -> bool {
        let env = match Envelope::with_payload(t, "", payload) {
            Ok(e) => e,
            Err(_) => return false,
        };
        let _guard = self.event_mu.lock().await;
        let session = self.active_session.load(Ordering::SeqCst);
        if session == 0 {
            return false;
        }
        match self.send_tx.try_send(SessionEnvelope {
            session,
            env,
            on_complete,
        }) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(q))
            | Err(mpsc::error::TrySendError::Closed(q)) => {
                q.complete(false);
                false
            }
        }
    }

    /// Progress/events tagged to an explicit session (invalid after reconnect).
    pub async fn send_event_for_session(
        &self,
        session: u64,
        t: MessageType,
        id: &str,
        payload: &impl serde::Serialize,
    ) {
        let env = match Envelope::with_payload(t, id, payload) {
            Ok(e) => e,
            Err(_) => return,
        };
        if session == 0 || self.active_session.load(Ordering::SeqCst) != session {
            return;
        }
        let _ = self.send_tx.try_send(SessionEnvelope {
            session,
            env,
            on_complete: None,
        });
    }

    /// Blocking try-send used for command results / heartbeats (waits for capacity or cancel).
    #[allow(dead_code)]
    pub async fn try_send(
        &self,
        cancel: &tokio::sync::watch::Receiver<bool>,
        session: u64,
        env: Envelope,
    ) {
        if session == 0
            || *cancel.borrow()
            || self.active_session.load(Ordering::SeqCst) != session
        {
            return;
        }
        let queued = SessionEnvelope {
            session,
            env,
            on_complete: None,
        };
        tokio::select! {
            _ = wait_cancelled(cancel.clone()) => {}
            res = self.send_tx.send(queued) => {
                if let Err(e) = res {
                    e.0.complete(false);
                }
            }
        }
    }

    /// Connects and stays connected until `shutdown` becomes true.
    pub async fn run(self: Arc<Self>, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        let mut backoff = MIN_RECONNECT_BACKOFF;
        loop {
            if *shutdown.borrow() {
                return;
            }
            let started = std::time::Instant::now();
            let err = self.connect_once(shutdown.clone()).await;
            if *shutdown.borrow() {
                return;
            }
            let connected_for = started.elapsed();
            if let Err(e) = err {
                if connected_for >= STABLE_CONNECTION_TIME {
                    backoff = MIN_RECONNECT_BACKOFF;
                }
                let delay = jitter_reconnect_backoff(backoff);
                warn!(
                    "agent: connection lost: {e:#}; retry in {}ms",
                    delay.as_millis()
                );
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {}
                    _ = shutdown.changed() => {
                        if *shutdown.borrow() {
                            return;
                        }
                    }
                }
                backoff = following_reconnect_backoff(backoff, connected_for);
            } else {
                backoff = MIN_RECONNECT_BACKOFF;
            }
        }
    }

    async fn connect_once(&self, shutdown: tokio::sync::watch::Receiver<bool>) -> Result<()> {
        let token = self.token();
        let mut req = self
            .ws_url
            .as_str()
            .into_client_request()
            .context("build ws request")?;
        req.headers_mut().insert(
            http::header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}"))
                .map_err(|e| anyhow!("authorization header: {e}"))?,
        );

        let ws_cfg = WebSocketConfig::default()
            .max_message_size(Some(MAX_AGENT_MESSAGE_SIZE))
            .max_frame_size(Some(MAX_AGENT_MESSAGE_SIZE));

        let stream = dial_ws_stream(&self.ws_url, self.insecure)
            .await
            .with_context(|| format!("dial {}", self.ws_url))?;

        let (ws, _resp) = client_async_with_config(req, stream, Some(ws_cfg))
            .await
            .with_context(|| format!("ws handshake {}", self.ws_url))?;

        info!("agent: connected to {}", self.ws_url);

        let session = self.next_session.fetch_add(1, Ordering::SeqCst) + 1;
        let (mut sink, reader) = ws.split();

        // Register synchronously before starting the asynchronous writer.
        {
            let reg = self
                .callbacks
                .read()
                .ok()
                .and_then(|cbs| cbs.register.clone());
            if let Some(reg) = reg {
                let env = Envelope::with_payload(MessageType::Register, "", &reg())?;
                write_json(&mut sink, &env)
                    .await
                    .context("write register event")?;
            }
        }

        self.active_session.store(session, Ordering::SeqCst);
        if let Ok(cbs) = self.callbacks.read() {
            if let Some(cb) = cbs.on_connected.as_ref() {
                cb();
            }
        }

        let mut rx = self
            .send_rx
            .lock()
            .await
            .take()
            .ok_or_else(|| anyhow!("send receiver already taken"))?;

        let sink = Arc::new(Mutex::new(sink));
        let writer_shutdown = shutdown.clone();
        let sink_w = Arc::clone(&sink);
        let writer = tokio::spawn(async move {
            write_loop(writer_shutdown, sink_w, &mut rx, session).await;
            rx
        });

        let hb_shutdown = shutdown.clone();
        let hb_interval = self.heartbeat_interval;
        let hb_callbacks = Arc::clone(&self.callbacks);
        let hb_tx = self.send_tx.clone();
        let heartbeat = tokio::spawn(async move {
            periodic_heartbeat(hb_shutdown, hb_interval, hb_callbacks, hb_tx, session).await;
        });

        let read_err = read_loop(
            shutdown.clone(),
            reader,
            Arc::clone(&self.callbacks),
            self.send_tx.clone(),
            session,
            Arc::clone(&self.active_session),
        )
        .await;

        // Stop producers, join writer, drain queue.
        {
            let _guard = self.event_mu.lock().await;
            let _ = self.active_session.compare_exchange(
                session,
                0,
                Ordering::SeqCst,
                Ordering::SeqCst,
            );
        }
        heartbeat.abort();
        {
            let mut s = sink.lock().await;
            let _ = s.close().await;
        }

        match writer.await {
            Ok(mut rx) => {
                discard_queued_events(&mut rx);
                *self.send_rx.lock().await = Some(rx);
            }
            Err(e) => warn!("agent: writer join error: {e}"),
        }

        if let Ok(cbs) = self.callbacks.read() {
            if let Some(cb) = cbs.on_disconnected.as_ref() {
                cb();
            }
        }

        read_err
    }
}

async fn periodic_heartbeat(
    shutdown: tokio::sync::watch::Receiver<bool>,
    interval: Duration,
    callbacks: Arc<std::sync::RwLock<ClientCallbacks>>,
    send_tx: mpsc::Sender<SessionEnvelope>,
    session: u64,
) {
    let d = if interval.is_zero() {
        Duration::from_secs(30)
    } else {
        interval
    };
    let mut tick = tokio::time::interval(d);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    // Skip the immediate first tick so we match Go's NewTicker behaviour
    // (first fire after `d`).
    tick.tick().await;
    loop {
        tokio::select! {
            _ = wait_cancelled(shutdown.clone()) => return,
            _ = tick.tick() => {
                let evt = callbacks
                    .read()
                    .ok()
                    .and_then(|cbs| cbs.heartbeat.as_ref().map(|h| h()));
                if let Some(payload) = evt {
                    if let Ok(env) = Envelope::with_payload(MessageType::Heartbeat, "", &payload) {
                        let _ = send_tx.try_send(SessionEnvelope {
                            session,
                            env,
                            on_complete: None,
                        });
                    }
                }
            }
        }
    }
}

async fn write_loop(
    shutdown: tokio::sync::watch::Receiver<bool>,
    sink: Arc<Mutex<WsSink>>,
    rx: &mut mpsc::Receiver<SessionEnvelope>,
    session: u64,
) {
    let mut ping = tokio::time::interval(PING_PERIOD);
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ping.tick().await; // skip immediate
    loop {
        tokio::select! {
            _ = wait_cancelled(shutdown.clone()) => return,
            queued = rx.recv() => {
                let Some(queued) = queued else { return };
                if *shutdown.borrow() {
                    queued.complete(false);
                    return;
                }
                if queued.session != session {
                    queued.complete(false);
                    continue;
                }
                let mut w = sink.lock().await;
                match write_json(&mut *w, &queued.env).await {
                    Ok(()) => queued.complete(true),
                    Err(_) => {
                        queued.complete(false);
                        let _ = w.close().await;
                        return;
                    }
                }
            }
            _ = ping.tick() => {
                let mut w = sink.lock().await;
                let send = w.send(Message::Ping(Vec::new().into()));
                match tokio::time::timeout(WRITE_WAIT, send).await {
                    Ok(Ok(())) => {}
                    _ => {
                        let _ = w.close().await;
                        return;
                    }
                }
            }
        }
    }
}

async fn read_loop(
    shutdown: tokio::sync::watch::Receiver<bool>,
    mut reader: WsReader,
    callbacks: Arc<std::sync::RwLock<ClientCallbacks>>,
    send_tx: mpsc::Sender<SessionEnvelope>,
    session: u64,
    active_session: Arc<AtomicU64>,
) -> Result<()> {
    loop {
        let next = tokio::time::timeout(PONG_WAIT, reader.next());
        tokio::select! {
            _ = wait_cancelled(shutdown.clone()) => {
                return Err(anyhow!("cancelled"));
            }
            msg = next => {
                let msg = match msg {
                    Ok(Some(Ok(m))) => m,
                    Ok(Some(Err(e))) => return Err(e.into()),
                    Ok(None) => return Err(anyhow!("ws closed")),
                    Err(_) => return Err(anyhow!("pong wait exceeded")),
                };
                match msg {
                    Message::Pong(_) => continue,
                    Message::Ping(_) => continue,
                    Message::Close(_) => return Err(anyhow!("ws close frame")),
                    Message::Frame(_) => continue,
                    Message::Binary(b) => {
                        handle_incoming(
                            &b,
                            &callbacks,
                            &send_tx,
                            session,
                            Arc::clone(&active_session),
                            &shutdown,
                        )
                        .await;
                    }
                    Message::Text(t) => {
                        handle_incoming(
                            t.as_bytes(),
                            &callbacks,
                            &send_tx,
                            session,
                            Arc::clone(&active_session),
                            &shutdown,
                        )
                        .await;
                    }
                }
            }
        }
    }
}

async fn handle_incoming(
    data: &[u8],
    callbacks: &Arc<std::sync::RwLock<ClientCallbacks>>,
    send_tx: &mpsc::Sender<SessionEnvelope>,
    session: u64,
    active_session: Arc<AtomicU64>,
    shutdown: &tokio::sync::watch::Receiver<bool>,
) {
    let env: Envelope = match serde_json::from_slice(data) {
        Ok(e) => e,
        Err(_) => return,
    };
    if env.type_ == MessageType::TrafficAck {
        if let Ok(ack) = env.decode::<TrafficAck>() {
            if let Ok(cbs) = callbacks.read() {
                if let Some(cb) = cbs.on_traffic_ack.as_ref() {
                    cb(ack);
                }
            }
        }
        return;
    }
    let on_command = callbacks
        .read()
        .ok()
        .and_then(|cbs| cbs.on_command.clone());
    let Some(on_command) = on_command else {
        return;
    };
    let send_tx = send_tx.clone();
    let shutdown = shutdown.clone();
    let cmd_id = env.id.clone();
    tokio::spawn(async move {
        let res = on_command(session, env).await;
        if let Ok(out) = Envelope::with_payload(MessageType::CommandResult, &cmd_id, &res) {
            if session == 0
                || *shutdown.borrow()
                || active_session.load(Ordering::SeqCst) != session
            {
                return;
            }
            let queued = SessionEnvelope {
                session,
                env: out,
                on_complete: None,
            };
            tokio::select! {
                _ = wait_cancelled(shutdown) => {}
                r = send_tx.send(queued) => {
                    if let Err(e) = r {
                        e.0.complete(false);
                    }
                }
            }
        }
    });
}

fn discard_queued_events(rx: &mut mpsc::Receiver<SessionEnvelope>) {
    while let Ok(queued) = rx.try_recv() {
        queued.complete(false);
    }
}

async fn write_json(sink: &mut WsSink, env: &Envelope) -> Result<()> {
    let data = serde_json::to_string(env)?;
    let send = sink.send(Message::Text(data.into()));
    tokio::time::timeout(WRITE_WAIT, send)
        .await
        .map_err(|_| anyhow!("write deadline exceeded"))?
        .context("ws write")?;
    Ok(())
}

async fn wait_cancelled(mut rx: tokio::sync::watch::Receiver<bool>) {
    loop {
        if *rx.borrow() {
            return;
        }
        if rx.changed().await.is_err() {
            return;
        }
    }
}

/// Prevents silently downgrading the control channel.
pub fn validate_panel_url(raw: &str, insecure: bool, environment: &str) -> Result<()> {
    let trimmed = raw.trim();
    let u = Url::parse(trimmed).map_err(|_| anyhow!("panel URL must be an absolute URL"))?;
    if u.host_str().is_none() || u.host_str() == Some("") {
        bail!("panel URL must be an absolute URL");
    }
    if !u.username().is_empty() || u.password().is_some() || !u.query().unwrap_or("").is_empty() || u.fragment().is_some()
    {
        bail!("panel URL must not contain credentials, query, or fragment");
    }
    let path = u.path();
    if !path.is_empty() && path != "/" && path != "/api/agent/ws" {
        bail!("panel URL path must be empty or /api/agent/ws");
    }
    let dev = environment.trim().eq_ignore_ascii_case("development");
    match u.scheme() {
        "https" | "wss" => {}
        "http" | "ws" => {
            if !dev {
                bail!(
                    "panel URL must use HTTPS/WSS in production; set SINGBOX_PANEL_ENV=development only for local testing"
                );
            }
        }
        _ => bail!("panel URL scheme must be https, wss, http, or ws"),
    }
    if insecure && !dev {
        bail!("--insecure is allowed only when SINGBOX_PANEL_ENV=development");
    }
    Ok(())
}

pub fn derive_ws_url(base: &str) -> String {
    let trimmed = base.trim();
    let Ok(mut u) = Url::parse(trimmed) else {
        return trimmed.to_string();
    };
    if u.host_str().is_none() {
        return trimmed.to_string();
    }
    match u.scheme() {
        "http" => {
            let _ = u.set_scheme("ws");
        }
        "https" => {
            let _ = u.set_scheme("wss");
        }
        _ => {}
    }
    if !u.path().contains("/api/agent/ws") {
        let base_path = u.path().trim_end_matches('/');
        u.set_path(&format!("{base_path}/api/agent/ws"));
    }
    u.to_string()
}

fn preferred_ip_family(addrs: &[SocketAddr]) -> (bool /* v4 */, Vec<SocketAddr>) {
    let mut v4 = Vec::new();
    let mut v6 = Vec::new();
    for a in addrs {
        match a {
            SocketAddr::V4(_) => v4.push(*a),
            SocketAddr::V6(v6a) => {
                // Unmap IPv4-mapped IPv6 if present.
                if let Some(v4a) = v6a.ip().to_ipv4_mapped() {
                    v4.push(SocketAddr::from((v4a, a.port())));
                } else {
                    v6.push(*a);
                }
            }
        }
    }
    if !v4.is_empty() {
        (true, v4)
    } else {
        (false, v6)
    }
}

async fn dial_ipv4_first(host: &str, port: u16) -> Result<TcpStream> {
    // Literal IP?
    if let Ok(ip) = host.parse::<std::net::IpAddr>() {
        let addr = SocketAddr::new(ip, port);
        return tokio::time::timeout(Duration::from_secs(5), TcpStream::connect(addr))
            .await
            .context("dial timeout")?
            .with_context(|| format!("dial {addr}"));
    }

    let addrs: Vec<SocketAddr> = tokio::net::lookup_host((host, port))
        .await
        .with_context(|| format!("resolve {host}"))?
        .collect();
    let (_v4, preferred) = preferred_ip_family(&addrs);
    if preferred.is_empty() {
        bail!("no IP address found for {host}");
    }
    let mut errs = Vec::new();
    for addr in preferred {
        match tokio::time::timeout(Duration::from_secs(5), TcpStream::connect(addr)).await {
            Ok(Ok(s)) => {
                s.set_nodelay(true).ok();
                return Ok(s);
            }
            Ok(Err(e)) => errs.push(format!("{addr}: {e}")),
            Err(_) => errs.push(format!("{addr}: timeout")),
        }
    }
    bail!("dial for {host}: {}", errs.join("; "))
}

async fn dial_ws_stream(ws_url: &str, insecure: bool) -> Result<MaybeTlsStream<TcpStream>> {
    let u = Url::parse(ws_url).context("parse ws url")?;
    let host = u
        .host_str()
        .ok_or_else(|| anyhow!("ws url missing host"))?
        .to_string();
    let port = u
        .port_or_known_default()
        .ok_or_else(|| anyhow!("ws url missing port"))?;
    let tcp = dial_ipv4_first(&host, port).await?;

    match u.scheme() {
        "ws" => Ok(MaybeTlsStream::Plain(tcp)),
        "wss" => {
            let tls = build_tls_connector(insecure)?;
            let server_name = rustls::pki_types::ServerName::try_from(host.clone())
                .map_err(|_| anyhow!("invalid server name"))?;
            let handshake = tls.connect(server_name, tcp);
            let tls_stream = tokio::time::timeout(Duration::from_secs(15), handshake)
                .await
                .context("tls handshake timeout")?
                .context("tls handshake")?;
            Ok(MaybeTlsStream::Rustls(tls_stream))
        }
        other => bail!("unsupported ws scheme: {other}"),
    }
}

fn build_tls_connector(insecure: bool) -> Result<TlsConnector> {
    let config = if insecure {
        let verifier = Arc::new(NoCertVerifier);
        rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(verifier)
            .with_no_client_auth()
    } else {
        let mut roots = rustls::RootCertStore::empty();
        roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth()
    };
    Ok(TlsConnector::from(Arc::new(config)))
}

#[derive(Debug)]
struct NoCertVerifier;

impl rustls::client::danger::ServerCertVerifier for NoCertVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &rustls::pki_types::CertificateDer<'_>,
        _dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        Ok(rustls::client::danger::HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

pub(crate) fn following_reconnect_backoff(
    current: Duration,
    connected_for: Duration,
) -> Duration {
    if connected_for >= STABLE_CONNECTION_TIME {
        return MIN_RECONNECT_BACKOFF;
    }
    let mut current = current;
    if current < MIN_RECONNECT_BACKOFF {
        current = MIN_RECONNECT_BACKOFF;
    }
    if current >= MAX_RECONNECT_BACKOFF / 2 {
        return MAX_RECONNECT_BACKOFF;
    }
    current * 2
}

pub(crate) fn jitter_reconnect_backoff(base: Duration) -> Duration {
    let base = if base.is_zero() {
        MIN_RECONNECT_BACKOFF
    } else {
        base
    };
    let delta = base / 5; // ±20%
    if delta.is_zero() {
        return base;
    }
    let span = delta.as_nanos() * 2 + 1;
    let jitter = rand::thread_rng().gen_range(0..span);
    base - delta + Duration::from_nanos(jitter as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    #[test]
    fn preferred_ip_family_chooses_v4_when_available() {
        let addrs = vec![
            SocketAddr::from((Ipv6Addr::LOCALHOST, 443)),
            SocketAddr::from((Ipv4Addr::new(192, 0, 2, 10), 443)),
            SocketAddr::from((Ipv4Addr::new(192, 0, 2, 11), 443)),
        ];
        let (v4, preferred) = preferred_ip_family(&addrs);
        assert!(v4);
        assert_eq!(preferred.len(), 2);
    }

    #[test]
    fn reconnect_backoff_resets_after_stable() {
        assert_eq!(
            following_reconnect_backoff(MAX_RECONNECT_BACKOFF, STABLE_CONNECTION_TIME),
            MIN_RECONNECT_BACKOFF
        );
    }

    #[test]
    fn reconnect_backoff_caps() {
        let mut backoff = MIN_RECONNECT_BACKOFF;
        for _ in 0..10 {
            backoff = following_reconnect_backoff(backoff, Duration::from_secs(5));
        }
        assert_eq!(backoff, MAX_RECONNECT_BACKOFF);
    }

    #[test]
    fn derive_ws_url_appends_path() {
        assert_eq!(
            derive_ws_url("https://panel.example.com"),
            "wss://panel.example.com/api/agent/ws"
        );
        assert_eq!(
            derive_ws_url("http://localhost:8080"),
            "ws://localhost:8080/api/agent/ws"
        );
    }

    #[test]
    fn validate_rejects_http_in_production() {
        assert!(validate_panel_url("http://localhost", false, "").is_err());
        assert!(validate_panel_url("http://localhost", false, "development").is_ok());
    }
}
