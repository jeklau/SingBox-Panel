//! Hand-rolled gRPC-Web SubscribeConnections client (sing-box 1.14+ API).

use std::collections::HashMap;
#[cfg(test)]
use std::io::{Cursor, Read};
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use rand::Rng;
use thiserror::Error;

use super::guard::parse_reality_guard_inbounds;
use super::TrafficSampler;

const STATS_STREAM_METHOD: &str = "/daemon.StartedService/SubscribeConnections";
const STATS_STREAM_MAX_FRAME: u32 = 64 << 20;
const STATS_STREAM_INTERVAL_MS: u64 = 1000;
const STATS_STREAM_MAX_BACKOFF: Duration = Duration::from_secs(30);
const STATS_STREAM_STABLE_AFTER: Duration = Duration::from_secs(30);
const STATS_STREAM_IDLE_RETRY: Duration = Duration::from_secs(15);

#[derive(Debug, Error)]
pub enum StatsStreamError {
    #[error("sing-box api service unavailable")]
    Unavailable,
    #[error("{0}")]
    Other(String),
}

impl StatsStreamError {
    fn unavailable(msg: impl Into<String>) -> Self {
        let _ = msg;
        Self::Unavailable
    }
}

#[derive(Clone, Debug)]
pub struct StatsStreamConfig {
    pub url: String,
    pub secret: String,
}

/// Decoded subset of daemon.ConnectionEvent.
#[derive(Clone, Debug, Default)]
pub struct ConnectionEvent {
    pub type_: i32, // 0 new, 1 update, 2 closed
    pub id: String,
    pub connection: Option<StreamConnection>,
    pub uplink_delta: i64,
    pub downlink_delta: i64,
    pub closed_at: i64,
}

pub const CONNECTION_EVENT_NEW: i32 = 0;
pub const CONNECTION_EVENT_UPDATE: i32 = 1;
pub const CONNECTION_EVENT_CLOSED: i32 = 2;

/// Decoded subset of daemon.Connection.
#[derive(Clone, Debug, Default)]
pub struct StreamConnection {
    pub id: String,
    pub inbound: String,
    pub inbound_type: String,
    pub user: String,
    pub created_at: i64, // unix milliseconds
    pub closed_at: i64,
    pub uplink_total: i64,
    pub downlink_total: i64,
}

#[derive(Clone, Debug, Default)]
pub struct ConnectionEvents {
    pub events: Vec<ConnectionEvent>,
    pub reset: bool,
}

/// Per-subscription view for turning UPDATE deltas into cumulative totals.
pub struct StreamSession {
    pub subscribed_at: DateTime<Utc>,
    pub totals: HashMap<String, [u64; 2]>,
    pub guard_inbounds: HashMap<String, bool>,
}

impl StreamSession {
    pub fn new(subscribed_at: DateTime<Utc>) -> Self {
        Self {
            subscribed_at,
            totals: HashMap::new(),
            guard_inbounds: HashMap::new(),
        }
    }
}

/// Owns one Agent-lifetime subscription loop.
pub struct StatsStream {
    sampler: Arc<TrafficSampler>,
    client: reqwest::Client,
    read_config: Box<dyn Fn() -> anyhow::Result<Vec<u8>> + Send + Sync>,
}

impl StatsStream {
    pub fn new(sampler: Arc<TrafficSampler>) -> Self {
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(3))
            .pool_max_idle_per_host(0)
            .build()
            .expect("stats stream client");
        Self {
            sampler,
            client,
            read_config: Box::new(crate::config_read::read_config_file),
        }
    }

    /// Keeps a subscription open for the Agent's lifetime.
    pub async fn run(&self, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        let mut backoff = Duration::from_secs(1);
        loop {
            if *shutdown.borrow() {
                return;
            }
            let started = Utc::now();
            let err = self.subscribe_once(&shutdown).await;
            if *shutdown.borrow() {
                return;
            }
            let delay = if matches!(err, Err(StatsStreamError::Unavailable)) {
                STATS_STREAM_IDLE_RETRY
            } else if Utc::now()
                .signed_duration_since(started)
                .to_std()
                .unwrap_or_default()
                >= STATS_STREAM_STABLE_AFTER
            {
                backoff = Duration::from_secs(1);
                backoff
            } else {
                backoff = (backoff * 2).min(STATS_STREAM_MAX_BACKOFF);
                backoff
            };
            let jitter = {
                let mut rng = rand::thread_rng();
                Duration::from_millis(rng.gen_range(0..=(delay.as_millis() as u64 / 5 + 1)))
            };
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        return;
                    }
                }
                _ = tokio::time::sleep(delay + jitter) => {}
            }
        }
    }

    async fn subscribe_once(
        &self,
        shutdown: &tokio::sync::watch::Receiver<bool>,
    ) -> Result<(), StatsStreamError> {
        let raw = (self.read_config)().map_err(|_| StatsStreamError::Unavailable)?;
        let cfg = parse_stats_stream_config(&raw)?;
        let mut request = Vec::new();
        append_tag(&mut request, 1, WIRE_VARINT);
        append_varint(&mut request, STATS_STREAM_INTERVAL_MS);
        let body = grpc_web_frame(&request);

        let req = self
            .client
            .post(&cfg.url)
            .header("Content-Type", "application/grpc-web+proto")
            .header("Accept", "application/grpc-web+proto")
            .header("X-Grpc-Web", "1")
            .header("Authorization", format!("Bearer {}", cfg.secret))
            .body(body)
            .build()
            .map_err(|e| StatsStreamError::Other(e.to_string()))?;

        let subscribed_at = Utc::now();
        let res = self
            .client
            .execute(req)
            .await
            .map_err(|e| StatsStreamError::Other(e.to_string()))?;

        if res.status() == reqwest::StatusCode::NOT_FOUND {
            return Err(StatsStreamError::Unavailable);
        }
        if res.status() != reqwest::StatusCode::OK {
            return Err(StatsStreamError::Other(format!(
                "api service returned HTTP {}",
                res.status()
            )));
        }
        if let Some(status) = res.headers().get("grpc-status") {
            let status = status.to_str().unwrap_or("");
            let message = res
                .headers()
                .get("grpc-message")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            if let Err(e) = grpc_status_error(status, message) {
                return Err(e);
            }
        }

        let mut session = StreamSession::new(subscribed_at);
        session.guard_inbounds = parse_reality_guard_inbounds(&raw);

        let mut stream = res.bytes_stream();
        use futures_util::StreamExt;
        let mut buf: Vec<u8> = Vec::new();
        let result = async {
            loop {
                if *shutdown.borrow() {
                    return Ok(());
                }
                // Try to read a frame from buf first.
                while buf.len() >= 5 {
                    let flags = buf[0];
                    let length = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]);
                    if flags & 0x01 != 0 {
                        return Err(StatsStreamError::Other(
                            "compressed grpc-web frame not supported".into(),
                        ));
                    }
                    if length > STATS_STREAM_MAX_FRAME {
                        return Err(StatsStreamError::Other(format!(
                            "grpc-web frame of {length} bytes exceeds limit"
                        )));
                    }
                    let total = 5 + length as usize;
                    if buf.len() < total {
                        break;
                    }
                    let payload = buf[5..total].to_vec();
                    let trailer = flags & 0x80 != 0;
                    buf.drain(..total);
                    if trailer {
                        trailer_status(&payload)?;
                        return Ok(());
                    }
                    let events = decode_connection_events(&payload).map_err(|e| {
                        StatsStreamError::Other(format!("decode connection events: {e}"))
                    })?;
                    self.sampler.set_stats_stream_available(true);
                    self.sampler
                        .apply_stream_events(&mut session, events, Utc::now());
                }
                match stream.next().await {
                    Some(Ok(chunk)) => buf.extend_from_slice(&chunk),
                    Some(Err(e)) => {
                        return Err(StatsStreamError::Other(e.to_string()));
                    }
                    None => {
                        return Err(StatsStreamError::Other("unexpected EOF".into()));
                    }
                }
            }
        }
        .await;
        self.sampler.set_stats_stream_available(false);
        result
    }
}

impl super::TrafficSamplerInner {
    pub(crate) fn apply_stream_events(
        &mut self,
        session: &mut StreamSession,
        events: ConnectionEvents,
        now: DateTime<Utc>,
    ) {
        self.activate_connection_accounting(&session.guard_inbounds);
        let mut active: Option<HashMap<String, bool>> = None;
        if events.reset {
            active = Some(HashMap::with_capacity(events.events.len()));
            session.totals.clear();
        }
        for event in events.events {
            let mut id = event.id.clone();
            if id.is_empty() {
                if let Some(ref c) = event.connection {
                    id = c.id.clone();
                }
            }
            if id.is_empty() {
                continue;
            }
            match event.type_ {
                CONNECTION_EVENT_NEW => {
                    let Some(ref connection) = event.connection else {
                        continue;
                    };
                    let totals = stream_totals(connection);
                    let created = DateTime::from_timestamp_millis(connection.created_at)
                        .unwrap_or(DateTime::<Utc>::UNIX_EPOCH);
                    let baseline = created < self.started_at;
                    self.observe_connection_from(
                        &id,
                        &connection.inbound,
                        &connection.inbound_type,
                        &connection.user,
                        totals,
                        baseline,
                        created,
                        now,
                        Some(&session.guard_inbounds),
                    );
                    if connection.closed_at != 0 {
                        self.close_connection(&id);
                        continue;
                    }
                    session.totals.insert(id.clone(), totals);
                    if let Some(ref mut a) = active {
                        a.insert(id, true);
                    }
                }
                CONNECTION_EVENT_UPDATE => {
                    let Some(totals) = session.totals.get_mut(&id) else {
                        continue;
                    };
                    totals[0] += non_negative(event.uplink_delta);
                    totals[1] += non_negative(event.downlink_delta);
                    let totals = *totals;
                    self.observe_connection(&id, "", "", totals, false, now);
                }
                CONNECTION_EVENT_CLOSED => {
                    if let Some(ref connection) = event.connection {
                        let created = DateTime::from_timestamp_millis(connection.created_at)
                            .unwrap_or(DateTime::<Utc>::UNIX_EPOCH);
                        let baseline = created < self.started_at;
                        self.observe_connection_from(
                            &id,
                            &connection.inbound,
                            &connection.inbound_type,
                            &connection.user,
                            stream_totals(connection),
                            baseline,
                            created,
                            now,
                            Some(&session.guard_inbounds),
                        );
                    } else if let Some(totals) = session.totals.get(&id).copied() {
                        self.observe_connection(&id, "", "", totals, false, now);
                    }
                    session.totals.remove(&id);
                    self.close_connection(&id);
                }
                _ => {}
            }
        }
        if let Some(active) = active {
            self.close_missing(&active, session.subscribed_at);
        }
    }
}

fn stream_totals(connection: &StreamConnection) -> [u64; 2] {
    [
        non_negative(connection.uplink_total),
        non_negative(connection.downlink_total),
    ]
}

fn non_negative(v: i64) -> u64 {
    if v < 0 {
        0
    } else {
        v as u64
    }
}

pub fn parse_stats_stream_config(raw: &[u8]) -> Result<StatsStreamConfig, StatsStreamError> {
    #[derive(serde::Deserialize)]
    struct Root {
        #[serde(default)]
        services: Vec<Service>,
    }
    #[derive(serde::Deserialize)]
    struct Service {
        #[serde(default, rename = "type")]
        type_: String,
        #[serde(default)]
        tag: String,
        #[serde(default)]
        listen: String,
        #[serde(default)]
        listen_port: i32,
        #[serde(default)]
        secret: String,
        #[serde(default)]
        tls: Option<Tls>,
    }
    #[derive(serde::Deserialize)]
    struct Tls {
        #[serde(default)]
        enabled: bool,
    }
    let root: Root =
        serde_json::from_slice(raw).map_err(|_| StatsStreamError::Unavailable)?;
    let mut found: Option<StatsStreamConfig> = None;
    for service in root.services {
        if service.type_ != "api"
            || service.secret.is_empty()
            || service.listen_port <= 0
            || service.listen_port > 65535
        {
            continue;
        }
        if service.tls.as_ref().map(|t| t.enabled).unwrap_or(false) {
            continue;
        }
        let mut host = service.listen.trim().trim_matches(|c| c == '[' || c == ']');
        match host {
            "localhost" => host = "127.0.0.1",
            _ => {
                let Ok(addr) = host.parse::<std::net::IpAddr>() else {
                    continue;
                };
                if !addr.is_loopback() {
                    continue;
                }
            }
        }
        let host_port = if host.contains(':') {
            format!("[{host}]:{}", service.listen_port)
        } else {
            format!("{host}:{}", service.listen_port)
        };
        let cfg = StatsStreamConfig {
            url: format!("http://{host_port}{STATS_STREAM_METHOD}"),
            secret: service.secret,
        };
        if service.tag == "panel-stats" {
            return Ok(cfg);
        }
        if found.is_none() {
            found = Some(cfg);
        }
    }
    found.ok_or(StatsStreamError::Unavailable)
}

// ---- protobuf wire helpers ----

const WIRE_VARINT: u8 = 0;
const WIRE_BYTES: u8 = 2;

fn append_tag(buf: &mut Vec<u8>, field: u64, wire: u8) {
    append_varint(buf, (field << 3) | wire as u64);
}

fn append_varint(buf: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        buf.push((v as u8) | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
}

#[cfg(test)]
fn append_bytes(buf: &mut Vec<u8>, data: &[u8]) {
    append_varint(buf, data.len() as u64);
    buf.extend_from_slice(data);
}

#[cfg(test)]
fn append_string(buf: &mut Vec<u8>, s: &str) {
    append_bytes(buf, s.as_bytes());
}

fn consume_varint(b: &[u8]) -> Result<(u64, usize), String> {
    let mut x = 0u64;
    let mut s = 0u32;
    for (i, &c) in b.iter().enumerate() {
        if i == 10 {
            return Err("varint overflow".into());
        }
        if c < 0x80 {
            if i == 9 && c > 1 {
                return Err("varint overflow".into());
            }
            return Ok((x | ((c as u64) << s), i + 1));
        }
        x |= ((c & 0x7f) as u64) << s;
        s += 7;
    }
    Err("unexpected EOF".into())
}

fn consume_tag(b: &[u8]) -> Result<(u64, u8, usize), String> {
    let (tag, n) = consume_varint(b)?;
    Ok((tag >> 3, (tag & 7) as u8, n))
}

fn consume_bytes(b: &[u8]) -> Result<(&[u8], usize), String> {
    let (len, n) = consume_varint(b)?;
    let len = len as usize;
    if b.len() < n + len {
        return Err("unexpected EOF".into());
    }
    Ok((&b[n..n + len], n + len))
}

fn skip_field(wire: u8, b: &[u8]) -> Result<usize, String> {
    match wire {
        WIRE_VARINT => {
            let (_, n) = consume_varint(b)?;
            Ok(n)
        }
        WIRE_BYTES => {
            let (_, n) = consume_bytes(b)?;
            Ok(n)
        }
        1 => {
            // fixed64
            if b.len() < 8 {
                return Err("unexpected EOF".into());
            }
            Ok(8)
        }
        5 => {
            if b.len() < 4 {
                return Err("unexpected EOF".into());
            }
            Ok(4)
        }
        _ => Err(format!("unsupported wire type {wire}")),
    }
}

fn walk_proto<F>(mut b: &[u8], mut visit: F) -> Result<(), String>
where
    F: FnMut(u64, u8, &[u8], u64) -> Result<(), String>,
{
    while !b.is_empty() {
        let (num, typ, n) = consume_tag(b)?;
        b = &b[n..];
        match typ {
            WIRE_VARINT => {
                let (varint, n) = consume_varint(b)?;
                b = &b[n..];
                visit(num, typ, &[], varint)?;
            }
            WIRE_BYTES => {
                let (value, n) = consume_bytes(b)?;
                // value is a subslice of original; advance b first conceptually
                let value = value.to_vec();
                b = &b[n..];
                visit(num, typ, &value, 0)?;
            }
            _ => {
                let n = skip_field(typ, b)?;
                b = &b[n..];
            }
        }
    }
    Ok(())
}

pub fn decode_connection_events(b: &[u8]) -> Result<ConnectionEvents, String> {
    let mut out = ConnectionEvents::default();
    walk_proto(b, |num, typ, value, varint| {
        match (num, typ) {
            (1, WIRE_BYTES) => {
                out.events.push(decode_connection_event(value)?);
            }
            (2, WIRE_VARINT) => {
                out.reset = varint != 0;
            }
            _ => {}
        }
        Ok(())
    })?;
    Ok(out)
}

fn decode_connection_event(b: &[u8]) -> Result<ConnectionEvent, String> {
    let mut out = ConnectionEvent::default();
    walk_proto(b, |num, typ, value, varint| {
        match (num, typ) {
            (1, WIRE_VARINT) => out.type_ = varint as i32,
            (2, WIRE_BYTES) => out.id = String::from_utf8_lossy(value).into_owned(),
            (3, WIRE_BYTES) => out.connection = Some(decode_stream_connection(value)?),
            (4, WIRE_VARINT) => out.uplink_delta = varint as i64,
            (5, WIRE_VARINT) => out.downlink_delta = varint as i64,
            (6, WIRE_VARINT) => out.closed_at = varint as i64,
            _ => {}
        }
        Ok(())
    })?;
    Ok(out)
}

fn decode_stream_connection(b: &[u8]) -> Result<StreamConnection, String> {
    let mut out = StreamConnection::default();
    walk_proto(b, |num, typ, value, varint| {
        match (num, typ) {
            (1, WIRE_BYTES) => out.id = String::from_utf8_lossy(value).into_owned(),
            (2, WIRE_BYTES) => out.inbound = String::from_utf8_lossy(value).into_owned(),
            (3, WIRE_BYTES) => out.inbound_type = String::from_utf8_lossy(value).into_owned(),
            (10, WIRE_BYTES) => out.user = String::from_utf8_lossy(value).into_owned(),
            (12, WIRE_VARINT) => out.created_at = varint as i64,
            (13, WIRE_VARINT) => out.closed_at = varint as i64,
            (16, WIRE_VARINT) => out.uplink_total = varint as i64,
            (17, WIRE_VARINT) => out.downlink_total = varint as i64,
            _ => {}
        }
        Ok(())
    })?;
    Ok(out)
}

pub fn grpc_web_frame(message: &[u8]) -> Vec<u8> {
    let mut frame = vec![0u8; 5 + message.len()];
    frame[1..5].copy_from_slice(&(message.len() as u32).to_be_bytes());
    frame[5..].copy_from_slice(message);
    frame
}

#[cfg(test)]
pub fn read_grpc_web_frame(r: &mut impl Read) -> Result<(Vec<u8>, bool), String> {
    let mut header = [0u8; 5];
    r.read_exact(&mut header).map_err(|e| e.to_string())?;
    let flags = header[0];
    if flags & 0x01 != 0 {
        return Err("compressed grpc-web frame not supported".into());
    }
    let length = u32::from_be_bytes([header[1], header[2], header[3], header[4]]);
    if length > STATS_STREAM_MAX_FRAME {
        return Err(format!("grpc-web frame of {length} bytes exceeds limit"));
    }
    let mut payload = vec![0u8; length as usize];
    r.read_exact(&mut payload).map_err(|e| e.to_string())?;
    Ok((payload, flags & 0x80 != 0))
}

fn grpc_status_error(status: &str, message: &str) -> Result<(), StatsStreamError> {
    let status = status.trim();
    if status.is_empty() || status == "0" {
        return Ok(());
    }
    if status == "12" || status == "16" {
        return Err(StatsStreamError::unavailable(format!(
            "grpc status {status} {message}"
        )));
    }
    Err(StatsStreamError::Other(format!(
        "grpc status {status}: {message}"
    )))
}

fn trailer_status(payload: &[u8]) -> Result<(), StatsStreamError> {
    // MIME headers: key: value\r\n
    let text = String::from_utf8_lossy(payload);
    let mut status = String::new();
    let mut message = String::new();
    for line in text.split(&['\r', '\n'][..]) {
        if let Some((k, v)) = line.split_once(':') {
            let k = k.trim().to_ascii_lowercase();
            let v = v.trim();
            if k == "grpc-status" {
                status = v.to_string();
            } else if k == "grpc-message" {
                message = v.to_string();
            }
        }
    }
    grpc_status_error(&status, &message)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode_test_connection(
        id: &str,
        inbound: &str,
        user: &str,
        inbound_type: &str,
        created_at: i64,
        closed: i64,
        up: i64,
        down: i64,
    ) -> Vec<u8> {
        let mut b = Vec::new();
        append_tag(&mut b, 1, WIRE_BYTES);
        append_string(&mut b, id);
        append_tag(&mut b, 2, WIRE_BYTES);
        append_string(&mut b, inbound);
        append_tag(&mut b, 3, WIRE_BYTES);
        let it = if inbound_type.is_empty() {
            "vless"
        } else {
            inbound_type
        };
        append_string(&mut b, it);
        if !user.is_empty() {
            append_tag(&mut b, 10, WIRE_BYTES);
            append_string(&mut b, user);
        }
        append_tag(&mut b, 12, WIRE_VARINT);
        append_varint(&mut b, created_at as u64);
        if closed != 0 {
            append_tag(&mut b, 13, WIRE_VARINT);
            append_varint(&mut b, closed as u64);
        }
        append_tag(&mut b, 16, WIRE_VARINT);
        append_varint(&mut b, up as u64);
        append_tag(&mut b, 17, WIRE_VARINT);
        append_varint(&mut b, down as u64);
        append_tag(&mut b, 21, WIRE_BYTES);
        append_string(&mut b, "direct");
        b
    }

    fn encode_test_events(
        reset: bool,
        events: &[(i32, &str, Option<Vec<u8>>, i64, i64)],
    ) -> Vec<u8> {
        let mut b = Vec::new();
        for (typ, id, conn, up, down) in events {
            let mut ev = Vec::new();
            append_tag(&mut ev, 1, WIRE_VARINT);
            append_varint(&mut ev, *typ as u64);
            append_tag(&mut ev, 2, WIRE_BYTES);
            append_string(&mut ev, id);
            if let Some(c) = conn {
                append_tag(&mut ev, 3, WIRE_BYTES);
                append_bytes(&mut ev, c);
            }
            if *up != 0 {
                append_tag(&mut ev, 4, WIRE_VARINT);
                append_varint(&mut ev, *up as u64);
            }
            if *down != 0 {
                append_tag(&mut ev, 5, WIRE_VARINT);
                append_varint(&mut ev, *down as u64);
            }
            append_tag(&mut b, 1, WIRE_BYTES);
            append_bytes(&mut b, &ev);
        }
        if reset {
            append_tag(&mut b, 2, WIRE_VARINT);
            append_varint(&mut b, 1);
        }
        b
    }

    #[test]
    fn parse_stats_stream_config_prefers_panel_stats() {
        let raw = br#"{"services":[
            {"type":"api","tag":"other","listen":"127.0.0.1","listen_port":1,"secret":"a"},
            {"type":"api","tag":"panel-stats","listen":"127.0.0.1","listen_port":29092,"secret":"s"}]}"#;
        let cfg = parse_stats_stream_config(raw).unwrap();
        assert_eq!(
            cfg.url,
            "http://127.0.0.1:29092/daemon.StartedService/SubscribeConnections"
        );
        assert_eq!(cfg.secret, "s");
        for raw in [
            br#"{}"# as &[u8],
            br#"{"services":[{"type":"api","listen":"127.0.0.1","listen_port":29092}]}"#,
            br#"{"services":[{"type":"api","listen":"0.0.0.0","listen_port":29092,"secret":"s"}]}"#,
            br#"{"services":[{"type":"api","listen":"127.0.0.1","listen_port":29092,"secret":"s","tls":{"enabled":true}}]}"#,
            br#"{"services":[{"type":"ssm-api","listen":"127.0.0.1","listen_port":29092,"secret":"s"}]}"#,
        ] {
            assert!(matches!(
                parse_stats_stream_config(raw),
                Err(StatsStreamError::Unavailable)
            ));
        }
    }

    #[test]
    fn decode_connection_events_round_trip() {
        let conn = encode_test_connection(
            "a",
            "in",
            "alice",
            "",
            1700000000123,
            0,
            10,
            1 << 40,
        );
        let payload = encode_test_events(
            true,
            &[
                (CONNECTION_EVENT_NEW, "a", Some(conn), 0, 0),
                (CONNECTION_EVENT_UPDATE, "a", None, 5, 7),
                (CONNECTION_EVENT_CLOSED, "b", None, 0, 0),
            ],
        );
        let events = decode_connection_events(&payload).unwrap();
        assert!(events.reset);
        assert_eq!(events.events.len(), 3);
        let first = &events.events[0];
        let c = first.connection.as_ref().unwrap();
        assert_eq!(c.user, "alice");
        assert_eq!(c.inbound, "in");
        assert_eq!(c.inbound_type, "vless");
        assert_eq!(c.downlink_total, 1 << 40);
        assert_eq!(c.created_at, 1700000000123);
        assert_eq!(events.events[1].type_, CONNECTION_EVENT_UPDATE);
        assert_eq!(events.events[1].uplink_delta, 5);
        assert_eq!(events.events[1].downlink_delta, 7);
        assert_eq!(events.events[2].type_, CONNECTION_EVENT_CLOSED);
        assert!(events.events[2].connection.is_none());
        assert!(decode_connection_events(&[0x0a, 0x05, 0x01]).is_err());
    }

    #[test]
    fn read_grpc_web_frame_limits() {
        let mut frames = grpc_web_frame(b"abc");
        let mut trailer = grpc_web_frame(b"grpc-status: 0\r\ngrpc-message: \r\n");
        trailer[0] = 0x80;
        frames.extend_from_slice(&trailer);
        let mut r = Cursor::new(frames);
        let (payload, is_trailer) = read_grpc_web_frame(&mut r).unwrap();
        assert!(!is_trailer);
        assert_eq!(payload, b"abc");
        let (payload, is_trailer) = read_grpc_web_frame(&mut r).unwrap();
        assert!(is_trailer);
        assert!(trailer_status(&payload).is_ok());

        let mut compressed = grpc_web_frame(b"x");
        compressed[0] = 0x01;
        assert!(read_grpc_web_frame(&mut Cursor::new(compressed)).is_err());
        let huge = [0u8, 0xff, 0xff, 0xff, 0xff];
        assert!(read_grpc_web_frame(&mut Cursor::new(huge)).is_err());
        assert!(matches!(
            trailer_status(b"grpc-status: 16\r\ngrpc-message: unauthorized"),
            Err(StatsStreamError::Unavailable)
        ));
    }

    #[test]
    fn ledger_stream_after_clash_does_not_double_count() {
        use crate::traffic::mod_tests_helpers::clash_conn;
        use crate::traffic::{ClashTrafficResponse, TrafficSampler};

        let sampler = TrafficSampler::new();
        let t0 = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        {
            let mut s = sampler.inner.lock().unwrap();
            s.started_at = t0;
            let created = t0 + chrono::Duration::seconds(1);
            s.ingest(
                "e",
                &ClashTrafficResponse {
                    upload_total: 0,
                    download_total: 0,
                    connections: vec![clash_conn("x", "in", 0, 0, created)],
                },
                t0 + chrono::Duration::seconds(2),
            );
            s.ingest(
                "e",
                &ClashTrafficResponse {
                    upload_total: 0,
                    download_total: 0,
                    connections: vec![clash_conn("x", "in", 40, 400, created)],
                },
                t0 + chrono::Duration::seconds(3),
            );
            let mut session = StreamSession::new(t0);
            // Stream reports the same cumulative total Clash already counted.
            s.apply_stream_events(
                &mut session,
                ConnectionEvents {
                    reset: false,
                    events: vec![ConnectionEvent {
                        type_: CONNECTION_EVENT_NEW,
                        id: "x".into(),
                        connection: Some(StreamConnection {
                            id: "x".into(),
                            inbound: "in".into(),
                            inbound_type: "vless".into(),
                            user: String::new(),
                            created_at: created.timestamp_millis(),
                            closed_at: 0,
                            uplink_total: 40,
                            downlink_total: 400,
                        }),
                        ..Default::default()
                    }],
                },
                t0 + chrono::Duration::seconds(4),
            );
            let port = s.pending_ports.get("in").cloned().unwrap();
            assert_eq!((port.upload, port.download), (40, 400));
        }
    }
}
