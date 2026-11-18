//! Local traffic sampling: Clash poll + shared ledger + UTC-minute buckets.
//! Faithful port of `internal/services/agent/traffic*.go` and `statsstream.go`.

mod buckets;
mod guard;
mod ledger;
mod stats_stream;

pub use stats_stream::StatsStream;

use std::collections::HashMap;
use std::fs;
use std::net::IpAddr;
use std::path::Path;
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

use chrono::{DateTime, Utc};
use serde::Deserialize;

use crate::paths::CONFIG_FILE;
use crate::protocol::{
    PortTrafficSnapshot, TrafficAck, TrafficReport, TrafficSnapshot, UserTrafficSnapshot,
    ACCOUNTING_VERSION,
};

use buckets::CollectedTrafficBucket;
use ledger::{LedgerEntry, UserTrafficKey};

/// Bounds one `/connections` response (16 MiB).
const MAX_TRAFFIC_RESPONSE_SIZE: u64 = 16 << 20;

/// Public traffic sampler API (Clash poll + ledger + accounting reports).
pub struct TrafficSampler {
    pub(crate) inner: Mutex<TrafficSamplerInner>,
    client: reqwest::Client,
}

pub(crate) struct TrafficSamplerInner {
    pub(crate) buckets: HashMap<i64, CollectedTrafficBucket>,
    pub(crate) accounting_session: String,
    pub(crate) report_sequence: u64,
    pub(crate) report: Option<TrafficReport>,
    pub(crate) connection_accounting: bool,
    pub(crate) stats_stream_available: bool,
    pub(crate) node_have_sample: bool,
    pub(crate) node_started_at: DateTime<Utc>,
    pub(crate) node_counted: [u64; 2],
    pub(crate) node_window: [u64; 2],

    pub(crate) endpoint: String,
    pub(crate) last_upload: u64,
    pub(crate) last_download: u64,
    pub(crate) last_sample: DateTime<Utc>,
    pub(crate) have_sample: bool,

    pub(crate) ledger: HashMap<String, LedgerEntry>,
    pub(crate) tombstones: HashMap<String, LedgerEntry>,
    pub(crate) tombstone_order: Vec<String>,
    pub(crate) started_at: DateTime<Utc>,
    pub(crate) have_connections: bool,
    pub(crate) port_window: HashMap<String, [u64; 2]>,
    pub(crate) pending_ports: HashMap<String, PortTrafficSnapshot>,
    pub(crate) pending_users: HashMap<UserTrafficKey, UserTrafficSnapshot>,
    pub(crate) available: bool,
    pub(crate) upload_rate: u64,
    pub(crate) download_rate: u64,
    pub(crate) peak_upload: u64,
    pub(crate) peak_download: u64,
    pub(crate) tcp_connections: i32,
    pub(crate) udp_connections: i32,

    config_cached: bool,
    config_dev: u64,
    config_ino: u64,
    config_mod_time: SystemTime,
    config_size: u64,
    config_checked_at: DateTime<Utc>,
    config_value: LocalTrafficConfig,
}

#[derive(Clone, Default)]
pub(crate) struct LocalTrafficConfig {
    pub endpoint: String,
    pub secret: String,
    pub guard_inbounds: HashMap<String, bool>,
}

#[derive(Debug, Default, Deserialize)]
pub(crate) struct ClashTrafficResponse {
    #[serde(default, rename = "uploadTotal")]
    pub(crate) upload_total: u64,
    #[serde(default, rename = "downloadTotal")]
    pub(crate) download_total: u64,
    #[serde(default)]
    pub(crate) connections: Vec<ClashConnection>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct ClashConnection {
    #[serde(default)]
    pub(crate) id: String,
    #[serde(default)]
    pub(crate) upload: u64,
    #[serde(default)]
    pub(crate) download: u64,
    #[serde(default)]
    pub(crate) start: Option<DateTime<Utc>>,
    #[serde(default)]
    pub(crate) metadata: ClashMetadata,
}

#[derive(Debug, Default, Deserialize)]
pub(crate) struct ClashMetadata {
    #[serde(default)]
    #[allow(dead_code)]
    pub(crate) network: String,
    #[serde(default, rename = "type")]
    pub(crate) type_: String,
}

impl TrafficSampler {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(TrafficSamplerInner::new()),
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(3))
                .build()
                .expect("reqwest client"),
        }
    }

    /// Samples Clash `/connections` every second until `shutdown` is true.
    pub async fn run(&self, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        self.poll().await;
        let mut ticker = tokio::time::interval(Duration::from_secs(1));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        return;
                    }
                }
                _ = ticker.tick() => {
                    self.poll().await;
                }
            }
        }
    }

    /// Heartbeat summary: node totals/rates only (no port/user deltas, no peak).
    pub fn summary_snapshot(&self) -> Option<TrafficSnapshot> {
        let s = self.inner.lock().expect("traffic lock");
        if !s.traffic_available() {
            return None;
        }
        let (upload, download) = s.node_totals();
        Some(TrafficSnapshot {
            accounting_version: ACCOUNTING_VERSION,
            upload_total: upload,
            download_total: download,
            upload_rate: s.upload_rate,
            download_rate: s.download_rate,
            tcp_connections: s.tcp_connections,
            udp_connections: s.udp_connections,
            sampled_at: s.last_sample.timestamp(),
            ..Default::default()
        })
    }

    /// Freezes ≤128 UTC minutes / ≤4096 port+user rows into an immutable report.
    pub fn accounting_snapshot(&self) -> TrafficSnapshot {
        self.accounting_snapshot_inner()
    }

    /// Clears the frozen report only when session+sequence match.
    pub fn acknowledge_report(&self, ack: TrafficAck) {
        let mut s = self.inner.lock().expect("traffic lock");
        s.acknowledge_report(ack);
    }

    /// Legacy pending-port/user ack (tests / older snapshot path).
    pub fn acknowledge(&self, snapshot: &TrafficSnapshot) {
        let mut s = self.inner.lock().expect("traffic lock");
        if s.peak_upload <= snapshot.peak_upload_rate {
            s.peak_upload = 0;
        }
        if s.peak_download <= snapshot.peak_download_rate {
            s.peak_download = 0;
        }
        let mut remove_ports = Vec::new();
        for sent in &snapshot.ports {
            let Some(pending) = s.pending_ports.get_mut(&sent.inbound) else {
                continue;
            };
            pending.upload = subtract_counter(pending.upload, sent.upload);
            pending.download = subtract_counter(pending.download, sent.download);
            if pending.upload_rate <= sent.upload_rate {
                pending.upload_rate = 0;
            }
            if pending.download_rate <= sent.download_rate {
                pending.download_rate = 0;
            }
            if pending.upload == 0
                && pending.download == 0
                && pending.upload_rate == 0
                && pending.download_rate == 0
            {
                remove_ports.push(sent.inbound.clone());
            }
        }
        for inbound in remove_ports {
            s.pending_ports.remove(&inbound);
        }
        let mut remove_users = Vec::new();
        for sent in &snapshot.users {
            let key = UserTrafficKey {
                inbound: sent.inbound.clone(),
                user: sent.user.clone(),
            };
            let Some(pending) = s.pending_users.get_mut(&key) else {
                continue;
            };
            pending.upload = subtract_counter(pending.upload, sent.upload);
            pending.download = subtract_counter(pending.download, sent.download);
            if pending.upload == 0 && pending.download == 0 {
                remove_users.push(key);
            }
        }
        for key in remove_users {
            s.pending_users.remove(&key);
        }
    }

    /// Full live snapshot including pending port/user deltas (does not drain).
    pub fn snapshot(&self) -> Option<TrafficSnapshot> {
        let mut s = self.inner.lock().expect("traffic lock");
        s.snapshot()
    }

    pub(crate) fn set_stats_stream_available(&self, available: bool) {
        let mut s = self.inner.lock().expect("traffic lock");
        s.stats_stream_available = available;
    }

    pub(crate) fn apply_stream_events(
        &self,
        session: &mut stats_stream::StreamSession,
        events: stats_stream::ConnectionEvents,
        now: DateTime<Utc>,
    ) {
        let mut s = self.inner.lock().expect("traffic lock");
        s.apply_stream_events(session, events, now);
    }

    async fn poll(&self) {
        let cfg = {
            let mut s = self.inner.lock().expect("traffic lock");
            match s.discover_local_traffic_config() {
                Ok(cfg) => cfg,
                Err(_) => {
                    s.available = false;
                    return;
                }
            }
        };
        let requested_at = Utc::now();
        let mut req = match self.client.get(&cfg.endpoint).build() {
            Ok(r) => r,
            Err(_) => {
                self.inner.lock().expect("traffic lock").available = false;
                return;
            }
        };
        if !cfg.secret.is_empty() {
            req.headers_mut().insert(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {}", cfg.secret)
                    .parse()
                    .unwrap_or(reqwest::header::HeaderValue::from_static("")),
            );
        }
        let res = match self.client.execute(req).await {
            Ok(r) => r,
            Err(_) => {
                self.inner.lock().expect("traffic lock").available = false;
                return;
            }
        };
        if res.status() != reqwest::StatusCode::OK {
            let _ = res.bytes().await;
            self.inner.lock().expect("traffic lock").available = false;
            return;
        }
        let body = match res.bytes().await {
            Ok(b) => b,
            Err(_) => {
                self.inner.lock().expect("traffic lock").available = false;
                return;
            }
        };
        if body.len() as u64 > MAX_TRAFFIC_RESPONSE_SIZE {
            self.inner.lock().expect("traffic lock").available = false;
            return;
        }
        let counters: ClashTrafficResponse = match serde_json::from_slice(&body) {
            Ok(c) => c,
            Err(_) => {
                self.inner.lock().expect("traffic lock").available = false;
                return;
            }
        };
        let now = Utc::now();
        let mut s = self.inner.lock().expect("traffic lock");
        s.ingest_with_layout_at(
            &cfg.endpoint,
            &counters,
            Some(&cfg.guard_inbounds),
            requested_at,
            now,
        );
    }
}

impl Default for TrafficSampler {
    fn default() -> Self {
        Self::new()
    }
}

impl TrafficSamplerInner {
    fn new() -> Self {
        Self {
            buckets: HashMap::new(),
            accounting_session: String::new(),
            report_sequence: 0,
            report: None,
            connection_accounting: false,
            stats_stream_available: false,
            node_have_sample: false,
            node_started_at: Utc::now(),
            node_counted: [0, 0],
            node_window: [0, 0],
            endpoint: String::new(),
            last_upload: 0,
            last_download: 0,
            last_sample: Utc::now(),
            have_sample: false,
            ledger: HashMap::new(),
            tombstones: HashMap::new(),
            tombstone_order: Vec::new(),
            started_at: Utc::now(),
            have_connections: false,
            port_window: HashMap::new(),
            pending_ports: HashMap::new(),
            pending_users: HashMap::new(),
            available: false,
            upload_rate: 0,
            download_rate: 0,
            peak_upload: 0,
            peak_download: 0,
            tcp_connections: 0,
            udp_connections: 0,
            config_cached: false,
            config_dev: 0,
            config_ino: 0,
            config_mod_time: SystemTime::UNIX_EPOCH,
            config_size: 0,
            config_checked_at: DateTime::<Utc>::UNIX_EPOCH,
            config_value: LocalTrafficConfig::default(),
        }
    }

    fn discover_local_traffic_config(&mut self) -> anyhow::Result<LocalTrafficConfig> {
        use std::os::unix::fs::MetadataExt;
        let meta = fs::metadata(CONFIG_FILE)?;
        let now = Utc::now();
        if self.config_cached
            && meta.dev() == self.config_dev
            && meta.ino() == self.config_ino
            && meta.len() == self.config_size
            && meta.modified().ok() == Some(self.config_mod_time)
            && now
                .signed_duration_since(self.config_checked_at)
                .num_seconds()
                < 30
        {
            return Ok(self.config_value.clone());
        }
        let raw = crate::config_read::read_config_file()?;
        let cfg = parse_local_traffic_config(&raw)?;
        self.config_cached = true;
        self.config_dev = meta.dev();
        self.config_ino = meta.ino();
        self.config_mod_time = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
        self.config_size = meta.len();
        self.config_checked_at = now;
        self.config_value = cfg.clone();
        Ok(cfg)
    }

    /// Fold one Clash `/connections` response (tests).
    #[cfg(test)]
    pub(crate) fn ingest(
        &mut self,
        endpoint: &str,
        counters: &ClashTrafficResponse,
        now: DateTime<Utc>,
    ) {
        self.ingest_with_layout_at(endpoint, counters, None, now, now);
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn ingest_at(
        &mut self,
        endpoint: &str,
        counters: &ClashTrafficResponse,
        requested_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) {
        self.ingest_with_layout_at(endpoint, counters, None, requested_at, now);
    }

    pub(crate) fn ingest_with_layout_at(
        &mut self,
        endpoint: &str,
        counters: &ClashTrafficResponse,
        guards: Option<&HashMap<String, bool>>,
        requested_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) {
        let empty_guards = HashMap::new();
        let guards_map = guards.unwrap_or(&empty_guards);
        self.activate_connection_accounting(guards_map);
        if !self.endpoint.is_empty() && self.endpoint != endpoint {
            self.have_sample = false;
            self.node_have_sample = false;
        }
        if !self.have_sample {
            self.started_at = now;
        }
        if self.connection_accounting && !self.node_have_sample {
            self.node_started_at = now;
        }

        let mut upload_rate = 0u64;
        let mut download_rate = 0u64;
        let mut sample_seconds = 1.0f64;
        if self.have_sample && self.endpoint == endpoint && now > self.last_sample {
            sample_seconds = (now - self.last_sample).num_milliseconds() as f64 / 1000.0;
            if sample_seconds > 0.0 && !self.connection_accounting {
                upload_rate = (counter_delta(counters.upload_total, self.last_upload) as f64
                    / sample_seconds) as u64;
                download_rate = (counter_delta(counters.download_total, self.last_download) as f64
                    / sample_seconds) as u64;
            }
        }
        if sample_seconds <= 0.0 {
            sample_seconds = 1.0;
        }
        let have_sample = self.have_sample;
        let connection_accounting = self.connection_accounting;
        let last_upload = self.last_upload;
        let last_download = self.last_download;
        if have_sample && !connection_accounting {
            let b = self.bucket(now);
            b.upload += counter_delta(counters.upload_total, last_upload);
            b.download += counter_delta(counters.download_total, last_download);
        }

        let mut present = HashMap::with_capacity(counters.connections.len());
        for connection in &counters.connections {
            if connection.id.is_empty() {
                continue;
            }
            present.insert(connection.id.clone(), true);
            let mut baseline = !self.have_connections;
            if let Some(start) = connection.start {
                baseline = start < self.started_at;
            }
            let inbound_type = connection
                .metadata
                .type_
                .split_once('/')
                .map(|(t, _)| t.to_string())
                .unwrap_or_default();
            let inbound = parse_inbound_tag(&connection.metadata.type_);
            let created_at = connection.start.unwrap_or_else(|| DateTime::<Utc>::UNIX_EPOCH);
            self.observe_connection_from(
                &connection.id,
                &inbound,
                &inbound_type,
                "",
                [connection.upload, connection.download],
                baseline,
                created_at,
                now,
                Some(guards_map),
            );
        }
        self.close_missing(&present, requested_at);
        if self.connection_accounting {
            upload_rate = (self.node_window[0] as f64 / sample_seconds) as u64;
            download_rate = (self.node_window[1] as f64 / sample_seconds) as u64;
            self.node_window = [0, 0];
        }
        {
            let b = self.bucket(now);
            b.upload_rate = b.upload_rate.max(upload_rate);
            b.download_rate = b.download_rate.max(download_rate);
        }
        let port_window: Vec<(String, [u64; 2])> =
            self.port_window.drain().collect();
        for (inbound, sample) in port_window {
            let delta = self.pending_ports.entry(inbound.clone()).or_insert_with(|| {
                PortTrafficSnapshot {
                    inbound: inbound.clone(),
                    upload: 0,
                    download: 0,
                    upload_rate: 0,
                    download_rate: 0,
                }
            });
            let up_rate = (sample[0] as f64 / sample_seconds) as u64;
            let down_rate = (sample[1] as f64 / sample_seconds) as u64;
            if up_rate > delta.upload_rate {
                delta.upload_rate = up_rate;
            }
            if down_rate > delta.download_rate {
                delta.download_rate = down_rate;
            }
            {
                let b = self.bucket(now);
                let port = b.ports.entry(inbound.clone()).or_insert_with(|| {
                    PortTrafficSnapshot {
                        inbound: inbound.clone(),
                        upload: 0,
                        download: 0,
                        upload_rate: 0,
                        download_rate: 0,
                    }
                });
                port.inbound = inbound.clone();
                port.upload_rate = port.upload_rate.max(up_rate);
                port.download_rate = port.download_rate.max(down_rate);
            }
        }
        self.have_connections = true;
        self.endpoint = endpoint.to_string();
        self.last_upload = counters.upload_total;
        self.last_download = counters.download_total;
        self.last_sample = now;
        self.have_sample = true;
        if self.connection_accounting {
            self.node_have_sample = true;
        }
        self.available = true;
        self.upload_rate = upload_rate;
        self.download_rate = download_rate;
        if upload_rate > self.peak_upload {
            self.peak_upload = upload_rate;
        }
        if download_rate > self.peak_download {
            self.peak_download = download_rate;
        }
        let (tcp, udp) = host_connection_counts();
        self.tcp_connections = tcp;
        self.udp_connections = udp;
        {
            let b = self.bucket(now);
            b.tcp_connections = tcp;
            b.udp_connections = udp;
        }
    }

    fn snapshot(&mut self) -> Option<TrafficSnapshot> {
        if !self.traffic_available() {
            return None;
        }
        let (upload, download) = self.node_totals();
        let mut snapshot = TrafficSnapshot {
            upload_total: upload,
            download_total: download,
            upload_rate: self.upload_rate,
            download_rate: self.download_rate,
            tcp_connections: self.tcp_connections,
            udp_connections: self.udp_connections,
            sampled_at: self.last_sample.timestamp(),
            peak_upload_rate: self.peak_upload,
            peak_download_rate: self.peak_download,
            ports: self.pending_ports.values().cloned().collect(),
            users: self.pending_users.values().cloned().collect(),
            ..Default::default()
        };
        snapshot.ports.sort_by(|a, b| a.inbound.cmp(&b.inbound));
        snapshot.users.sort_by(|a, b| {
            a.inbound
                .cmp(&b.inbound)
                .then_with(|| a.user.cmp(&b.user))
        });
        Some(snapshot)
    }

    #[cfg(test)]
    pub(crate) fn pending_user(&self, inbound: &str, user: &str) -> UserTrafficSnapshot {
        self.pending_users
            .get(&UserTrafficKey {
                inbound: inbound.to_string(),
                user: user.to_string(),
            })
            .cloned()
            .unwrap_or(UserTrafficSnapshot {
                inbound: String::new(),
                user: String::new(),
                upload: 0,
                download: 0,
            })
    }
}

fn parse_local_traffic_config(raw: &[u8]) -> anyhow::Result<LocalTrafficConfig> {
    #[derive(Deserialize)]
    struct Root {
        experimental: Option<Experimental>,
    }
    #[derive(Deserialize)]
    struct Experimental {
        clash_api: Option<ClashApi>,
    }
    #[derive(Deserialize)]
    struct ClashApi {
        external_controller: Option<String>,
        secret: Option<String>,
    }
    let root: Root = serde_json::from_slice(raw)?;
    let clash = root
        .experimental
        .and_then(|e| e.clash_api)
        .ok_or_else(|| anyhow::anyhow!("clash api disabled"))?;
    let address = clash
        .external_controller
        .unwrap_or_default()
        .trim()
        .to_string();
    if address.is_empty() {
        anyhow::bail!("clash api disabled");
    }
    let secret = clash.secret.unwrap_or_default();
    let (host, port) = split_host_port(&address)?;
    let mut host = host.trim_matches(|c| c == '[' || c == ']').to_string();
    match host.as_str() {
        "" | "0.0.0.0" | "localhost" => host = "127.0.0.1".into(),
        "::" => host = "::1".into(),
        _ => {
            let addr: IpAddr = host
                .parse()
                .map_err(|_| anyhow::anyhow!("clash api is not loopback-only"))?;
            if !addr.is_loopback() {
                anyhow::bail!("clash api is not loopback-only");
            }
        }
    }
    let host_port = if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    };
    Ok(LocalTrafficConfig {
        endpoint: format!("http://{host_port}/connections"),
        secret,
        guard_inbounds: guard::parse_reality_guard_inbounds(raw),
    })
}

fn split_host_port(address: &str) -> anyhow::Result<(String, String)> {
    // Handle [ipv6]:port and host:port
    if let Some(rest) = address.strip_prefix('[') {
        let (host, rest) = rest
            .split_once(']')
            .ok_or_else(|| anyhow::anyhow!("invalid clash api address"))?;
        let port = rest
            .strip_prefix(':')
            .ok_or_else(|| anyhow::anyhow!("invalid clash api address"))?;
        return Ok((host.to_string(), port.to_string()));
    }
    let (host, port) = address
        .rsplit_once(':')
        .ok_or_else(|| anyhow::anyhow!("invalid clash api address"))?;
    Ok((host.to_string(), port.to_string()))
}

fn parse_inbound_tag(connection_type: &str) -> String {
    connection_type
        .split_once('/')
        .map(|(_, tag)| tag.trim().to_string())
        .unwrap_or_default()
}

fn counter_delta(current: u64, previous: u64) -> u64 {
    if current >= previous {
        current - previous
    } else {
        current
    }
}

fn subtract_counter(current: u64, sent: u64) -> u64 {
    if current <= sent {
        0
    } else {
        current - sent
    }
}

/// Host-level TCP/UDP socket counts from `/proc/net/*` (excludes listening TCP).
pub fn host_connection_counts() -> (i32, i32) {
    let tcp = count_proc_sockets(Path::new("/proc/net/tcp"), true)
        + count_proc_sockets(Path::new("/proc/net/tcp6"), true);
    let udp = count_proc_sockets(Path::new("/proc/net/udp"), false)
        + count_proc_sockets(Path::new("/proc/net/udp6"), false);
    (tcp, udp)
}

pub fn count_proc_sockets(path: &Path, exclude_listen: bool) -> i32 {
    let Ok(data) = fs::read_to_string(path) else {
        return 0;
    };
    let mut count = 0i32;
    for (line_index, line) in data.lines().enumerate() {
        if line_index == 0 || line.trim().is_empty() {
            continue;
        }
        let fields: Vec<&str> = line.split_whitespace().collect();
        if fields.len() < 4 {
            continue;
        }
        if exclude_listen && fields[3] == "0A" {
            continue;
        }
        let sequence = fields[0].trim_end_matches(':');
        if sequence.parse::<u32>().is_ok() {
            count += 1;
        }
    }
    count
}

#[cfg(test)]
pub(crate) mod mod_tests_helpers {
    use super::*;

    pub(crate) fn clash_conn(
        id: &str,
        inbound: &str,
        up: u64,
        down: u64,
        start: DateTime<Utc>,
    ) -> ClashConnection {
        ClashConnection {
            id: id.into(),
            upload: up,
            download: down,
            start: Some(start),
            metadata: ClashMetadata {
                network: String::new(),
                type_: format!("vless/{inbound}"),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn traffic_conn(id: &str, inbound: &str, up: u64, down: u64) -> ClashConnection {
        ClashConnection {
            id: id.into(),
            upload: up,
            download: down,
            start: None,
            metadata: ClashMetadata {
                network: String::new(),
                type_: format!("vless/{inbound}"),
            },
        }
    }

    #[test]
    fn count_proc_sockets_accepts_sequence_colon() {
        let mut f = NamedTempFile::new().unwrap();
        writeln!(
            f,
            "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n\
               0: 0100007F:1F90 0200007F:C350 01 00000000:00000000 00:00000000 00000000 1000 0 12345 1\n\
               1: 00000000:1F91 00000000:0000 0A 00000000:00000000 00:00000000 00000000 1000 0 12346 1"
        )
        .unwrap();
        assert_eq!(count_proc_sockets(f.path(), false), 2);
        assert_eq!(count_proc_sockets(f.path(), true), 1);
    }

    #[test]
    fn traffic_snapshot_requires_acknowledgement_before_draining() {
        let sampler = TrafficSampler::new();
        {
            let mut s = sampler.inner.lock().unwrap();
            s.available = true;
            s.have_sample = true;
            s.last_sample = Utc::now();
            s.pending_ports.insert(
                "vless-in".into(),
                PortTrafficSnapshot {
                    inbound: "vless-in".into(),
                    upload: 120,
                    download: 80,
                    upload_rate: 50,
                    download_rate: 40,
                },
            );
        }
        let first = sampler.snapshot().unwrap();
        let second = sampler.snapshot().unwrap();
        assert_eq!(first.ports.len(), 1);
        assert_eq!(second.ports[0].upload, 120);

        {
            let mut s = sampler.inner.lock().unwrap();
            let pending = s.pending_ports.get_mut("vless-in").unwrap();
            pending.upload += 30;
            pending.download += 20;
            pending.upload_rate = 75;
        }
        sampler.acknowledge(&first);
        let remaining = sampler.snapshot().unwrap();
        assert_eq!(remaining.ports.len(), 1);
        assert_eq!(remaining.ports[0].upload, 30);
        assert_eq!(remaining.ports[0].download, 20);
        assert_eq!(remaining.ports[0].upload_rate, 75);
        sampler.acknowledge(&remaining);
        assert!(sampler.snapshot().unwrap().ports.is_empty());
    }

    #[test]
    fn traffic_summary_does_not_carry_port_deltas() {
        let sampler = TrafficSampler::new();
        {
            let mut s = sampler.inner.lock().unwrap();
            s.available = true;
            s.have_sample = true;
            s.last_sample = Utc::now();
            s.pending_ports.insert(
                "in".into(),
                PortTrafficSnapshot {
                    inbound: "in".into(),
                    upload: 10,
                    download: 0,
                    upload_rate: 0,
                    download_rate: 0,
                },
            );
        }
        let summary = sampler.summary_snapshot().unwrap();
        assert!(summary.ports.is_empty());
        assert_eq!(sampler.snapshot().unwrap().ports.len(), 1);
    }

    #[test]
    fn traffic_sampler_counts_new_and_short_lived_connections() {
        let sampler = TrafficSampler::new();
        let start = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        {
            let mut s = sampler.inner.lock().unwrap();
            s.ingest(
                "e",
                &ClashTrafficResponse {
                    upload_total: 100,
                    download_total: 1000,
                    connections: vec![traffic_conn("old", "in", 100, 1000)],
                },
                start,
            );
            s.ingest(
                "e",
                &ClashTrafficResponse {
                    upload_total: 160,
                    download_total: 1600,
                    connections: vec![
                        traffic_conn("old", "in", 110, 1100),
                        traffic_conn("new", "in", 50, 500),
                    ],
                },
                start + chrono::Duration::seconds(1),
            );
        }
        let snap = sampler.snapshot().unwrap();
        assert_eq!(snap.ports.len(), 1);
        let port = &snap.ports[0];
        assert_eq!((port.upload, port.download), (60, 600));
        assert_eq!((port.upload_rate, port.download_rate), (60, 600));
        assert_eq!((snap.peak_upload_rate, snap.peak_download_rate), (60, 600));
    }

    #[test]
    fn traffic_sampler_peak_survives_until_acknowledged() {
        let sampler = TrafficSampler::new();
        let start = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        {
            let mut s = sampler.inner.lock().unwrap();
            s.ingest(
                "e",
                &ClashTrafficResponse {
                    upload_total: 0,
                    download_total: 0,
                    connections: vec![],
                },
                start,
            );
            s.ingest(
                "e",
                &ClashTrafficResponse {
                    upload_total: 0,
                    download_total: 5000,
                    connections: vec![],
                },
                start + chrono::Duration::seconds(1),
            );
            s.ingest(
                "e",
                &ClashTrafficResponse {
                    upload_total: 0,
                    download_total: 5100,
                    connections: vec![],
                },
                start + chrono::Duration::seconds(2),
            );
        }
        let snap = sampler.snapshot().unwrap();
        assert_eq!(snap.download_rate, 100);
        assert_eq!(snap.peak_download_rate, 5000);
        assert_eq!(
            sampler.summary_snapshot().unwrap().peak_download_rate,
            0
        );
        sampler.acknowledge(&snap);
        assert_eq!(sampler.snapshot().unwrap().peak_download_rate, 0);
    }

    #[test]
    fn parse_local_traffic_config_loopback_only() {
        let raw = br#"{"experimental":{"clash_api":{"external_controller":"127.0.0.1:29091","secret":"s"}}}"#;
        let cfg = parse_local_traffic_config(raw).unwrap();
        assert_eq!(cfg.endpoint, "http://127.0.0.1:29091/connections");
        assert_eq!(cfg.secret, "s");
        let bad = br#"{"experimental":{"clash_api":{"external_controller":"0.0.0.0:29091"}}}"#;
        // 0.0.0.0 remapped to 127.0.0.1 — allowed
        assert!(parse_local_traffic_config(bad).is_ok());
        let public = br#"{"experimental":{"clash_api":{"external_controller":"1.2.3.4:29091"}}}"#;
        assert!(parse_local_traffic_config(public).is_err());
    }
}
