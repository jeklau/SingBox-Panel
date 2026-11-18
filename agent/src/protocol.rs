//! Fixed message set exchanged between the Panel and Agents over the reverse
//! WebSocket channel. Field names and message types must stay panel-compatible.

use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

/// Loopback-only Clash API endpoint used by managed sing-box configs.
pub const LOCAL_TRAFFIC_ADDRESS: &str = "127.0.0.1:29091";

/// Loopback-only sing-box 1.14+ API service for per-user connection accounting.
pub const LOCAL_STATS_API_ADDRESS: &str = "127.0.0.1:29092";

/// Traffic accounting protocol version used by current agents.
/// Version 2 accounts only timestamped, acknowledged batches.
pub const ACCOUNTING_VERSION: i32 = 2;

/// Message type string constants (panel wire format).
pub mod msg {
    pub const CMD_INSTALL_SINGBOX: &str = "install_singbox";
    pub const CMD_APPLY_CONFIG: &str = "apply_config";
    pub const CMD_SERVICE_ACTION: &str = "service_action";
    pub const CMD_GET_STATUS: &str = "get_status";
    pub const CMD_GET_CONFIG: &str = "get_config";
    pub const CMD_UPDATE_AGENT: &str = "update_agent";
    pub const CMD_TEST_OUTBOUND: &str = "test_outbound";
    pub const CMD_GET_LOGS: &str = "get_logs";
    pub const CMD_STREAM_LOGS: &str = "stream_logs";
    pub const CMD_UNINSTALL_SINGBOX: &str = "uninstall_singbox";
    pub const CMD_UNINSTALL_AGENT: &str = "uninstall_agent";
    pub const EVT_TRAFFIC_ACK: &str = "traffic_ack";

    pub const EVT_REGISTER: &str = "register";
    pub const EVT_HEARTBEAT: &str = "heartbeat";
    pub const EVT_COMMAND_RESULT: &str = "command_result";
    pub const EVT_LOG: &str = "log";
    pub const EVT_TRAFFIC: &str = "traffic";
    pub const EVT_PROGRESS: &str = "progress";
}

/// MessageType enumerates the fixed command/event set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MessageType {
    #[serde(rename = "install_singbox")]
    InstallSingbox,
    #[serde(rename = "apply_config")]
    ApplyConfig,
    #[serde(rename = "service_action")]
    ServiceAction,
    #[serde(rename = "get_status")]
    GetStatus,
    #[serde(rename = "get_config")]
    GetConfig,
    #[serde(rename = "update_agent")]
    UpdateAgent,
    #[serde(rename = "test_outbound")]
    TestOutbound,
    #[serde(rename = "get_logs")]
    GetLogs,
    #[serde(rename = "stream_logs")]
    StreamLogs,
    #[serde(rename = "uninstall_singbox")]
    UninstallSingbox,
    #[serde(rename = "uninstall_agent")]
    UninstallAgent,
    #[serde(rename = "traffic_ack")]
    TrafficAck,
    #[serde(rename = "register")]
    Register,
    #[serde(rename = "heartbeat")]
    Heartbeat,
    #[serde(rename = "command_result")]
    CommandResult,
    #[serde(rename = "log")]
    Log,
    #[serde(rename = "traffic")]
    Traffic,
    #[serde(rename = "progress")]
    Progress,
}

impl MessageType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::InstallSingbox => msg::CMD_INSTALL_SINGBOX,
            Self::ApplyConfig => msg::CMD_APPLY_CONFIG,
            Self::ServiceAction => msg::CMD_SERVICE_ACTION,
            Self::GetStatus => msg::CMD_GET_STATUS,
            Self::GetConfig => msg::CMD_GET_CONFIG,
            Self::UpdateAgent => msg::CMD_UPDATE_AGENT,
            Self::TestOutbound => msg::CMD_TEST_OUTBOUND,
            Self::GetLogs => msg::CMD_GET_LOGS,
            Self::StreamLogs => msg::CMD_STREAM_LOGS,
            Self::UninstallSingbox => msg::CMD_UNINSTALL_SINGBOX,
            Self::UninstallAgent => msg::CMD_UNINSTALL_AGENT,
            Self::TrafficAck => msg::EVT_TRAFFIC_ACK,
            Self::Register => msg::EVT_REGISTER,
            Self::Heartbeat => msg::EVT_HEARTBEAT,
            Self::CommandResult => msg::EVT_COMMAND_RESULT,
            Self::Log => msg::EVT_LOG,
            Self::Traffic => msg::EVT_TRAFFIC,
            Self::Progress => msg::EVT_PROGRESS,
        }
    }
}

/// Envelope is the wire format for every message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    #[serde(rename = "type")]
    pub type_: MessageType,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<Box<RawValue>>,
}

impl Envelope {
    /// Marshals payload into an Envelope. A `None` payload yields no field.
    pub fn new(t: MessageType, id: impl Into<String>, payload: Option<&impl Serialize>) -> anyhow::Result<Self> {
        let mut e = Self {
            type_: t,
            id: id.into(),
            payload: None,
        };
        if let Some(p) = payload {
            let raw = serde_json::to_string(p)?;
            e.payload = Some(RawValue::from_string(raw)?);
        }
        Ok(e)
    }

    /// Convenience: build with a serializable payload value.
    pub fn with_payload(t: MessageType, id: impl Into<String>, payload: &impl Serialize) -> anyhow::Result<Self> {
        Self::new(t, id, Some(payload))
    }

    /// Convenience: build with no payload.
    pub fn without_payload(t: MessageType, id: impl Into<String>) -> Self {
        Self {
            type_: t,
            id: id.into(),
            payload: None,
        }
    }

    /// Unmarshals the envelope payload into `T`.
    /// An empty/missing payload leaves `T::default()` semantics to the caller
    /// by deserializing nothing — matching Go's `Decode` (no-op on empty).
    pub fn decode_into<T: for<'de> Deserialize<'de>>(&self, dest: &mut T) -> anyhow::Result<()> {
        match &self.payload {
            None => Ok(()),
            Some(raw) => {
                *dest = serde_json::from_str(raw.get())?;
                Ok(())
            }
        }
    }

    /// Unmarshals the envelope payload into a fresh `T`. Empty payload yields `T::default()`.
    pub fn decode<T: for<'de> Deserialize<'de> + Default>(&self) -> anyhow::Result<T> {
        match &self.payload {
            None => Ok(T::default()),
            Some(raw) => Ok(serde_json::from_str(raw.get())?),
        }
    }
}

// ---- Install channel / method constants ----

pub const CHANNEL_STABLE: &str = "stable";
pub const CHANNEL_BETA: &str = "beta";

pub const METHOD_SCRIPT: &str = "script";
pub const METHOD_APT: &str = "apt";
pub const METHOD_DNF: &str = "dnf";

pub const SVC_START: &str = "start";
pub const SVC_STOP: &str = "stop";
pub const SVC_RESTART: &str = "restart";
pub const SVC_RELOAD: &str = "reload";
pub const SVC_ENABLE: &str = "enable";
pub const SVC_DISABLE: &str = "disable";
pub const SVC_STATUS: &str = "status";

// ---- Panel -> Agent payloads ----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InstallSingboxCmd {
    pub channel: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub version: String,
    pub method: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApplyConfigCmd {
    pub config: Box<RawValue>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceActionCmd {
    pub action: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestOutboundCmd {
    pub host: String,
    pub port: i32,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub network: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub obfuscated: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestOutboundData {
    pub ok: bool,
    pub latency_ms: i64,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub inconclusive: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub note: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetLogsCmd {
    #[serde(default, skip_serializing_if = "is_zero_i32")]
    pub lines: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogsData {
    pub text: String,
}

// ---- Agent -> Panel payloads ----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisterEvt {
    pub agent_version: String,
    pub hostname: String,
    pub os: String,
    pub arch: String,
    pub kernel: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub public_ip: String,
    pub singbox_installed: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub singbox_version: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub panel_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatEvt {
    pub ts: i64,
    pub load1: f64,
    pub mem_used: u64,
    pub mem_total: u64,
    pub uptime: i64,
    pub singbox_active: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub singbox_version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub traffic: Option<TrafficSnapshot>,
}

/// TrafficSnapshot contains sing-box cumulative node counters and per-inbound
/// deltas. Accounting version 2 uses only timestamped, acknowledged batches.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TrafficSnapshot {
    #[serde(default, skip_serializing_if = "is_zero_i32")]
    pub accounting_version: i32,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub unavailable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub report: Option<TrafficReport>,
    pub upload_total: u64,
    pub download_total: u64,
    pub upload_rate: u64,
    pub download_rate: u64,
    pub tcp_connections: i32,
    pub udp_connections: i32,
    pub sampled_at: i64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub peak_upload_rate: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub peak_download_rate: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ports: Vec<PortTrafficSnapshot>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub users: Vec<UserTrafficSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrafficReport {
    pub session: String,
    pub sequence: u64,
    pub buckets: Vec<TrafficTimeBucket>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TrafficAck {
    pub session: String,
    pub sequence: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TrafficTimeBucket {
    pub time: i64,
    pub upload: u64,
    pub download: u64,
    pub upload_rate: u64,
    pub download_rate: u64,
    pub tcp_connections: i32,
    pub udp_connections: i32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub ports: Vec<PortTrafficSnapshot>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub users: Vec<UserTrafficSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserTrafficSnapshot {
    pub inbound: String,
    pub user: String,
    pub upload: u64,
    pub download: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortTrafficSnapshot {
    pub inbound: String,
    pub upload: u64,
    pub download: u64,
    pub upload_rate: u64,
    pub download_rate: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandResultEvt {
    pub id: String,
    pub ok: bool,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub error: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub output: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Box<RawValue>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InboundBrief {
    pub tag: String,
    #[serde(rename = "type")]
    pub type_: String,
    pub listen_port: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StatusData {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub agent_version: String,
    pub singbox_installed: bool,
    pub singbox_version: String,
    pub service_active: bool,
    pub service_enabled: bool,
    pub has_config: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inbounds: Vec<InboundBrief>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub hostname: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub os: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub arch: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub kernel: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub public_ip: String,
    #[serde(default, skip_serializing_if = "is_zero_f64")]
    pub load1: f64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub mem_used: u64,
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub mem_total: u64,
    #[serde(default, skip_serializing_if = "is_zero_i64")]
    pub uptime: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigData {
    pub path: String,
    pub exists: bool,
    pub raw: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub inbounds: Vec<InboundBrief>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEvt {
    pub level: String,
    pub msg: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrafficEvt {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub traffic: Option<TrafficSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgressEvt {
    pub id: String,
    pub line: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub stream: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamLogsCmd {
    pub enable: bool,
    #[serde(default, skip_serializing_if = "is_zero_i32")]
    pub lines: i32,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub session_id: String,
}

fn is_zero_i32(v: &i32) -> bool {
    *v == 0
}
fn is_zero_i64(v: &i64) -> bool {
    *v == 0
}
fn is_zero_u64(v: &u64) -> bool {
    *v == 0
}
fn is_zero_f64(v: &f64) -> bool {
    *v == 0.0
}
