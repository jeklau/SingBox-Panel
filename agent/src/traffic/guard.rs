//! Reality-guard inbound exclusion and connection-accounting node totals.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde_json::{json, Value};

use super::ledger::LedgerEntry;
use super::TrafficSamplerInner;

/// Only the generated direct → loopback VLESS/REALITY chain is redundant.
pub fn parse_reality_guard_inbounds(raw: &[u8]) -> HashMap<String, bool> {
    #[derive(Clone, Debug)]
    struct Inbound {
        type_: String,
        #[allow(dead_code)]
        tag: String,
        listen: String,
        listen_port: i64,
        #[allow(dead_code)]
        override_address: String,
        override_port: i64,
        tls_enabled: bool,
        reality_enabled: bool,
    }

    let Ok(root) = serde_json::from_slice::<Value>(raw) else {
        return HashMap::new();
    };
    // Empty map = known unguarded layout; we never return "nil" in Rust — empty
    // HashMap is the unguarded sentinel (same as Go's empty non-nil map).
    let mut guards = HashMap::new();

    let Some(outbounds) = root.get("outbounds").and_then(|v| v.as_array()) else {
        return guards;
    };
    let mut direct = 0;
    for outbound in outbounds {
        let tag = outbound.get("tag").and_then(|v| v.as_str()).unwrap_or("");
        if tag == "direct" {
            let typ = outbound.get("type").and_then(|v| v.as_str()).unwrap_or("");
            if typ != "direct" {
                return guards;
            }
            direct += 1;
        }
    }
    if direct != 1 {
        return guards;
    }

    let Some(raw_inbounds) = root.get("inbounds").and_then(|v| v.as_array()) else {
        return guards;
    };
    let mut inbounds: HashMap<String, Inbound> = HashMap::new();
    let mut by_port: HashMap<i64, Vec<Inbound>> = HashMap::new();
    let mut raw_inbound_map: HashMap<String, Value> = HashMap::new();
    for raw_in in raw_inbounds {
        let tag = raw_in
            .get("tag")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if tag.is_empty() {
            return guards;
        }
        if inbounds.contains_key(&tag) {
            return guards;
        }
        let tls = raw_in.get("tls");
        let inb = Inbound {
            type_: raw_in
                .get("type")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            tag: tag.clone(),
            listen: raw_in
                .get("listen")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            listen_port: raw_in
                .get("listen_port")
                .and_then(|v| v.as_i64())
                .unwrap_or(0),
            override_address: raw_in
                .get("override_address")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            override_port: raw_in
                .get("override_port")
                .and_then(|v| v.as_i64())
                .unwrap_or(0),
            tls_enabled: tls
                .and_then(|t| t.get("enabled"))
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
            reality_enabled: tls
                .and_then(|t| t.get("reality"))
                .and_then(|r| r.get("enabled"))
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
        };
        by_port
            .entry(inb.listen_port)
            .or_default()
            .push(inb.clone());
        raw_inbound_map.insert(tag.clone(), raw_in.clone());
        inbounds.insert(tag, inb);
    }

    let Some(rules) = root
        .get("route")
        .and_then(|r| r.get("rules"))
        .and_then(|v| v.as_array())
    else {
        return guards;
    };

    let mut i = 0;
    while i + 2 < rules.len() {
        let sniff = &rules[i];
        let inbound_arr = sniff.get("inbound").and_then(|v| v.as_array());
        let Some(inbound_arr) = inbound_arr else {
            break;
        };
        if inbound_arr.len() != 1 {
            break;
        }
        let Some(tag) = inbound_arr[0].as_str() else {
            break;
        };
        let Some(gate) = inbounds.get(tag).cloned() else {
            break;
        };
        if gate.type_ != "direct"
            || gate.listen_port < 1
            || gate.listen_port > 65535
            || gate.override_port < 1
            || gate.override_port > 65535
            || gate.listen_port == gate.override_port
            || by_port.get(&gate.listen_port).map(|v| v.len()).unwrap_or(0) != 1
            || by_port
                .get(&gate.override_port)
                .map(|v| v.len())
                .unwrap_or(0)
                != 1
        {
            break;
        }
        let target = by_port.get(&gate.override_port).unwrap()[0].clone();
        if target.type_ != "vless"
            || target.listen != "127.0.0.1"
            || !target.tls_enabled
            || !target.reality_enabled
        {
            break;
        }
        let expected_gate = json!({
            "type": "direct",
            "tag": tag,
            "listen": "::",
            "listen_port": gate.listen_port,
            "network": "tcp",
            "override_address": "127.0.0.1",
            "override_port": gate.override_port,
        });
        if !traffic_json_equal(raw_inbound_map.get(tag).unwrap(), &expected_gate) {
            break;
        }
        let Some(timeout_str) = sniff.get("timeout").and_then(|v| v.as_str()) else {
            break;
        };
        if parse_go_duration(timeout_str).map(|d| d <= 0.0).unwrap_or(true) {
            break;
        }
        let expected_sniff = json!({
            "inbound": [tag],
            "action": "sniff",
            "sniffer": ["tls"],
            "timeout": timeout_str,
        });
        if !traffic_json_equal(sniff, &expected_sniff) {
            break;
        }
        let allow = &rules[i + 1];
        let allow_rules = allow.get("rules").and_then(|v| v.as_array());
        let Some(allow_rules) = allow_rules else {
            break;
        };
        if allow_rules.len() != 2 {
            break;
        }
        let domain = allow_rules[0]
            .get("domain")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        if domain.is_empty() {
            break;
        }
        let expected_allow = json!({
            "type": "logical",
            "mode": "and",
            "action": "route",
            "outbound": "direct",
            "rules": [
                {"inbound": [tag], "domain": domain},
                {"ip_cidr": ["127.0.0.1/32"], "port": [gate.override_port]},
            ],
        });
        if !traffic_json_equal(allow, &expected_allow) {
            break;
        }
        let deny = &rules[i + 2];
        let expected_deny = json!({
            "inbound": [tag],
            "action": "reject",
            "method": "drop",
        });
        if !traffic_json_equal(deny, &expected_deny) {
            break;
        }
        guards.insert(tag.to_string(), true);
        i += 3;
    }
    guards
}

fn traffic_json_equal(raw: &Value, expected: &Value) -> bool {
    // Canonicalize via marshal round-trip like Go's json.Marshal equality.
    let Ok(got) = serde_json::to_string(raw) else {
        return false;
    };
    let Ok(want) = serde_json::to_string(expected) else {
        return false;
    };
    // Also compare normalized Value equality (key order independent).
    if got == want {
        return true;
    }
    // Re-parse both for order-independent compare.
    let Ok(got_v): Result<Value, _> = serde_json::from_str(&got) else {
        return false;
    };
    let Ok(want_v): Result<Value, _> = serde_json::from_str(&want) else {
        return false;
    };
    got_v == want_v
}

/// Parse a subset of Go duration strings ("300ms", "1s", "2m", …).
fn parse_go_duration(s: &str) -> Option<f64> {
    if s.is_empty() {
        return None;
    }
    // Try common suffixes
    let units: &[(&str, f64)] = &[
        ("ns", 1e-9),
        ("us", 1e-6),
        ("µs", 1e-6),
        ("ms", 1e-3),
        ("s", 1.0),
        ("m", 60.0),
        ("h", 3600.0),
    ];
    for (suffix, mult) in units {
        if let Some(num) = s.strip_suffix(suffix) {
            let v: f64 = num.parse().ok()?;
            return Some(v * mult);
        }
    }
    None
}

impl TrafficSamplerInner {
    /// Once activated, keep this accounting boundary until the Agent exits.
    pub(crate) fn activate_connection_accounting(&mut self, guards: &HashMap<String, bool>) {
        if guards.is_empty() || self.connection_accounting {
            return;
        }
        self.connection_accounting = true;
        self.node_window = [0, 0];
    }

    /// Without the stream's final CLOSED totals, polling alone cannot observe
    /// all short-lived connections under connection-accounting mode.
    pub(crate) fn traffic_available(&self) -> bool {
        self.available
            && self.have_sample
            && (!self.connection_accounting
                || (self.node_have_sample && self.stats_stream_available))
    }

    pub(crate) fn observe_node_traffic(
        &mut self,
        entry: &mut LedgerEntry,
        created_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) {
        if !self.connection_accounting {
            return;
        }
        if self.node_have_sample && !entry.guard_classified {
            return;
        }
        if !entry.node_observed {
            entry.node_observed = true;
            if created_at.timestamp() == 0 || created_at < self.node_started_at {
                entry.node_counted = entry.counted;
            }
        }
        let mut delta = [0u64; 2];
        for i in 0..2 {
            let total = entry.counted[i];
            if total > entry.node_counted[i] {
                delta[i] = total - entry.node_counted[i];
                entry.node_counted[i] = total;
            }
        }
        if !self.node_have_sample || entry.guard_outer {
            return;
        }
        {
            let bucket = self.bucket(now);
            bucket.upload += delta[0];
            bucket.download += delta[1];
        }
        for i in 0..2 {
            self.node_counted[i] += delta[i];
            self.node_window[i] += delta[i];
        }
    }

    pub(crate) fn node_totals(&self) -> (u64, u64) {
        if self.connection_accounting {
            (self.node_counted[0], self.node_counted[1])
        } else {
            (self.last_upload, self.last_download)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_config_is_unguarded_not_unknown() {
        let got = parse_reality_guard_inbounds(b"{}");
        assert!(got.is_empty());
    }

    #[test]
    fn recognizes_minimal_generated_guard_layout() {
        let raw = r#"{
            "outbounds":[{"type":"direct","tag":"direct"}],
            "inbounds":[
                {"type":"direct","tag":"front","listen":"::","listen_port":31000,"network":"tcp",
                 "override_address":"127.0.0.1","override_port":31001},
                {"type":"vless","tag":"secure","listen":"127.0.0.1","listen_port":31001,
                 "tls":{"enabled":true,"reality":{"enabled":true}}}
            ],
            "route":{"rules":[
                {"inbound":["front"],"action":"sniff","sniffer":["tls"],"timeout":"300ms"},
                {"type":"logical","mode":"and","action":"route","outbound":"direct",
                 "rules":[
                    {"inbound":["front"],"domain":["music.apple.com"]},
                    {"ip_cidr":["127.0.0.1/32"],"port":[31001]}
                 ]},
                {"inbound":["front"],"action":"reject","method":"drop"}
            ]}
        }"#;
        let guards = parse_reality_guard_inbounds(raw.as_bytes());
        assert_eq!(guards.get("front"), Some(&true));
    }

    #[test]
    fn does_not_infer_from_public_override() {
        let raw = r#"{
            "outbounds":[{"type":"direct","tag":"direct"}],
            "inbounds":[
                {"type":"direct","tag":"front","listen":"::","listen_port":31000,"network":"tcp",
                 "override_address":"192.0.2.1","override_port":31001},
                {"type":"vless","tag":"secure","listen":"127.0.0.1","listen_port":31001,
                 "tls":{"enabled":true,"reality":{"enabled":true}}}
            ],
            "route":{"rules":[
                {"inbound":["front"],"action":"sniff","sniffer":["tls"],"timeout":"300ms"},
                {"type":"logical","mode":"and","action":"route","outbound":"direct",
                 "rules":[
                    {"inbound":["front"],"domain":["music.apple.com"]},
                    {"ip_cidr":["127.0.0.1/32"],"port":[31001]}
                 ]},
                {"inbound":["front"],"action":"reject","method":"drop"}
            ]}
        }"#;
        assert!(parse_reality_guard_inbounds(raw.as_bytes()).is_empty());
    }
}
