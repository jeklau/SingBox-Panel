//! Landing-server reachability probes (Go `testoutbound.go`).
//!
//! TCP connect for ordinary landings; QUIC version-negotiation for hysteria2/tuic.
//! Obfuscated QUIC only validates DNS and reports inconclusive.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use rand::RngCore;
use tokio::net::{TcpStream, UdpSocket};

use crate::protocol::{TestOutboundCmd, TestOutboundData};

/// When true, probes may target loopback (unit tests only).
pub static ALLOW_LOOPBACK_TARGET_FOR_TEST: AtomicBool = AtomicBool::new(false);

const TEST_OUTBOUND_TIMEOUT: Duration = Duration::from_secs(8);
const QUIC_PROBE_ATTEMPTS: u32 = 3;
const QUIC_PROBE_SIZE: usize = 1200;
const QUIC_PROBE_VERSION: u32 = 0x1a2a3a4a;

/// Checks whether a landing server is reachable from this node.
pub async fn test_outbound(cmd: TestOutboundCmd) -> TestOutboundData {
    let host = cmd.host.trim().to_string();
    let port = cmd.port;
    if host.is_empty() || port <= 0 || port > 65535 {
        return TestOutboundData {
            ok: false,
            latency_ms: 0,
            error: "落地地址或端口无效".into(),
            inconclusive: false,
            note: String::new(),
        };
    }
    if !ALLOW_LOOPBACK_TARGET_FOR_TEST.load(Ordering::Relaxed) && is_disallowed_probe_host(&host) {
        return TestOutboundData {
            ok: false,
            latency_ms: 0,
            error: "落地地址不能指向本机回环或未指定地址".into(),
            inconclusive: false,
            note: String::new(),
        };
    }
    let addr = join_host_port(&host, port);

    match tokio::time::timeout(TEST_OUTBOUND_TIMEOUT, async {
        if cmd.network == "quic" {
            if cmd.obfuscated {
                resolve_only(&host).await
            } else {
                probe_quic(&addr).await
            }
        } else {
            probe_tcp(&addr).await
        }
    })
    .await
    {
        Ok(data) => data,
        Err(_) => TestOutboundData {
            ok: false,
            latency_ms: TEST_OUTBOUND_TIMEOUT.as_millis() as i64,
            error: "probe timed out".into(),
            inconclusive: false,
            note: String::new(),
        },
    }
}

fn join_host_port(host: &str, port: i32) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

fn is_disallowed_probe_host(raw: &str) -> bool {
    let mut host = raw.trim().trim_matches(|c| c == '[' || c == ']').to_lowercase();
    if let Some(stripped) = host.strip_suffix('.') {
        host = stripped.to_string();
    }
    if let Some(zone_at) = host.rfind('%') {
        host.truncate(zone_at);
    }
    if host == "localhost" || host.ends_with(".localhost") {
        return true;
    }
    if let Ok(ip) = host.parse::<IpAddr>() {
        return ip.is_loopback() || ip.is_unspecified();
    }
    false
}

async fn resolve_filtered(addr: &str) -> Result<Vec<SocketAddr>, String> {
    let allow_loopback = ALLOW_LOOPBACK_TARGET_FOR_TEST.load(Ordering::Relaxed);
    let mut addrs = tokio::net::lookup_host(addr)
        .await
        .map_err(|e| e.to_string())?
        .collect::<Vec<_>>();
    if addrs.is_empty() {
        return Err("no addresses resolved".into());
    }
    if !allow_loopback {
        addrs.retain(|a| {
            let ip = a.ip();
            !(ip.is_loopback() || ip.is_unspecified())
        });
        if addrs.is_empty() {
            return Err("落地地址解析到本机回环或未指定地址，已拒绝探测".into());
        }
    }
    Ok(addrs)
}

async fn probe_tcp(addr: &str) -> TestOutboundData {
    let start = Instant::now();
    let addrs = match resolve_filtered(addr).await {
        Ok(a) => a,
        Err(e) => {
            return TestOutboundData {
                ok: false,
                latency_ms: start.elapsed().as_millis() as i64,
                error: e,
                inconclusive: false,
                note: String::new(),
            };
        }
    };
    let mut last_err = String::new();
    for a in addrs {
        match TcpStream::connect(a).await {
            Ok(_stream) => {
                return TestOutboundData {
                    ok: true,
                    latency_ms: start.elapsed().as_millis() as i64,
                    error: String::new(),
                    inconclusive: false,
                    note: String::new(),
                };
            }
            Err(e) => last_err = e.to_string(),
        }
    }
    TestOutboundData {
        ok: false,
        latency_ms: start.elapsed().as_millis() as i64,
        error: last_err,
        inconclusive: false,
        note: String::new(),
    }
}

async fn resolve_only(host: &str) -> TestOutboundData {
    let start = Instant::now();
    if host.parse::<IpAddr>().is_err() {
        // Resolve via dummy port for lookup_host.
        let lookup = format!("{host}:0");
        let resolved = tokio::net::lookup_host(lookup.as_str()).await;
        match resolved {
            Ok(iter) => {
                let allow_loopback = ALLOW_LOOPBACK_TARGET_FOR_TEST.load(Ordering::Relaxed);
                for a in iter {
                    if !allow_loopback {
                        let ip = a.ip();
                        if ip.is_loopback() || ip.is_unspecified() {
                            return TestOutboundData {
                                ok: false,
                                latency_ms: start.elapsed().as_millis() as i64,
                                error: "落地地址解析到本机回环或未指定地址，已拒绝探测"
                                    .into(),
                                inconclusive: false,
                                note: String::new(),
                            };
                        }
                    }
                }
            }
            Err(e) => {
                return TestOutboundData {
                    ok: false,
                    latency_ms: start.elapsed().as_millis() as i64,
                    error: e.to_string(),
                    inconclusive: false,
                    note: String::new(),
                };
            }
        }
    }
    TestOutboundData {
        ok: false,
        latency_ms: 0,
        error: String::new(),
        inconclusive: true,
        note: "已启用混淆的 QUIC 落地不会响应未握手的探测包，仅验证了地址解析，无法确认 UDP 端口是否可达"
            .into(),
    }
}

async fn probe_quic(addr: &str) -> TestOutboundData {
    let start = Instant::now();
    let addrs = match resolve_filtered(addr).await {
        Ok(a) => a,
        Err(e) => {
            return TestOutboundData {
                ok: false,
                latency_ms: start.elapsed().as_millis() as i64,
                error: e,
                inconclusive: false,
                note: String::new(),
            };
        }
    };
    let target = addrs[0];
    let bind = if target.is_ipv4() {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0)
    } else {
        SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0)
    };
    let sock = match UdpSocket::bind(bind).await {
        Ok(s) => s,
        Err(e) => {
            return TestOutboundData {
                ok: false,
                latency_ms: start.elapsed().as_millis() as i64,
                error: e.to_string(),
                inconclusive: false,
                note: String::new(),
            };
        }
    };
    if let Err(e) = sock.connect(target).await {
        return TestOutboundData {
            ok: false,
            latency_ms: start.elapsed().as_millis() as i64,
            error: e.to_string(),
            inconclusive: false,
            note: String::new(),
        };
    }

    let (packet, dcid, scid) = match build_quic_probe() {
        Ok(v) => v,
        Err(e) => {
            return TestOutboundData {
                ok: false,
                latency_ms: 0,
                error: e,
                inconclusive: false,
                note: String::new(),
            };
        }
    };

    let deadline = Instant::now() + TEST_OUTBOUND_TIMEOUT;
    let slice = TEST_OUTBOUND_TIMEOUT / QUIC_PROBE_ATTEMPTS;
    let mut buf = vec![0u8; 2048];

    for attempt in 0..QUIC_PROBE_ATTEMPTS {
        let sent = Instant::now();
        if let Err(e) = sock.send(&packet).await {
            return TestOutboundData {
                ok: false,
                latency_ms: start.elapsed().as_millis() as i64,
                error: e.to_string(),
                inconclusive: false,
                note: String::new(),
            };
        }
        let mut attempt_end = sent + slice;
        if attempt == QUIC_PROBE_ATTEMPTS - 1 || attempt_end > deadline {
            attempt_end = deadline;
        }
        loop {
            let wait = attempt_end.saturating_duration_since(Instant::now());
            if wait.is_zero() {
                break;
            }
            match tokio::time::timeout(wait, sock.recv(&mut buf)).await {
                Ok(Ok(n)) => {
                    if is_quic_version_negotiation(&buf[..n], &dcid, &scid) {
                        return TestOutboundData {
                            ok: true,
                            latency_ms: sent.elapsed().as_millis() as i64,
                            error: String::new(),
                            inconclusive: false,
                            note: String::new(),
                        };
                    }
                    // Unrelated datagram: keep waiting within this attempt.
                    continue;
                }
                Ok(Err(e)) => {
                    let msg = e.to_string();
                    if msg.contains("Connection refused")
                        || e.raw_os_error() == Some(libc::ECONNREFUSED)
                    {
                        return TestOutboundData {
                            ok: false,
                            latency_ms: sent.elapsed().as_millis() as i64,
                            error: "UDP 端口未开放（对端返回 ICMP 端口不可达）".into(),
                            inconclusive: false,
                            note: String::new(),
                        };
                    }
                    return TestOutboundData {
                        ok: false,
                        latency_ms: start.elapsed().as_millis() as i64,
                        error: msg,
                        inconclusive: false,
                        note: String::new(),
                    };
                }
                Err(_) => break, // attempt timeout → retransmit
            }
        }
        if Instant::now() >= deadline {
            break;
        }
    }

    TestOutboundData {
        ok: false,
        latency_ms: start.elapsed().as_millis() as i64,
        error: "未收到 QUIC 响应：UDP 端口可能未开放或被防火墙丢弃".into(),
        inconclusive: false,
        note: String::new(),
    }
}

fn build_quic_probe() -> Result<(Vec<u8>, Vec<u8>, Vec<u8>), String> {
    let mut packet = vec![0u8; QUIC_PROBE_SIZE];
    rand::thread_rng().fill_bytes(&mut packet);
    packet[0] = 0xc0 | (packet[0] & 0x3f);
    packet[1..5].copy_from_slice(&QUIC_PROBE_VERSION.to_be_bytes());
    packet[5] = 8;
    let dcid = packet[6..14].to_vec();
    packet[14] = 8;
    let scid = packet[15..23].to_vec();
    Ok((packet, dcid, scid))
}

fn is_quic_version_negotiation(p: &[u8], dcid: &[u8], scid: &[u8]) -> bool {
    if p.len() < 7 || p[0] & 0x80 == 0 {
        return false;
    }
    let version = u32::from_be_bytes([p[1], p[2], p[3], p[4]]);
    if version != 0 {
        return false;
    }
    let mut off = 5;
    let dl = p[off] as usize;
    off += 1;
    if p.len() < off + dl + 1 || &p[off..off + dl] != scid {
        return false;
    }
    off += dl;
    let sl = p[off] as usize;
    off += 1;
    if p.len() < off + sl || &p[off..off + sl] != dcid {
        return false;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_loopback_hosts() {
        assert!(is_disallowed_probe_host("127.0.0.1"));
        assert!(is_disallowed_probe_host("localhost"));
        assert!(is_disallowed_probe_host("::1"));
        assert!(is_disallowed_probe_host("0.0.0.0"));
        assert!(!is_disallowed_probe_host("1.1.1.1"));
    }

    #[test]
    fn quic_probe_layout() {
        let (pkt, dcid, scid) = build_quic_probe().unwrap();
        assert_eq!(pkt.len(), QUIC_PROBE_SIZE);
        assert_eq!(dcid.len(), 8);
        assert_eq!(scid.len(), 8);
        assert_eq!(pkt[0] & 0xc0, 0xc0);
    }

    #[test]
    fn version_negotiation_detect() {
        let (_, dcid, scid) = build_quic_probe().unwrap();
        let mut resp = vec![0x80, 0, 0, 0, 0, scid.len() as u8];
        resp.extend_from_slice(&scid);
        resp.push(dcid.len() as u8);
        resp.extend_from_slice(&dcid);
        assert!(is_quic_version_negotiation(&resp, &dcid, &scid));
    }
}
