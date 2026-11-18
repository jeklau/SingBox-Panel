//! Host metrics reported to the panel (Linux /proc; degrades gracefully).

use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const PUBLIC_IPV4_ENDPOINTS: &[&str] = &[
    "https://api4.ipify.org",
    "https://ipv4.icanhazip.com",
];

/// Snapshot of host metrics reported to the panel.
#[derive(Debug, Clone, Default)]
pub struct SysInfo {
    pub hostname: String,
    pub os: String,
    pub arch: String,
    pub kernel: String,
    pub load1: f64,
    pub mem_used: u64,
    pub mem_total: u64,
    pub uptime: i64,
}

/// Gathers host metrics. Arch is Go-style (`amd64` / `arm64`) for panel downloads.
pub async fn collect_sys_info() -> SysInfo {
    let mut si = SysInfo {
        arch: go_arch(),
        ..Default::default()
    };
    si.hostname = hostname::get()
        .ok()
        .and_then(|h| h.into_string().ok())
        .unwrap_or_default();
    si.os = os_pretty_name();
    if let Ok(out) = crate::exec::run_checked("uname", &["-r"]).await {
        si.kernel = out;
    }
    si.load1 = load_avg1();
    let (used, total) = mem_info();
    si.mem_used = used;
    si.mem_total = total;
    si.uptime = uptime_seconds();
    si
}

/// Asks IPv4-only endpoints for the host's public address over tcp4.
pub async fn detect_public_ipv4() -> String {
    detect_public_ipv4_from(PUBLIC_IPV4_ENDPOINTS).await
}

async fn detect_public_ipv4_from(endpoints: &[&str]) -> String {
    for endpoint in endpoints {
        match fetch_ipv4_body(endpoint).await {
            Ok(body) => {
                let trimmed = body.trim();
                if let Ok(addr) = std::net::Ipv4Addr::from_str(trimmed) {
                    return addr.to_string();
                }
            }
            Err(_) => continue,
        }
    }
    String::new()
}

async fn fetch_ipv4_body(endpoint: &str) -> anyhow::Result<String> {
    let url = url::Url::parse(endpoint)?;
    let host = url
        .host_str()
        .ok_or_else(|| anyhow::anyhow!("missing host"))?
        .to_string();
    let port = url.port_or_known_default().unwrap_or(443);
    let use_tls = url.scheme() == "https";

    // Resolve A records only (IPv4), matching Go DialContext(..., "tcp4", ...).
    let mut addrs = tokio::net::lookup_host((host.as_str(), port)).await?;
    let addr: SocketAddr = addrs
        .find(|a| a.is_ipv4())
        .ok_or_else(|| anyhow::anyhow!("no IPv4 for {host}"))?;

    let connect = TcpStream::connect(addr);
    let stream = tokio::time::timeout(Duration::from_secs(2), connect).await??;
    stream.set_nodelay(true).ok();

    let path = if url.path().is_empty() {
        "/"
    } else {
        url.path()
    };

    let req = format!(
        "GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\nUser-Agent: singbox-panel-agent\r\n\r\n"
    );

    let body = if use_tls {
        let mut root_store = rustls::RootCertStore::empty();
        root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        let config = rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();
        let connector = tokio_rustls::TlsConnector::from(std::sync::Arc::new(config));
        let server_name = rustls::pki_types::ServerName::try_from(host.clone())
            .map_err(|_| anyhow::anyhow!("bad SNI"))?;
        let handshake = connector.connect(server_name, stream);
        let mut tls = tokio::time::timeout(Duration::from_secs(2), handshake).await??;
        tls.write_all(req.as_bytes()).await?;
        read_http_body_limited(&mut tls, 64).await?
    } else {
        let mut stream = stream;
        stream.write_all(req.as_bytes()).await?;
        read_http_body_limited(&mut stream, 64).await?
    };

    Ok(body)
}

async fn read_http_body_limited<S: AsyncReadExt + Unpin>(
    stream: &mut S,
    limit: usize,
) -> anyhow::Result<String> {
    let mut buf = Vec::with_capacity(512);
    let mut tmp = [0u8; 256];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, stream.read(&mut tmp)).await {
            Ok(Ok(0)) => break,
            Ok(Ok(n)) => {
                buf.extend_from_slice(&tmp[..n]);
                if buf.len() > 4096 {
                    break;
                }
            }
            Ok(Err(_)) | Err(_) => break,
        }
        // Enough to parse headers + tiny body.
        if buf.windows(4).any(|w| w == b"\r\n\r\n") && buf.len() > 64 {
            // Keep reading a little more for the body, then stop.
            if let Ok(Ok(n)) =
                tokio::time::timeout(Duration::from_millis(200), stream.read(&mut tmp)).await
            {
                if n > 0 {
                    buf.extend_from_slice(&tmp[..n]);
                }
            }
            break;
        }
    }

    let text = String::from_utf8_lossy(&buf);
    if let Some(idx) = text.find("\r\n\r\n") {
        let status_line = text.lines().next().unwrap_or("");
        if !status_line.contains(" 200") {
            anyhow::bail!("non-200 response: {status_line}");
        }
        let body = text[idx + 4..].trim();
        let body: String = body.chars().take(limit).collect();
        Ok(body)
    } else {
        anyhow::bail!("incomplete HTTP response")
    }
}

/// Maps Rust/`uname` arch names to Go GOARCH values the panel expects.
pub fn go_arch() -> String {
    match std::env::consts::ARCH {
        "x86_64" => "amd64".to_string(),
        "aarch64" => "arm64".to_string(),
        other => other.to_string(),
    }
}

fn os_pretty_name() -> String {
    let data = std::fs::read_to_string("/etc/os-release").ok();
    if let Some(content) = data {
        for line in content.lines() {
            if let Some(v) = line.strip_prefix("PRETTY_NAME=") {
                return v.trim().trim_matches('"').to_string();
            }
        }
    }
    std::env::consts::OS.to_string()
}

fn load_avg1() -> f64 {
    let data = match std::fs::read_to_string("/proc/loadavg") {
        Ok(d) => d,
        Err(_) => return 0.0,
    };
    data.split_whitespace()
        .next()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0.0)
}

fn mem_info() -> (u64, u64) {
    let f = match std::fs::File::open("/proc/meminfo") {
        Ok(f) => f,
        Err(_) => return (0, 0),
    };
    use std::io::{BufRead, BufReader};
    let mut mem_total = 0u64;
    let mut mem_avail = 0u64;
    for line in BufReader::new(f).lines().flatten() {
        let mut fields = line.split_whitespace();
        let key = match fields.next() {
            Some(k) => k,
            None => continue,
        };
        let val = match fields.next().and_then(|v| v.parse::<u64>().ok()) {
            Some(v) => v,
            None => continue,
        };
        match key {
            "MemTotal:" => mem_total = val,
            "MemAvailable:" => mem_avail = val,
            _ => {}
        }
    }
    // values are in kB.
    mem_total *= 1024;
    mem_avail *= 1024;
    if mem_avail > mem_total {
        mem_avail = mem_total;
    }
    (mem_total - mem_avail, mem_total)
}

fn uptime_seconds() -> i64 {
    let data = match std::fs::read_to_string("/proc/uptime") {
        Ok(d) => d,
        Err(_) => return 0,
    };
    data.split_whitespace()
        .next()
        .and_then(|s| s.parse::<f64>().ok())
        .map(|v| v as i64)
        .unwrap_or(0)
}
