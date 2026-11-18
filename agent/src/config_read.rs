//! Read `/etc/sing-box/config.json` (size-capped) and parse inbound briefs.

use crate::paths::{CONFIG_FILE, MAX_CONFIG_SIZE};
use crate::protocol::InboundBrief;

/// Reads the official config file and extracts an inbound summary.
/// `exists` is false when no config is present.
pub fn read_config() -> (Vec<u8>, Vec<InboundBrief>, bool) {
    match read_config_file() {
        Ok(data) => {
            let briefs = parse_inbound_briefs(&data);
            (data, briefs, true)
        }
        Err(_) => (Vec::new(), Vec::new(), false),
    }
}

/// Reads `/etc/sing-box/config.json` capped at `MAX_CONFIG_SIZE`.
pub fn read_config_file() -> anyhow::Result<Vec<u8>> {
    use std::io::Read;
    let mut f = std::fs::File::open(CONFIG_FILE)?;
    let mut data = Vec::new();
    // Read one byte past the limit so we can detect overflow (matches Go LimitReader).
    let limit = (MAX_CONFIG_SIZE as usize).saturating_add(1);
    let mut limited = f.by_ref().take(limit as u64);
    limited.read_to_end(&mut data)?;
    if data.len() > MAX_CONFIG_SIZE as usize {
        anyhow::bail!("config exceeds {MAX_CONFIG_SIZE}-byte limit");
    }
    Ok(data)
}

pub fn parse_inbound_briefs(data: &[u8]) -> Vec<InboundBrief> {
    #[derive(serde::Deserialize)]
    struct Cfg {
        #[serde(default)]
        inbounds: Vec<Ib>,
    }
    #[derive(serde::Deserialize)]
    struct Ib {
        #[serde(default)]
        tag: String,
        #[serde(default, rename = "type")]
        type_: String,
        #[serde(default)]
        listen_port: i32,
    }

    let cfg: Cfg = match serde_json::from_slice(data) {
        Ok(c) => c,
        Err(_) => return Vec::new(),
    };
    cfg.inbounds
        .into_iter()
        .map(|ib| InboundBrief {
            tag: ib.tag,
            type_: ib.type_,
            listen_port: ib.listen_port,
        })
        .collect()
}
