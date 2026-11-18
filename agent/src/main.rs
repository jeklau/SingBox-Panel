//! SingBox Panel VPS-side daemon. Reverse-connects to the panel and manages
//! the official sing-box on the host.

use std::collections::HashMap;
use std::path::PathBuf;

use clap::Parser;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use agent::client::validate_panel_url;
use agent::paths::AGENT_CONF_FILE;
use agent::{agent as agent_run, agent::Config};

fn agent_version() -> String {
    if let Ok(v) = std::env::var("AGENT_VERSION") {
        let v = v.trim();
        if !v.is_empty() {
            return v.to_string();
        }
    }
    option_env!("AGENT_VERSION")
        .unwrap_or("v1.1.0")
        .to_string()
}

#[derive(Debug, Parser)]
#[command(name = "singbox-panel-agent", disable_version_flag = true)]
struct Cli {
    /// Panel base URL, e.g. https://panel.example.com
    #[arg(long = "url")]
    url: Option<String>,

    /// Agent token
    #[arg(long = "token")]
    token: Option<String>,

    /// Agent config file (KEY=VALUE)
    #[arg(long = "config", default_value = AGENT_CONF_FILE)]
    config: PathBuf,

    /// Skip TLS verification (development environment only)
    #[arg(long = "insecure", default_value_t = false)]
    insecure: bool,

    /// Print version and exit
    #[arg(long = "version", default_value_t = false)]
    version: bool,
}

#[tokio::main]
async fn main() {
    // rustls 0.23 requires an explicit process-wide crypto provider.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let cli = Cli::parse();
    let version = agent_version();

    if cli.version {
        println!("singbox-panel-agent {version}");
        return;
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let file_conf = read_conf_file(&cli.config);

    let panel_url = first_non_empty(&[
        cli.url.as_deref().unwrap_or(""),
        &std::env::var("SINGBOX_PANEL_URL").unwrap_or_default(),
        file_conf.get("URL").map(String::as_str).unwrap_or(""),
    ]);

    let flag_token = cli.token.as_deref().unwrap_or("").trim();
    let env_token = std::env::var("SINGBOX_PANEL_TOKEN").unwrap_or_default();
    let env_token = env_token.trim();
    let token = first_non_empty(&[
        flag_token,
        env_token,
        file_conf.get("TOKEN").map(String::as_str).unwrap_or(""),
    ]);

    let insecure = cli.insecure
        || file_conf
            .get("INSECURE")
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
        || std::env::var("SINGBOX_PANEL_INSECURE")
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);

    let environment = first_non_empty(&[
        &std::env::var("SINGBOX_PANEL_ENV").unwrap_or_default(),
        file_conf
            .get("ENVIRONMENT")
            .map(String::as_str)
            .unwrap_or(""),
    ])
    .to_ascii_lowercase();

    if panel_url.is_empty() || token.is_empty() {
        error!("agent: missing panel URL or token (set --url/--token, env SINGBOX_PANEL_URL/SINGBOX_PANEL_TOKEN, or the config file)");
        std::process::exit(1);
    }
    if let Err(e) = validate_panel_url(&panel_url, insecure, &environment) {
        error!("agent: {e}");
        std::process::exit(1);
    }

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let mut sig_int = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())
        .expect("SIGINT handler");
    let mut sig_term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .expect("SIGTERM handler");

    tokio::spawn(async move {
        tokio::select! {
            _ = sig_int.recv() => {}
            _ = sig_term.recv() => {}
        }
        let _ = shutdown_tx.send(true);
    });

    info!("singbox-panel-agent {version} starting; panel={panel_url}");
    agent_run::run(
        Config {
            panel_url,
            token,
            insecure,
            agent_version: version,
        },
        shutdown_rx,
    )
    .await;
    info!("singbox-panel-agent stopped");
}

fn read_conf_file(path: &std::path::Path) -> HashMap<String, String> {
    let mut out = HashMap::new();
    let content = match std::fs::read_to_string(path) {
        Ok(c) => c,
        Err(_) => return out,
    };
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((k, v)) = line.split_once('=') else {
            continue;
        };
        let key = k.trim().to_ascii_uppercase();
        let val = v.trim().trim_matches('"').to_string();
        out.insert(key, val);
    }
    out
}

fn first_non_empty(vals: &[&str]) -> String {
    for v in vals {
        let t = v.trim();
        if !t.is_empty() {
            return t.to_string();
        }
    }
    String::new()
}
