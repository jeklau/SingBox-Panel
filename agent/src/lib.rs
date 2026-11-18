//! SingBox Panel VPS agent library (Rust port of the Go agent).

pub mod agent;
pub mod apply_config;
pub mod atomic_file;
pub mod client;
pub mod config_read;
pub mod exec;
pub mod install;
pub mod logs;
pub mod paths;
pub mod protocol;
pub mod self_uninstall;
pub mod self_update;
pub mod service;
pub mod sysinfo;
pub mod test_outbound;
pub mod traffic;
pub mod uninstall_singbox;
