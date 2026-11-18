fn main() {
    // Prefer AGENT_VERSION from the environment (Makefile / Docker / release),
    // else embed the contents of VERSION next to Cargo.toml.
    let version = std::env::var("AGENT_VERSION")
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .or_else(|| std::fs::read_to_string("VERSION").ok().map(|v| v.trim().to_string()))
        .unwrap_or_else(|| "v1.1.0".to_string());
    println!("cargo:rustc-env=AGENT_VERSION={version}");
    println!("cargo:rerun-if-env-changed=AGENT_VERSION");
    println!("cargo:rerun-if-changed=VERSION");
}
