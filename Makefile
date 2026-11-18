VERSION ?= v1.1.0
COMMIT ?= $(shell git rev-parse HEAD 2>/dev/null)
AGENT_VERSION ?= $(shell cat agent/VERSION 2>/dev/null || echo "v1.1.0")
PANEL_LDFLAGS := -s -w -X main.version=$(VERSION) -X main.commit=$(COMMIT)
DIST := dist
AGENT_DIR := agent
CARGO_ZIGBUILD ?= $(shell command -v cargo-zigbuild 2>/dev/null)
ZIG ?= $(shell command -v zig 2>/dev/null)

.PHONY: all build panel agents web clean tidy test agent-test

all: build

## build everything: frontend + panel + agents
build: web panel agents

## build the panel binary (host platform)
panel:
	go build -ldflags "$(PANEL_LDFLAGS)" -o $(DIST)/singbox-panel ./cmd/panel

## cross-compile the Rust agent for the common Linux VPS architectures (static musl)
agents:
	@if [ -z "$(CARGO_ZIGBUILD)" ] || [ -z "$(ZIG)" ]; then \
		echo 'agents: need zig and cargo-zigbuild on PATH (cargo install cargo-zigbuild)'; \
		exit 1; \
	fi
	mkdir -p $(DIST)/agents
	cp $(AGENT_DIR)/VERSION $(DIST)/agents/VERSION
	cd $(AGENT_DIR) && AGENT_VERSION=$(AGENT_VERSION) cargo zigbuild --release --target x86_64-unknown-linux-musl
	cd $(AGENT_DIR) && AGENT_VERSION=$(AGENT_VERSION) cargo zigbuild --release --target aarch64-unknown-linux-musl
	cp $(AGENT_DIR)/target/x86_64-unknown-linux-musl/release/singbox-panel-agent $(DIST)/agents/singbox-panel-agent-linux-amd64
	cp $(AGENT_DIR)/target/aarch64-unknown-linux-musl/release/singbox-panel-agent $(DIST)/agents/singbox-panel-agent-linux-arm64
	chmod 755 $(DIST)/agents/singbox-panel-agent-linux-amd64 $(DIST)/agents/singbox-panel-agent-linux-arm64

## build the frontend into web/dist (served by the panel in production)
web:
	cd web && npm ci && npm run build

tidy:
	go mod tidy

test:
	go test ./...
	$(MAKE) agent-test

agent-test:
	cd $(AGENT_DIR) && cargo test

clean:
	rm -rf $(DIST) web/dist
	cd $(AGENT_DIR) && cargo clean
