# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

SingBox-Panel is a web panel for managing sing-box proxy nodes (shadowsocks, vless, hysteria2, tuic, anytls, snell, socks, mixed) across multiple VPS hosts. Go backend (Gin) + React web frontend. Two binaries: `panel` (control plane) and `agent` (runs on each VPS).

## Commands

Backend (Go):
- Build everything: `make build` (web + panel + cross-compiled agents); individual targets: `make web`, `make panel`, `make agents` (linux amd64/arm64 into `dist/agents/`)
- Test all: `go test ./...`; vet: `go vet ./...`
- Single test: `go test ./internal/domain/singbox -run TestName -v`
- Live regression against a real sing-box (skipped by default; `*_live_test.go`): `SING_BOX_BINARY=<absolute path> go test ./internal/domain/singbox -run TestRealityGuardLive -v`

Frontend (`web/`, Vite + vitest):
- `npm ci`, `npm run dev`, `npm run build`, `npm run typecheck`, `npm test`
- Single test: `cd web && npx vitest run src/configApply.test.ts` (add `-t "case name"` to filter)

CI (`.github/workflows/checks.yml`) runs: `python3 scripts/test-docker-metadata.py`, `scripts/test-docker-smoke.py`, `go test ./...`, `go vet ./...`, then web `npm ci && npm test && npm run typecheck && npm run build` (Go 1.26.6, Node 24).

## Architecture

- `cmd/panel/main.go` loads config and starts `panel.NewApp`; `cmd/agent/main.go` is the VPS daemon (`--url`, `--token`, `--config`). Agent version lives in `cmd/agent/VERSION`.
- **Config push flow**: web/REST changes the DB → `services/panel` `Orchestrator` (`orchestrator.go`) calls `singbox.BuildServerConfig` → `hub.SendCommand(apply_config)` over the agent WebSocket (`/api/agent/ws`; agents dial the panel) → agent `applyconfig.go` writes a temp file, runs `sing-box check -c`, atomically replaces the live config and restarts, rolling back on failure. Pushes are serialized per server.
- `internal/domain/protocol` defines the fixed panel↔agent message set (`apply_config`, `install_singbox`, `get_logs`, `traffic`, `heartbeat`, …). The agent never runs arbitrary shell. Changing the protocol means updating this package and the agent version together.
- `internal/domain/singbox` is pure config generation/parsing: `inbound.go` (`BuildInbound`, per-protocol), `config.go` (`BuildServerConfig`), `subscription.go` (client outbounds and share links), `parselink.go`/`subscription_import.go` (import), `reality_guard*.go` (Reality SNI front filter), `route.go`.
- `internal/services/panel`: Gin server (`server.go`, `api_*.go`), JWT HS256 auth, bcrypt passwords, login rate limiting, encrypted backup/restore (argon2), self-update, and generation of node install commands/scripts.
- `internal/services/agent`: WSS client with reconnect, sing-box install, systemd/OpenRC service management, traffic collection (UTC-minute buckets, back-filled after disconnect), self-update/uninstall.
- `internal/config`: YAML plus `SINGBOX_PANEL_*` env vars; DB driver sqlite (default), mysql or postgres.
- The web UI is **not** `go:embed`ed: the panel serves static files from `SINGBOX_PANEL_WEB_DIR` (default `./web/dist`) and falls back to `index.html`. Agent binaries are served by the panel for download (Docker image builds them to `/app/agents`).
- Deployment: Dockerfile (node build → go build + agent cross-compile → alpine, non-root uid 10001, data in `/app/data`, listens on `:8080`, health at `/api/ready`; see `docs/docker.md`), `compose.yaml`/`compose.build.yaml` (`.env.example` lists `PANEL_*` vars), and `install.sh` for native installs.

## Gotchas

- snell requires sing-box 1.14+; the stats API is only enabled on 1.14.0+ (`statsAPISupported`). Agent-local Clash API is `127.0.0.1:29091`, stats API `127.0.0.1:29092`; generated Reality internal ports must avoid these and bind to `127.0.0.1`.
- Only some shadowsocks methods support multi-user (`SupportsMultiUser`); others use the shared `SSServerPSK`. SS2022 per-user keys come from `DeriveSSUserKey`.
- Renamed inbound tags are retained permanently for traffic accounting and cannot be reused by another inbound.
- Production requires an HTTPS `base_url` unless `SINGBOX_PANEL_ENV=development`. With empty trusted proxies, all loopback/private peers are trusted for `X-Real-IP`.

## Repo conventions (from AGENTS.md)

- Make local git commits per independent feature using Conventional Commits (e.g. `feat(web): add server filtering`). Do not push, tag or publish, and do not force-push/reset destructively, without user authorization.
- Frontend uses shadcn/ui (Radix + Tailwind), React Hook Form, Lucide, Sonner. Do not reintroduce Ant Design.
- Never set CSS `zoom` or a page-wide `transform: scale` on `html`/`body`/root. Dialogs are centered.
