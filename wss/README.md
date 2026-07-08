# Stratum-over-WebSocket — Nginx Proxy

## Rationale

Browser environments cannot open raw TCP sockets. The Stratum server speaks raw TCP only
(newline-delimited JSON-RPC on port `3334`). The bridge:

```
Browser ──WSS──▶ Nginx (:8443) ──TCP──▶ stratum-server-nng (:3334)
```

Nginx's `http` module handles the WebSocket HTTP Upgrade handshake, then proxies raw TCP
to the stratum backend. After the upgrade completes, the proxy is byte-transparent — no
message parsing or transformation is required.

### Why Nginx (and not a Rust sidecar)?

| Criterion            | Nginx Proxy | Rust Sidecar | Native WS in Server |
|----------------------|:-----------:|:------------:|:-------------------:|
| Code changes         | **None**    | New binary   | Invasive rewrite    |
| TLS management       | Cert files  | rustls crate  | rustls crate        |
| Ops complexity       | Config only | Deploy + monitor | Modify core binary |
| Per-session awareness| No          | Yes          | Yes                 |
| Latency              | ~0.1ms      | ~0.1ms       | 0                   |

**Phase 1** = Nginx proxy (this config). Get browser mining working immediately.

**Phase 2** = When you need JWT auth, origin validation, or per-browser analytics, either:
  - (a) Add a thin auth proxy in front of nginx (still no server changes), or
  - (b) Fold WebSocket support directly into `stratum-server-nng` using `tokio-tungstenite`.

The protocol engine (`engine.rs`) is already cleanly separated from the transport layer
(`server.rs`), so (b) is a natural evolution when the time comes.

## Files

| File | Purpose | Install Path |
|------|---------|-------------|
| `nginx.conf` | Main config with `stream {}` block | `/etc/nginx/nginx.conf` |
| `conf.d/stratum-wss.conf` | HTTP vhost: WSS listener on :8443 | `/etc/nginx/conf.d/` |
| `conf.d/stratum-tcp.conf` *(optional)* | Stream block: raw TCP proxy on :3334 | `/etc/nginx/stream.d/` |

## Decision Tree

```
Do you need browser mining?
│
├── No → Use raw TCP on :3334 directly. Skip all of this.
│
└── Yes
    │
    ├── Is this Phase 1 (just make it work)?
    │   └── YES → Install nginx.conf + conf.d/stratum-wss.conf
    │             Connect stratum-client-ts to wss://pool.lotusia.org:8443/stratum
    │
    └── Is this Phase 2 (need auth / analytics / multi-origin)?
        │
        ├── Want minimal changes?
        │   └── Add a small auth middleware proxy in front of nginx.
        │       Still no stratum-server-nng changes.
        │
        └── Want everything in one binary?
            └── Add tokio-tungstenite WebSocket listener to stratum-server-nng.
                Share the same session engine. Feature-gate behind --ws flag.
```

## Quick Start

### 1. Install nginx with stream module

```bash
sudo zypper install nginx nginx-mod-stream
```

On openSUSE Leap the stream module ships as a separate package. Verify:

```bash
nginx -V 2>&1 | grep -o 'with-stream'
```

### 2. Back up existing config

```bash
sudo cp /etc/nginx/nginx.conf /etc/nginx/nginx.conf.bak
```

### 3. Deploy configs

```bash
sudo cp wss/nginx.conf /etc/nginx/nginx.conf
sudo cp wss/conf.d/stratum-wss.conf /etc/nginx/conf.d/
sudo mkdir -p /etc/nginx/stream.d
sudo cp wss/conf.d/stratum-tcp.conf /etc/nginx/stream.d/   # optional
sudo mkdir -p /etc/nginx/ssl
sudo cp your-cert.pem /etc/nginx/ssl/pool.lotusia.org.crt
sudo cp your-key.pem  /etc/nginx/ssl/pool.lotusia.org.key
```

### 4. Adjust paths and permissions

```bash
# Ensure nginx can read the cert/key
sudo chown root:nginx /etc/nginx/ssl/pool.lotusia.org.crt
sudo chown root:nginx /etc/nginx/ssl/pool.lotusia.org.key
sudo chmod 640 /etc/nginx/ssl/pool.lotusia.org.key
```

### 5. Test and reload

```bash
sudo nginx -t
sudo systemctl reload nginx
```

### 6. Verify WebSocket connectivity

```bash
# Using websocat
websocat wss://pool.lotusia.org:8443/stratum

# Or test with the stratum-client-ts transport:
# const transport = new WebSocketStratumTransport('wss://pool.lotusia.org:8443/stratum')
# await transport.connect()
```

## Connection Tuning Notes

- **`proxy_read_timeout 86400s`** — Mining connections are long-lived. A browser miner may
  stay connected for hours. This prevents nginx from killing idle-seeming connections.
- **`proxy_send_timeout 86400s`** — Same rationale for the write direction.
- **`proxy_connect_timeout 10s`** — Short timeout for the initial TCP connection to the
  stratum backend. If the server is down, fail fast.
- **`proxy_buffering off`** — Stratum is latency-sensitive. Disabling buffering ensures
  frames flow through immediately.
- **`keepalive 64`** — Upstream keepalive connections reduce TCP handshake overhead when
  browser miners reconnect (e.g. page refresh).

## Security Notes

- The proxy does **no protocol inspection**. It forwards bytes verbatim. All Stratum-level
  validation (rate limiting, idle timeout, request shape) happens in `stratum-server-nng`.
- The `stratum_bind` in the Rust server should be set to `127.0.0.1:3334` (localhost only)
  when nginx is the sole ingress, to prevent direct TCP access bypassing WSS.
- If you want to expose raw TCP *alongside* WSS (e.g. for legacy ASIC miners), use the
  optional `stream.d/stratum-tcp.conf` on a separate port.
