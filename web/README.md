# Operator Console (`web/`)

React + TypeScript + Vite operator console for the Market Data Gateway. The
production artifact is a static bundle served by Nginx; there is no Node
runtime in the deployed image.

## Local development

```bash
cd web
npm ci
npm run dev          # http://127.0.0.1:5173
```

The Vite dev server proxies same-origin paths to the local gateway:

| Dev path | Default target | Env override |
|---|---|---|
| `/api`, `/healthz`, `/readyz`, `/status`, `/metrics`, `/openapi.json`, `/docs`, `/ws` | `http://127.0.0.1:8080` | `VITE_DEV_API_TARGET` |
| `/ops/collector/*` | `http://127.0.0.1:8081` | `VITE_DEV_COLLECTOR_OPS_TARGET` |
| `/ops/replay/*` | `http://127.0.0.1:8082` | `VITE_DEV_REPLAY_OPS_TARGET` |
| `/prometheus/*` | `http://127.0.0.1:9090` | `VITE_DEV_PROMETHEUS_TARGET` |

See `.env.example`. All values are non-secret build/dev settings. Identity,
tokens and CSRF credentials are never stored in `localStorage` or URLs.

Quality gates:

```bash
npm run typecheck
npm run lint
npm test             # Vitest + Testing Library
npm run build        # typecheck + production bundle
npm run test:e2e     # Playwright against the real local stack (see below)
```

## Runtime modes

The console has exactly two production modes and one explicit unknown state,
resolved by capability discovery at boot (`GET /api/v1/admin/capabilities`):

| Mode | Resolution | Data access |
|---|---|---|
| `current-contract` | capability endpoint returns 404/405/501 | Anonymous read-only REST, `/ws`, per-Role `/status`, `/ops/*`, intranet Prometheus |
| `admin-api` | capability endpoint returns 200/401 | Authenticated `/api/v1/admin/*`, authenticated monitoring proxy; no unauthenticated Prometheus or ops resources |
| `capabilities-unknown` | network/timeout/5xx/malformed | Writes disabled, cached values labelled with last success time; never degrades to Current Contract |

### Current Contract is intranet-only

The Current Contract API is anonymous and read-only. It has no authentication,
TLS, CORS or origin checks. **Deploy Current Contract only on a trusted
intranet; never expose it to the public internet.** The console shows this
warning in its footer.

### Admin mode and ops/Prometheus

`MDGW_RUNTIME_MODE=admin-api` removes the unauthenticated observability
surface at the proxy, not just in the UI:

- `/prometheus/*`, `/metrics`, `/status`, `/ws`, `/ops/collector/*` and
  `/ops/replay/*` return `404` from Nginx with the public error envelope.
- `/api/*` (including `/api/v1/admin/*` and the authenticated Admin WebSocket
  upgrade at `/api/v1/admin/ws`), `/healthz` and `/readyz` are proxied.

The deployed front-end does not fall back to those denied paths:

- **Service Instance (Gateway Console and Monitoring ledger)** uses the
  contract target `GET /api/v1/admin/service-instances`, gated by the
  `service_lifecycle` capability. If that capability is missing or
  unsupported, the panel shows the explicit gap
  **“需要 Admin API 服务实例接口（未实现）”** and makes no direct ops request.
- **QPS / latency trends** use only the authenticated, fixed-template
  monitoring proxy (`/api/v1/admin/monitoring/query`); direct PromQL is never
  used in Admin mode.
- **Feed Freshness** depends on `collector /status.health.tracked`. Because
  Admin mode does not expose `/ops/collector`, that panel shows the same
  explicit capability gap instead of a fabricated or stale value.
- **Dataset Freshness and Instrument Reference** continue to use the public
  `/api/v1/status` and `/api/v1/instruments` reads.

The Admin API does not exist in the current repository. All Admin-mode
behaviour is contract-tested (MSW unit tests and Playwright request
interception); it is not real backend integration. See
[`docs/acceptance-report.md`](docs/acceptance-report.md).

## Production container

Build:

```bash
cd web
docker build -t mdgw-operator-console:local .
```

The build stage runs `npm ci` against the committed `package-lock.json` and
`npm run build`. The final stage is `nginx:1.27-alpine` with only the static
bundle and Nginx configuration; there is no Node process in the image.

### Current Contract deployment

```bash
docker run --rm -p 8080:80 \
  -e MDGW_RUNTIME_MODE=current-contract \
  -e MDGW_API_UPSTREAM=http://api:8080 \
  -e MDGW_COLLECTOR_OPS_UPSTREAM=http://collector:8081 \
  -e MDGW_REPLAY_OPS_UPSTREAM=http://replay:8082 \
  -e MDGW_PROMETHEUS_UPSTREAM=http://prometheus:9090 \
  mdgw-operator-console:local
```

| Variable | Default | Meaning |
|---|---|---|
| `MDGW_RUNTIME_MODE` | `current-contract` | `current-contract` or `admin-api`; resolved at container start |
| `MDGW_API_UPSTREAM` | `http://api:8080` | API role (REST/WS/status/metrics) |
| `MDGW_COLLECTOR_OPS_UPSTREAM` | `http://collector:8081` | Collector health/ops port |
| `MDGW_REPLAY_OPS_UPSTREAM` | `http://replay:8082` | Replay health/ops port |
| `MDGW_PROMETHEUS_UPSTREAM` | `http://prometheus:9090` | Prometheus root (Current Contract only) |
| `MDGW_DNS_RESOLVER` | `127.0.0.11` | Docker embedded DNS used for per-request upstream resolution |

Upstreams are resolved per request through Docker's embedded DNS, so an
optional service (for example Prometheus without the observability compose
override) can be absent: only its routes return `502`, and the static console
still starts. The mode selects
`nginx/current-contract.conf.template` or `nginx/admin-api.conf.template` and
renders it with `envsubst` at container start.

### Same-origin routes

Current Contract proxies `/api`, `/healthz`, `/readyz`, `/status`, `/metrics`,
`/ws` (Upgrade), `/ops/collector`, `/ops/replay` and `/prometheus`.
Admin mode proxies only `/api` (with Upgrade for the Admin WebSocket),
`/healthz` and `/readyz`, and denies the rest explicitly.

The SPA history fallback serves `index.html` for unknown paths.
`index.html` is `no-cache`; content-hashed files under `/assets/` are cached
for one year. gzip is enabled for text/JSON/JS/SVG.

### Proxy header trust boundary

Nginx **overwrites** `X-Forwarded-For`, `X-Real-IP` and `X-Forwarded-Proto`
from the real peer (`$remote_addr` / `$scheme`) and clears `Forwarded`.
Client-supplied forwarding headers never reach the backend. The API must be
configured to trust forwarding headers only from this proxy; if a TLS
terminator or load balancer is placed in front, configure `real_ip_header` and
`set_real_ip_from` for that peer so `$remote_addr` remains the true client.

### Security headers

Every response includes:

- `Content-Security-Policy` with `default-src 'self'` (no CDN/external
  origins), `object-src 'none'`, `frame-ancestors 'none'`, `base-uri 'self'`,
  `form-action 'self'`; `style-src 'self' 'unsafe-inline'` is required by
  ECharts/Radix inline styles.
- `X-Content-Type-Options: nosniff`
- `X-Frame-Options: DENY`
- `Referrer-Policy: no-referrer`
- `Permissions-Policy` disabling camera/microphone/geolocation/payment/USB
- `Cross-Origin-Opener-Policy: same-origin`
- `Cross-Origin-Resource-Policy: same-origin`

HSTS is **not** emitted by default because the container serves plain HTTP.
Enable `Strict-Transport-Security` at the TLS terminator (or uncomment it in
`nginx/security-headers.conf` when this Nginx terminates TLS).

## Visual direction and prototype provenance

The production UI is a rewrite of **Variant B · 浅色分析工作台** selected on
2026-11-09. The throwaway prototype lived on branch
`prototype/operator-console-ui` at commit `dfe3c53`; the production tree does
not contain the prototype switcher, variants, mock trend data or
`/prototype` routes.

## End-to-end acceptance

`npm run test:e2e` starts, with the local conda interpreter
`D:/Anaconda3/envs/nautilus/python.exe`:

- a real in-process Current Contract role stack (`collector` + `api` +
  `replay`) on temporary ports and a temporary Dataset under `E:/temp`;
- a real Prometheus container (`prom/prometheus:v2.55.1`) scraping the role
  stack, proxied under `/prometheus`;
- the Vite dev server with same-origin proxy targets.

The stack uses no external venue. Admin API flows are exercised only through
Playwright network interception and are labelled contract-only.
