# Operator Console — Production Verification & Acceptance Report

Ticket: `.scratch/operator-console/issues/08-production-verification-and-deployment.md`
Branch: `ticket/08-production-verification` (based on `integration/operator-console` at `42406cf`)
Date: 2026-11-09
Scope: `web/` only. No Python, Compose, `src/`, docs or ticket files outside `web/` were modified.

Status vocabulary:

- **REAL INTEGRATION PASSED** — exercised against the real local FastAPI/test-Dataset/Prometheus/WS stack, with the command and evidence recorded.
- **CONTRACT-ONLY** — exercised in the browser/unit tests through MSW or Playwright request interception because the Admin API does not exist. Not real backend integration.
- **BACKEND-BLOCKED** — requires an Admin API endpoint/capability that the current backend does not implement.
- **NOT RUN** — with the reason stated.

---

## 1. Quality gates

| Gate | Command | Result |
|---|---|---|
| Install | `npm ci` | PASS |
| Typecheck | `npm run typecheck` | PASS (`tsc --noEmit`, 0 errors) |
| Lint | `npm run lint` | PASS (`eslint . --max-warnings 0`, 0 errors/warnings) |
| Unit/component | `npm test` | PASS — **52 test files, 439 tests** |
| Production build | `npm run build` | PASS — Vite 6.4.5 build. Non-blocking warning: main chunk `index-CojH4foC.js` 541.80 kB (gzip 162.22 kB) exceeds the 500 kB advisory limit. No build failure. |
| Browser E2E | `npm run test:e2e` | PASS — **17 tests, 25.9 s**, Chrome channel (installed `C:/Program Files/Google/Chrome/Application/chrome.exe`) |
| Browser E2E (Edge) | `MDGW_E2E_CHANNEL=msedge npx playwright test e2e/layout-keyboard.spec.ts` | PASS — 4 layout/keyboard tests in Microsoft Edge |
| Docker image | `docker build --pull=false -t mdgw-operator-console:08 .` | PASS — image `mdgw-operator-console:08`, id `65b52b62f892`, **75.4 MB**, digest `sha256:65b52b62f8925a53925ae9822b5ae559f893687c16229419db2bdb11ebfb14ed` |

Baseline before this ticket: 50 test files / 423 tests. This ticket adds 2 test files and 16 tests
(admin service-instance source 7, WS target 5, Admin service-instance seam 2, Admin monitoring
gating 1, lifecycle 401 1).

The Docker build initially failed while Docker Hub metadata was unreachable (IPv6 route timeout).
After the base images (`node:22-alpine`, `nginx:1.27-alpine`) were pulled, the build completed.
This was an external registry connectivity issue, not a Dockerfile failure.

---

## 2. Deployment-mode decision for ops/Prometheus (required statement)

**Chosen path: (b) — implement the Admin-API service-instance/monitoring source seam as a contract
target and gate it by capability, plus defense-in-depth denial in the Admin Nginx template.**

- Current Contract keeps the real, unauthenticated reads: `/status`, `/metrics`, `/ops/collector`,
  `/ops/replay`, `/ws`, `/prometheus`.
- Admin mode (`MDGW_RUNTIME_MODE=admin-api`) proxies only `/api/*` (including
  `/api/v1/admin/*` and the Admin WebSocket upgrade path), `/healthz` and `/readyz`.
  `/prometheus/*`, `/metrics`, `/status`, `/ws`, `/ops/collector/*` and `/ops/replay/*`
  return **404** with the public error envelope. They are not exposed.
- The console no longer reads direct ops endpoints in Admin mode:
  - Gateway Console → Service Instance and the Monitoring Service Instance ledger use the
    contract target `GET /api/v1/admin/service-instances`, gated by the `service_lifecycle`
    capability. If the capability is missing/unsupported, the UI shows
    **“需要 Admin API 服务实例接口（未实现）”** and issues no direct ops request.
  - Monitoring QPS/latency uses only the authenticated fixed-template monitoring proxy
    (`/api/v1/admin/monitoring/query`); it never falls back to `/prometheus`.
  - Feed Freshness (which depends on `collector /status.health.tracked`) shows the explicit
    capability gap in Admin mode rather than a stale/fabricated value.
  - Admin WebSocket Debug targets the capability-declared `/api/v1/admin/ws`; it never falls back
    to the public `/ws` in Admin mode.
- `GET /api/v1/status` and `GET /api/v1/instruments` remain reachable through `/api` in Admin mode
  because they are public read APIs, not ops management resources.

Evidence:
- Unit/component tests: `src/runtime/service-instance-source.test.ts`,
  `src/pages/console/console-service-instance.test.tsx`,
  `src/pages/monitoring/MonitoringPage.test.tsx` (Admin mode records zero `/status`, `/metrics`,
  `/ops/*`, `/prometheus/*` requests).
- Nginx against the real stack (section 5.3): `/status`, `/metrics`, `/ops/*`, `/prometheus`,
  `/ws` all `404` in Admin mode; `/api/v1/status` and `/healthz` `200`.

---

## 3. Ticket checklist itemization

| # | Requirement | Status | Evidence |
|---|---|---|---|
| 1 | 1280/1440 px, Chrome/Edge, keyboard, status not color-only, chart data-table readouts | **REAL INTEGRATION PASSED** | `e2e/layout-keyboard.spec.ts` (4 tests, 1280/1440, Chrome), rerun in Edge (4 passed), `e2e/bars-chart-cursor.spec.ts` (chart table + keyboard), `e2e/monitoring-status.spec.ts` (icon+text status). No horizontal overflow at either width. |
| 2 | Self-hosted static assets, CSP/security headers, intranet-only Current Contract warning | **REAL INTEGRATION PASSED** | Docker Nginx smoke: `Cache-Control: no-cache` on `/`, `max-age=31536000` on hashed assets, gzip on JS, CSP/HSTS/frame/referrer/permissions headers present; WS upgrade works; footer + entrypoint warning present; `web/README.md`. |
| 3 | Admin mode does not expose unauthenticated Prometheus/ops; proxy trust boundary correct | **REAL INTEGRATION PASSED** (proxy) / **CONTRACT-ONLY** (Admin UI) | Nginx Admin real-stack denial (section 5.3). Echo-upstream test: spoofed `X-Forwarded-For: 6.6.6.6` / `X-Real-IP: 6.6.6.6` / `Forwarded: for=6.6.6.6` were replaced with the real peer `172.17.0.1`, and `Forwarded` was cleared. Admin UI seam is MSW/Playwright-intercepted because the backend is absent. |
| 4 | Real local FastAPI, test Dataset, Prometheus and WS integration without an external venue | **REAL INTEGRATION PASSED** | Playwright starts a real in-process CollectorService + ApiService + ReplayServiceRole on a temporary `E:/temp` Dataset and a real `prom/prometheus:v2.55.1` container scraping all three roles. No venue/network feed is used. |
| 5 | Small CSV download, disconnect, non-advancing cursor, 401/403/409/429, high-risk confirmation reproducible | **CSV/WS/cursor REAL INTEGRATION PASSED; 401/403/409/429 + confirmation CONTRACT-ONLY** | `e2e/bars-csv.spec.ts` (real bytes/filename), `e2e/websocket-debug.spec.ts` (real segment disconnect/reconnect), `e2e/bars-chart-cursor.spec.ts` (5001-revision corpus stops after one page), `e2e/admin-mode.contract.spec.ts` (6 contract-only tests, labelled). |
| 6 | Report itemizes real/contract/blocked/not-run, no hidden failures | **DONE** | This report. One intermediate implementation failure (ECharts overwrote the chart `aria-label`) was found and fixed with `aria-labelledby`; it is covered by `src/components/bar-chart.test.tsx` and the E2E chart test. |
| 7 | Prototype branch/commit recorded; production tree has no switcher/variants | **DONE** | Branch `prototype/operator-console-ui`, commit `dfe3c53` (“prototype(console): add three monitoring UI variants”), selected **Variant B · 浅色分析工作台** on 2026-11-09. Scans in section 7. |

---

## 4. Spec Testing Decisions itemization

| Spec requirement | Status | Evidence |
|---|---|---|
| Vitest + Testing Library: Access Role matrix | **PASS** | `src/runtime/permissions.test.ts`, Service Instance role matrix, security capability-gating tests; part of 439 passing tests. |
| Revision dedupe (max revision, tie-break) | **PASS** | `src/lib/bar-data.test.ts`, `src/components/bar-chart.test.tsx`. |
| UTC and closed/open interval semantics | **PASS** | `src/lib/time-range.test.ts`; Bars URL uses inclusive start/exclusive end. |
| Job state machine | **PASS** | `src/lib/export-policy.test.ts`, `src/pages/console/lifecycle.test.ts`, backfill/export contract tests. |
| Filename parsing (`filename*`, quoted, path cleanup, fallback) | **PASS** | `src/lib/content-disposition.test.ts`. |
| Unknown-length progress | **PASS** | `src/api/endpoints/bars.test.ts`, `src/components/csv-download-panel.test.tsx`. |
| Blob error envelope | **PASS** | `src/api/http/errors.test.ts`, `src/api/endpoints/bars.test.ts`. |
| CSRF/401/403/409 UI behavior | **PASS** | `src/runtime/session-lifecycle.test.tsx`, `src/runtime/csrf.test.ts`, `src/pages/console/console-service-instance.test.tsx` (including a new lifecycle 401 test). |
| Playwright: real browser download | **REAL INTEGRATION PASSED** | `e2e/bars-csv.spec.ts` — downloaded `1m-bars.csv`, exact server header/rows, `Content-Disposition: inline` still triggers a download. |
| Playwright: chart linkage and data-table readout | **REAL INTEGRATION PASSED** | `e2e/bars-chart-cursor.spec.ts` — exact decimal/timestamp strings, keyboard End/ArrowLeft/Home readout. |
| Playwright: confirmation dialog | **CONTRACT-ONLY** | `e2e/admin-mode.contract.spec.ts` — reason + exact instance name required; CSRF and `Idempotency-Key` asserted; 202 ≠ success; one POST only. |
| Playwright: WebSocket virtual list | **REAL INTEGRATION PASSED** | `e2e/websocket-debug.spec.ts` — 80 real Bars through collector→ZMQ→API→browser; mounted DOM rows < retained count; local/server drop counters separate; ping/pong “已同步”; segment 1 interruption and segment 2 reconnect. |
| Playwright: keyboard operation | **REAL INTEGRATION PASSED** | `e2e/layout-keyboard.spec.ts` (Tab/Enter navigation, chart arrow keys), `src/components/confirm-dialog.tsx` keyboard dialog. |
| Playwright: 1280/1440 layout | **REAL INTEGRATION PASSED** | `e2e/layout-keyboard.spec.ts` — both widths, all five modules, no horizontal overflow; rerun in Edge. |
| Current Contract: local test Dataset, real FastAPI, Prometheus, no real venue | **REAL INTEGRATION PASSED** | `playwright.config.ts` generates a real role stack and Prometheus config; Prometheus targets `mdgw-api`, `mdgw-collector`, `mdgw-replay` all `up`. |
| Admin API: contract tests only, labelled “not real integration” | **CONTRACT-ONLY** | `e2e/admin-mode.contract.spec.ts` test titles are prefixed `CONTRACT-ONLY`; unit tests use MSW fixtures. |
| Each production PR passes typecheck/lint/unit/build | **PASS** | Section 1. |

---

## 5. Real integration evidence

### 5.1 Playwright real stack

`playwright.config.ts` starts three web servers:

1. `role-stack.py` — real `CollectorService`, `ApiService`, `ReplayServiceRole` in one Python
   process, temporary Dataset at `E:/temp/mdgw-playwright-dataset`, API `8099`, collector `8098`,
   replay `8097`, control `8096`. The control endpoint only enqueues closed Bars; WebSocket frames
   travel through the real collector → ZMQ → WebSocketHub path.
2. `prometheus-run.py` — real `prom/prometheus:v2.55.1` container on `9099`, scraping
   `host.docker.internal:8099/8098/8097` every 1 s.
3. Vite dev server on `5174` with same-origin proxy targets for `/api`, `/ops/*`, `/prometheus`.

The generated Dataset contains:

- the two exact 2024-01-01 `binance:BTC/USDT` rows expected by the pre-existing CSV acceptance;
- 5001 raw revisions sharing `2024-01-02T00:00:00Z` for `binance:ETH/USDT`, used by the
  non-advancing cursor test;
- 20 live Bars per instrument ingested through the real collector at start.

### 5.2 Playwright results (17/17)

```
17 passed (25.9s)
  admin-mode.contract.spec.ts     6 passed (CONTRACT-ONLY)
  bars-chart-cursor.spec.ts       2 passed
  bars-csv.spec.ts                1 passed
  layout-keyboard.spec.ts         4 passed
  monitoring-status.spec.ts       3 passed
  websocket-debug.spec.ts         1 passed
```

Edge rerun: `layout-keyboard.spec.ts` — 4 passed.

Further observations from the real runs:

- Prometheus `/api/v1/targets` showed `mdgw-api up`, `mdgw-collector up`, `mdgw-replay up`.
- The cursor test issued one page request and stopped with
  “分页已停止：next_start 未严格前进…”, returning 5000 rows / partial results (asserted `< 4`
  requests including React StrictMode duplicate mounts).
- The WebSocket test retained ≥20 messages, mounted fewer rows than retained (virtualization),
  then showed “接收段 1 中断” and reconnected as “接收段 2”.

### 5.3 Production Nginx container against the real stack

Image: `mdgw-operator-console:08`. A user-defined Docker network was used so the embedded DNS
resolver (`127.0.0.11`) is available.

Current Contract container (`MDGW_RUNTIME_MODE=current-contract`, upstreams
`http://host.docker.internal:<port>`):

| Request | Result |
|---|---|
| `/healthz` | `200 {"status":"ok","service":"api"}` |
| `/status` | `200` real API status (Dataset + API + collector) |
| `/ops/collector/status` | `200` real collector status |
| `/ops/replay/readyz` | `200` real replay readiness |
| `/api/v1/bars?…2024-01-01…` | `200`, 2 real rows |
| `/prometheus/api/v1/query?query=up` | `200`, jobs `mdgw-api`, `mdgw-collector`, `mdgw-replay` |
| `ws://…/ws` subscribe + ping | `{"op":"pong"}` through the Nginx Upgrade proxy |
| `/` | `200`, `Cache-Control: no-cache`, CSP + security headers |
| `/assets/index-*.js` | `Cache-Control: max-age=31536000`, `Content-Encoding: gzip` |
| `/monitoring` | `200 text/html` SPA fallback |
| Strict-Transport-Security | absent by design (TLS-termination dependent; documented) |

Admin mode container (`MDGW_RUNTIME_MODE=admin-api`):

| Request | Result |
|---|---|
| `/api/v1/status` | `200` (public read via `/api`) |
| `/healthz` | `200` |
| `/prometheus/api/v1/query` | `404` error envelope |
| `/metrics` | `404` |
| `/status` | `404` |
| `/ops/collector/status` | `404` |
| `/ops/replay/readyz` | `404` |
| `ws://…/ws` | handshake rejected with HTTP `404` |

Proxy trust boundary (echo upstream through Current Contract Nginx):

- Request headers sent: `X-Forwarded-For: 6.6.6.6`, `X-Real-IP: 6.6.6.6`,
  `Forwarded: for=6.6.6.6`.
- Upstream observed: `X-Forwarded-For: 172.17.0.1`, `X-Real-IP: 172.17.0.1`,
  `Forwarded: None` (cleared).
- `/ops/collector/status` reached upstream as `/status`;
  `/prometheus/api/v1/query` reached upstream as `/api/v1/query`.

Optional-service behavior: a Current Contract container with all four upstream hostnames
unresolvable still starts and serves the static bundle (`/` → `200`); proxied routes return `502`.
Nginx resolves upstreams per request through `MDGW_DNS_RESOLVER` (default `127.0.0.11`).

Static-image check:

```
NO_NODE
NO_NPM
nginx version: nginx/1.27.5
/usr/share/nginx/html: index.html, assets, 50x.html
```

---

## 6. Admin API contract-only evidence

The current backend has no `/api/v1/admin/*` routes. All Admin-mode flows are exercised with:

- MSW unit/component tests (`src/pages/console/console-service-instance.test.tsx`,
  `src/pages/monitoring/MonitoringPage.test.tsx`, `src/runtime/*.test.ts`);
- Playwright request interception (`e2e/admin-mode.contract.spec.ts`, titles prefixed
  `CONTRACT-ONLY Admin API mode (backend absent)`).

Covered:

| Behaviour | Evidence |
|---|---|
| Admin service-instance seam; zero direct `/status` or `/ops/*` requests | E2E test 1; unit tests assert request flags |
| High-risk confirmation: reason + exact instance name; CSRF + `Idempotency-Key`; accepted ≠ success; no auto-retry | E2E test 1 |
| 401 stops writes and returns to login entry | E2E test 2; unit test in `console-service-instance.test.tsx` |
| 403 names the missing role without logout | E2E test 3 |
| 409 manual-merge conflict | E2E test 4 |
| 429 honours `Retry-After` | E2E test 5 |
| Missing `service_lifecycle` capability → explicit gap, no fallback rows | E2E test 6; unit test |
| Admin monitoring proxy only; no direct Prometheus | `MonitoringPage.test.tsx` Admin-mode gating; existing contract tests |

These are **not** real backend integration and must not be counted as completed Admin features.

---

## 7. Prototype provenance and production-tree scan

- Prototype branch: `prototype/operator-console-ui`
- Prototype commit: `dfe3c53` — “prototype(console): add three monitoring UI variants”
- Decision: **Variant B · 浅色分析工作台** (top navigation, unified filter row, light neutral
  surfaces, border-divided KPI band, dense tables, event timeline). Production code was rewritten,
  not promoted from the prototype.

Production-tree scans (after the final build):

```
grep -rnE "PrototypeMark|PROTOTYPE|模拟数据|prototype/operator-console|/prototype/|\?variant=|variant=[ABC]|mock-data|mockData" \
  src e2e index.html vite.config.ts package.json
→ NO_MATCHES

grep -rlE "PrototypeMark|PROTOTYPE ·|prototype/operator-console|/prototype/|\?variant=|mock-data" dist
→ NO_MATCHES

Routes: /monitoring, /bars, /console, /quality, /security, * (NotFound)
```

The only `Math.random()` uses are idempotency-key fallbacks in
`src/api/endpoints/admin-console.ts` and `src/api/endpoints/backfill.ts`; they generate opaque
keys, not mock data.

---

## 8. Unresolved gaps / backend blockers / not run

1. **Admin API absent.** Capability discovery, OIDC/session, Access Roles, service-instance
   lifecycle, feed subscriptions/flow control, Export Jobs, Backfill Jobs, persistent gaps, audit
   and the monitoring proxy are contract targets only. No real Admin integration was performed.
2. **Admin service-instance endpoint absent.** The UI implements the seam and the explicit gap
   “需要 Admin API 服务实例接口（未实现）”; the backend must implement
   `GET /api/v1/admin/service-instances` and declare `service_lifecycle`.
3. **Admin-mode Feed Freshness unavailable.** It derives from `collector /status`, which Admin mode
   deliberately does not expose; the panel shows the capability gap. A future Admin monitoring
   service-instance/metrics source is required.
4. **Admin WebSocket is contract-only.** The UI targets the capability-declared
   `/api/v1/admin/ws`; no real Admin WebSocket exists. The real WebSocket E2E covers the public
   Current Contract `/ws` only.
5. **HSTS is not emitted by the container.** It must be enabled at the TLS terminator, or by
   uncommenting the header when this Nginx terminates TLS.
6. **CSP uses `style-src 'self' 'unsafe-inline'`.** Required by ECharts/Radix inline style
   attributes; no external origin is allowed.
7. **Docker DNS constraint.** `MDGW_DNS_RESOLVER=127.0.0.11` is Docker's embedded DNS. Use a
   user-defined network/Compose (or IP upstreams/another resolver) for plain `docker run`;
   otherwise absent hostnames log resolver errors and return `502`.
8. **Build advisory.** The main JS chunk is 541.80 kB (gzip 162.22 kB), above Vite's 500 kB
   advisory. Build passes; no code-splitting change was made.
9. **Not run:** no mobile viewport acceptance (mobile is out of the desktop design target beyond
   key states); no real Edge run of the full suite (Edge ran the 4 layout/keyboard tests); no
   Docker Compose deployment of the whole gateway (only the web image and its Nginx proxy were
   built/run, because this ticket must not modify Compose).
