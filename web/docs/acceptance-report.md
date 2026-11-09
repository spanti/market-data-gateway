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
| Unit/component | `npm test` | PASS — **54 test files, 458 tests** (the code-review pass added focused cases across bars, WS, quality, console, export and Retry-After) |
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
  - **Quality & Backfill** no longer calls the process-local `/ops/collector/status` in Admin
    mode. It renders a note that the snapshot is Current-Contract-only and points to the
    persistent Backfill Gap list. With capabilities unknown it renders unknown and issues zero
    requests. The zero-direct-ops claim therefore covers Monitoring **and** Quality **and**
    Console.
  - Admin WebSocket Debug and Live Follow target the capability-declared
    `/api/v1/admin/ws`; they never fall back to the public `/ws` in Admin mode. When capability
    discovery is pending/failed, or `ws_admin_session` is unavailable, the target resolves to
    `null` and no socket is constructed.
  - An incompatible Admin contract (`compatible === false`) disables every Admin-controlled
    surface through one shared gate, with **“Admin API 版本不兼容”** as the reason.
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
| Revision dedupe (max revision, tie-break) | **PASS** | `src/lib/bar-data.test.ts` — highest revision wins; equal revision breaks by latest `ingest_ts` (Data Contract §9), with a case where the later ingest appears earlier in the response; only equal revision **and** equal ingest fall back to response order. `src/components/bar-chart.test.tsx`. |
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
| Admin service-instance seam; zero direct `/status` or `/ops/*` requests | E2E test 1; unit tests assert request flags; `quality-page.test.tsx` asserts zero `/ops/collector/status` requests in Admin and unknown modes |
| High-risk confirmation: reason + exact instance name; CSRF + `Idempotency-Key`; accepted ≠ success; no auto-retry | E2E test 1 (dialog now RHF + Zod; same rules) |
| 401 stops writes and returns to login entry | E2E test 2; unit test in `console-service-instance.test.tsx` |
| 403 names the missing role without logout | E2E test 3 |
| 409 manual-merge conflict | E2E test 4 |
| 429 honours `Retry-After` and disables writes until it expires | E2E test 5 (notice + countdown); `retry-after.test.tsx` (fake timers), `csrf.test.ts` (local rejection, zero network writes), `export-job-panel.contract.test.tsx` (control disabled → no POST → re-enabled) |
| Missing `service_lifecycle` capability → explicit gap, no fallback rows | E2E test 6; unit test |
| Admin monitoring proxy only; no direct Prometheus | `MonitoringPage.test.tsx` Admin-mode gating; existing contract tests |
| Incompatible Admin contract disables controls with the reason and no writes | `capability-gating.test.tsx`; `export-job-panel.contract.test.tsx`; shared `adminCapabilityAvailability` |
| WS target modes: Current Contract public `/ws`, Admin admin WS only, unknown = no socket | `ws-target.test.ts`, `BarsPage.test.tsx`, `console-websocket-debug.test.tsx`, `console-mode-banner.test.tsx` |
| >366 days: independent per-chunk Export Jobs with explicit confirmation | `export-job-panel.contract.test.tsx` (confirmation, exact chunk body, one job at a time, quota error) |

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
8. **Build advisory.** The main JS chunk is 591.59 kB (gzip 180.57 kB), above Vite's 500 kB
   advisory (it grew with React Hook Form/Zod form code). Build passes; no code-splitting change
   was made in this pass.
9. **Not run:** no mobile viewport acceptance (mobile is out of the desktop design target beyond
   key states); no real Edge run of the full suite (Edge ran the 4 layout/keyboard tests); no
   Docker Compose deployment of the whole gateway (only the web image and its Nginx proxy were
   built/run, because this ticket must not modify Compose).

---

## 9. Code review fixes (two-axis review of `integration/operator-console`)

Scope: `web/` only, plus this report. No Python, Compose, `src/`, root docs or ticket files were
changed. Branch: `integration/operator-console`.

Final gates after the fixes:

| Gate | Result |
|---|---|
| `npm run typecheck` | PASS (0 errors) |
| `npm run lint` | PASS (`--max-warnings 0`) |
| `npm test` | PASS — **54 files / 458 tests** |
| `npm run build` | PASS — Vite 6.4.5; same non-blocking chunk-size advisory (`index-*.js` 591.59 kB / gzip 180.57 kB) |
| `npm run test:e2e` | PASS — **17 tests / 23.9 s** on the real local stack (Chrome channel), rerun after the final code changes; no flaky retry was needed |

### 9.1 Spec / functional findings

| # | Finding | Fix | Evidence |
|---|---|---|---|
| S1 | Max-Revision tie-break ignored `ingest_ts` | `deriveMaxRevisionBars` now keeps highest revision → latest `ingest_ts` → later response order; chart caption/comment updated | `src/lib/bar-data.ts`, `src/lib/bar-data.test.ts` (later-ingest row appears earlier in the response), `src/components/bar-chart.tsx` |
| S2 | WS target/unknown capabilities and live follow | Current Contract → public `/ws`; Admin → declared `ws_admin_session` path only; unknown/pending → `url: null` + “能力未知，暂不可用” and source `unknown`. Live Follow uses the resolved target and the `ws.session.subscribe` role gate; no socket when unavailable | `src/api/ws/ws-target.ts`, `src/api/ws/ws-target.test.ts`, `src/hooks/use-bar-live-feed.ts`, `src/components/live-follow-panel.tsx`, `src/pages/console/WebSocketDebugView.tsx`, `src/pages/bars/BarsPage.test.tsx`, `src/pages/console/console-websocket-debug.test.tsx` |
| S3 | Quality always called `/ops/collector/status` | Query enabled only in `current-contract`; Admin mode shows a Current-Contract-only note and points to the persistent Backfill Gap list; unknown renders unknown with no request; Quality header follows the mode | `src/pages/quality/collector-gap-snapshot.tsx`, `src/pages/quality/QualityPage.tsx`, `src/pages/quality/quality-page.test.tsx` (asserts zero requests in Admin/unknown) |
| S4 | `compatible === false` not enforced everywhere | One `adminCapabilityAvailability` / `adminCompatibilityReason` gate (typed `CAPABILITY_KEYS`) now backs quality gates, export job, monitoring proxy, service-instance source, login, WS target, feed views, lifecycle and Backfill | `src/runtime/capability-gates.ts`, `src/runtime/permissions.ts`, `src/runtime/service-instance-source.ts`, `src/runtime/login-availability.ts`, `src/api/monitoring/admin-proxy.ts`, `src/pages/console/FeedSubscriptionView.tsx`, `src/pages/console/ServiceInstanceView.tsx`, `src/components/export-job-panel.tsx`, `src/components/export-job-panel.contract.test.tsx` |
| S5 | Console banner was hardcoded to Current Contract | Banner now names Current Contract real status + public `/ws`, Admin service-instance seam + admin WS, or unknown/no-connect | `src/pages/console/ConsolePage.tsx`, `src/pages/console/console-mode-banner.test.tsx` |
| S6 | >366-day ranges could not be created | Each split chunk gets an independent Export Job control with explicit confirmation; a single combined job stays disabled; created chunks / an active job / an in-flight create disable the rest; quota errors stay visible; no multi-instrument ZIP | `src/components/export-job-panel.tsx`, `src/components/export-job-panel.contract.test.tsx` |
| S7 | 429 only showed a notice | HTTP-error-bus-fed Retry-After store + ticking `useRetryAfter`; the API client rejects unsafe requests locally while blocked; lifecycle, feed update/delete, flow control, Backfill create/cancel/retry and Export Job create/cancel disable with “限流中，请在 N 秒后重试”; global notice counts down | `src/runtime/retry-after.ts`, `src/runtime/use-retry-after.ts`, `src/api/http/client.ts`, `src/components/global-http-notice.tsx`, `src/runtime/retry-after.test.tsx` (fake timers), `src/runtime/csrf.test.ts` (zero network writes), `src/components/export-job-panel.contract.test.tsx` |
| S8 | Forms were manually validated | RHF + Zod now backs `ConfirmDialog` (reason + exact instance name + max length), both flow-control editors, Backfill Job create and the split-chunk creation dialog. Submission bodies and accessible messages are unchanged | `src/components/confirm-dialog.tsx`, `src/pages/console/FeedSubscriptionView.tsx`, `src/pages/quality/backfill-job-create-form.tsx`, `src/components/export-job-panel.tsx`, `src/lib/validation.ts`; existing form tests plus `quality-page.test.tsx`/`export-job-panel.contract.test.tsx` cover the preserved behavior |

Notes on S8: feed enable/disable is a single toggle button with no fields, the feed delete dialog
has no fields, and the 31–366-day “创建 Export Job” action is a single confirm button with no
fields. These remain direct controls (the delete dialog still runs through the RHF+Zod
`ConfirmDialog`). The split-chunk dialog is a real RHF+Zod form (chunk select + explicit
confirmation checkbox).

### 9.2 Standards / refactor findings

| # | Finding | Fix | Evidence |
|---|---|---|---|
| T1 | Copied mode → compatibility → capability gates | Single `adminCapabilityAvailability` + typed `CAPABILITY_KEYS` used by quality, export, service-instance, admin proxy, login, WS target, monitoring and feed views; distinct disabled copy is preserved | `src/runtime/capability-gates.ts` and its call sites |
| T2 | `ROLE_ACTIONS` and `REQUIRED_ROLES` maintained separately | `REQUIRED_ROLES` is derived from the single role matrix; `can`/`disabledReasonFor` behavior unchanged | `src/runtime/permissions.ts`, `src/runtime/permissions.test.ts` |
| T3 | Duplicated service domain types and gap naming | One `SERVICE_ROLES`/`ServiceRole`, one `ServiceLifecycleAction` source, canonical `ServiceInstanceRowData` shared by Monitoring and Console; `UnrecoverableBackfillGapEvent` and Backfill-prefixed gap identifiers/components | `src/api/endpoints/admin.ts`, `src/api/endpoints/admin-service-instances.ts`, `src/api/endpoints/ops.ts`, `src/pages/console/lifecycle.ts`, `src/pages/console/ServiceInstanceView.tsx`, `src/pages/monitoring/use-monitoring-data.ts`, `src/components/monitoring/service-instance-list.tsx`, `src/pages/quality/*` |
| T4 | Bare `AuditEvent` / WS `subscriptions` vocabulary | `OperatorAuditEvent`, `OperatorAuditEventPage`, `parseOperatorAuditEventPage`, `fetchOperatorAuditEvent(s)`, `OperatorAuditQuerySection`, `OperatorAuditEventDetail`; files renamed to `operator-audit.ts` / `OperatorAudit*.tsx`; WS-debug snapshot uses `topicPatterns` and the UI says “当前会话 Topic 模式” | `src/api/endpoints/operator-audit.ts`, `src/pages/security/OperatorAuditQuerySection.tsx`, `src/pages/security/OperatorAuditEventDetail.tsx`, `src/pages/console/ws-debug-session.ts`, `src/pages/console/WebSocketDebugView.tsx` |
| T5 | Duplicated formatters/error helpers/hardcoded polling | One `formatUtcTimestamp` in `src/lib/format.ts` (used by quality format, Identity, Feed, Service Instance and mode banner); one `errorText` and one `queryErrorInfo` in `src/lib/errors.ts`; named `OPERATION_POLL_INTERVAL_MS` / `ACTIVE_JOB_POLL_INTERVAL_MS` and configured copy instead of literal “5 秒轮询”; removed unused `AdminError`/`AdminErrorEnvelope` | `src/lib/format.ts`, `src/lib/errors.ts`, `src/lib/monitoring-config.ts`, `src/pages/monitoring/MonitoringPage.tsx`, `src/pages/console/ServiceInstanceView.tsx`, `src/pages/quality/*`, `src/api/endpoints/admin.ts` |
| T6 | Duplicated supervisor gate; risky large refactors | Extracted `serviceLifecycleSupervisorState` used by both the Service Instance seam and lifecycle buttons | `src/runtime/service-instance-source.ts`, `src/pages/console/ServiceInstanceView.tsx` |

### 9.3 Deliberately deferred structural refactors

- No wholesale rewrite of `use-monitoring-data.ts`, `MonitoringPage.tsx` or
  `ServiceInstanceView.tsx`. They are large but their current behavior is covered by real and
  contract tests; a rewrite would add regression risk without removing a proven duplication beyond
  the supervisor/row-type consolidation above.
- The Monitoring view rows (`ServiceInstanceRow` in `service-instance-list.tsx`) and the Console
  display row (`ServiceRow` in `ServiceInstanceView.tsx`) remain distinct presentation models.
  Only the parsed Admin source row (`ServiceInstanceRowData`) is shared; merging the presentation
  models would couple two different layouts for little gain.
- `validateLifecycleConfirmation` remains as the body-shaping/defense-in-depth check even though
  the dialog now validates with Zod. It prevents a malformed programmatic call from sending an
  invalid body; it is not a second form validation path.

### 9.4 Honest limitations of the new behavior

- A 429 on any REST call (including a read) enters the shell Retry-After gate, so write controls
  stay disabled until it expires. This is deliberately conservative; reads remain available.
- Export Job, Backfill, Feed Subscription and lifecycle writes are still **CONTRACT-ONLY**: the
  backend routes do not exist. The new >366 creation and 429 gates are exercised with MSW/Playwright
  interception, not a real Admin backend.
- The split-chunk list is rendered in a scrollable container; very large split counts remain
  usable but were not performance-tested beyond the existing 3-chunk contract fixture.
