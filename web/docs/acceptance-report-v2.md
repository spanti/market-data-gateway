# Operator Console v2 — Ticket 09 Acceptance Report

Ticket: `.scratch/operator-console-v2/issues/09-contract-and-full-console-verification.md`
Branch: `ticket/operator-console-v2-09`
Base / integration tip: `integration/operator-console-v2` at `e9d13ed`
Date: 2026-11-16
Scope: `web/` only. No Python, REST/WS contract, backend, Admin API, OIDC, Access Role,
deployment, historical acceptance report, prototype worktree, ticket Status line or ticket
Comment was modified.

Status vocabulary:

- **REAL** — exercised against the real local FastAPI Collector/Api/Replay stack, the real test
  Dataset, a real `prom/prometheus:v2.55.1` container and the real public WebSocket.
- **CONTRACT-ONLY** — exercised in a real browser through Playwright request/WebSocket
  interception or in Vitest with MSW because the Admin API and real OIDC session do not exist.
  Not real Admin integration.
- **BACKEND-BLOCKED** — requires an Admin API/OIDC capability the repository does not implement.
- **NOT RUN** — with the reason stated.

---

## 1. Gates and exact counts

Commands were run from `web/` on 2026-11-16:

| Gate | Command | Result |
|---|---|---|
| Typecheck | `npm run typecheck` | PASS — `tsc --noEmit`, 0 errors |
| Lint | `npm run lint` | PASS — `eslint . --max-warnings 0`, 0 errors/warnings |
| Unit/component | `npm test` | PASS — **61 test files, 544 tests** (23.36 s) |
| Production build | `npm run build` | PASS — Vite 6.4.5, 8,561 modules, 7.84 s |
| Full browser E2E | `MDGW_E2E_WEB_PORT=5179 MDGW_E2E_DATA_ROOT=E:/temp/mdgw-e2e-oc2-09 npx playwright test` | PASS — **56 tests, 1.8 min**, Chrome channel, 0 failures, 0 retries |

The full-suite run used the requested ports/data root. Web port 5174 was left untouched for the
read-only prototype Vite server. Default service ports 8096–8099/9099 and ZMQ 5655–5658 were
used by the real stack. The final full suite was re-run after `git merge
integration/operator-console-v2` reported “Already up to date”; it passed 56/56 again.

### Full Playwright breakdown

| Spec | Tests | REAL | CONTRACT-ONLY |
|---|---:|---:|---:|
| `acceptance-v2.spec.ts` | 6 | 4 | 2 |
| `admin-mode.contract.spec.ts` | 13 | 0 | 13 |
| `bars-chart-cursor.spec.ts` | 4 | 4 | 0 |
| `bars-csv.spec.ts` | 1 | 1 | 0 |
| `export-job.contract.spec.ts` | 3 | 0 | 3 |
| `layout-keyboard.spec.ts` | 20 | 19 | 1 |
| `monitoring-status.spec.ts` | 6 | 6 | 0 |
| `theme-preset.spec.ts` | 2 | 2 | 0 |
| `websocket-debug.spec.ts` | 1 | 1 | 0 |
| **Total** | **56** | **37** | **19** |

CONTRACT-ONLY tests are: all 13 Admin API contract tests, all 3 Export Job contract tests, the
Service Instance lifecycle keyboard test, the injected Collector-failure state test, and the
injected raw WebSocket frame test. The raw WebSocket and failure-state tests are frontend
behaviour checks with intercepted responses; they are not counted as real backend integration.

---

## 2. Compatibility shrink and zero-caller evidence

Every file below was searched across `web/src` and `web/e2e` before deletion; only self-references
remained.

| Removed | Zero-caller evidence |
|---|---|
| `src/components/kpi-cell.tsx` (`KpiCell`) | `KpiCell` only appeared in its own definition; no import or JSX caller. Monitoring uses `MetricCard`; the other modules use `Card`/business cards. |
| `src/components/module-placeholder.tsx` (`ModulePlaceholder`) | Only self-references; no module still renders a placeholder. |
| `src/components/toolbar.tsx` (`FilterRow`, `Toolbar`) | Only self-references; all pages migrated to local `flex`/`Card` groupings. |
| `src/components/event-timeline.tsx` (`EventTimeline`) | Only self-references; Monitoring uses `AlertsCard`, Security uses `AuditOperationTimeline`. |
| `src/components/ui/panel.tsx` (`Panel`, `PanelHeader`, `PanelTitle`, `PanelDescription`) | `PanelHeader`/`PanelTitle`/`PanelDescription` already had zero callers. `Panel` had only `ModulePlaceholder` and `NotFoundPage`; `NotFoundPage` now uses `Card`, then the module had zero importers. |
| `src/components/ui/badge-variants.ts` | Zero importers; `ui/badge.tsx` owns its own variants. |
| `src/components/ui/button-variants.ts` | Zero importers; `ui/button.tsx` owns its own variants. |

Compatibility props removed after zero-caller checks:

- `PageHeader.meta` — no `meta=` call site; the right-aligned meta render branch was removed.
- `ChartPanel.description` and `TrendChart.description` — no caller passed a description. The
  chart card now keeps definitions in `?` help; `buildTrendChartOption` still receives an empty
  ECharts ARIA description and the title remains the accessible name.
- `HelpTrigger.className` and `HelpTrigger.contentClassName` — no call sites; the fixed D trigger
  and readable help-surface classes are retained internally.

Kept because still used: `Card*`, `MetricCard`, `ChartPanel` (help/legend/state/data table),
`StatusBadge` (`live`, tones), `HelpTrigger` (`label`, `content`, `side`), `PageHeader`
(`title`, optional quiet `description`, `status`, `actions`, `help`), `states`, `Table`, `Dialog`,
`ConfirmDialog`, `DisabledReason`/`DisabledAction`, `Pagination`, `PermissionGate`.

No five-module external behaviour changed. The complete 544-test Vitest suite and 56-test browser
suite pass after the shrink.

---

## 3. Global UI copy sweep

### Source and shell

- `rg -n '·|：' src --glob '!**/*.test.ts' --glob '!**/*.test.tsx'` finds only comments and the
  shared test-helper constant. No UI-generated literal in non-test source contains either symbol.
- `index.html` still had `<title>MDGW · Operator Console</title>` in the built artifact. It is now
  `<title>MDGW Operator Console</title>`; the acceptance spec asserts the browser title has no
  banned symbol and is non-empty. Built assets were rescanned: no `·` or `：`.
- Full-page notice rules were removed from `GlobalHttpNotice` and `SessionExpiryNotice`
  (`border-b border-border` deleted). Top nav and mode banner already had no rule. Remaining
  directional rules are local: table header/row separators, card footer, dialog footer, a card's
  internal definition-list rule, the WS message row rule, and local scroll containers.
- No numeric underlined headings remain. `rg` finds no `border-y`/`border-t-2`/`divide-y` page
  rules, and the browser spec fails on any heading whose text starts with a number. The only
  remaining underlines are ordinary links.

### Browser DOM sweep

`expectNoBannedUiSymbols` walks visible text nodes, excludes `data-raw-evidence="true"` and
`sr-only`, and was applied to:

- all five default module pages at 1440;
- all three Console tabs (Feed Subscription, Service Instance, WebSocket Debug) at 1024, 1280
  and 1440;
- Quality and Security default pages at 1024;
- every migrated Bars region and all seven Bars help triggers;
- expanded `?` help in Monitoring, Bars (all seven triggers), Quality, Security, Feed
  Subscription, Service Instance and WebSocket Debug;
- form validation and high-risk confirmation dialogs (reason/instance-name mismatch, split-chunk
  confirmation, Backfill create/cancel/retry);
- Loading, Empty, Error and Unknown states reached by the real pages and the injected failure state;
- operation results for service lifecycle success, 401/403/409/429 notices, Export Job
  create/download/failure, Feed last-error, Backfill create/cancel/retry/conflict and audit
  detail/timeline.

### Raw evidence exception (samples containing both `·` and `：`)

| Evidence | Sample | Assertion |
|---|---|---|
| WebSocket raw JSON | `{"note":"a·b：c"}` injected through a real browser WebSocket session | `ws-message-detail` has `data-raw-evidence="true"`, text contains `"note": "a·b：c"`, and the surrounding page sweep passes (CONTRACT-ONLY WebSocket frame injection) |
| Operator Audit Event redacted Diff / reasons | `{"note":"a·b：c"}`, `/secret·path：x`, `left · right：value`, `upstream · failed：timeout` | Before/after `<pre>`, redacted path, reason, failure reason and timeline reason all carry `data-raw-evidence="true"` and match byte-for-byte |
| Backend raw errors | `upstream · failed：timeout`, `collector · unavailable：timeout` | Export Job error, Quality Job error and Feed Last Error render the exact message inside `data-raw-evidence="true"`; surrounding copy stays clean |

The sweep never rewrites raw evidence. Test fixtures containing the banned symbols are limited to
tests and the injected contract fixtures.

Necessary risk, permission, validation, operation-result and Request ID information remains
inline. Help contains supplementary definitions only. Examples: confirmation dialogs show the
target instance, reason requirement, drain/reload consequences and mismatch error inline; error
states render Request IDs visibly; `ErrorState` and operation summaries never move Request ID into
`?` help.

---

## 4. Help interaction acceptance

| Module | Hover | Keyboard focus | Touch repeated tap | Escape | Accessible name / non-empty text | Collision / never covers trigger |
|---|---|---|---|---|---|---|
| Monitoring | pass | pass | pass | pass | pass | pass |
| Bar 图与导出 (7 triggers) | pass | pass | pass | pass | pass | pass |
| Console — Feed Subscription | pass | pass | pass | pass | pass | pass |
| Console — Service Instance | pass | pass | pass | pass | pass | pass |
| Console — WebSocket Debug | pass | pass | pass | pass | pass | pass |
| Quality & Backfill | pass | pass | pass | pass | pass | pass |
| Security & Audit | pass | pass | pass | pass | pass | pass |

Evidence: `monitoring-status.spec.ts`, `bars-chart-cursor.spec.ts`, `layout-keyboard.spec.ts`
(feed/WS/service/quality/security), and the new `acceptance-v2.spec.ts` touch/tab and chart-role
tests. `expectTooltipUsable` asserts bounded width, readable tooltip box and zero overlap with the
trigger. Help text is read from `data-help-content` and is non-empty.

Chart value readout and help remain distinct: the new test asserts the chart is `role="img"`, the
keyboard value readout is `role="status"`, the help surface is `role="tooltip"` with
`data-help-content`, the help tooltip contains definitions (`数据源`) but no `role="status"`
readout, and the readout does not contain the help definition. Closing help returns to zero
`role="tooltip"` elements.

---

## 5. Width, theme, keyboard and touch acceptance

| Width / mode | Coverage | Result |
|---|---|---|
| 1440 light | all five modules, all three Console tabs, expanded help, chart readout, screenshots | PASS — no page overflow; wide content scrolls locally |
| 1280 light | all five modules, all three Console tabs, help, keyboard navigation/chart, dialog | PASS — no page overflow |
| 1024 light | all five modules, all three Console tabs, touch contexts | PASS — no page-level horizontal overflow; Revision/Service Instance tables and WebSocket virtual list scroll inside their cards |
| Dark | all five modules screenshot regression, Monitoring dark 1440 screenshot, status pills | PASS — readable body text/background contrast, icon + text pills, no white-card glare |
| Keyboard | top nav Tab/Enter, chart arrows/Home/End, confirm-dialog Escape/Enter, help focus/Escape | PASS |
| Touch | help repeated tap, Console tab taps at 1024 | PASS |

Local overflow evidence:

- Bars Revision table: `table-container` scrollWidth > clientWidth at 1024; page width unchanged.
- Console Service Instance table: local `table-container` scroll; page width unchanged at 1024.
- WebSocket Debug virtual list: local `auto/scroll` on both axes; page width unchanged.
- Quality persistent gap and Backfill Job tables: local `table-container` scroll at 1024.
- Audit Diff before/after: local focusable `overflow: auto` scroll containers; page width unchanged.
- Audit timeline wraps long actor/reason/action text; no page overflow.
- Dialogs (Graceful Stop, Export Job split, Backfill create/cancel/retry) are usable at 1024 and 1280/1440; chart readouts work at 1024, 1280 and 1440.

The browser spec also asserts near-white cards (`min(r,g,b) ≥ 245` in light), card radius ≥ 8 px,
non-`none` soft shadow, card border, rounded status pills with icon + text + non-transparent
background, no full-page decorative divider-like elements, and no numeric headings.

### Curated screenshots

Saved under `web/docs/acceptance-v2/` and produced by `e2e/acceptance-v2.spec.ts`:

| Screenshot | Content |
|---|---|
| `acceptance-v2/light-1440-monitoring.png` | Monitoring, populated KPI cards, charts, freshness, detail cards |
| `acceptance-v2/light-1440-monitoring-help.png` | Monitoring with the Realtime QPS `?` help expanded |
| `acceptance-v2/light-1440-bars.png` | Bars query, chart, Revision table, CSV, Export Job, live follow |
| `acceptance-v2/light-1440-console-feed.png` | Console Feed Subscription |
| `acceptance-v2/light-1440-console-service-instance.png` | Console Service Instance |
| `acceptance-v2/light-1440-console-websocket.png` | Console WebSocket Debug |
| `acceptance-v2/light-1440-quality.png` | Quality gap snapshot, persistent gap and Backfill Job |
| `acceptance-v2/light-1440-security.png` | Security identity and forbidden audit state |
| `acceptance-v2/light-1024-bars.png` | 1024 Bars with a locally scrolling Revision table |
| `acceptance-v2/dark-1440-monitoring.png` | Dark Monitoring readability |

Visual review confirms white/near-white cards with micro-rounded corners, very light shadows,
status pills, no full-page decorative rules and no numeric underlined headings in the five
modules. The dark screenshot shows readable status text/pills and no light-card glare.

---

## 6. Real state truthfulness

The real Loading/Empty/Error/Unknown states never keep a stale healthy KPI or health pill:

- `MonitoringPage.test.tsx` — “shows a stale entry as failed and never as healthy”, “moves to
  unknown with the last success time when a source fails”, “marks trend KPIs unknown when
  Prometheus fails, with the last success”, “keeps cached last-success evidence without showing old
  KPIs as healthy”, “shows explicit empty states without generating health or alert records”.
- `trend-chart.test.tsx` — “hides the stale table and shows an error state on failure”, “shows the
  contract gap state without inventing a chart”, “shows an empty state without auto-expanding the
  range”.
- New browser test (CONTRACT-ONLY failure injection): a failed Collector `/status` produces
  “Collector /status 加载失败”, a visible last-success line, no `data-tone="good"` badge inside the
  Feed Freshness region, and an inline raw error. No old Ready/healthy pill survives.

Cached values retain `lastSuccessAt` evidence and are not relabelled as current health. Sources
that have not succeeded show `最后成功 —` or the last successful timestamp with the explicit
failure copy.

---

## 7. Prototype and production-tree scan

After the final build:

```
rg -n 'PrototypeMark|PROTOTYPE|prototype/operator-console|/prototype/|\?variant=|mock-data|mockData|state=loading|state=empty|state=error' \
  src e2e index.html vite.config.ts package.json
→ NO_MATCHES

rg -n 'variant=[ABCD]\b' src e2e index.html vite.config.ts package.json
→ NO_MATCHES

rg -n 'PrototypeMark|prototype/operator-console|/prototype/|\?variant=|mock-data|mockData|state=loading|state=empty|state=error' \
  dist
→ NO_MATCHES

rg -n '·|：' dist/index.html dist/assets/*.js
→ NO_MATCHES
```

The only `prototype` grep hits in `e2e`/tests are JavaScript `HTMLElement.prototype` spies, not
prototype UI. `variant=` hits in source are Radix component variant attributes (`variant="outline"`
etc.), not a prototype switcher. “Variant D” appears only in design comments/tokens.

Routes remain `/monitoring`, `/bars`, `/console`, `/quality`, `/security` and `*` → `NotFoundPage`.
The production tree has no prototype route, variant switcher, mock-data URL, `state=`
simulation, `PrototypeMark`, prototype CSS or fourth Service Role.

---

## 8. Bundle assessment

| Measurement | Value |
|---|---|
| Pre-v2 baseline main chunk (historical `acceptance-report.md`) | 541.80 kB / 162.22 kB gzip |
| Main chunk before this ticket's vendor split | 656.66 kB / 197.60 kB gzip |
| Final main chunk (`index-*.js`) | **568.23 kB / 168.07 kB gzip** |
| Final delta vs pre-v2 baseline | **+26.43 kB raw (+4.9%), +5.85 kB gzip (+3.6%)** |
| Final ECharts chunk | 490.83 kB / 166.13 kB gzip |
| Final total JS | 1,424,744 bytes across 9 chunks |
| Final CSS | 60.68 kB / 14.08 kB gzip |
| Final JS+CSS total | 1,485,421 bytes; 464.99 kB gzip by the Vite-reported gzip sizes |
| Final `dist/` including fonts and HTML | 1,581,059 bytes |

Assessment: the main chunk remains above Vite's 500 kB advisory, exactly as the pre-v2 baseline
did; this is a continuing advisory, not a new build failure. The v2 growth comes from the shared D
components, HelpTrigger, the Monitoring cards/freshness/alerts surfaces and the migrated Console,
Quality and Security views. A safe, dependency-free vendor split (`radix-ui`, React Hook Form,
Lucide/Hugeicons, TanStack Virtual into their own chunks) reduced the main chunk by 88.43 kB raw /
29.53 kB gzip from the pre-split v2 build. ECharts remains isolated. No further unassessed
regression was found; route-level lazy loading was not introduced because it would change loading
behaviour beyond this ticket's cleanup/acceptance scope.

---

## 9. REAL / CONTRACT-ONLY / BACKEND-BLOCKED / NOT RUN

### REAL

- Real local Collector, Api and Replay service roles on the documented ports with a temporary
  Dataset under `E:/temp/mdgw-e2e-oc2-09`.
- Real FastAPI REST/WS, real Prometheus container, real public `/ws`.
- Five module default pages, three Console tabs, Bars query/chart/Revision/cursor/CSV, WebSocket
  Debug bounded list/segments/ping barrier, Monitoring trends/freshness, layouts at 1024/1280/1440,
  keyboard/touch help, light/dark theme and curated screenshots.

### CONTRACT-ONLY

- Admin API mode, OIDC/session, Access Roles, service-instance lifecycle writes, Feed Subscription
  and flow-control writes, Export Jobs, Backfill Jobs and persistent gaps, Operator Audit Event
  query/diff/timeline and Admin WebSocket.
- 401/403/409/429 contract scenarios, CSRF/idempotency headers, high-risk confirmations, split-chunk
  creation and the injected frontend failure/raw-WS states.
- Evidence: `admin-mode.contract.spec.ts` (13), `export-job.contract.spec.ts` (3), one
  `layout-keyboard.spec.ts` lifecycle test, two `acceptance-v2.spec.ts` injected-state tests, plus
  MSW Vitest contract suites. These are not real Admin/OIDC end-to-end evidence.

### BACKEND-BLOCKED

- No real `/api/v1/admin/*`, OIDC provider/session, Admin WebSocket, persistent Gap/Job store,
  Export Job worker, Feed Subscription API or flow-control API exists in this repository.
- Real Admin end-to-end and real OIDC login cannot be claimed until those backends exist.

### NOT RUN

- Real Admin API/OIDC end-to-end (backend absent).
- External venue/network feed (deliberately not used; the real stack is local and Dataset-backed).
- Docker/Compose deployment of this branch (not part of ticket 09 and would touch deployment outside
  the allowed scope). Historical acceptance report retains the earlier Docker evidence.
- Full Edge suite and full mobile navigation (out of this ticket's desktop acceptance requirement;
  touch contexts cover the key help/tab interactions).

---

## 10. Deviations, failures and flake handling

1. **Built title copy.** The production artifact still had `MDGW · Operator Console` in
   `index.html`. Fixed to `MDGW Operator Console`; a browser assertion now covers the document
   title.
2. **Full-page notice rules.** `GlobalHttpNotice` and `SessionExpiryNotice` still carried
   `border-b`; removed. Local card/table/dialog separators were intentionally kept.
3. **Compatibility API surface.** `HelpTrigger.className`/`contentClassName`,
   `PageHeader.meta`, `ChartPanel.description` and `TrendChart.description` were removed after
   confirming zero callers. This is an internal API shrink; no page behavior changed.
4. **Touch-help flake during an intermediate full run.** One intermediate full-suite run reported
   `bars-chart-cursor.spec.ts` touch help failing to close on the second tap. That run overlapped
   with edits to `index.html` and the new spec, which triggered Vite dev-server reloads while the
   suite was executing. The test passed in an isolated rerun (1/1, 3.0 s), in the clean full run
   (56/56, 1.9 min) and in the post-merge full run (56/56, 1.8 min). No product change was needed.
5. **Test-authoring corrections.** The first screenshot draft used `[data-slot="card"]` only, but
   `Card` receives component-specific slots such as `metric-card`/`chart-panel`; the selector was
   corrected. The monitoring failure injection initially expected an ops error body, but the ops
   client only surfaces `Ops endpoint returned HTTP 500`; the test now asserts the visible failure
   and absence of a healthy badge, while banned-symbol raw samples are covered by the Admin,
   Export Job, Feed and WebSocket specs.
6. **Bundle advisory.** Main chunk is 568.23 kB / 168.07 kB gzip, still above 500 kB like the
   baseline. The vendor split and full assessment are recorded in section 8; no build failure.

No ticket Status line or `## Comments` section was modified. `web/docs/acceptance-report.md` was
not edited.
