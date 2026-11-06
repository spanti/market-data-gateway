# Market Data Gateway

A framework-neutral market data gateway. It normalizes a Venue feed into one
versioned Arrow Data Contract with Polars batch rules, persists closed **Bars**
as immutable Parquet Data Files under a Manifest, and serves the same data over
REST, WebSocket, ZeroMQ and a deterministic Replay Protocol. Reads resolve a
Manifest Snapshot and a Dataset Read Lease, then stream Arrow batches from
DuckDB 2.x using explicit Data File lists; PyArrow remains the write, IPC,
export and Compaction boundary. v1 carries Bar data only.

Three Service Roles share the Dataset:

| Role | Responsibility |
|---|---|
| **collector** | ingest -> normalize -> aggregate -> write Parquet + advance the High Water Mark -> publish live Bars |
| **api** | REST history, WebSocket fan-out, and ZMQ re-publish of the collector stream |
| **replay** | historical Replay Sessions over ZMQ control + market endpoints |

The consumer-facing contract is [`docs/data-contract.md`](docs/data-contract.md).
Design rationale is in [`DESIGN.md`](DESIGN.md); terminology is in
[`GLOSSARY.md`](GLOSSARY.md). The storage migration decisions are recorded in
[ADR-0013](docs/adr/0013-manifest-aware-duckdb-reads.md) (Manifest-aware
DuckDB reads), [ADR-0014](docs/adr/0014-polars-batch-normalization-with-quarantine.md)
(Polars batch Normalization and Quarantine) and
[ADR-0015](docs/adr/0015-bounded-replay-with-explicit-failure-termination.md)
(bounded Replay and failure termination).

## Architecture

```
 Feed Handler (ccxt | fake)
        |
        | RawBar
        v
   normalize (Polars batch) ---->  Quarantine (rejected rows, independent Dataset)
        |
        v
   BarAggregator (5m/15m/1h/1d parents)
        |
        v
   Parquet Dataset + Manifest          High Water Mark (state.db)
   <data_root>/bars/...                 <data_root>/state.db
        |
        v
   Store: Manifest Snapshot + Dataset Read Lease -> DuckDB explicit file lists
        |
        +-------------+
        |             |
        v             v
   api Service Role   replay Service Role
 REST /api/v1/* /ws   ZMQ control (REQ/REP)
 ZMQ PUB market.bar.* ZMQ PUB replay.<id>.market.bar.*
        ^             |
        |             |
 collector PUB ---> external consumers
```

Live topics are `market.bar.<interval>.<instrument_id>`; replay topics are
prefixed `replay.<session_id>.` (see the Data Contract).

## Run everything with one command

### Docker (offline fake feed)

```bash
docker compose up --build
```

This starts collector, api and replay from `config/gateway.docker.toml`. The
fake feed never reaches a Venue. API: <http://127.0.0.1:8080>.

If a previous attempt left containers or ports behind, reset before retrying:

```bash
docker compose down --remove-orphans
docker compose up --build -d
```

If dependency installation is slow or blocked, point pip at a mirror without
editing the Dockerfile:

```bash
PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple docker compose up --build -d
```

### Optional observability stack

Prometheus and Grafana are opt-in. Plain `docker compose up` does not start
them; add the override file:

```bash
docker compose -f docker-compose.yml -f docker-compose.observability.yml up -d
```

Prometheus listens on <http://localhost:9090> and scrapes the collector, api
and replay `/metrics` endpoints every 15 seconds. Grafana listens on
<http://localhost:3000> (admin password from `GRAFANA_ADMIN_PASSWORD`, default
`admin` for local use) with the Prometheus datasource and the gateway
dashboard pre-provisioned. Registry mirrors can be supplied with
`PROMETHEUS_IMAGE` and `GRAFANA_IMAGE`.

### Local Python

Requires the `nautilus` conda environment (Python 3.13) used by this project.

```bash
conda activate nautilus
pip install -e .

# Terminal 1: collector -> api (REST/WS on 127.0.0.1:8080)
mdgw up --config config/gateway.demo.toml

# Terminal 2: replay role (control 5557, market PUB 5558)
mdgw run --service replay --config config/gateway.demo.toml
```

`config/gateway.demo.toml` binds every port to loopback and writes to
`./data-demo`, so it does not mix with a real `./data` Dataset. The fake feed
emits 20 closed 1m Bars for BTC/USDT and ETH/USDT, once per collector start.

## Runtime requirements

- Python 3.13 (`nautilus` conda environment).
- `pyarrow`, **DuckDB 2.x** (`duckdb>=2.0,<3`) and **Polars 2.1**
  (`polars==2.1.0`), plus the packages declared in `pyproject.toml`.
- DuckDB 1.x is unsupported: 2.0 is the first line that preserves Parquet UTC
  nanoseconds. There is no `use_duckdb` switch and no fallback read engine:
  startup fails if DuckDB is missing or not 2.x.
- DuckDB reads Bars from explicit Manifest file lists and streams Arrow
  RecordBatches; Polars runs only at the batch Normalization boundary
  (Collector, Backfill and archive import), never on the read path; PyArrow
  remains the Data File write, Arrow IPC/ZMQ, export, Compaction and test
  boundary.

## Storage resources and operator maintenance

The read path has explicit resource bounds. The relevant configuration
defaults are:

```toml
[storage.duckdb]
memory_limit = "1GB"
threads = 4
# temp_directory unset = DuckDB's own default temporary directory
max_temp_directory_size = "10GB"
arrow_batch_size = 65536

[replay]
max_sessions = 4
heartbeat_s = 1.0
batch_size = 4096          # Bars per DuckDB Arrow batch
prefetch_batches = 2       # bounded queue depth

[storage.quarantine]
# root unset = <storage.root>/quarantine (must not be inside the Bars Dataset)
schema_version = 1
rule_version = "polars-1"
retention_days = 30        # 0 = keep forever
```

The DuckDB settings are resource limits, not an engine switch. Replay keeps
about `batch_size * (prefetch_batches + 2)` Bars in flight instead of the whole
range, but a global full-range sort can still use DuckDB temporary disk, so
`temp_directory` and `max_temp_directory_size` should be set for the deployment.
Quarantine records are independent of the Bars Dataset; its retention defaults
to 30 days and is enforced only by an explicit command.

Operator commands:

```bash
mdgw compact --older-than 24h             # merge small Data Files per partition
mdgw gc --dry-run                         # default; report expired whole months
mdgw gc --apply                           # delete expired Bars months
mdgw quarantine-gc --dry-run              # default; report expired Quarantine months
mdgw quarantine-gc --apply                # delete expired Quarantine months
mdgw quarantine-gc --apply --retention-days 90
```

Compaction and Bars GC skip partitions/months protected by an active Dataset
Read Lease or Replay Session lock and report how many were skipped. Expired
heartbeats (60-second window) are ignored, so a crashed reader cannot block
maintenance forever. Quarantine GC removes only complete UTC months past the
retention cutoff; the current UTC month is always protected, and there is no
automatic background job.

## Verify

```bash
curl http://127.0.0.1:8080/healthz
curl http://127.0.0.1:8080/readyz
curl "http://127.0.0.1:8080/api/v1/bars?interval=1m&format=json"
curl http://127.0.0.1:8080/api/v1/status

# Generated API documentation (FastAPI/ASGI)
curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:8080/openapi.json   # 200
curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:8080/docs           # 200
curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:8080/redoc          # 404
```

`/readyz` returns 200 once the collector handshake is observed. The JSON Bars
response wraps each row in a Message Envelope; decimals and timestamps are
strings (see the Data Contract). `docs/data-contract.md` is the normative
contract; `/openapi.json` and `/docs` are generated documentation only.

Runnable consumers live in [`examples/`](examples/README.md): direct Parquet,
REST, WebSocket and Replay.

## Entry points

| Surface | Entry |
|---|---|
| CLI | `mdgw up`, `mdgw run --service {collector,api,replay}`, `mdgw compact`, `mdgw gc`, `mdgw quarantine-gc`, `mdgw backfill --archive` |
| HTTP surface | FastAPI/ASGI served by `AsgiServer` (uvicorn on a background thread) with resource-split routes and layered request-context/rate-limit middleware |
| REST | `/api/v1/bars`, `/api/v1/bars/export`, `/api/v1/instruments`, `/api/v1/status`, `/healthz`, `/readyz`, `/status`, `/metrics` |
| API docs | `/openapi.json` (OpenAPI) and `/docs` (Swagger UI); `/redoc` is not served |
| WebSocket | `/ws` (`subscribe`, `unsubscribe`, `ping`) |
| ZMQ live | `collector_pub_bind` / `api_pub_bind` (multi-frame `[topic][Arrow IPC stream]`) |
| ZMQ replay | `[replay].control_port` (REQ/REP JSON) and `publish_port` (market PUB) |
| Config | `config/gateway.toml` (real feed), `config/gateway.demo.toml` (local fake), `config/gateway.docker.toml` (compose) |
| Instrument Reference Data | `config/instruments/*.yaml` |
| Client SDK | `src/mdgw_client` (`GatewayClient`, `ReplayClient`, `LiveSubscription`) |
| Data Contract | [`docs/data-contract.md`](docs/data-contract.md) |
| Examples | [`examples/`](examples/README.md) |
| Benchmarks | [`benchmarks/`](benchmarks/README.md) and [`RESULTS.md`](benchmarks/RESULTS.md) |

## Tests and benchmarks

Run the full offline suite from the repository root:

```bash
conda run -n nautilus python -m pytest -q
```

Network tests are marked `network` and deselected by default
(`addopts = -m 'not network'`). The full offline suite is the acceptance gate.

```bash
python benchmarks/bench_ingest.py
python benchmarks/bench_query.py
python benchmarks/bench_replay.py
```

## Known limitations (v1)

- **Bar-only.** No trades, quotes, order book or tick data.
- **History freshness.** Historical reads only see flushed Data Files. The
  collector Service Role buffers writes (64 MiB or 1 hour, whichever comes
  first; set `flush_max_bytes` or `flush_max_age_s` to `0` for immediate flush)
  and advances its data High-Water Mark only after a successful flush. Bars are
  live on the Streaming Interface before they appear in history; consult
  `last_flushed_ts` in `/api/v1/status` and use the live stream when freshness
  matters. A Bar dropped by a full queue is counted in lag and caps the HWM
  below itself, so the next Backfill recovers it. `GatewayRuntime` writes
  immediately.
- **Archive import and collector share state.** Run `mdgw backfill --archive`
  while the collector is stopped (or otherwise coordinate them): both use the
  same Dataset and High Water Mark database.
- **Fake feed is one-shot.** It publishes its fixed Bar run only when the
  collector starts. Restart the collector to see live WebSocket events again.
- **Replay is bounded, with explicit failure termination.** All Replay Modes
  stream a DuckDB ordered query in bounded Arrow batches with a bounded
  prefetch (`[replay].batch_size`, `prefetch_batches`), so process memory does
  not grow with the total result row count. A global full-range sort can still
  use DuckDB temporary disk, bounded by `[storage.duckdb]`. A read failure
  ends the session with `reason="error"`, a sanitized stable `error` object and
  the exact `bars_sent`; it is never reported as an empty replay. The SDK
  raises `ReplayFailedError`.
- **Quarantine is not a public API.** Rejected inputs are written to
  `<data_root>/quarantine` (configurable) and summarized in collector
  `/status` and `/metrics`; REST, export and Replay never read or expose
  Quarantine records. Its retention is enforced only by the explicit
  `mdgw quarantine-gc` command.
- **Loopback, no auth.** The gateway defaults to `127.0.0.1` and has no
  authentication, TLS or CORS; do not expose it directly.
- **JSON row ceiling.** `/api/v1/bars?format=json` rejects ranges over 100,000
  Bars with `range_too_large`; use Arrow/CSV/export for larger reads. There is
  no offset pagination, only time cursors.
- **Single-Instrument pagination only.** The `page` object (`has_more`,
  `next_start`) is returned only for JSON queries that supply both
  `instrument` and `limit`. Paginated JSON is ordered by `open_ts` ascending;
  non-paginated JSON, Arrow, CSV and export make no total-order promise (not
  even physical storage order). Multi-Instrument pagination is not offered.
  REST returns every stored raw Revision; Revision-heavy Time Cursor behavior
  is tracked separately in
  [`docs/revision-heavy-time-cursor.md`](docs/revision-heavy-time-cursor.md).
- **HTTP surface details.** HEAD is not part of the contract, trailing slashes
  are not redirected (`redirect_slashes=False`), and `/docs` cannot be
  disabled (Swagger UI is always served; `/redoc` is not). An empty instrument
  id such as `/api/v1/instruments/` returns `not_found` (404), matching the
  retired route. Errors always use the
  `{"error": {code, message, details, request_id}}` envelope with
  `X-Request-ID`; collector/replay health endpoints keep their existing
  response shapes.
- **Rate limiting is opt-in and in-process.** `[rate_limit]` is disabled by
  default. When enabled it is a per-IP/per-route token bucket with fixed
  exemptions (`/healthz`, `/readyz`, `/metrics`, `/openapi.json`, `/docs`);
  state is per process, so it assumes a single worker and does not coordinate
  across processes. Over-limit requests get 429, `Retry-After` and
  `rate_limited`.
- **aiohttp is no longer a direct dependency.** The gateway imports neither
  aiohttp nor its old `HttpServer`; aiohttp may still be present in the
  environment only as a transitive dependency of `ccxt`.
- **Compaction/GC are operator commands**, not background jobs.
