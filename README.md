# Market Data Gateway

A framework-neutral market data gateway. It normalizes a Venue feed into one
versioned Arrow Data Contract, persists closed **Bars** as a Parquet Dataset,
and serves the same data over REST, WebSocket, ZeroMQ and a deterministic
Replay Protocol. v1 carries Bar data only.

Three Service Roles share the Dataset:

| Role | Responsibility |
|---|---|
| **collector** | ingest -> normalize -> aggregate -> write Parquet + advance the High Water Mark -> publish live Bars |
| **api** | REST history, WebSocket fan-out, and ZMQ re-publish of the collector stream |
| **replay** | historical Replay Sessions over ZMQ control + market endpoints |

The consumer-facing contract is [`docs/data-contract.md`](docs/data-contract.md).
Design rationale is in [`DESIGN.md`](DESIGN.md); terminology is in
[`GLOSSARY.md`](GLOSSARY.md).

## Architecture

```
 Feed Handler (ccxt | fake)
        |
        | RawBar
        v
   normalize  ------------------>  BarAggregator (5m/15m/1h/1d parents)
        |                                   |
        +----------------+------------------+
                         v
              Parquet Dataset + Manifest          High Water Mark (state.db)
              <data_root>/bars/...                 <data_root>/state.db
                         |
             +-----------+------------+
             |                        |
             v                        v
      api Service Role          replay Service Role
   REST /api/v1/*  /ws          ZMQ control (REQ/REP)
   ZMQ PUB  market.bar.*        ZMQ PUB  replay.<id>.market.bar.*
             ^                        |
             |                        |
     collector PUB  ------------->  external consumers
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
dashboard pre-provisioned.

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
| CLI | `mdgw up`, `mdgw run --service {collector,api,replay}`, `mdgw compact`, `mdgw gc`, `mdgw backfill --archive` |
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
- **Replay reads before publishing.** An `asap` session materializes every
  matching Bar before the first message; very large ranges cost memory.
- **Loopback, no auth.** The gateway defaults to `127.0.0.1` and has no
  authentication, TLS or CORS; do not expose it directly.
- **JSON row ceiling.** `/api/v1/bars?format=json` rejects ranges over 100,000
  Bars with `range_too_large`; use Arrow/CSV/export for larger reads. There is
  no offset pagination, only time cursors.
- **Single-Instrument pagination only.** The `page` object (`has_more`,
  `next_start`) is returned only for JSON queries that supply both
  `instrument` and `limit`. Paginated JSON is ordered by `open_ts` ascending;
  non-paginated JSON, Arrow, CSV and export keep storage order and make no
  total-order promise. Multi-Instrument pagination is not offered.
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
