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
```

`/readyz` returns 200 once the collector handshake is observed. The JSON Bars
response wraps each row in a Message Envelope; decimals and timestamps are
strings (see the Data Contract).

Runnable consumers live in [`examples/`](examples/README.md): direct Parquet,
REST, WebSocket and Replay.

## Entry points

| Surface | Entry |
|---|---|
| CLI | `mdgw up`, `mdgw run --service {collector,api,replay}`, `mdgw compact`, `mdgw gc`, `mdgw backfill --archive` |
| REST | `/api/v1/bars`, `/api/v1/bars/export`, `/api/v1/instruments`, `/api/v1/status`, `/healthz`, `/readyz`, `/status`, `/metrics` |
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

```bash
conda run -n nautilus python -m pytest tests -q
```

Network tests are marked `network` and deselected by default
(`addopts = -m 'not network'`). The baseline suite is offline.

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
  matters. `GatewayRuntime` writes immediately.
- **Archive import and collector share state.** Run `mdgw backfill --archive`
  while the collector is stopped (or otherwise coordinate them): both use the
  same Dataset and High Water Mark database.
- **Fake feed is one-shot.** It publishes its fixed Bar run only when the
  collector starts. Restart the collector to see live WebSocket events again.
- **Replay reads before publishing.** An `asap` session materializes every
  matching Bar before the first message; very large ranges cost memory.
- **Loopback, no auth.** The gateway defaults to `127.0.0.1` and has no
  authentication; do not expose it directly.
- **JSON row ceiling.** `/api/v1/bars?format=json` rejects ranges over 100,000
  Bars with `range_too_large`; use Arrow/CSV/export for larger reads. There is
  no offset pagination, only time cursors.
- **Compaction/GC are operator commands**, not background jobs.
