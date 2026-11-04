# Consumer examples

Five runnable consumers, one per interface. Every script accepts `--help` and
takes endpoints/ports as arguments. The first four need no runtime dependency
beyond what the project already ships (`pyarrow`, `pyzmq`, the standard library,
and `websockets` for the WebSocket example). The NautilusTrader example needs
the optional `nautilus` extra.

## Start the demo stack

From the repository root:

```bash
conda activate nautilus
pip install -e .

# Terminal 1: collector -> api (offline fake feed, REST/WS on 127.0.0.1:8080)
mdgw up --config config/gateway.demo.toml

# Terminal 2: replay role (control 5557, market PUB 5558)
mdgw run --service replay --config config/gateway.demo.toml
```

Demo data is written to `./data-demo/bars`. The fake feed emits 20 closed 1m
Bars for BTC/USDT and ETH/USDT exactly once per collector start.

Wait for readiness before running the examples:

```bash
curl http://127.0.0.1:8080/readyz
```

## 1. Direct Parquet (pyarrow only)

No gateway import. Reads the Hive-partitioned Dataset directly.

```bash
python examples/parquet_history.py --data-root ./data-demo --interval 1m
python examples/parquet_history.py --data-root ./data-demo \
    --instrument binance:BTC/USDT --start 2026-01-01T00:00:00Z --limit 5
```

If you see no rows, check that the collector has flushed (the demo feed writes
on its first ingest pass) and that `./data-demo/bars` exists.

## 2. REST history

```bash
python examples/rest_history.py --base-url http://127.0.0.1:8080 \
    --instrument binance:BTC/USDT --interval 1m

# Render a pandas DataFrame (optional dependency)
python examples/rest_history.py --base-url http://127.0.0.1:8080 \
    --interval 1m --pandas
```

Decimal columns stay `decimal128(38, 18)` and decode to `decimal.Decimal`; the
pandas view is `object` dtype. Never call `astype(float)` on them.

## 3. Live WebSocket

```bash
python examples/ws_live.py --base-url http://127.0.0.1:8080 \
    --topic market.bar. --count 5
```

The demo fake feed is one-shot: it publishes its Bars only when the collector
starts. To watch live events, start `ws_live.py` first and then restart the
collector (`Ctrl-C` on `mdgw up` and run it again), or point the script at a
real Feed Handler. Topics are prefixes, so `market.bar.` matches every Bar and
`market.bar.1m.binance:` matches every instrument at 1m. Press `Ctrl-C` to
exit.

## 4. Replay

The replay role logs its resolved endpoints at startup. Its structured JSON log
contains an `event: "replay.started"` line with `control_endpoint` and
`publish_endpoint` fields:

```json
{"event": "replay.started", "status": "ready",
 "control_endpoint": "tcp://127.0.0.1:5557",
 "publish_endpoint": "tcp://127.0.0.1:5558"}
```

The demo config pins those ports, so the commands are:

```bash
python examples/replay.py \
    --control-endpoint tcp://127.0.0.1:5557 \
    --publish-endpoint tcp://127.0.0.1:5558 \
    --instrument binance:BTC/USDT --interval 1m --mode asap

# Step mode: the session starts paused and consumes Step Credits
python examples/replay.py \
    --control-endpoint tcp://127.0.0.1:5557 \
    --publish-endpoint tcp://127.0.0.1:5558 \
    --mode step --steps 5
```

`asap` pushes as fast as possible. `realtime` paces Bars by `--speed`; use a
large value such as `--speed 1000` for a short demo. The script prints the
End of Session message (`reason` and `bars_sent`) when the range is exhausted.

## 5. NautilusTrader MACD backtest

`nautilus_macd_backtest.py` fetches Arrow Bar history with `mdgw_client`,
converts it to a pandas frame (latest revision, ascending time), and uses
NautilusTrader's `BarDataWrangler` to produce `Bar` objects, then runs a MACD
crossover strategy through `BacktestEngine`. Decimals and nanosecond timestamps
stay exact; no price passes through a float.

Install the optional consumer stack (NautilusTrader 2.x is a pre-release):

```bash
pip install -e ".[nautilus]"
# or: pip install -U --pre nautilus_trader numpy pandas
```

Then, with the Docker stack running (`docker compose up -d`):

```bash
python examples/nautilus_macd_backtest.py \
    --base-url http://127.0.0.1:8080 \
    --instrument binance:BTC/USDT --interval 1m --reports
```

The Compose demo feed emits a flat price series, so MACD produces zero
crossings. Point `--base-url` at a gateway with real history (or backfilled /
replayed data) to see signals, fills and PnL. The script prints the fetched Bar
count, MACD crossings, submitted entries, fills and the final account total.

### Real history

Two ways to give the gateway real Bars:

```bash
# A) Live ccxt collector (needs network access to the venue; backfills 24h)
pip install -e .
mdgw up --config config/gateway.toml --root data-live --log-level INFO

# B) Binance archive backfill (no venue API key; uses data.binance.vision)
mdgw backfill --archive --config config/gateway.toml --root data-real \
    --instrument binance:BTC/USDT --venue binance --market-type spot \
    --interval 1m --start 2025-01-01 --end 2025-01-03
mdgw run --service api --config config/gateway.toml --root data-real
```

Stop Docker first (`docker compose down`) so the local API can bind 8080. Then
run the example as above with `--base-url http://127.0.0.1:8080`. The archive
mode reads the public Binance K-line ZIPs and writes the same Parquet Dataset,
so the REST/WS contract and the example are unchanged. `--start/--end` select
the real window; one day of 1m Bars is already enough for MACD(12, 26, 9).

If the venue API requires a proxy, add it to the `[[feeds]]` entry; the same
proxy is used for ccxt.pro REST and WSS:

```toml
[[feeds]]
# ...
proxy = "http://127.0.0.1:12334"
```

The gateway itself never imports NautilusTrader; this is a pure consumer
example. `ADR-0001` still holds: no framework adapters live under `src/`.

## Notes

- The gateway binds loopback by default. Remote access needs a deliberate
  bind change; v1 has no authentication.
- `parquet_history.py` bypasses `_manifest.json`, so it may briefly see files
  that Compaction has marked inactive. Use REST or the store reader when that
  distinction matters.
- The examples are also smoke-tested by
  `tests/integration/test_examples_docs_smoke.py`.
