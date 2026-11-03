# Consumer examples

Four runnable consumers, one per interface. Every script accepts `--help`,
takes endpoints/ports as arguments, and needs no runtime dependency beyond what
the project already ships (`pyarrow`, `pyzmq`, the standard library, and
`websockets` for the WebSocket example).

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

## Notes

- The gateway binds loopback by default. Remote access needs a deliberate
  bind change; v1 has no authentication.
- `parquet_history.py` bypasses `_manifest.json`, so it may briefly see files
  that Compaction has marked inactive. Use REST or the store reader when that
  distinction matters.
- The examples are also smoke-tested by
  `tests/integration/test_examples_docs_smoke.py`.
