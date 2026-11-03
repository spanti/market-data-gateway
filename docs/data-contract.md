# Data Contract v1

This document is the consumer-facing contract for the Market Data Gateway v1.
It describes the Arrow schema, the JSON/CSV wire forms, the Parquet Dataset
layout, the Streaming Interface topics, the Replay Protocol and the stable
error codes. `DESIGN.md` is the design rationale; this file is what a client
should code against.

v1 carries **Bar** data only. Every payload has `schema_version = 1`.

## 1. Representation rules

The canonical representation is Arrow. All interfaces derive from the same
logical schema, so a value never changes meaning between Parquet, ZMQ, Arrow
IPC, JSON and CSV.

| Concept | Arrow type | Notes |
|---|---|---|
| Timestamps | `timestamp[ns, tz=UTC]` | int64 nanoseconds since the Unix epoch; UTC only |
| Prices, volume, sizes, multipliers | `decimal128(38, 18)` | exact decimal, never `float64` |
| Identity, intervals, names | `string` | UTF-8 |
| Counters, seq, revision | integer types | see the field tables |

Conversion by surface:

- **Arrow / Parquet / Arrow IPC / CSV** keep the exact types above.
- **JSON** has no decimal or nanosecond type. Timestamps are ISO-8601 strings
  with exactly nine fractional digits and a `Z` suffix
  (`2026-10-30T12:34:56.123456789Z`); decimals are decimal strings
  (`"65000.1"`). They must be parsed with `decimal.Decimal`, not `float`.
- **CSV** prints timestamps in the same ISO form and decimals as exact decimal
  text. A price never passes through a float on any output path.
- **Instrument Reference Data** is YAML at rest (`config/instruments/*.yaml`)
  and is exported to an Arrow Instrument Snapshot. Decimals and timestamps are
  strings in the JSON API, with the same rules.

Readers must reject a Dataset whose `schema_version` does not match instead of
guessing (the gateway raises `schema_version_mismatch`).

## 2. Bar

A Bar is a closed interval. `close_ts = open_ts + interval`, and
`ingest_ts >= close_ts` is enforced before a Bar enters the canonical stream.
Partial/current intervals are never published.

The logical Bar schema is `BAR_SCHEMA` in `src/mdgw/core/schema.py`:

| Field | Arrow type | Nullable | Semantics |
|---|---|---|---|
| `schema_version` | `uint16` | no | Data Contract version; `1` |
| `data_type` | `string` | no | always `"bar"` |
| `instrument_id` | `string` | no | `<venue>:<symbol>`, e.g. `binance:BTC/USDT` |
| `interval` | `string` | no | one of `1m`, `5m`, `15m`, `1h`, `1d` |
| `open_ts` | `timestamp[ns, tz=UTC]` | no | inclusive start of the Bar period |
| `close_ts` | `timestamp[ns, tz=UTC]` | no | exclusive end; `open_ts + interval` |
| `event_ts` | `timestamp[ns, tz=UTC]` | no | event time carried by the envelope; equals `close_ts` for Bars |
| `ingest_ts` | `timestamp[ns, tz=UTC]` | no | when the gateway accepted/generated this revision |
| `feed` | `string` | no | Feed Handler name, e.g. `binance_spot` or `fake_spot` |
| `seq` | `int64` | yes | source sequence number when the Venue supplies one; otherwise null |
| `aggregation_source` | `string` | no | `external` for native Venue Bars, `internal` for gateway aggregation |
| `open` | `decimal128(38, 18)` | no | first trade price in the period |
| `high` | `decimal128(38, 18)` | no | highest price |
| `low` | `decimal128(38, 18)` | no | lowest price |
| `close` | `decimal128(38, 18)` | no | last trade price |
| `volume` | `decimal128(38, 18)` | no | base-asset volume |
| `trade_count` | `uint32` | yes | trade count when the Venue provides it; otherwise null |
| `revision` | `uint32` | no | version of this Bar identity; `0` for the first publication |

Notes:

- `interval` is a Hive partition column in Parquet. The physical Data File
  omits it, but every read path restores it, so consumers see the full logical
  schema.
- Aggregated parent Bars are produced only from complete base periods. Their
  `aggregation_source` is `internal`, `event_ts = close_ts`, and `seq` is null.
- `trade_count` on an aggregate is the sum of child counts, or null when every
  child count is null.

## 3. Message Envelope

Every streaming message carries these envelope fields alongside the Bar
columns. They are also present as columns of `BAR_SCHEMA` (see
`MESSAGE_ENVELOPE_SCHEMA` in `src/mdgw/core/schema.py`):

| Field | Arrow type | Nullable | Semantics |
|---|---|---|---|
| `schema_version` | `uint16` | no | `1` |
| `data_type` | `string` | no | `"bar"` |
| `instrument_id` | `string` | no | Bar identity |
| `event_ts` | `timestamp[ns, tz=UTC]` | no | equals `close_ts` for Bars |
| `ingest_ts` | `timestamp[ns, tz=UTC]` | no | gateway ingest time |
| `feed` | `string` | no | Feed Handler name |
| `seq` | `int64` | yes | optional source sequence |

JSON Message Envelope example (the shape used by the WebSocket `event` data
and by `GET /api/v1/bars?format=json`):

```json
{
  "schema_version": 1,
  "data_type": "bar",
  "instrument_id": "binance:BTC/USDT",
  "event_ts": "2026-10-30T12:35:00.000000000Z",
  "ingest_ts": "2026-10-30T12:35:00.130000000Z",
  "feed": "binance_spot",
  "seq": 123456,
  "payload": {
    "interval": "1m",
    "open_ts": "2026-10-30T12:34:00.000000000Z",
    "close_ts": "2026-10-30T12:35:00.000000000Z",
    "aggregation_source": "external",
    "open": "65000.1",
    "high": "65010.0",
    "low": "64990.0",
    "close": "65005.5",
    "volume": "12.34",
    "trade_count": 128,
    "revision": 0
  }
}
```

The JSON `bars` endpoint returns `{"bars": [<envelope>, ...]}`.

## 4. Instrument

`INSTRUMENT_SCHEMA` in `src/mdgw/core/schema.py`:

| Field | Arrow type | Nullable | Semantics |
|---|---|---|---|
| `instrument_id` | `string` | no | `<venue>:<symbol>`, primary key |
| `venue` | `string` | no | `binance`, `ctp`, ... |
| `symbol` | `string` | no | native Venue spelling |
| `asset_class` | `string` | no | `crypto`, `futures`, `equity`, ... |
| `instrument_type` | `string` | no | `spot`, `perp`, `future`, `stock`, ... |
| `base` | `string` | no | base asset |
| `quote` | `string` | no | quote asset |
| `tick_size` | `decimal128(38, 18)` | no | minimum price increment |
| `lot_size` | `decimal128(38, 18)` | no | minimum quantity increment |
| `multiplier` | `decimal128(38, 18)` | no | contract multiplier; `1` for spot |
| `timezone` | `string` | no | IANA description of the source; partitioning uses UTC |
| `listing_ts` | `timestamp[ns, tz=UTC]` | no | listing time |
| `delisted_ts` | `timestamp[ns, tz=UTC]` | yes | non-null means delisted |

## 5. Parquet Dataset layout

The Dataset root is `<data_root>/bars`. Every level is Hive-style
`key=value` so `pyarrow.dataset` can prune partitions without reading files:

```
<data_root>/
  bars/
    asset_class=crypto/
      venue=binance/
        symbol=BTC%2FUSDT/
          interval=1m/
            year=2026/
              month=11/
                data-<start_ns>-<end_ns>-<uuid>.parquet
  instruments/
    snapshot-<config_hash>.parquet
```

- `symbol` is percent-encoded (`urllib.parse.quote(symbol, safe="")`), so
  `BTC/USDT` becomes the single path segment `BTC%2FUSDT`. The row always
  carries the decoded `instrument_id`.
- `year`/`month` come from `open_ts` in UTC.
- File name: `data-<start_ns>-<end_ns>-<uuid>.parquet`, where `start_ns` is the
  minimum `open_ts` and `end_ns` the maximum `close_ts` in the file.
- Data Files are **immutable**. A write only appends a new file and updates the
  Manifest.
- Parquet writer settings: format version `2.6` (native nanosecond
  timestamps), `use_deprecated_int96_timestamps=False`, `zstd` compression,
  `row_group_size=1_000_000`.

### `_dataset.json`

One per Dataset root. It is descriptive metadata:

```json
{
  "schema_version": 1,
  "data_type": "bar",
  "arrow_schema": {"fields": [{"name": "...", "type": "...", "nullable": true}]},
  "partitioning": ["asset_class", "venue", "symbol", "interval", "year", "month"],
  "partitioning_flavor": "hive",
  "file_naming": "data-<start_ns>-<end_ns>-<uuid>.parquet",
  "last_flushed_ns": 1793000000000000000
}
```

### `_manifest.json`

`_manifest.json` is authoritative: a reader may read only entries with
`active = true` that still exist. Each entry carries `path`, `start_ns`,
`end_ns`, `row_count`, `active`, `size_bytes`, `created_ns`, `max_ingest_ns`
and the `partition` map.

Raw `pyarrow.dataset` scans of the directory bypass the Manifest and can see
files that Compaction has already marked inactive. Use the REST API or
`ParquetBarStore` when Manifest authority matters.

### Compaction and Retention

- `mdgw compact --older-than <duration>` merges eligible Data Files within a
  partition into one new Data File, then atomically rewrites the Manifest so
  the old entries are inactive, then deletes the old files. At least two
  eligible files are required. A partition protected by an active Replay
  Session lock is skipped.
- `mdgw gc --dry-run|--apply` deletes **whole month partitions** older than
  `[retention].bars_days` when `[retention].enabled = true`. `bars_days = 0`
  keeps data forever, and the current UTC month is never deleted.
- Both operations are Manifest-first: the Manifest is switched before any old
  Data File is removed.

## 6. Streaming Interface

### Topics

| Stream | Topic |
|---|---|
| Live Bars | `market.bar.<interval>.<instrument_id>` |
| Replay Bars | `replay.<session_id>.market.bar.<interval>.<instrument_id>` |
| Replay control | `replay.<session_id>.control` |

Topics are **prefix subscriptions**: `market.bar.` matches every Bar, and
`market.bar.1m.binance:` matches every instrument at 1m.

### ZMQ wire format

A market message is a multipart message with exactly two frames:

```
[topic][Arrow IPC stream]
```

The second frame is a complete Arrow IPC **stream**: one schema followed by
one or more RecordBatches, all using `BAR_SCHEMA`. A consumer subscribes to a
prefix and decodes each frame independently. The gateway also publishes an
empty `_internal.ready` message periodically so a late SUB can prove its
subscription has been forwarded.

### WebSocket wire format

Connect to `/ws` (configurable via `[server].ws_path`). Client messages:

```json
{"op": "subscribe",   "topics": ["market.bar.1m.binance:BTC/USDT"]}
{"op": "unsubscribe", "topics": ["market.bar.1m.binance:BTC/USDT"]}
{"op": "ping"}
```

Server messages:

```json
{"op": "pong"}
{"op": "event", "topic": "market.bar.1m.binance:BTC/USDT", "data": {<Message Envelope>}}
{"op": "lag_notice", "dropped": 12, "since": "2026-10-30T12:35:00.000000000Z"}
```

Each Bar row is sent as its own Message Envelope. A slow client is never
disconnected: the oldest queued message is dropped and a throttled
`lag_notice` reports the loss. `[streaming].queue_size` and
`[streaming].lag_notice_interval_ms` control this behavior.

## 7. Replay Protocol

Replay control is JSON over ZMQ REQ/REP, separate from the market PUB stream.
`create_session` returns the resolved `publish_endpoint`, which may differ from
the configured one when port `0` selected an ephemeral port.

### Session lifecycle

```
create_session(subscriptions, start_ts, end_ts, replay_mode, speed?)
  -> { ok: true, session_id, publish_endpoint }

pause(session_id) / resume(session_id)
seek(session_id, ts)
set_speed(session_id, speed)
step(session_id, credits)
stop(session_id)
  -> { ok: true, session_id }
  -> { ok: false, error: { code, message } }
```

A subscription is `{ "data_type": "bar", "instrument_id": ..., "interval": ... }`.
`start_ts` is inclusive and `end_ts` exclusive, both applied to `close_ts`.
`speed` is a finite number greater than zero.

### Replay Modes

| Mode | Behavior | Legal controls |
|---|---|---|
| `asap` | one pass at full speed; the Replay Clock never throttles | `stop` |
| `realtime` | the Replay Clock paces Bars by `speed` (default 1.0) | `pause`, `resume`, `seek`, `set_speed`, `stop` |
| `step` | starts paused; each `step(n)` grants `n` Step Credits, and each published Bar spends one | `pause` (no-op), `seek`, `step`, `stop` |

`seek` is legal only while paused. In `realtime` that means `pause` first; in
`step` the session is paused except while spending credits. `seek` moves the
cursor to the first Bar with `close_ts >= ts`, and discards credits granted
before the seek. Credits granted after the seek survive even if the Parquet
read has not completed yet.

### Ordering and End of Session

Bars from every subscription are merged in this total order:

1. `close_ts` (the envelope `event_ts`)
2. `instrument_id`
3. `seq` (numeric sequence before null)
4. `interval`
5. `open_ts`

When the range is exhausted, or after `stop`, the session publishes one End of
Session message on `replay.<session_id>.control`:

```json
{
  "op": "end_of_session",
  "session_id": "rpl-...",
  "reason": "exhausted",
  "bars_sent": 42
}
```

`reason` is `exhausted` or `stopped`.

## 8. Stable error codes

The constants in `src/mdgw/core/errors.py` are part of the contract. HTTP
status is only a coarse classification; branch on `code`, not on the prose or
the status.

| Code | Emitted by | HTTP status |
|---|---|---|
| `invalid_instrument` | malformed `<venue>:<symbol>` | 400 |
| `instrument_not_found` | unknown Instrument in the snapshot | 404 |
| `invalid_interval` | interval outside the Data Contract | 400 |
| `invalid_parameter` | missing/invalid control or query field | 400 |
| `invalid_format` | unsupported `format` value | 400 |
| `invalid_range` | inverted/empty range, bad timestamp or limit | 400 |
| `range_too_large` | JSON response would exceed 100,000 Bars | 400 |
| `schema_version_mismatch` | Dataset written under another contract version | 409 |
| `session_not_found` | control op references an unknown Replay Session | 404 |
| `internal_error` | unexpected server failure | 500 |

REST errors use the envelope
`{"error": {"code", "message", "details", "request_id"}}`; the same
`request_id` is returned in the `X-Request-ID` response header and in the JSON
request log. Replay control errors use
`{"ok": false, "error": {"code", "message"}}` with the same codes.

The client SDK additionally reports transport-level failures under its own
codes (`connection_error`, `timeout`, `http_error`, `protocol_error`,
`ws_unavailable`, `replay_unavailable`, `configuration_error`). Those are
client errors, not gateway Data Contract codes.

`DESIGN.md` mentions `dataset_not_found` and `feed_unavailable` as possible
future codes. They are not constants in `errors.py` and this checkout does not
emit them.

## 9. Revisions and deduplication

A Bar identity is `(instrument_id, interval, open_ts)`. A source correction
appends a new row with the same identity and a higher `revision`. Readers must
therefore:

1. keep only the row with the maximum `revision` per identity;
2. never assume one row per identity in the raw Dataset.

The gateway applies this rule on every persistence path: live ingest and
Backfill merge against the existing identity (including Bars still buffered for
writing), and readers such as `ReplayBarSource` keep the highest revision and
use `ingest_ts` to break equal-revision ties. Downstream consumers must do the
same before treating Bars as a unique series.

Aggregated parent Bars follow the rule too: a changed child can move the
parent to `parent_revision + 1`.

## 10. History freshness

Historical REST reads see only flushed Data Files. The collector Service Role
writes through a bounded queue (default 10,000 Bars; `[collector].write_queue_size`)
and an independent writer thread. The writer flushes on the first of:

- `[storage].flush_max_bytes` (default 64 MiB), estimated from the queued Bars;
- `[storage].flush_max_age_s` (default 3600 s), measured from the oldest queued
  Bar.

Setting either value to `0` or less flushes after every ingest batch. A full
queue drops its oldest Bar and increments the collector's dropped/lag counters;
that Bar was already published on the Streaming Interface but is absent from
history, and Backfill can recover it. On shutdown the remaining queue is
flushed before the data High-Water Mark store is closed.

Bars are therefore already on the live stream before they appear in history.
The data High-Water Mark and health/freshness tracking advance only after a
flush succeeds, so buffered Bars never look durable. The single-process
`GatewayRuntime` uses the immediate sink and its history is fresh right after
`ingest()`.

`GET /api/v1/status` reports the Dataset status, including `last_flushed_ts`
(per Dataset and per interval), so a client can decide whether the history is
fresh enough. The collector `/status` and `/metrics` additionally expose the
write-queue depth, dropped/lag counters, and the last flush timestamp/duration.
For real-time data, consume the Streaming Interface instead of polling history.

## 11. pandas notes

`table.to_pandas()` follows Arrow semantics that often surprise numeric code:

- `decimal128(38, 18)` columns become pandas `object` dtype holding
  `decimal.Decimal`. Do not call `astype(float)`: it silently destroys
  precision.
- pandas 3 may display `timestamp[ns, tz=UTC]` at microsecond resolution even
  though the Arrow value is nanosecond-precise. Keep the Arrow table when exact
  nanoseconds matter.
- For analysis that needs floats, convert explicitly at the boundary and
  document the loss; the contract itself never uses floats.

## Related documents

- [`README.md`](../README.md): run the gateway and the consumer examples.
- [`examples/`](../examples/): runnable consumers for each interface.
- [`DESIGN.md`](../DESIGN.md): design rationale and non-goals.
- [`GLOSSARY.md`](../GLOSSARY.md): terminology.
- [`docs/adr/`](adr/): accepted architecture decisions.
