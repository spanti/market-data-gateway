# 1m Bar 来自交易所原生 K 线，高周期由 1m Bar 聚合

v1 只做 Bar：采集 `watch_ohlcv`/`fetch_ohlcv` 的 1m K 线，5m/15m/1h/1d 全部由已收盘的 1m Bar 聚合（Bar Aggregation），不单独采集高周期。只接受已收盘的 K 线（WS 的 `x` 标志，REST 的 `closeTime < 服务器时间`），正在形成的最后一根永远不进入规范流。若同一 `(instrument_id, interval, open_ts)` 因源修正被重新发布，`revision` 自增，父周期同步重发。理由：在没有逐笔的前提下，1m 原生 K 线是唯一可得的原始粒度；统一从 1m 派生高周期，能保证各周期之间可加、可复算。
