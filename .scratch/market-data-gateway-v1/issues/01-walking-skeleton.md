# 01: Walking skeleton — 假 Feed Handler → Bar → Parquet → REST

**What to build:** 用户可以用一条命令启动 Gateway，配一个假 Feed Handler；它发出已收盘的 1m Bar，Gateway 把 Bar 写入 Data File，并通过 REST 返回同一根 Bar。整条链路在单进程内通过 GatewayRuntime 跑通，不触网。

**Blocked by:** None (can start immediately)

**Status:** resolved

- [x] 假 Feed Handler 实现 FeedHandler Protocol，可配置一个 InstrumentId 与已收盘 1m Bar 序列。
- [x] GatewayRuntime 组合 feed → normalize → store → api，使用 inproc ZMQ；测试可从该 seam 驱动全链路。
- [x] Bar 按 Data Contract 最小字段写入 Hive 分区的 Parquet Dataset，并可被 pyarrow.dataset 读回。
- [x] `GET /api/v1/bars` 返回与写入一致的 Bar（JSON）。
- [x] 时间字段为 UTC 纳秒，价格/数量为 decimal128(38,18)，JSON 中 decimal 为字符串。
- [x] `pyproject.toml`、`environment.yml`、pytest 配置就绪；`conda run -n nautilus pytest` 全绿。
- [x] 网关包不 import 任何回测/交易框架。

## Comments

- Resolved in `integration/market-data-gateway-v1` after the v1 delivery code review and follow-up fixes.
