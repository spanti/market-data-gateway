# 02: 迁移 CollectorService

**What to build:** collector Service Role 组合 `StreamingInterface`，用现有 `collector_pub_bind` 绑定 PUB、用 `api_pub_connect` 连接 SUB；通过模块发布 Bar 与 status、声明 READY、按配置等待 api READY。collector 不再自己拼 topic、维护 READY 心跳或同时使用两种就绪表示。

**Blocked by:** 01

**Status:** ready-for-agent

- [ ] `CollectorService` 使用 `StreamingInterface` 完成 PUB 绑定与 SUB 连接；`publish_endpoint` 委托给 `endpoints`。
- [ ] Bar 发布走 `publish_bars`；`_internal.status` 走 `publish_status`；删除本地 `MARKET_BAR_TOPIC_PREFIX`、`READY_INTERVAL_S` 与 `_publish_bars` 的分组/编码逻辑。
- [ ] READY 由 `mark_ready()` 统一发布；删除 `_run_ready_loop`、`_api_ready` handler 与 `_api_ready_signal` 的双重表示。
- [ ] `wait_until_peer_ready(api_ready_timeout_s)` 保留原有策略：未启用时不等待；超时只记录 `collector.api_ready_timeout` 并继续 ingest。
- [ ] `mdgw_collector_zmq_publishes_total`、`mdgw_collector_bars_ingested_total`、`mdgw_collector_bars_written_total` 等指标名称与语义不变。
- [ ] `status_payload` 的字段与值不变；shutdown 顺序仍保证 sink flush、HWM 关闭与 Feed Handler 关闭。
- [ ] collector 单元测试与多进程 streaming 测试通过；离线套件无新增失败。
- [ ] 不修改配置字段、topic 字符串、collector ingest 语义或 supervisor 行为。

## Comments
