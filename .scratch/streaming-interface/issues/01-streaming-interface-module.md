# 01: 新增 StreamingInterface 模块

**What to build:** 一个深模块，把 Streaming Interface 的全部实现收进一个小接口：Bar topic 语法与 namespace、Arrow IPC 编解码、Bar 发布/订阅、READY 握手、`_internal.status` 通道、Replay 的 `{namespace}.control` 事件通道、转发与计数器。模块公开 `StreamingInterface`、`bar_topic`、`bar_topic_prefix`、`table_to_ipc`、`ipc_to_table` 与 `READY_TOPIC`；不感知 aiohttp、`WebSocketHub` 或任何 Service Role。`mdgw.bus` 暂时保留并 re-export 编解码与 `READY_TOPIC`，不迁移任何调用方。

**Blocked by:** None (can start immediately)

**Status:** ready-for-agent

- [ ] `mdgw.streaming` 只导出 `StreamingInterface`、`bar_topic`、`bar_topic_prefix`、`table_to_ipc`、`ipc_to_table` 与 `READY_TOPIC`；其余 socket、线程、订阅记录与计数器均为私有。
- [ ] `bar_topic` 生成 `market.bar.<interval>.<instrument_id>`；`namespace` 以段为单位前置且模块补分隔符；`bar_topic_prefix` 支持可选 `interval` / `instrument_id` 与同一 `namespace`。
- [ ] `_internal.status` 与 `_internal.ready` 始终为全局 topic；`publish_event` 发布到 `{namespace}.control`，空 namespace 抛 `ValueError`。
- [ ] `start()` / `stop()` 幂等；`start()` 失败完整清理 context、socket 与线程并重新抛出原始异常；`endpoints` 返回解析后的绑定 endpoint；实例 `stop()` 后不可再次 `start()`。
- [ ] `publish_*`、`subscribe_*`、`mark_ready`、`wait_until_peer_ready` 在 `start()` 之前抛 `RuntimeError`；`publish_*` 在 `stop()` 之后为 no-op；`peer_ready` 在启动前与停止后为 `False`。
- [ ] `publish_bars` 按 `(interval, instrument_id)` 分组、编码并线程安全发送；`publish_status` 与 `publish_event` 使用同一 JSON 编码规则。
- [ ] 每个实例最多一个活动 Bar 订阅与一个活动 status 订阅；重复调用抛 `RuntimeError`；`unsubscribe_bars()` 返回是否移除、保持 SUB 连接并允许再次订阅；不提供 `unsubscribe_status`。
- [ ] `subscribe_bars(..., forward=True)` 在 handler 之前逐字节转发原帧；转发失败仅在关停竞态下被抑制，handler 仍执行。
- [ ] `subscribe_bars(await_ready=True)` 使用内部 READY ping 等待自己的 SUB 生效，2.0 秒超时后抛 `TimeoutError`，不要求先 `mark_ready()`。
- [ ] `mark_ready()` 幂等并启动 50 ms READY 心跳直到 `stop()`；`peer_ready` 在 `connect` 上首次收到 READY 后保持为真；`wait_until_peer_ready(timeout)` 超时或无 `connect` 时返回 `False`。
- [ ] malformed Bar 帧与 malformed status JSON 被记录、计入 `decode_errors` 并丢弃；handler 异常被记录、计入 `handler_errors`，订阅线程继续。
- [ ] 计数器 `published_frames`、`received_frames`、`decode_errors`、`handler_errors` 单调且线程安全；status、READY 与 event 帧不计入前两个。
- [ ] 编解码实现移入 `mdgw.streaming`；`mdgw.bus` 只 re-export 编解码与 `READY_TOPIC`，不存在两份实现。
- [ ] 新增模块单元/契约测试覆盖 spec 列出的 15 个接缝用例；`tests/contract/test_zmq_arrow_ipc.py` 改写为 `StreamingInterface`；新增客户端 topic/READY parity 契约测试。
- [ ] 不迁移任何调用方；现有离线测试套件无新增失败；不新增配置字段，不修改 ADR、`GLOSSARY.md` 或线上 topic 字符串。

## Comments
