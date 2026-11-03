# 03: 迁移 ApiService

**What to build:** api Service Role 组合 `StreamingInterface`，用现有 `api_pub_bind` 绑定 PUB、用 `collector_pub_connect` 连接 collector；订阅 Bar 时逐字节转发到自己的 PUB，同时订阅 status，并用 `peer_ready` / `received_frames` 判断 collector 就绪。api 不再自己维护 READY 循环或 ready event。

**Blocked by:** 01

**Status:** ready-for-agent

- [ ] `ApiService` 使用 `StreamingInterface` 完成 PUB 绑定与 collector SUB 连接；`publish_endpoint` 委托给 `endpoints`。
- [ ] Bar 订阅使用 `subscribe_bars(handler, forward=True)`；外部 ZMQ 消费者收到的帧与 collector 发出的原帧逐字节一致。
- [ ] status 订阅使用 `subscribe_status`；bar 与 status handler 仍把 JSON Message Envelope 交给 `WebSocketHub`，线程交接保持 `call_soon_threadsafe`。
- [ ] `_collector_is_ready()` 使用 `peer_ready or received_frames > 0`；收到首帧后调用 `mark_ready()`；删除 `_ready_event`、`_run_ready_loop` 与本地 `READY_INTERVAL_S`。
- [ ] `/readyz` 在无 collector 且 `collector_required=false` 时的行为不变；`collector_required=true` 时仍等待 collector 握手。
- [ ] `mdgw_api_messages_received_total`、`mdgw_api_messages_forwarded_total` 与 WS 指标的名称与语义不变。
- [ ] api 进程内测试与多进程 streaming 测试通过；离线套件无新增失败。
- [ ] 不修改 HTTP/WS 端点、`WebSocketHub`、配置字段或 topic 字符串。

## Comments
