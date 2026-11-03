# 05: Replay 发布接缝改为类型化回调

**What to build:** Replay Session 与 Replay Service 不再构造 topic 或 IPC 帧，而是通过类型化回调 `publish_bar(bar)` 与 `publish_event(mapping)` 发布。`ReplayServer` 暂时保留自己的 PUB，把两个回调适配到现有发布路径，并用 `mdgw.streaming` 的 topic helper 生成 `replay.<session_id>.` 前缀。Replay 行为与测试保持不变。

**Blocked by:** 01

**Status:** ready-for-agent

- [ ] `ReplaySession` 只调用 `publish_bar(bar)` 与 `publish_event(mapping)`；删除 `_market_topic`、`_publish_bar` 中的 topic/IPC 构造与 `table_to_ipc` 直接依赖。
- [ ] `ReplayService` 与 `ReplayServer` 的发布接缝改为两个类型化回调；`set_publish` 的旧 `(topic, payload)` 形状被移除。
- [ ] `ReplayServer` 的临时适配器使用 `mdgw.streaming` 的 `bar_topic` / `bar_topic_prefix` 生成带 `replay.<session_id>` namespace 的 topic，并继续发布 End of Session。
- [ ] `GatewayRuntime.create_replay_session(publish=...)` 的测试接缝同步改为两个类型化回调；测试仍能观察同一份 Bar 与 End of Session 帧。
- [ ] Replay asap / controls / client replay 测试通过；End of Session 的字段、topic 与顺序不变。
- [ ] `ReplayServer` 的 REP 控制循环、控制命令语义与 Session 生命周期不变。
- [ ] 本 ticket 不删除 `mdgw.bus`，也不替换 `ReplayServer` 的 PUB/READY 实现；离线套件无新增失败。

## Comments
