# 06: Replay 组合 StreamingInterface 并删除总线

**What to build:** `ReplayServer` 用 `StreamingInterface` 发布 Bar 与 End of Session（`namespace="replay.<session_id>"`），用 `mark_ready()` 替代自己的 READY 循环，保留 REP 控制 socket 与控制语义。随后删除 `mdgw.bus`，更新所有剩余 import，完成 Streaming Interface 的单一拥有者迁移。

**Blocked by:** 01, 02, 03, 04, 05

**Status:** ready-for-agent

- [ ] `ReplayServer` 组合 `StreamingInterface`：发布 Bar 走 `publish_bars`，End of Session 走 `publish_event`，READY 走 `mark_ready()`；删除自己的 PUB、`SNDHWM`、`_run_ready` 与 `_publish_message`。
- [ ] 数据发布保留 `replay.<session_id>` namespace；`publish_endpoint` 委托给 `endpoints`。
- [ ] REP 控制 socket 与控制循环保留（可使用独立 `zmq.Context`）；所有控制命令语义与错误响应不变。
- [ ] 停止顺序为 `ReplayService.close()` → `StreamingInterface.stop()` → 控制 socket 关闭，保证 Session 仍能发布 `reason="stopped"` 的 End of Session。
- [ ] 删除 `mdgw.bus` 包；所有调用方与测试改为从 `mdgw.streaming` 导入 `READY_TOPIC`、`table_to_ipc`、`ipc_to_table`。
- [ ] 全仓库不再引用 `ZMQBus`、`wait_for_subscriber` 或 `mdgw.bus`；`test_zmq_arrow_ipc.py` 已在上游 ticket 改写。
- [ ] 完整离线测试套件（`pytest -m 'not network'`）无新增失败；外部 topic、READY、Replay 控制与 End of Session 行为不变。
- [ ] 不修改配置字段、Replay Mode / Replay Protocol 语义、Data Contract 或 `mdgw_client`。

## Comments
