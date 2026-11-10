# 05: 通过 mdgw Supervisor 实现受控 Service Instance 生命周期

**What to build:** 扩展 Supervisor 统一管理 collector/api/replay，通过本地 IPC 提供 Start、Graceful Stop、Reload 和 Operation 查询。实现 ready/drain/timeout/force、配置预检、API 自身异步重载和真实状态，不实现 Docker Socket 或任意命令。

**Blocked by:** 01, 02, 03

**Status:** resolved

- [ ] 仅 SuperAdmin 可调用，所有动作有原因、审计、Operation ID 和超时。
- [ ] Start 等待真实 ready，PID 不作为 ready。
- [ ] Graceful Stop 处理在途请求、Collector flush、Replay End of Session 和超时强制停止。
- [ ] Reload 配置失败零重启，成功时明确是有中断重启。
- [ ] API 自身 Stop/Reload 返回 202 后可由 Supervisor 继续执行并查询。
- [ ] accepted/running/succeeded/failed/unknown 与断连语义准确，非幂等写不自动重放。
- [ ] 受控测试进程覆盖启动顺序、drain、强停、重载、IPC 非法命令和未知恢复。

## Comments

**Resolved (2026-11-10):** Supervisor 控制的 Service Instance 生命周期已合并 `63a1134`（feature `e79e631`）。迁移 005；collector/api/replay 统一管理，Start 等待真实 ready，Graceful Stop 撤销 ready→排空→超时强制，Reload 先校验配置（失败零重启）；Operation 持久化 accepted/running/succeeded/failed/unknown，202 后断连不是失败，重启将未确认操作标为 unknown/interrupted；Idempotency-Key 与审计接入。合并时 1644 passed / 5 个既有 CLI 失败。评审拆分 supervisor 控制端口（S3）并补 ADR-0017。
