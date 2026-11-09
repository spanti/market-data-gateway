# 05: 通过 mdgw Supervisor 实现受控 Service Instance 生命周期

**What to build:** 扩展 Supervisor 统一管理 collector/api/replay，通过本地 IPC 提供 Start、Graceful Stop、Reload 和 Operation 查询。实现 ready/drain/timeout/force、配置预检、API 自身异步重载和真实状态，不实现 Docker Socket 或任意命令。

**Blocked by:** 01, 02, 03

**Status:** ready-for-agent

- [ ] 仅 SuperAdmin 可调用，所有动作有原因、审计、Operation ID 和超时。
- [ ] Start 等待真实 ready，PID 不作为 ready。
- [ ] Graceful Stop 处理在途请求、Collector flush、Replay End of Session 和超时强制停止。
- [ ] Reload 配置失败零重启，成功时明确是有中断重启。
- [ ] API 自身 Stop/Reload 返回 202 后可由 Supervisor 继续执行并查询。
- [ ] accepted/running/succeeded/failed/unknown 与断连语义准确，非幂等写不自动重放。
- [ ] 受控测试进程覆盖启动顺序、drain、强停、重载、IPC 非法命令和未知恢复。

## Comments
