# Admin 控制通道使用受控的环回 TCP JSON，而非 ZMQ REQ/REP

Status: accepted（2026-11-10）

ADR-0003 规定 Service Role 之间的控制面走 ZMQ REQ/REP；ADR-0016 引入 Operator Console 与 Admin API 后，新增了两条控制通道：Admin API → Collector（Feed Subscription 同步、Backfill Job 启停/查询、Gap 快照）和 Admin API → Supervisor（Service Instance start/stop/reload）。这两条通道不是行情总线的一部分，也不承载任意命令。Windows 版 pyzmq 无法绑定 `ipc://`，且 Admin 控制请求需要逐请求关联、超时、取消与并发 Supervisor 操作；若改用 REQ/REP，还要在网关里再维护一套 ZMQ socket 生命周期与 Windows 传输替代。因此 Admin 控制通道采用**仅绑定环回地址的 TCP**：Collector 侧为带长度前缀的 JSON 信封，Supervisor 侧为 token 行加 JSON 行；两端共享环境变量中的 token，使用常数时间比较，并在服务端执行 action 允许列表、payload schema、大小与超时校验。

本决定**仅取代 ADR-0003 中“控制走 ZMQ REQ/REP”的条款（Admin 控制通道部分）**。行情总线仍只用 ZMQ PUB/SUB，Replay 协议与 Manifest/Dataset 语义不变；不引入 Redis、第二个消息中间件、通用命令/路径/shell 参数或 Docker Socket 控制。Collector 与 Supervisor 各自使用独立的环回端点（`collector_host/collector_port`、`supervisor_host/supervisor_port`），端口 0 表示禁用；同时启用两者不得复用同一 host+port。

## Consequences

- 控制通道必须在环回地址上监听；token 只从配置命名的环境变量读取，永不写入 TOML、日志或审计。
- 每条消息都带 schema_version、correlation id、timeout 与 action 允许列表；未知 action、额外字段、超长消息与目标不匹配在业务处理前被拒绝。
- Collector 保持唯一 Dataset 写入方；Supervisor 只执行允许列表内的生命周期动作；Admin API 不直接 spawn/signal 进程。
- Windows 与 Linux 使用同一套传输实现，无需为 `ipc://` 维护分支；代价是控制通道不是跨主机的消息总线，只服务于同机 Service Role 管理。
- ADR-0003 的其余部分（行情 PUB/SUB、HWM/SQLite 本地状态、无 Redis）继续有效。
