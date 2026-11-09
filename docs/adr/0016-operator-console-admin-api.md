# Operator Console 通过现有 FastAPI 增加受控 Admin API

Status: accepted（2026-11-09）

Operator Console 作为独立 React 静态前端引入，管理写能力则通过现有 api Service Role 中版本化的 `/api/v1/admin` 提供，不新增 Node 后端或第四个 Service Role。这一决定正式重开 v1 的“无 GUI、无认证”非目标，并在 ADR-0001 的四类中性消费接口之外增加需要 OIDC 身份、Access Role 和 Operator Audit Event 的管理面；选择现有 FastAPI/ASGI 是为了延续 ADR-0011 的单一服务器栈、错误信封和 Request ID，而不是建立第二套服务技术体系。

## Consequences

- 浏览器只消费同源 REST/WS；React 不直接访问 ZMQ、Dataset 文件、Docker Socket 或任意 shell。
- Feed Subscription 与 Backfill Job 的实际执行仍由 Collector 承担，Collector 保持唯一 Dataset 写入方；生命周期动作只允许经过 Supervisor 的受控本地 IPC。
- Current Contract 部署仍是无认证、内网、匿名只读模式；Admin API 部署必须具备 OIDC、服务端授权、CSRF、防越权和追加写 Operator Audit Event，前端禁用按钮不构成安全边界。
- Export Job 只读 Dataset 并在 Dataset 之外生成有期限文件，不改变 Manifest、Data Contract 或 Replay 语义。
- 不采用独立 Node/BFF、直接改 TOML、Docker Socket 通用控制或新建第四个 Service Role；这些替代方案会分别破坏单服务器栈、配置/运行时期望状态边界或最小控制面。
