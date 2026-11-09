# Spec: Gateway Admin API（Operator Console 后端改造）

Status: ready-for-agent（issue 10 的公开历史分页契约仍需维护者决策）

## Problem Statement

当前 Gateway 的 REST 面只有只读 GET，WebSocket 只支持单连接订阅，配置和生命周期由静态文件、CLI、信号及 ZMQ 控制。系统没有认证、Access Role、Operator Audit Event、持久任务、动态 Feed Subscription、Service Instance 控制或浏览器大文件导出。Operator Console 的真实管理能力因此不能通过前端封装凭空实现，必须增加一个受控、可审计、保持既有 Data Contract 和写入边界的 Admin API。

改造不能破坏现有架构：api 继续承担 HTTP/WS，Collector 保持唯一 Dataset 写入方，Replay Protocol 不被浏览器绕行，Instrument Reference Data 仍由 YAML 真相源管理，Data Files 保持不可变，所有读取遵守 Manifest Snapshot 与 Dataset Read Lease。

## Solution

在现有 api Service Role 中增加 `/api/v1/admin`，以独立管理 SQLite 保存会话、期望状态、任务、审计和导出元数据。身份使用 OIDC Authorization Code + PKCE 和服务端会话 Cookie；所有管理写操作在服务端执行 Access Role 校验与审计。Collector 和 Supervisor 通过受控通道执行实际动作，API 不直接写 Dataset、不执行任意 shell、不暴露 Docker Socket。

新增 Export Job、Backfill Job、持久 Unrecoverable Backfill Gap、Feed Subscription Desired/Observed 状态和 Service Instance 生命周期契约；同时补充真实上游与 I/O 指标。公开 Revision-heavy Time Cursor 的修复因可能改变 ADR-0012 和 Data Contract 语义，单独作为维护者决策 issue，不在本 spec 中静默改版。

## User Stories

1. 作为未认证调用者，我不能读取 Admin API 的受保护资源或执行管理动作。
2. 作为 OIDC 用户，我只会获得服务端确定映射的一个 Access Role，无法由客户端选择或提升权限。
3. 作为 SuperAdmin，我可以通过受控 Supervisor 执行 Start、Graceful Stop 和 Reload，并追踪真实结果。
4. 作为 Operator，我可以修改 Feed Subscription、流控和触发 Backfill，但不能执行生命周期或身份管理。
5. 作为 Auditor，我可以读取、导出和建立只读 WebSocket Session Subscription，但不能改变 Gateway 或他人会话状态。
6. 作为审计人员，我可以关联一次异步动作的 requested 与最终结果，并查看脱敏前后值。
7. 作为系统维护者，我希望高危动作在审计不可用时 fail-closed，以免产生无记录的控制行为。
8. 作为 Operator，我希望 Feed Desired/Observed 状态和版本冲突可见，以免覆盖他人修改或误判已生效。
9. 作为数据质量人员，我希望 Backfill Job 可持久跟踪、部分成功、协作取消并关联 Gap。
10. 作为大文件消费者，我希望 Export Job 在 Dataset 外生成带校验和与过期时间的文件，并支持 Range 下载。
11. 作为监控维护者，我希望新增指标使用准确名称和测量边界，不把事件时间差冒充网络延迟。
12. 作为既有客户端维护者，我希望现有公开 REST、Streaming、Replay 和 Dataset 契约不被 Admin API 静默改变。

## Implementation Decisions

### 架构与 API 约定

- 管理面位于现有 FastAPI `api` Service Role 的 `/api/v1/admin`，遵循 ADR-0016；不新增 Node/BFF 或第四个 Service Role。
- `GET /api/v1/admin/capabilities` 返回管理面版本、可用适配器、认证要求和功能能力；网络失败与“不支持”必须由前端区分。
- 所有 Admin REST 失败使用 `{"error":{"code","message","details","request_id"}}` 与 `X-Request-ID`，不泄露 FastAPI 默认 422/`detail`。
- 新稳定错误码至少覆盖 `unauthenticated`、`forbidden`、`conflict`、`invalid_parameter`、`not_found`、`audit_unavailable`、`control_unavailable`、`job_not_found`、`export_expired`、`quota_exceeded`、`rate_limited`。
- 管理列表使用服务端过滤和有界分页；时间统一 UTC ISO-8601，十进制保持字符串，写操作使用明确版本或 Operation ID。
- 现有公开 GET、WS、ZMQ、Replay、Manifest 和 Data File 布局保持不变；公开分页修复只能经 issue 10 的独立契约决策实施。
- 同源部署不增加宽松 CORS。Admin 生产环境由 Nginx 终止 TLS，FastAPI 只信任显式配置的代理头来源，避免所有限流用户退化为同一个代理 IP。

### 管理存储与一致性

- 使用独立 SQLite 管理数据库，不把管理记录写入 Bars/Quarantine Dataset，不引入 Redis。数据库包含 migrations 和明确单 API 实例约束。
- 保存 OIDC 会话、Feed Subscription 期望状态与版本、Backfill/Export Job、Gap、Export 元数据和 Operator Audit Event；OIDC Client Secret 由受控配置/secret provider 提供，不写入审计 Diff。
- 多写者通过事务、唯一约束和乐观版本防止丢失更新；冲突返回 409，不自动最后写入覆盖。
- API 重启后恢复持久状态。无法确认执行结果的 Job/Operation 标为中断或未知并允许查询/人工重试，不能默认为成功。
- Collector 和 Supervisor 控制通道使用允许列表、schema 校验、超时、相关 ID 和最小权限，不接受任意命令、路径或参数拼接。

### OIDC、会话与 Access Role

- FastAPI 完成 OIDC Authorization Code + PKCE，建立不透明服务端会话；Cookie 为 HttpOnly、Secure、SameSite=Lax 或更严格，写操作要求 CSRF Token 和同源检查。
- 浏览器不保存密码、Client Secret、长期 Token 或可供 JS 读取的会话；注销使服务端会话失效。
- Group/Claim 到 SuperAdmin/Operator/Auditor 的映射具有显式优先级，每个会话只解析一个 Access Role；未映射或冲突默认拒绝并记录安全事件。
- 会话 30 分钟空闲超时、8 小时绝对超时；前端提前 5 分钟提示，任何 401 都阻止后续写操作。
- 每个 Admin 请求在服务端重新执行授权，资源所有权另行校验；前端路由守卫和禁用按钮不是安全边界。
- Auditor 的 WebSocket Session Subscription 属于当前连接只读过滤，允许受控 subscribe/unsubscribe/ping；Admin 自由 JSON 调试和其他写操作仍受角色限制。

### Operator Audit Event

- 独立追加写存储记录单调 Event ID、UTC 时间、操作者、Access Role、动作、目标、脱敏前后值、结果、失败原因、操作原因、Request ID、Operation/Job/Instance ID。
- 哈希链提供篡改检测，不宣称抵御同时控制数据库和密钥的攻击者，也不宣称 WORM/合规级不可变存储。
- 所有管理写和认证/授权失败记录事件；普通查询、CSV 下载和 WS 报文不逐条审计。
- 高危动作先持久化 requested 事件，再执行；审计不可用则 fail-closed。完成时追加 succeeded/failed，不原地更新 requested。
- 创建/删除分别允许 before=null 或 after=null；Secret、Token、Cookie、密码和 OIDC 敏感声明只能记录稳定 `[REDACTED]`。
- 查询支持时间、操作者、角色、动作、对象、结果和 Request ID，详情按 Operation ID 组成时间线；无编辑/删除 API，首版不自动清理、不提供审计导出。

### Feed Subscription 与流控

- TOML 只作为启动初始配置，运行时期望状态存管理数据库；管理 API 不直接改 TOML，也不修改 YAML Instrument Reference Data。
- Feed Subscription 以 InstrumentId 引用只读参考数据，校验 Venue、Data Type=bar、Bar Interval 和重复关系；新增/删除产生新版本。
- API 将带版本的期望状态下发 Collector，分别持久化 Desired、Observed、Drift、Last Error；只有 Collector 确认后才算生效。
- 下发失败保留漂移，不自动把 Desired 回滚成貌似恢复；操作者刷新后人工处理。删除订阅只停止未来采集，不删除历史 Bar、Instrument 或审计。
- API Rate Limit 与 WebSocket Slow Consumer 分开建模、版本化和审计；不包含 Venue 限流、Collector 写队列或存储背压。
- 动态限流仍遵守进程作用域和真实来源 IP；配置热应用与需要重启的项必须在 capabilities 中区分。

### Service Instance 生命周期

- 首个适配器是现有 mdgw Supervisor，扩展其统一管理 collector/api/replay，并通过受控本地 IPC 接收允许列表动作；不实现 Docker Socket 适配器。
- Service Instance 身份来自受控 Supervisor 注册，不虚构 Master/Worker、Leader、分片或 HA。healthz、readyz、PID、期望状态与最后观测分别呈现。
- Start 按依赖顺序启动并等待真实 ready；PID 存在不代表 ready。
- Graceful Stop 先撤销 ready、停止接新工作，在可配置宽限期内完成在途请求、Collector flush 和 Replay End of Session，超时才强制停止。
- Reload 先校验配置，失败则不重启；成功后优雅重启受影响实例，不承诺热加载或零停机。
- API 自身 Stop/Reload 由 Supervisor 异步执行，API 返回 202 与 Operation ID；客户端重连后查询结果，断连不直接等于失败。
- 所有控制有超时、accepted/running/succeeded/failed/unknown 状态和审计；非幂等写请求不自动重放。

### Backfill Job 与 Gap

- Backfill Job 由 API 校验、授权、审计和持久化，由 Collector 作为唯一 Dataset 写入方执行；API 不直接写 Parquet/HWM。
- 状态为 `queued → running → succeeded / partially_succeeded / failed`，支持 `cancelling → cancelled`；记录扫描、缺口、补采、拒绝计数与当前区间。
- 同一 InstrumentId + Bar Interval 的冲突 Job 串行或拒绝，和自动 Backfill 共享并发约束；重试创建关联新 Job。
- 目标高周期时计划固定为补采 1m Bar + 内部 Bar Aggregation，不调用 Venue 原生高周期接口，不降低 HWM、不修改既有 Data File、不绕过 Revision/Data Contract。
- 取消是协作式，不撤回已经写入的 Revision；API/Collector 重启后恢复或明确标记中断，不能默认为成功。
- 持久 Unrecoverable Backfill Gap 记录首次/最近发现、原因、Feed、InstrumentId、Bar Interval、范围和 `open / resolved`，成功 Job 可关联解决；不物理删除。
- 本功能不实现通用 Sequence 连续性算法，不假定 `seq + 1`，也不改变可选来源 `seq` 的 Data Contract。

### Export Job

- Export Job 只读单 InstrumentId、单 Bar Interval、显式范围，保留所有原始 Revision、BAR_SCHEMA 列顺序和精确十进制文本，不改变 Dataset。
- 状态为 `queued / running / succeeded / failed`，支持 `cancelling → cancelled`，文件到期为 `expired`；31 天以内由现有前端直接 GET Blob，31–366 天使用 Job，更大范围拆分。
- 每用户默认最多一个运行中 Job，服务端提供全局并发、存储配额和最大范围校验；超限返回 quota_exceeded 或 invalid_parameter。
- 读取使用 Store/API、Manifest Snapshot 和 Dataset Read Lease；在 Dataset 外写临时文件，完成后 fsync/原子发布，并记录准确大小、SHA-256、创建者和 24 小时过期时间。
- 下载 URL 只接受不透明 Job ID，不接受服务器路径；校验资源所有权，Operator/Auditor 只看本人，SuperAdmin 可查看全部。
- 下载返回 `Content-Disposition: attachment`、Content-Length、安全文件名和校验信息，支持 HTTP Range；过期清理不触碰 Bars/Quarantine。
- 大文件由浏览器原生下载直接写盘，前端不读取成 Blob；进度无法准确计算时暴露已处理量和未知状态，不伪造百分比。

### 监控与真实指标

- 保留现有 per-role health/status/metrics 和 Prometheus 为时序来源；Admin 模式的浏览器监控查询经 OIDC 授权的服务端模板代理，不开放任意 PromQL。
- 现有 HTTP QPS 和 histogram 明确包含完整响应流耗时；新增指标不得复用不相关计数冒充网络或 I/O。
- 为 Feed Handler 增加真实连接状态、状态开始时间、最后心跳/事件、重连计数和最后错误；Venue 列表从实际配置/运行状态派生，不硬编码 Binance/OKX/FIX。
- 分别测量可真实获得的 API 响应字节、Collector 存储写入字节和队列指标，使用准确名称；无法从底层库获得的上游字节数继续标记不可用。
- `ingest_ts - event_ts` 只能命名为 Ingestion Delay，并提示源时钟偏差；只有实际协议 RTT 测量才能命名为 Network Latency。
- Freshness 继续区分 Feed Freshness 与 Dataset Freshness，服务端阈值是唯一失活判定来源。

### Revision-heavy 历史分页

- 当前前端必须检测 `next_start` 不前进并停止分页；Admin API 工作不得偷偷依赖这个已知缺陷。
- issue 10 由维护者选择并记录独立 Data Contract/ADR：是保持 v1 并新增修订安全版本，还是明确改变单 Instrument Time Cursor 的页边界语义。
- 任何方案必须保证同一 open_ts 的 Revision 组不会跨页造成重复或死循环，保持单 Instrument、升序、start 闭/end 开和 100,000 原始行保护，并有 revision-heavy 回归测试。
- 在该决策完成前，本 spec 不授权修改公开 `/api/v1/bars` 的线上契约。

## Testing Decisions

- OIDC 覆盖未映射、冲突映射、会话固定、PKCE、CSRF、注销、空闲/绝对超时、401/403 和资源所有权；测试客户端不能选择角色。
- Access Role 参数化覆盖每个路由和方法，尤其 Auditor WS 只读订阅、Operator 无生命周期、SuperAdmin Export 全量可见。
- 审计覆盖敏感字段脱敏、哈希链检测、requested/result 关联、异步失败和审计不可用时高危动作零执行。
- Feed 测试覆盖版本冲突、Desired/Observed 漂移、Collector 拒绝、重启恢复和删除不清理历史。
- Supervisor 使用受控测试进程验证启动顺序、ready、drain、超时强制停止、配置校验失败零重启、API 自身重载和未知状态。
- Backfill 使用真实临时 Dataset 验证 Collector 唯一写入、1m+内部聚合、HWM 单调、部分成功、取消、冲突任务、重启恢复和 Gap 关联。
- Export 使用真实临时 Dataset 验证 Read Lease、原始 CSV 字节、原子发布、大小/SHA-256、Range、权限、配额、取消、过期清理和 Dataset 不变。
- 指标测试验证计数语义与标签，不用时间差伪造 Network Latency；监控代理只允许预定查询模板。
- issue 10 的契约测试必须构造单 open_ts 多 Revision 且跨页的输入，证明无死循环、无重复、无跳行并保留原始行上限。
- 全部测试离线运行，不依赖真实 Venue 或外部 OIDC；端到端另提供受控 OIDC/Supervisor 测试替身和真实 FastAPI/Collector/SQLite 集成。

## Out of Scope

- Node 后端、新 Service Role、宽松 CORS、本地密码/IAM、用户或角色 CRUD。
- Docker Socket 通用控制、任意 shell、直接改 TOML/YAML。
- Tick、Depth、1s Bar、通用 Sequence 连续性检测；另立 Data Contract 扩展。
- 浏览器 Replay 控制、归档导入、Compaction、GC、Quarantine GC、原始 Quarantine API。
- 外部告警、审计导出、多 Instrument ZIP、多集群、HA、多租户。
- 采用未跟踪 `handoff.md` 的 seq_id、Topic、Ring Buffer、XPUB/XSUB 或 `/bars/recent` 提案。
- 未经 issue 10 独立决策就改变公开历史分页契约。

## Further Notes

- 本 spec 与 `.scratch/operator-console/spec.md` 分离；本轮前端实施不修改 Python。
- 规范真相源仍为 `GLOSSARY.md`、正式 ADR 和 `docs/data-contract.md`；新增管理契约必须同步 OpenAPI，但 OpenAPI 不反成为规范来源。
- ADR-0016 只决定管理面的架构归属；issue 10 若改变公开历史契约，需要自己的 ADR，不得复用 ADR-0016 作为授权。
