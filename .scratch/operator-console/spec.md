# Spec: Operator Console（React 管理控制台）

Status: ready-for-agent（视觉方向已选择 B：浅色分析工作台）

## Problem Statement

行情网关当前只提供只读 REST、单连接 WebSocket、分 Service Role 的运维端点和可选 Prometheus，没有浏览器控制台、认证、Access Role、管理写接口或 Operator Audit Event。运维人员需要在同一个桌面界面观察服务与数据状态、查询 Bar、下载 CSV、调试 WebSocket，并在 Admin API 落地后执行受权限约束的管理动作。

本需求必须同时避免两种误导：把当前 API 不支持的能力做成貌似可用的 Mock；以及为了等待全部后端改造而放弃当前已经可真实交付的监控、Bar 查询、CSV 和 WebSocket 功能。前端因此需要明确区分 Current Contract 与 Admin API 两种运行状态，并对每项能力给出真实可用性。

## Solution

在仓库新增独立的 `web/` React + TypeScript + npm 工程，以 shadcn/ui 和 Tailwind CSS 4.0 构建桌面端 Operator Console。生产构建为静态文件，由同源 Nginx 代理现有 API、WS、运维端点和受控监控查询；不新增 Node 后端，不修改现有 Python 服务或 Compose 文件。

首版覆盖监控、Bar 图与 CSV、Gateway Console、Quality & Backfill、Security & Audit 五个模块。Current Contract 模式匿名只读，Admin API 模式通过 OIDC 和真实 Access Role 开放完整能力。三个结构不同的监控原型已完成验证，已选择 **B · 浅色分析工作台**作为生产视觉方向。

## User Stories

1. 作为内部运维人员，我想看到真实 Service Instance、QPS、HTTP 延迟和 Feed Freshness，以便识别服务或采集异常。
2. 作为数据分析人员，我想按 InstrumentId、Bar Interval 和显式时间范围查询最大 Revision Bar 图，以便检查市场行情。
3. 作为数据消费者，我想下载保留原始 Revision 和精确十进制文本的 CSV，以便在外部分析数据。
4. 作为大文件消费者，我想通过 Export Job 异步生成并下载文件，以避免浏览器内存耗尽。
5. 作为 Operator，我想管理 Feed Subscription 和明确的流控设置，并看到 Desired/Observed 漂移，以便安全改变运行状态。
6. 作为 SuperAdmin，我想对 Service Instance 执行经过确认和审计的 Start、Graceful Stop、Reload，以便完成生命周期操作。
7. 作为排障人员，我想建立 WebSocket Session Subscription 并观察有界报文流，以便诊断实时 Topic 和延迟丢弃。
8. 作为 Operator，我想查看 Unrecoverable Backfill Gap 并触发可跟踪的 Backfill Job，以便补采真实历史缺口。
9. 作为 Auditor，我想以只读方式查询数据、建立只读会话订阅并查看 Operator Audit Event，以便监督操作。
10. 作为安全维护者，我想让真实身份、权限、CSRF、防越权和审计由后端执行，以便前端不会成为唯一安全边界。
11. 作为界面使用者，我想在中文界面中保留规范英文领域标识，以便与 API、日志和文档准确对应。
12. 作为验收人员，我想明确区分真实联调通过与仅契约测试，以便不会把未实现的后端能力计为完成。

## Implementation Decisions

### 工程与技术边界

- 工程位于 `web/`，使用 Vite、React、TypeScript、npm 和提交的 lockfile。
- UI 使用 shadcn/ui + Tailwind CSS 4.0；React Router 管理路由与守卫。
- TanStack Query 管理 REST server state；Axios 负责统一错误与 Blob 进度；ECharts 负责监控和 Bar 图；TanStack Virtual 负责 WS 报文列表；React Hook Form + Zod 负责表单和边界校验。
- 默认使用局部状态与 Context，不预先引入 Redux/Zustand。生产只有静态资源和 Nginx，不增加 Node 服务运行时。
- 开发代理和生产部署配置与 `web/` 同处；不修改 `src/`、现有 Python 配置或现有 Compose 文件。
- 桌面 Chrome/Edge 为目标浏览器，1440px 为设计基准，1280px 最低完整可用；手机只保证关键状态可查看。
- 界面以简体中文为主，保留 `InstrumentId`、`Bar Interval`、`Service Role`、`Feed Freshness` 等规范英文标识。

### 运行模式与能力发现

- **Current Contract**：连接现有无认证 API，显示“匿名只读”；不分配 Access Role，不提供角色切换器或假登录，所有管理写操作禁用并注明依赖 Admin API。
- **Admin API**：能力发现和 OIDC 登录均成功后，按服务端返回的 Access Role 开放功能。
- 能力发现或网络失败显示“能力未知”，不得退化成貌似正常的只读数据页面；已缓存数据必须标注最后成功时间。
- Mock 只存在于隔离测试，不是生产模式，也不能通过 URL 或角色下拉框切换权限。

### 信息架构

- 默认路由为 `/monitoring`，使用五个一级模块：监控、Bar 图与导出、Gateway Console、Quality & Backfill、Security & Audit。
- Console 分为 Feed Subscription、Service Instance、WebSocket Debug；Quality 分为 Gap 与 Backfill Job；Security 首版只有当前身份和审计查询，不做 IAM 管理。
- 无权限操作显示禁用及原因，不做仅靠隐藏菜单的安全控制；路由守卫和 API 401/403 统一处理。
- Bar 图、Revision 表格和 CSV 共享 URL 中的 `instrument_id`、`interval`、`start`、`end`。非敏感主题/时区偏好可本地保存，身份与 Token 不可放入 URL 或 localStorage。

### Access Role

| 能力 | SuperAdmin | Operator | Auditor |
| --- | --- | --- | --- |
| 监控、Bar、Quality 查看 | ✓ | ✓ | ✓ |
| 直接与异步 CSV 导出 | ✓ | ✓ | ✓ |
| Feed Subscription 与流控修改 | ✓ | ✓ | — |
| Backfill Job 触发 | ✓ | ✓ | — |
| WebSocket Session Subscription | ✓ | ✓ | 受控 Topic 表单 |
| 自由 JSON WS 调试 | ✓ | ✓ | — |
| Start / Graceful Stop / Reload | ✓ | — | — |
| 用户与角色管理 | 后端能力存在时全权 | — | — |
| Operator Audit Event 查看 | ✓ | ✓ | ✓ |
| Export Job 可见范围 | 全部 | 本人 | 本人 |

WebSocket Session Subscription 只筛选当前会话的只读数据，不改变 Feed Subscription，因此 Auditor 可以使用受控订阅表单；自由 JSON 仅允许协议白名单中的 `subscribe / unsubscribe / ping`。

### 监控

- Service `/status` 默认 5 秒轮询，Prometheus 趋势默认 15 秒刷新；显示数据源、统计窗口和最后成功更新时间。
- QPS 来自 HTTP counter 的窗口速率；Peak QPS 必须标注时间范围。HTTP P50/P95/P99 来自现有 histogram，并注明包含完整响应流耗时。
- Current Contract 的内网开发部署可同源查询 Prometheus；Admin API 模式只能使用经过认证、限定查询模板的服务端监控代理，不公开任意 PromQL 或无认证 Prometheus。
- Feed Freshness 使用 `age_s / threshold_s / stale`，倒计时是阈值剩余时间；Dataset Freshness 单独展示，不能混为一个“新鲜度”。
- 超时或数据源不可用显示未知，不继续把旧数据标为健康。首版只有页面内告警，不做外部通知和告警规则 CRUD。
- 上游列表由真实 Instrument/Feed 数据派生，不硬编码 Binance、OKX、FIX。连接状态、心跳、重连、网络延迟、真实 I/O Rate 未暴露时显示能力缺口，不从 Freshness 推断连接健康。

### Bar 图、Revision 与时间

- 首版只有 `1m / 5m / 15m / 1h / 1d` Bar；没有 Tick、1s、Depth、多 Instrument 叠加或任意周期。
- 默认查询最近 24 小时，单次图表最多 31 个 UTC 天；无数据不自动扩大范围，原始行过多或游标异常时要求缩小范围。
- 对同一 `(instrument_id, interval, open_ts)` 仅保留最大 Revision，平级时按 Data Contract 取最新记录；原始 Revision 表格不去重。
- 十进制字符串保留在原始详情和 CSV 中；仅绘图坐标允许有损数值转换，不用 JS Number 重新生成 CSV 或做服务端聚合的替代品。
- 时间默认 UTC，可切换浏览器本地时间；请求使用带偏移量 ISO-8601，`start` 为闭区间、`end` 为开区间，原始字符串不被导出改写。
- 历史来自 REST，实时追加来自 WS。实时跟随只在用户明确开启且范围为当前时运行；历史范围不连接实时追加。
- WS 没有恢复位点，断线后建立新接收段并标记不连续；REST 重查只能补充已落盘历史，不能宣称无缝恢复。
- 已知的 Revision-heavy Time Cursor 不前进问题必须被检测并报错，禁止无限分页。ECharts Data Zoom 只改变可视窗口，不冒充降采样。

### CSV 与 Export Job

- 小文件路径：单 InstrumentId、最多 31 个 UTC 天，使用现有 `GET /api/v1/bars?format=csv` 和 `responseType: 'blob'`。
- 解析 `Content-Disposition` 的 `filename*`/`filename`，清理路径和危险字符并提供安全 fallback；即使响应是 `inline` 也触发浏览器下载。
- 有可信 Content-Length 时显示百分比，否则显示已下载字节与未知总量；支持 AbortController 取消。Blob 错误响应按统一错误信封解析并展示 Request ID。
- Blob 路径是内存缓冲，不宣传为直接流式写盘，不支持 Range 续传，不在前端重新序列化 CSV。
- 大文件路径：31 至 366 个 UTC 天创建 Export Job；更大范围拆分。生成完成后使用原生浏览器文件下载，不再读入 Blob。
- Export Job 显示 `queued / running / succeeded / failed / cancelling / cancelled / expired`、真实进度或不确定进度、文件大小、SHA-256、创建者和 24 小时过期时间。
- 每个用户默认最多一个运行中 Export Job；Operator/Auditor 只看本人任务，SuperAdmin 可查看全部。异步 API 未落地前不自动退回超大 Blob。
- 两种路径都保留所有原始 Revision、服务端列顺序和精确十进制文本，不提供多 Instrument ZIP。

### Console 与高危动作

- Feed Subscription 与 WebSocket Session Subscription 是不同页面概念。Current Contract 只真实实现后者；前者和动态流控在 Admin API 落地前禁用。
- Feed 页面分别展示 Desired、Observed、Drift、Last Error；409 冲突要求刷新后人工合并，不自动覆盖他人修改。
- 流控拆成 API Rate Limit 与 WebSocket Slow Consumer，不提供一个含义不明的通用阈值，也不控制 Venue、Collector 写队列或存储背压。
- Service Instance 使用 `collector / api / replay`，不展示 Master/Worker、Leader、分片或故障转移。
- Start、Graceful Stop、Reload、Backfill 均需确认与操作原因；Graceful Stop/Reload 还需输入目标实例名。提交中防重复，所有写操作默认不自动重放。
- 状态严格区分待提交、提交中、已接受/执行中、成功、失败、状态未知。Reload/Stop 导致连接断开时不得直接判为失败。
- Reload 是配置校验后的优雅重启，不宣传热加载或零停机；PID 存在不等于 ready。

### WebSocket Debug

- 支持 Topic 前缀提示、subscribe/unsubscribe、ping/pong 顺序屏障、lag_notice、连接/接收时间与原始 JSON 详情。
- 订阅没有 ACK，ping/pong 后只显示“已同步”，不显示“订阅已确认”。
- 内存最多保留 10,000 条，使用虚拟列表；本地溢出丢弃最旧项并显示“界面丢弃数”，与服务端 lag_notice 分开。
- 提供筛选、清空、暂停自动滚动，不持久化报文；原始 JSON 作为文本安全渲染，精确字符串不改写。
- 断线明确显示接收段中断； Auditor 仅使用受控 Topic 表单，Operator/SuperAdmin 才可使用白名单 JSON 调试。

### Quality、Backfill 与 Audit

- Current Contract 的 `gap_unrecoverable` 标注为 Collector 当前进程内快照，重启会丢失；前端不累积后冒充服务端历史。
- Admin API 模式展示持久 Unrecoverable Backfill Gap，含首次/最近发现、原因、范围、`open / resolved` 和关联 Backfill Job；不提供物理删除。
- Backfill Job 展示 `queued / running / succeeded / partially_succeeded / failed / cancelling / cancelled`、计数和当前区间。重试创建关联新 Job，不改写旧状态。
- 高周期 Backfill 显示其真实计划为“补采 1m + 内部 Bar Aggregation”，不是调用原生高周期接口。取消不撤回已写入 Revision。
- Operator Audit Event 提供时间、操作者、Access Role、动作、对象、结果、Request ID 筛选和脱敏 Diff 时间线；无编辑、删除和首版导出。
- 异步动作以 requested 与 succeeded/failed 等关联事件展示，不将追加写事件伪装成原地更新。

### 视觉原型与生产转化

- 在独立 throwaway worktree/branch 中提供 `/prototype/monitoring?variant=A|B|C`，三个结构明显不同的监控方案，共享明确标注的模拟数据。
- 底部浮动切换栏支持按钮与左右方向键，URL 可分享；输入控件聚焦时不拦截方向键，生产构建不包含切换栏。
- 原型无真实 mutation、无持久化、无生产级错误处理和测试；运行命令为 `npm run prototype`。
- 原型分支 `prototype/operator-console-ui`、commit `dfe3c53` 和验证结果已记录到 issue 01。2026-11-09 已选择 **B · 浅色分析工作台**：采用顶部导航、统一筛选行、浅色中性 surface、边框分隔 KPI、密集表格和事件时间线；生产代码按 spec 重写，不直接提升原型。
- 图表遵守统一数据可视化规范：单轴、2px 线、可访问状态图标+文本、图例/表格替代、hover 与键盘读数、无随机趋势和无颜色唯一编码。

### 错误、安全与可访问性

- REST 错误统一读取 `error.code/message/details/request_id`；运维端点和 Prometheus 的非统一响应使用独立适配器。
- 401 停止后续写操作并回到登录入口；403 展示缺少的权限；409 展示版本冲突；429 尊重 Retry-After。
- 写请求使用 CSRF 防护，会话 Cookie 不由 JS 读取；不保存 OIDC Token 到 localStorage。
- 生产静态资源自托管，Nginx 设置合理 CSP 和安全响应头；当前无认证服务只允许内网部署，不暴露公网。
- 状态不只依赖颜色；表格、表单、确认框和图表读数支持键盘，中文错误信息包含可操作下一步。

## Testing Decisions

- Vitest + Testing Library 覆盖 Access Role 矩阵、Revision 去重、UTC 与闭/开区间、Job 状态机、文件名解析、未知长度进度、Blob 错误信封、CSRF/401/403/409 UI 行为。
- 文件名测试覆盖 `filename*`、quoted filename、路径清理、缺失 header fallback；CSV 测试验证内容未被前端改写。
- Playwright 覆盖真实浏览器下载、图表联动、确认框、WebSocket 虚拟列表、键盘操作和 1280/1440px 布局。
- Current Contract 使用本地测试 Dataset、真实 FastAPI 服务和 Prometheus 集成验收，不依赖真实交易所。
- Admin API 未实现前只做基于 `.scratch/gateway-admin-api/spec.md` 的契约测试，并在报告中明确“未进行真实后端联调”，不得计为完整功能验收。
- 每个生产 PR 必须通过 typecheck、lint、unit test 和 production build；原型只要求可运行和构建验证，不按生产测试标准伪装成熟度。

## Out of Scope

- Tick、Depth、1s Bar、通用 Sequence 连续性检测；这些需要独立 Data Contract 扩展。
- 浏览器 Replay 控制、归档导入、Compaction、GC、Quarantine GC。
- 原始 Quarantine 记录查看/修复、Instrument Reference Data 编辑。
- 用户/角色 CRUD、自定义权限、OIDC Provider 管理 UI。
- 邮件/短信/Webhook 告警、审计导出、多 Instrument ZIP。
- Docker Socket 控制、Node 后端、多集群、HA、多租户。
- 采纳或改造未跟踪的 `handoff.md`。

## Further Notes

- 规范来源为当前代码、`GLOSSARY.md`、`docs/data-contract.md` 和正式 ADR；OpenAPI 是生成文档，不是规范真相源。
- 后端改动全部见 `.scratch/gateway-admin-api/spec.md`，本轮不修改 Python。
- 功能、权限、数据、视觉和验收边界均已确认；issue 01 已以 B 方案结论关闭，issue 02–08 可以按依赖顺序实施。
