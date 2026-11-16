# Spec: Operator Console v2（D 现代 SaaS 界面重设计）

Status: ready-for-agent

## Problem Statement

Operator Console 已具备监控、Bar 图与导出、Gateway Console、Quality & Backfill、Security & Audit 五个生产模块，但界面仍依赖大量常驻解释、行内分隔符、横贯页面的分割线和数字标题来组织信息。使用者需要在冗长文案与重复元数据中寻找当前健康、异常位置和下一步操作，界面生成的 `·` 与 `：` 也增加了视觉噪音。

旧 B 视觉方向强调分析工作台，但未解决上述一致性问题。本轮经 A、B、C 重做及 B 派生方案比较，用户于 2026-11-16 确认 **D 现代 SaaS 工作台**为新的主设计。需要把 D 的卡片层级、状态药丸、简洁标题和 `?` 悬浮帮助转化为五模块共享生产语言，同时保持现有业务功能、数据保真、权限和运行模式不变。

## Solution

以 D 为视觉与交互基准重构现有五个模块。浅色中性画布承载白底、微圆角、极浅阴影的 Metric Cards、Chart Cards 与业务面板，健康与告警使用带图标和文本的彩色 Status Badges。移除横贯页面的装饰性分割线、数字下划线标题和不必要的常驻解释，通过导航、卡片网格、标题、对齐、间距和状态层级让主流程自解释。

补充概念统一放入 `?` 悬浮帮助。鼠标悬停与键盘聚焦可打开，触屏点击可切换，Escape 可关闭，文本可读且不遮挡触发目标。权限、能力未知、数据过期或中断、校验错误、危险操作后果、操作结果和 Request ID 继续就地显示，不能为精简文案而隐藏。

界面生成的标题、标签、状态、说明、占位符、错误前缀与动态拼接文本禁止出现 `·` 和 `：`。半角 `:` 与时间格式不受限。WebSocket 原始 JSON、Operator Audit Event 的脱敏 Diff 和后端原始错误属于数据证据，允许原样展示，不得为了满足界面文案禁令而改写。

生产实现按现有组件与测试体系重写，不直接提升 throwaway 原型，不引入原型切换器或模拟状态到生产环境。

## User Stories

1. 作为内部运维人员，我想在监控首页看到清晰的 KPI 卡片和健康状态药丸，以便迅速判断 Gateway 是否需要介入。
2. 作为内部运维人员，我想分别看到 Feed Freshness 与 Dataset Freshness，以便区分实时采集失活与历史数据落盘更新程度。
3. 作为内部运维人员，我想在独立趋势卡片中比较 QPS 与 HTTP P50/P95/P99，以便关联吞吐和延迟异常。
4. 作为排障人员，我想在异常定位区域同时看到 Feed Freshness、Service Instance、页面内告警和上游状态，以便缩短定位路径。
5. 作为排障人员，我想看到连接、心跳和重连计数的能力未知状态，以便不会把 Freshness 误认为连接健康证明。
6. 作为数据分析人员，我想在清晰的查询卡片中按 InstrumentId、Bar Interval 和显式时间范围查询 Bar，以便完成现有分析任务。
7. 作为数据分析人员，我想在图表卡片与原始 Revision 表格之间保持相同查询上下文，以便核对最大 Revision 图与全部原始 Revision。
8. 作为数据消费者，我想让 CSV 与 Export Job 保留原始 Revision、服务端列顺序和精确十进制文本，以便界面重设计不影响数据可信度。
9. 作为 Export Job 使用者，我想通过状态药丸和就地进度识别 queued、running、succeeded、failed 等生命周期，以便采取下一步操作。
10. 作为 Operator，我想分别查看 Feed Subscription 的 Desired、Observed、Drift 和 Last Error，以便安全处理采集配置。
11. 作为 SuperAdmin，我想在 Service Instance 生命周期操作前看到就地危险后果、原因和确认要求，以便避免误操作。
12. 作为排障人员，我想在 WebSocket Debug 中保持有界报文、接收段中断、界面丢弃与服务端 lag_notice 的区别，以便准确诊断会话。
13. 作为 Operator，我想在 Quality 页面区分 Unrecoverable Backfill Gap 与 Backfill Job，以便理解缺口和补采生命周期。
14. 作为 Auditor，我想查询 Operator Audit Event 并查看原始准确的脱敏 Diff 与 Request ID，以便追溯管理动作。
15. 作为控制台使用者，我想通过五个一级模块导航进入各页面，以便保持现有任务路径与 URL 习惯。
16. 作为控制台使用者，我想看到简洁标题和一致卡片分组，而不是数字标题与整页分割线，以便快速扫描页面。
17. 作为控制台使用者，我想只阅读与当前任务相关的标签、单位、状态和操作，以便不被重复说明干扰。
18. 作为鼠标用户，我想把指针放在 `?` 上阅读字段定义和补充用法，以便在需要时获得帮助。
19. 作为键盘用户，我想通过 Tab 聚焦 `?` 打开相同帮助，以便不依赖鼠标完成操作。
20. 作为触屏用户，我想点击 `?` 打开或关闭帮助，以便在没有悬停的环境中使用控制台。
21. 作为键盘与触屏用户，我想按 Escape 关闭帮助，并让悬浮层避免遮挡当前目标，以便继续当前任务。
22. 作为控制台使用者，我想让权限、能力未知、数据过期、校验错误和操作结果始终就地可见，以便不会遗漏必要风险信息。
23. 作为控制台使用者，我想让状态药丸同时使用图标、文本和颜色，以便在色觉差异或单色环境下理解状态。
24. 作为控制台使用者，我想在加载时看到明确骨架或加载状态，以便不会把旧快照当作当前健康数据。
25. 作为控制台使用者，我想在空数据时看到当前范围没有数据的真实状态，以便不会误认为系统故障。
26. 作为控制台使用者，我想在请求失败时看到错误与可用的 Request ID，以便进行排障和升级。
27. 作为 Current Contract 使用者，我想清楚看到匿名只读与能力边界，以便不会把界面状态当成假登录或角色切换。
28. 作为 Admin API 使用者，我想让界面继续按服务端 Access Role 开放能力，以便重设计不会成为新的安全边界。
29. 作为桌面用户，我想在 1440px 获得 D 的完整卡片布局，以便高效使用监控与分析功能。
30. 作为桌面用户，我想在 1280px 完整使用五个模块，以便满足既有最低桌面验收要求。
31. 作为窄屏用户，我想在 1024px 不出现整页横向溢出，以便继续查看内容和操作控件。
32. 作为移动端使用者，我想查看关键状态并使用触屏帮助，以便完成基本状态确认。
33. 作为审计与排障人员，我想让原始 JSON、审计 Diff 和后端原始错误保持原样，以便界面规范不会破坏证据。
34. 作为中文界面使用者，我想保留规范英文领域标识，以便与 API、日志、GLOSSARY 和 ADR 准确对应。
35. 作为控制台维护者，我想通过共享卡片、状态和帮助组件迁移五个模块，以便避免每页复制一套视觉实现。
36. 作为验收人员，我想通过真实浏览器检查导航、状态、帮助、溢出和关键任务，以便确认用户行为而非内部实现细节。
37. 作为验收人员，我想继续区分真实 Current Contract 联调与 Admin API 契约测试，以便不会把未执行的真实 OIDC 联调计为通过。
38. 作为后续维护者，我想让新视觉决策与旧 B 历史记录并存，以便理解为什么生产方向从 B 演变为 D。

## Implementation Decisions

### 选定设计与边界

- D 是现代 SaaS 主设计，取代旧 B 的生产视觉方向。A 的固定侧栏、C 的服务泳道和 B 的数字分析路径不进入本次生产信息架构。
- 五个一级模块、现有路由、访问权限、API 客户端、TanStack Query server state、ECharts、WebSocket 生命周期与部署方式保持。
- 浅色 D 是默认和主要视觉验收基准。保留现有暗色主题能力的可用性回归，但不建立第二套信息架构，也不要求暗色卡片仍为白色。
- 不新增全局状态库、组件库、Node/BFF、Service Role 或后端端点。

### 共享视觉语言

- 页面使用极浅中性背景与合理最大内容宽度。顶部 Shell 通过留白和背景分层，不使用横贯全屏的装饰性分隔线。
- Metric Cards、Chart Cards 与业务面板共享白色 surface、约 12px 微圆角、极浅阴影和可选极浅边框。卡片内部通过网格、对齐和间距分组，不靠标题下划线。
- 移除 01、02 等数字章节标题及数字下划线标题。页面与卡片标题保持简短，章节顺序由任务而非编号表达。
- Status Badges 仅用于健康、告警、生命周期、权限与未知等状态，使用浅底色、较深文字、图标和文本。普通字段名与分类标签不全部药丸化。
- 表格保留必要的行分隔和局部横向滚动，不能让宽表造成整页横向溢出。
- 设计 token 同时支撑浅色 D 和现有暗色回归，页面不得各自复制一套硬编码卡片与状态色。

### 共享组件与接口

- 收敛 Page Header、Metric Card、Chart Card、业务 Panel、Status Badge、Help Trigger、Tooltip 与状态区域的公共接口，保留现有调用方能逐步迁移的兼容层。
- Page Header 以标题、必要状态和主操作为主，不再以常驻 description 或 meta 字符串堆叠说明。
- Metric Card 接收标签、值、单位、必要时间证据与可选帮助，不把所有补充文本放在卡片底部。
- Chart Card 保留 ECharts hover、键盘读数与数据表替代，字段定义进入 `?`，数值 Tooltip 与帮助 Tooltip 保持不同职责。
- Help Trigger 提供可访问名称、帮助内容和显示方向，统一 hover、focus、touch、Escape、碰撞偏移、最大宽度、换行和层级行为。
- Loading、Empty、Error、Unknown 状态由真实请求与能力数据驱动，按卡片或业务区域呈现，不引入生产 URL 模拟状态。
- 高风险确认、错误、权限、会话通知和操作结果继续就地展示，不新增自动消失 Toast，也不把 Request ID 藏入帮助。

### 文案与原始证据

- 建立两类内容边界。界面文案包括所有由前端生成或拼接的标题、标签、状态、说明、占位符、校验与错误前缀；其中禁止 `·` 和 `：`。
- 原始证据包括 WebSocket 原始 JSON、Operator Audit Event 脱敏 Diff 与后端原始错误，保持准确原文。禁令验收必须明确排除并单独验证这些区域。
- 字段分组优先改为列、定义列表、分行或卡片，不把禁用分隔符机械替换为另一套同样嘈杂的符号。
- 保留 InstrumentId、Bar Interval、Revision、Service Role、Service Instance、Access Role、Feed Freshness、Dataset Freshness、Backfill Job、Export Job 等规范术语。

### 五模块迁移

- **监控**：直接继承 D 的顶部状态摘要、四个 KPI 卡、两个趋势卡、异常定位卡片和数据明细卡。Feed Freshness 与 Dataset Freshness 独立呈现，Service Role 与 Service Instance 不混用，上游能力未知不从 Freshness 推断。
- **监控显示逻辑**：把内部 series identity 与显示名称分离，移除显示分隔符时不得继续用显示文案前缀匹配指标。QPS、Peak QPS、HTTP 百分位和最后成功时间的数据语义不变。
- **Bar 图与导出**：查询、Bar 图、CSV、Export Job 与 Revision 明细采用共享卡片与状态模式。保留 URL 查询、UTC/本地时间、最大 Revision 图、原始 Revision 表、Revision-safe Cursor、精确十进制文本和 WS 不连续标记。
- **Gateway Console**：Feed Subscription、Service Instance 和 WebSocket Debug 使用一致卡片层级。Desired/Observed/Drift、Service Instance 生命周期、WebSocket Session Subscription 和原始 JSON 证据保持独立概念与既有操作约束。
- **Quality & Backfill**：Unrecoverable Backfill Gap、Backfill Job 创建与任务列表采用一致状态与表单布局。操作原因、确认、部分成功、取消和关联新 Job 的语义不变。
- **Security & Audit**：当前身份、审计筛选、Operator Audit Event、脱敏 Diff 与 Operation 时间线采用一致卡片和状态药丸，原始证据例外保持。
- 其他模块只继承 D 的视觉语言与交互原则，不机械复制监控页的 KPI 数量或异常卡片网格。

### 实施顺序与完成门

1. **共享基础**：建立视觉 token、卡片、状态药丸、帮助触发器与内容分类，先用监控完成生产化样板。通过共享组件测试和监控浏览器验收后再扩展。
2. **监控迁移**：完成 D 布局、series identity 分离、真实状态和两类 Freshness 验收。不得直接复制原型组件。
3. **Bar 与导出迁移**：逐区替换文案与布局，保持图、表、游标、下载和实时跟随回归通过。
4. **Console 迁移**：覆盖三个页签、确认框、错误与原始 JSON，保持高危动作和 WS 行为不变。
5. **Quality 与 Security 迁移**：覆盖表单、任务状态、审计详情与原始 Diff，保持权限矩阵与状态机不变。
6. **全控制台验收**：清理剩余界面文案符号和冗余说明，完成五模块、三档宽度、帮助交互、明暗主题回归和完整测试。生产树不包含原型路由、Variant、切换器或模拟状态。
7. **原型留存**：throwaway 分支保留 A、B、C、D 和验证脚本作为 primary source，生产只保留经确认的决策与重写实现。

## Testing Decisions

### 主要测试面

- 以现有 Playwright 生产路由与本地真实服务栈作为**唯一主验收面**，从五个一级模块的外部行为验证视觉迁移，不为布局另建平行测试应用或生产 Mock 模式。
- 扩展现有五模块布局、键盘、主题、监控、Bar、Console、Quality、Security 与下载验收，优先复用当前导航、溢出、图表读数、状态与契约测试辅助函数。
- 好测试只断言用户可见或可操作的结果，包括路由、可见文本、状态、焦点、悬浮层、下载、原始证据和无横向溢出，不断言组件内部调用、私有 DOM 结构或 Tailwind 类名。

### 必测行为

- 在 1440、1280、1024 三档宽度遍历五模块及 Console 三个页签，验证完整导航、关键卡片、表格局部滚动和无整页横向溢出。1440 与 1280 保持完整桌面验收，1024 验证窄屏可用性。
- 对界面生成内容做可见 DOM 符号检查，覆盖默认页、三个 Console 页签、表单校验、危险确认、加载、空、错误、能力未知、操作结果与展开的 `?` 帮助。原始证据区域单独验证原文未被改写。
- 对共享帮助分别验证鼠标 hover、键盘 focus、触屏重复 tap、Escape 关闭、可访问名称、非空可读文本、碰撞处理和不遮挡触发目标。触屏测试使用真实 touch context。
- 验证加载、空、错误与未知状态不会继续展示貌似当前健康的旧 KPI 或状态药丸。真实缓存数据若保留，必须显示最后成功时间与过期或未知语义。
- 验证 Status Badges 不依赖颜色单独表达状态，图表仍有键盘读数和数据表替代，表单、确认框与主要操作保持键盘可达。
- 用截图进行 D 视觉审查，确认白色微圆角卡片、极浅阴影、状态药丸、无整页装饰分割线、无数字下划线标题和必要的明暗主题可读性。截图审查不替代行为断言。

### 针对性回归

- 保留并扩展监控指标显示回归，证明显示名称不含禁用符号后仍读取正确 series，且 identity 与 label 可独立变化。
- 保留 Revision 去重、Revision-safe Cursor、UTC 与闭开区间、精确十进制文本、CSV 内容、Export Job 状态机、Access Role 矩阵、CSRF、401/403/409/429、高危确认和 WS 有界列表测试。
- 对后端原始错误、WS 原始 JSON 和审计 Diff 使用包含 `·` 与 `：` 的样例，证明原始证据原样展示，而周围界面生成文案仍符合禁令。
- 每个迁移阶段运行 typecheck、lint、Vitest、production build 和相关 Playwright。最终运行完整前端套件与五模块浏览器验收。
- Current Contract 使用真实本地 FastAPI、测试 Dataset、Prometheus 与 WS，不依赖外部 Venue。Admin API 契约测试继续明确标注 CONTRACT-ONLY，没有真实 OIDC 证据时不得宣称真实 Admin 全链路通过。

### 原型先验证据

- A、B、C、D 共用固定模拟数据和四态预览，已通过 TypeScript、Vite build、禁用符号扫描及 33 项浏览器检查。
- 浏览器检查覆盖四个方案的三档宽度、hover、focus、touch 重复切换、Escape、状态 URL 保留、主要方案交互和无整页横向溢出。该结果只证明原型可比较，不代替生产验收。

## Out of Scope

- 不修改 REST/WS/Data Contract、Revision-safe Cursor、OIDC、Access Role、Operator Audit Event、Admin API 或 Python 服务。
- 不新增一级模块、路由、真实业务 mutation、外部告警、IAM 管理、审计导出、多 Instrument ZIP 或原规格排除的数据能力。
- 不采用 A 固定侧栏、C 服务泳道或 B 数字分析路径作为生产主布局。
- 不新增自动消失 Toast、全局状态库、新组件库、Node/BFF、第四个 Service Role 或手机专属完整导航。
- 不把原型 Mock、四态 URL、Variant 切换器或底部浮动栏带入生产。
- 不直接复制 throwaway 原型代码作为生产实现，不因视觉重构重写已稳定的业务状态机。
- 不为满足符号禁令改写原始 JSON、审计 Diff 或后端原始错误。
- 不把暗色主题扩展为另一套信息架构，也不以本轮浅色卡片验收替代暗色基本可读性回归。

## Further Notes

- 既有功能契约见 [Operator Console spec](../operator-console/spec.md)。本规格是独立的 v2 视觉与界面清晰度规格，其他功能契约继续有效。
- 旧 [issue 01](../operator-console/issues/01-monitoring-ui-prototype.md) 保留 B 在 2026-11-09 的历史选型与验收，不 reopen。D 于 2026-11-16 取代其生产视觉决策，旧原型、历史验收和生产功能契约保留。
- 领域语言遵循 [GLOSSARY.md](../../GLOSSARY.md)，管理边界遵循 [ADR-0016](../../docs/adr/0016-operator-console-admin-api.md) 与 [ADR-0017](../../docs/adr/0017-admin-control-loopback-tcp.md)，历史分页遵循 [ADR-0018](../../docs/adr/0018-revision-safe-history-pagination.md)。
- 原型位于 throwaway 分支 `prototype/operator-console-ui` 的独立 worktree，运行命令为 `npm run prototype -- --port 5174 --strictPort`。D 入口为 `http://127.0.0.1:5174/prototype/monitoring?variant=D`，可附加 `state=loading|empty|error`。
- 当前原型改动尚未提交，未合并到 master，未 push。实施时以本规格的决策为准，并保留该 worktree 中的 A、B、C、D、截图与验证脚本作为设计证据。
- 原型 build 仅有既有 ECharts chunk-size warning，无类型或构建错误。生产实现不得借视觉重构引入同样的未评估打包退化。
- **Decision (2026-11-16):** 用户确认 D 为主设计。规范由本轮 grilling、领域文档核对、生产界面盘点与四个原型验证结果综合生成，测试主面采用现有生产浏览器验收。
