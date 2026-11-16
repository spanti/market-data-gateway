# 03: Export Job 完整生命周期完成迁移

**What to build:** 大文件消费者可以在与 Bar 查询一致的 D 界面中创建、拆分、跟踪、取消和下载 Export Job，通过状态药丸和就地进度理解真实任务生命周期。任务仍由现有异步 API 执行，原始 Revision、精确文本、权限范围和下载方式不变。

**Blocked by:** 02 Bar 查询、图表、Revision 与直接 CSV 完成迁移

**Status:** ready-for-agent

- [ ] 31 至 366 个 UTC 天使用 Export Job，更大范围按现有策略拆分，不在异步能力不可用时自动退回超大 Blob。
- [ ] 创建表单、拆分确认、任务列表、进度、结果与错误采用共享卡片和状态药丸，必要说明与危险后果保持就地显示。
- [ ] queued、running、succeeded、failed、cancelling、cancelled、expired 的文本、图标和颜色语义准确，真实不确定进度不得伪装成百分比。
- [ ] 文件大小、SHA-256、创建者、24 小时过期时间和关联查询上下文清晰可读，Request ID 不被藏入帮助。
- [ ] SuperAdmin、Operator 与 Auditor 的任务可见范围及默认最多一个运行中任务的约束保持不变。
- [ ] 完成后继续使用原生浏览器文件下载，不重新读入 Blob，不改变原始 Revision、服务端列顺序或精确十进制文本。
- [ ] 取消、失败、过期与重试相关操作保持现有状态机，重复提交防护、网络错误和状态未知不被视觉迁移覆盖。
- [ ] Loading、Empty、Error、Unknown 使用真实状态，界面生成文案无 `·` 或 `：`，后端原始错误保持原样。
- [ ] 1440、1280、1024 下创建、确认、跟踪和下载流程无整页横向溢出，键盘可完成操作。
- [ ] typecheck、lint、相关 Vitest、production build 与 Export Job 契约及可用真实流程验收通过， CONTRACT-ONLY 与真实联调结果分别记录。

## Comments
