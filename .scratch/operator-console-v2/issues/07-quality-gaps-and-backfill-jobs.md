# 07: Quality Gap 与 Backfill Job 完成迁移

**What to build:** Operator 可以在完整 D 风格的 Quality 页面中区分 Unrecoverable Backfill Gap 与 Backfill Job，查看真实缺口并创建、确认、跟踪、取消或重试补采任务。既有 Gap、Job、权限和 Bar Aggregation 语义保持不变。

**Blocked by:** 01 监控全页建立 D 视觉与共享基础

**Status:** resolved

- [ ] Quality 页面、Gap、创建表单和 Job 列表使用共享卡片、状态药丸、简洁标题与 `?` 帮助，不保留数字标题、整页装饰分割线或冗余说明。
- [ ] Current Contract 的进程内 Gap 快照与 Admin API 的持久 Unrecoverable Backfill Gap 明确区分，不累积后冒充服务端历史。
- [ ] Gap 的首次/最近发现、原因、范围、open/resolved 与关联 Backfill Job 清晰可见，不扩展为通用 Sequence 连续性断言或物理删除。
- [ ] Backfill Job 创建继续要求操作原因与确认，按服务端 Access Role 开放，校验、危险后果和提交结果就地显示。
- [ ] queued、running、succeeded、partially_succeeded、failed、cancelling、cancelled 的文本、图标和颜色准确，计数与当前区间保持真实。
- [ ] 取消不撤回已写入 Revision，重试创建关联新 Job，不改写旧状态；Backfill Job 与 Export Job 不混用。
- [ ] 高周期 Backfill 继续表达补采 1m 加内部 Bar Aggregation，不暗示调用原生高周期接口。
- [ ] Loading、Empty、Error、Unknown 使用真实状态，界面生成文案无 `·` 或 `：`，后端原始错误与 Request ID 保持准确且就地可见。
- [ ] 1440、1280、1024 下 Gap、表单、确认框和 Job 列表无整页横向溢出，键盘与触屏帮助可用。
- [ ] typecheck、lint、相关 Vitest、production build 与 Gap、Job 状态机、权限、高危确认及布局浏览器验收通过。

## Comments

**Resolved (2026-11-16):** 已在 `65fbd9c` 实现并由 `0ce8d01` 补充 Quality loading 与 1024 宽表局部滚动覆盖，合并最新 integration 后以分支 tip `2ee94af` 全绿合并为 `d3e2e19`。Quality 页面、Gap、创建表单与 Job 列表采用共享卡片、状态药丸、简洁标题与 `?` 帮助；Current Contract 的进程内 Gap 快照与 Admin API 的持久 Unrecoverable Backfill Gap 保持为独立卡片，不累积后冒充服务端历史。Gap 的首次/最近发现、原因、范围、open/resolved 与关联 Backfill Job 就地可见；Backfill Job 创建要求操作原因与确认，queued、running、succeeded、partially_succeeded、failed、cancelling、cancelled 的文本、图标与颜色准确，运行中进度保持不确定。取消不撤回已写入 Revision，重试创建关联新 Job 且不改写旧状态；高周期 Backfill 继续表达补采 1m 加内部 Bar Aggregation。分支 tip `61` 个测试文件 / `509` 个 Vitest 测试通过（本次复核），typecheck、lint、production build 与定向 Playwright 通过，覆盖真实 Current Contract 快照与 1024/1280/1440 布局；Admin Gap/Job 流程仅有 CONTRACT-ONLY 证据，不宣称真实 Admin/OIDC 端到端通过（最终全量计数以 `web/docs/acceptance-report-v2.md` 为准）。
