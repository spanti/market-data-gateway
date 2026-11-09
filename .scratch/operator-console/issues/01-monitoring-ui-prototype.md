# 01: 用三个监控原型确定 Operator Console 视觉系统

**What to build:** 在独立 throwaway worktree/branch 的 `web/` 工程中实现 `/prototype/monitoring?variant=A|B|C`。三个方案共享同一组明确标注的模拟监控数据，但在布局、信息层级和主要操作affordance上显著不同；通过底部浮动栏和左右方向键切换，URL 可分享。原型只回答“高密度监控后台应该长什么样”，不接真实 API、不执行 mutation、不进入生产路由。

**Blocked by:** None (can start immediately)

**Status:** resolved

- [x] Variant A：深色运维指挥舱，固定侧栏、全局健康主状态、KPI 带、主趋势图和右侧 Freshness/告警栏。
- [x] Variant B：浅色分析工作台，顶部导航、统一筛选行、大分析图、密集指标表和底部 Freshness 矩阵，不做重复卡片网格。
- [x] Variant C：服务优先的事件矩阵，以 collector/api/replay 三条服务泳道和事件时间线组织信息，指标行内展开，不采用 A/B 的主布局。
- [x] 三个方案都呈现 QPS、Peak QPS、HTTP P50/P95/P99、Service Role、Feed Freshness、上游能力缺口、最后更新时间和 PROTOTYPE 标识。
- [x] 所有趋势使用固定模拟数据，不随机刷新，不把缺失指标绘制成真实数据；每个图表有表格或可访问文本读数。
- [x] 浮动栏包含上一方案/下一方案、当前 key 与名称、共享状态摘要；输入框聚焦时方向键不被拦截，生产构建不包含切换栏。
- [x] `npm run prototype` 一条命令可运行；记录实际 URL、分支和 commit。
- [x] 原型在 1440px 和 1280px 渲染检查，无标签碰撞、溢出或不可读图表。
- [x] 将分支和 commit 作为上下文指针追加到本 issue，供用户选择或组合方案。

## Comments

**Prototype captured:** branch `prototype/operator-console-ui`，worktree `E:/total-project/mdgw-worktrees/operator-console-prototype`，commit `dfe3c53`（`prototype(console): add three monitoring UI variants`）。三个方案和切换器只存在于该 throwaway 分支，未合并到 master、未 push。

**Run:** 在 worktree 的 `web/` 执行 `npm run prototype`。

- A：http://127.0.0.1:5173/prototype/monitoring?variant=A
- B：http://127.0.0.1:5173/prototype/monitoring?variant=B
- C：http://127.0.0.1:5173/prototype/monitoring?variant=C

**Verification (2026-11-09):**

- Tailwind CSS 与 Vite 插件固定在 4.0.17；4.0.0 在当前 Vite 构建中失败，升级到 4.0.x patch 后通过，未升级到 4.1。
- `npm run build` 通过（TypeScript + Vite production build）。ECharts 使原型主 bundle 为 1,430 kB，存在 chunk-size warning；throwaway 原型不为此增加生产分包设计。
- Chrome headless 对 A/B/C × 1440/1280px 共六组页面检查均为 HTTP 200、无浏览器 console error、无横向溢出；已逐张截图检查。
- 下一方案 URL 切换、共享 6h 状态、C 的 Collector 选择和 HTTP 延迟页签均通过。
- Light/dark categorical palette validator 均通过。Light aqua 对 surface 的 contrast 为 WARN，已由可见当前值、图例和完整数据表提供 relief；所有图表均有数据表。

**Verdict (2026-11-09):** 用户选择 **B · 浅色分析工作台**。生产设计采用浅色中性 surface、顶部导航、统一筛选行、边框分隔 KPI、密集 Service/Freshness 表格和清晰的指标/事件层级；不混用 A 的固定侧栏或 C 的服务泳道作为主导航。原型代码继续作为 throwaway 分支上的 primary source，生产组件必须按 spec 重写，不能直接提升原型。
