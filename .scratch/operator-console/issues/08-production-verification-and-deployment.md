# 08: 完成生产部署、端到端验证与交付报告

**What to build:** 提供 `web/` 内生产 Nginx/Dockerfile、安全响应头、静态构建、开发/生产代理说明和完整验收报告。运行 typecheck、lint、unit、build、Playwright 与 Current Contract 真实集成；清楚区分 Admin API 契约测试和真实联调。

**Blocked by:** 02, 03, 04, 05, 06, 07

**Status:** resolved

- [ ] 1280/1440px、Chrome/Edge、键盘、状态非颜色编码和图表表格读数通过检查。
- [ ] 静态资源自托管，CSP、安全头和内网-only Current Contract 警告完整。
- [ ] Admin 模式不公开无认证 Prometheus/ops 管理资源，代理头信任边界正确。
- [ ] 真实本地 FastAPI、测试 Dataset、Prometheus 和 WS 集成不依赖外部 Venue。
- [ ] 小 CSV 下载、断线、不前进游标、401/403/409/429、高危确认均有可重复验证。
- [ ] 报告逐项列出真实联调通过、仅契约测试、后端阻塞和未执行测试，不隐藏失败。
- [ ] 原型分支和最终视觉决策已记录，生产树不包含 throwaway switcher/variants。

## Comments

**Resolved (2026-11-09):** 合并提交 `e46ec46`，评审修复至 `ecb0c15`。交付 `web/Dockerfile`（Node 构建 → Nginx 静态，75.4 MB，无 Node 运行时）、`web/nginx/` 双模式模板（Current Contract 代理 REST/WS/ops/Prometheus；Admin 模式 404 `/prometheus`、`/metrics`、`/status`、`/ws`、`/ops/*`）、CSP/安全响应头与代理头信任边界、`web/README.md`、`web/docs/acceptance-report.md`。Playwright 17/17 通过（真实 Python collector/api/replay/Prometheus + Chrome）：真实 CSV 下载、图与游标、1280/1440 与键盘、监控状态、真实 WS 虚拟列表与断线分段；Admin 401/403/409/429/确认框为拦截式契约测试。Docker 构建成功；Edge 布局/键盘 4/4。验收报告逐项区分真实联调/契约测试/后端阻塞/未执行；生产树无原型切换器或变体。代码评审修复后最终套件 458 tests + 17 E2E。
