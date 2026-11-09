# 08: 完成生产部署、端到端验证与交付报告

**What to build:** 提供 `web/` 内生产 Nginx/Dockerfile、安全响应头、静态构建、开发/生产代理说明和完整验收报告。运行 typecheck、lint、unit、build、Playwright 与 Current Contract 真实集成；清楚区分 Admin API 契约测试和真实联调。

**Blocked by:** 02, 03, 04, 05, 06, 07

**Status:** ready-for-agent

- [ ] 1280/1440px、Chrome/Edge、键盘、状态非颜色编码和图表表格读数通过检查。
- [ ] 静态资源自托管，CSP、安全头和内网-only Current Contract 警告完整。
- [ ] Admin 模式不公开无认证 Prometheus/ops 管理资源，代理头信任边界正确。
- [ ] 真实本地 FastAPI、测试 Dataset、Prometheus 和 WS 集成不依赖外部 Venue。
- [ ] 小 CSV 下载、断线、不前进游标、401/403/409/429、高危确认均有可重复验证。
- [ ] 报告逐项列出真实联调通过、仅契约测试、后端阻塞和未执行测试，不隐藏失败。
- [ ] 原型分支和最终视觉决策已记录，生产树不包含 throwaway switcher/variants。

## Comments
