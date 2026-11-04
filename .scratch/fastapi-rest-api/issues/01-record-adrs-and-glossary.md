# 01: 记录 ADR 与 glossary 术语

**What to build:** 在实现开始前把这次迁移的契约决定写成两份 ADR，并补齐领域词汇。ADR-0011 记录 FastAPI/ASGI 取代 aiohttp 作为 HTTP 面，明确生命周期、Pydantic 边界与统一错误模型，并显式取代 `DESIGN.md` §12 的“FastAPI 等 Web 框架”条目。ADR-0012 记录分页要求单 Instrument 与 `open_ts` 排序契约，解释为什么多 Instrument 的时间游标分页不提供。`GLOSSARY.md` 新增 **REST API**（api Service Role 对外提供的历史查询 HTTP 接口；避免“服务层”）与 **Time Cursor**（闭区间 `start`、开区间 `end` 与 `next_start` 组成的读取游标）。旧 `http-server-lifecycle` spec 标记 superseded 并指向本 feature 的 spec。

**Blocked by:** None (can start immediately)

**Status:** ready-for-agent

- [ ] ADR-0011 存在，说明 FastAPI/ASGI、后台线程 uvicorn、Pydantic 只做入口、统一错误 envelope，并取代 `DESIGN.md` §12 的 FastAPI 条目
- [ ] ADR-0012 存在，说明分页只对单 Instrument 生效、按 `open_ts` 升序、`next_start` 为第一条未返回 Bar 的 `open_ts`
- [ ] `GLOSSARY.md` 新增 **REST API** 与 **Time Cursor**，且 REST API 条目明确避免“服务层”
- [ ] 旧 `http-server-lifecycle` spec 顶部标记 superseded，并指向本 feature spec
- [ ] 无生产代码行为变化；测试全绿
