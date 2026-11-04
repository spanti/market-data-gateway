# 06: Bars REST 对等：JSON、Arrow、CSV、export

**What to build:** 在新 FastAPI 应用上实现 `/api/v1/bars` 与 `/api/v1/bars/export` 的全部格式与流式行为，并对照 characterization 证明语义不变。JSON 继续返回 Message Envelope、decimal 字符串、纳秒时间字符串，并保留 100,000 行上限与 `range_too_large`；Arrow 继续逐批流式输出 `decimal128(38,18)` 与 `timestamp[ns]`；CSV 继续输出十进制原文且不经过浮点；export 继续返回一次性 Parquet/Arrow 文件下载。分页元数据不在本票范围。

**Blocked by:** 05 (FastAPI 应用骨架、运维与 Instrument 端点对等)

**Status:** ready-for-agent

- [ ] JSON Bars 的 envelope、decimal 字符串、纳秒时间与 100,000 行上限与迁移前一致
- [ ] Arrow 流的 schema、Content-Type 与逐批流式行为与迁移前一致
- [ ] CSV 文本、Content-Type 与“绝不经过浮点”的约束与迁移前一致
- [ ] export 的 Parquet/Arrow 文件、Content-Type 与临时文件清理与迁移前一致
- [ ] 参数校验与错误映射复用 05 的统一错误层
- [ ] characterization 测试全部通过
- [ ] 生产仍走旧 aiohttp 应用；既有测试全绿
