# 18: 发布三份 ADR 并同步公共文档

**What to build:** 让未来维护者和外部消费者能从正式文档理解为什么采用 Manifest-aware DuckDB、Polars Quarantine 和有界 Replay，以及哪些公共语义保持或有意改变。文档必须与最终代码一致，而不是只记录工具选型。

**Blocked by:** 17 迁移剩余 Store 调用方并删除旧生产路径。

**Status:** ready-for-agent

- [ ] 发布 Manifest-aware DuckDB reads ADR，记录 Manifest Snapshot、Dataset Read Lease、显式文件列表、REST/Replay 语义差异和 Arrow 边界。
- [ ] 发布 Polars batch Normalization with Quarantine ADR，记录批量边界、失败原子性、Reason Code、Rule Version、fail-closed 和 Retention 的权衡。
- [ ] 发布 Bounded Replay with explicit failure termination ADR，记录有界读取、seek 代际、取消、资源约束和 `reason=error` 的公共兼容影响。
- [ ] 每份 ADR 都说明真实备选方案、选择理由、代价和后果，不只描述最终依赖清单。
- [ ] Data Contract 同步描述 Manifest Rule Version 元数据、Quarantine、Read Lease、Replay 失败终止和稳定错误行为。
- [ ] 移除 DESIGN 中“所有读取使用 pyarrow.dataset”的过时实现陈述，并准确说明 Store 门面与 DuckDB/Polars/Arrow 的职责边界。
- [ ] README 在最终有界实现验收后更新 Replay 全量物化的已知限制、依赖要求、资源配置和运维维护方式。
- [ ] GLOSSARY 保持领域定义且无实现细节，统一使用 Normalization、Validation、Quarantine、Manifest Snapshot 和 Dataset Read Lease。
- [ ] 文档明确非分页输出无全序承诺、REST raw Revisions、Replay 五键排序，以及 Revision-heavy Time Cursor 仍是独立契约问题。
- [ ] 文档不再建议 DuckDB 1.x、Dataset 根目录 glob、生产旧引擎开关或把读取异常当成空结果。
