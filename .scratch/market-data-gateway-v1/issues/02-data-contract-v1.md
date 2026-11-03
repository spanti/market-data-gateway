# 02: 发布 Data Contract v1

**What to build:** 外部消费者可以依赖一份完整、版本化的 Data Contract：Instrument 与 Bar 的全部字段、Arrow IPC/CSV/JSON 三种历史输出、`/bars/export`、稳定错误码与 schema 版本校验。

**Blocked by:** 01

**Status:** resolved

- [x] Instrument 与 Bar schema 按 DESIGN §3.1 完整落地（含 aggregation_source、revision、trade_count）。
- [x] Message Envelope 字段完整并可编码为 Arrow IPC 与 JSON。
- [x] `/api/v1/bars` 支持 format=json|arrow|csv；JSON 超过 100,000 行返回 range_too_large；Arrow/CSV 流式输出。
- [x] `/api/v1/bars/export` 返回单个 Parquet 或 Arrow 文件。
- [x] schema_version 不匹配时返回 schema_version_mismatch。
- [x] 错误体与错误码稳定。
- [x] Data Contract 黄金测试与 decimal/IPC 往返测试通过。
- [x] CI 规则阻止网关包 import 回测框架。

## Comments

- Resolved in `integration/market-data-gateway-v1` after the v1 delivery code review and follow-up fixes.
