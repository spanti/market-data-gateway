# 05: Instrument Reference Data + Instrument Snapshot

**What to build:** 用户可以用稳定的 InstrumentId 查询元数据；元数据来自可 review 的 YAML，退市标的不消失。

**Blocked by:** 02

**Status:** resolved

- [x] YAML 真相源校验；重复 instrument_id 或缺失必填字段时报错。
- [x] 导出 Instrument Snapshot，所有进程只读快照。
- [x] `/api/v1/instruments` 与 `/api/v1/instruments/{instrument_id}`；include_delisted 过滤。
- [x] delisted_ts 软删除，历史身份保留。
- [x] 测试覆盖校验、快照、退市过滤。

## Comments

- Resolved in `integration/market-data-gateway-v1` after the v1 delivery code review and follow-up fixes.
