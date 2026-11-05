# 03: 记录 Revision-heavy Time Cursor 的契约缺口

**What to build:** 通过现有公共 REST 行为说明同一 `open_ts` 存在多个 Revisions 时，单时间值 Time Cursor 与“跨页不重不漏”承诺之间的结构性冲突，为后续独立契约决策留下可复现证据。本 ticket 只刻画和记录，不选择修复方案。

**Blocked by:** None (can start immediately).

**Status:** ready-for-agent

- [ ] 用真实 REST 请求复现同一 Instrument、同一 `open_ts` 多个 Revisions 跨页时的实际响应和下一页行为。
- [ ] 覆盖页内包含相同时间戳、重复时间戳跨页、最后一页以及 `limit=0` 的情况。
- [ ] 记录 raw Revisions、按 `open_ts` 升序、闭区间 start 和单值 `next_start` 为什么不足以表达页内进度。
- [ ] 区分现有测试已证明的唯一时间序列遍历行为与 Revision-heavy 数据下未被证明的行为。
- [ ] 说明隐式最新 Revision 去重、复合游标、不透明游标和扩大页边界分别会改变哪些既有契约，但不得在本 ticket 选定或实现其中一种。
- [ ] 将结论作为独立契约问题记录，并让分页迁移验收能够引用该记录而非隐藏差异。
- [ ] 不改变生产响应、raw Revision 行数、Time Cursor 形状或 JSON 行数上限。
