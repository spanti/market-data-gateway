# 03: 记录 Revision-heavy Time Cursor 的契约缺口

**What to build:** 通过现有公共 REST 行为说明同一 `open_ts` 存在多个 Revisions 时，单时间值 Time Cursor 与“跨页不重不漏”承诺之间的结构性冲突，为后续独立契约决策留下可复现证据。本 ticket 只刻画和记录，不选择修复方案。

**Blocked by:** None (can start immediately).

**Status:** resolved

- [x] 用真实 REST 请求复现同一 Instrument、同一 `open_ts` 多个 Revisions 跨页时的实际响应和下一页行为。
- [x] 覆盖页内包含相同时间戳、重复时间戳跨页、最后一页以及 `limit=0` 的情况。
- [x] 记录 raw Revisions、按 `open_ts` 升序、闭区间 start 和单值 `next_start` 为什么不足以表达页内进度。
- [x] 区分现有测试已证明的唯一时间序列遍历行为与 Revision-heavy 数据下未被证明的行为。
- [x] 说明隐式最新 Revision 去重、复合游标、不透明游标和扩大页边界分别会改变哪些既有契约，但不得在本 ticket 选定或实现其中一种。
- [x] 将结论作为独立契约问题记录，并让分页迁移验收能够引用该记录而非隐藏差异。
- [x] 不改变生产响应、raw Revision 行数、Time Cursor 形状或 JSON 行数上限。

## Comments

- Branch `ticket/03-time-cursor-gap`（commit `c37075e`）；已合并 `integration/duckdb-polars-storage`（`b9c4865`，merge 结果 "Already up to date"）。独立契约记录：[docs/revision-heavy-time-cursor.md](../../../docs/revision-heavy-time-cursor.md)；characterization 测试：[tests/integration/test_revision_heavy_time_cursor.py](../../../tests/integration/test_revision_heavy_time_cursor.py)。
- 复现行为：通过 `GatewayRuntime`/ASGI 真实 HTTP 请求，以两次 live ingest 产生同一 `open_ts` 的 revision 0/1。页内多个 Revisions 会全部返回；当页边界切在 Revision 组内部（例如 00:00 有 2 个 Revisions、`limit=1`）时，`next_start` 仍是已返回行的 `open_ts`，下一页与上一页完全相同，且组内剩余 Revision 与后续时间戳永远不可达；组恰好填满页（`limit=2`）时遍历正常，末页与 `limit=0` 探测按现有契约工作。该缺口由 raw Revision 行 + 单值 `open_ts` + 闭区间 `start` 共同导致，不是 DuckDB 迁移引入。
- 未改变生产响应、raw Revision 行数、Time Cursor 形状或 JSON 行数上限；测试只固定当前行为，缺陷断言以 `KNOWN CONTRACT GAP` 标注，未使用 `xfail`。
- 验证：`D:/Anaconda3/envs/nautilus/python.exe -m pytest -q tests/integration/test_revision_heavy_time_cursor.py` → 6 passed。完整离线套件：`D:/Anaconda3/envs/nautilus/python.exe -m pytest -q` → 710 passed, 5 failed, 1 deselected；5 个失败均为已知的 Typer/Click baseline（`tests/integration/test_archive_backfill.py` 4 个、`tests/unit/test_cli_flags.py` 1 个），与本次改动无关。
