# Revision-heavy 数据下的 Time Cursor 契约缺口

> 状态：**已记录，未解决**。本文件是独立可追踪的公共契约问题记录，对应 DuckDB/Polars 存储迁移 ticket 03（M0 characterization）。
> 相关契约：[ADR 0012](adr/0012-single-instrument-pagination.md)、[GLOSSARY](../GLOSSARY.md) 的 **Revision** 与 **Time Cursor**、迁移规格 `.scratch/duckdb-polars-storage/spec.md` 的「Implementation Decisions」4 与「Testing Decisions」REST。
> 复现测试：`tests/integration/test_revision_heavy_time_cursor.py`。
> **本文件不选择修复方案，也不授权在存储迁移中修复。**

## 1. 结论

单 Instrument JSON 分页使用单值 `open_ts` Time Cursor，但 `open_ts` 在数据中**不是行唯一键**：同一 `(instrument_id, interval, open_ts)` 可以存在多个 raw Revisions。当一页的边界落在同一个 `open_ts` 的 Revision 组内部时：

- `next_start` 仍然是第一条未返回行的 `open_ts`，也就是**已经返回过的那个时间戳**；
- 下一次请求把它作为闭区间 `start`，会从该时间戳的**第一个 Revision 重新返回**，产生跨页重复；
- 如果该 Revision 组的大小超过 `limit`，分页会**永远无法前进**：同一页逐字节重复，组内剩余 Revisions 和后续时间戳都不可达。

因此，在 Revision-heavy 数据上，「按 `open_ts` 升序 + 闭区间 `start` + 单值 `next_start`」无法同时满足 ADR 0012 承诺的“页与页之间既不重复也不遗漏”。这是既有 REST 契约的结构问题，不是 DuckDB/Polars 迁移引入的缺陷，也不是某个引擎的排序差异。迁移必须原样保持当前行为，把修复留给独立的契约决策。

## 2. 既有契约原文与冲突点

ADR 0012 与 GLOSSARY 对 Time Cursor 的定义是：

- 分页读取按 `open_ts` 升序；
- `next_start` 是第一条未返回 Bar 的 `open_ts`（ISO-8601 纳秒字符串）；
- 下一次请求把 `next_start` 作为**闭区间 `start`** 传回；
- 因而“页与页之间既不重复也不遗漏”；
- 它只表达时间位置，不携带行数或 offset；
- `has_more` 只统计 `end` 以内仍然存在的行；`limit=0` 返回空 `bars`，但仍报告是否存在匹配行以及首行 `open_ts`。

同时，REST 的其它契约要求：

- REST 返回**全部已存储 raw Revisions**，迁移不得偷偷改变行数或修正历史（spec「User Stories」11、14 与「Implementation Decisions」4）；
- JSON 行数上限基于 raw Revision 行数；
- Revision-heavy Time Cursor 是独立契约问题，迁移中不得通过隐式 `QUALIFY`、`DISTINCT`、复合游标或改变 REST raw Revisions 暗修（spec「Implementation Decisions」4）；
- Revision-heavy 分页只记录当前行为并链接本文件，不得用不可能满足的旧断言迫使实现暗中改契约（spec「Testing Decisions」REST）。

冲突点是“Bar”一词在分页契约中的含义。在唯一 `open_ts` 的数据上，一行就是一个时间位置；但存储层允许同一 `open_ts` 有多行 raw Revisions。分页响应按**行**返回，游标却只带**时间值**，于是游标无法区分同一时间戳组内的第 1 行与第 2 行。

## 3. 复现方式与真实响应

所有断言都通过 `GatewayRuntime` 的真实 ASGI 端口发出 HTTP 请求，接缝与 `tests/integration/test_bars_pagination.py` 相同。数据不是直接写 Store，而是由脚本化 Feed Handler 分两次 `ingest()` 产生真实修正：第一批写入 revision 0，第二批改变同一 `open_ts` 的内容，生产 merge 路径将其写成 revision 1。

复现命令：

```bash
D:/Anaconda3/envs/nautilus/python.exe -m pytest -q tests/integration/test_revision_heavy_time_cursor.py
```

### 3.1 场景 A：00:00 有两个 Revisions，00:01 有一个

存储中的 raw Revisions（顺序不属于契约）：

| open_ts | revision | close |
| --- | --- | --- |
| 00:00 | 0 | 100 |
| 00:00 | 1 | 200 |
| 00:01 | 0 | 100 |

非分页 JSON（`instrument` 但无 `limit`）返回全部 3 行 raw Revisions；分页 JSON 同样不在一页内去重。

**`limit=10`（页内包含多个 Revisions）**

```json
{
  "bars": [
    {"payload": {"open_ts": "2024-01-01T00:00:00.000000000Z", "revision": 0, "close": "100.0"}},
    {"payload": {"open_ts": "2024-01-01T00:00:00.000000000Z", "revision": 1, "close": "200.0"}},
    {"payload": {"open_ts": "2024-01-01T00:01:00.000000000Z", "revision": 0, "close": "100.0"}}
  ],
  "page": {"has_more": false, "next_start": null}
}
```

**`limit=2`（页边界恰好落在 Revision 组边界，工作正常）**

- 第 1 页返回 00:00 的 revision 0 与 revision 1，`next_start = 00:01`；
- 第 2 页以 `start=00:01` 返回 00:01 的 revision 0，`has_more=false`；
- 三行各返回一次，不重不漏。

**`limit=1`（页边界落入 Revision 组内部，契约失效）**

- 第 1 页：`bars=[(00:00, revision 0)]`，`page={"has_more": true, "next_start": "00:00"}`；
- 第 2 页（`start=第 1 页 next_start`）：与第 1 页**完全相同**，`next_start` 仍为 `00:00`；
- 第 3 页：仍然完全相同；
- `revision 1` 与 `00:01` 的 Bar 在跟随文档化游标的任何页中都不可达。

**`limit=0`**

```json
{"bars": [], "page": {"has_more": true, "next_start": "2024-01-01T00:00:00.000000000Z"}}
```

再次以 `limit=0&start=next_start` 请求会得到相同的探测结果，即 `limit=0` 自身永远不前进。切换到 `limit=2&start=00:00` 时两行 Revisions 都会返回，因为探测本身没有返回任何行，所以此处没有遗漏。

### 3.2 场景 B：00:00 有三个 Revisions，00:01 有一个

raw Revisions：00:00 的 revision 0/1/2，以及 00:01 的 revision 0。

- `limit=2`：第 1 页返回 00:00 的 revision 0/1，`next_start=00:00`；第 2 页与第 1 页相同；revision 2 与 00:01 永远不可达。
- `limit=3`：第 1 页返回 00:00 的全部三个 Revisions，`next_start=00:01`；第 2 页返回 00:01，`has_more=false`。组恰好填满页时遍历完整。

### 3.3 最后一页

只有 00:00 的 revision 0/1、没有更晚的时间戳时，`limit=10` 返回两行，`page={"has_more": false, "next_start": null}`。末页本身不产生缺口，缺口只在分页边界切开 Revision 组时出现。

### 3.4 关于同一时间戳内的行顺序

ADR 只承诺 `open_ts` 升序，没有定义相同 `open_ts` 内的 tie-break。当前实现中，相同时间戳的行顺序来自稳定排序与存储顺序（复现中为 revision 0 在前），测试按当前可观察结果固定该顺序；这不构成新的公共排序承诺。

## 4. 为什么单值 `next_start` 无法表达页内进度

设匹配结果按 `open_ts` 升序为原始行序列，页请求参数为 `(snapshot, start, end, limit)`。

当前分页实现（`src/mdgw/api/routes/bars.py::_read_earliest_bars` / `_respond_json`）等价于：

1. 取全局最早的 `limit + 1` 个 raw Revision 行（`open_ts >= start` 且 `open_ts < end`）；
2. 返回前 `limit` 行；
3. `has_more = 存在第 limit+1 行`；
4. `next_start = 第 limit+1 行的 open_ts`。

考虑时间戳 `t` 处的 Revision 组 `G = [r0, r1, ..., r(k-1)]`，并假设某页在 `G` 中间结束：该页返回了 `r0..r(j-1)`（`0 < j < k`），且第 `limit+1` 行是 `rj`。于是：

- `next_start = rj.open_ts = t`；
- 下一次请求使用闭区间 `start = t`，谓词 `open_ts >= t` 会把 `G` 从 `r0` 开始全部重新选中；
- 客户端若严格使用 `start > t` 来跳过已返回行，就会同时跳过 `rj..r(k-1)`，产生遗漏。

游标没有携带“已经返回了 `G` 中几行”的信息，所以不存在第三种选择：

- 闭区间 `t`：重复已返回的 Revisions，违反对“不重复”的承诺；
- 开区间 `t`（或任何 `t` 之后的最小时间）：跳过组内剩余 Revisions，违反对“不遗漏”的承诺；
- 时间戳之间没有可用的“位置”：同一 `open_ts` 的所有行在时间轴上完全重合。

当 `k > limit` 时，页边界必然落在组内，分页会停滞：每页都返回相同的 `limit` 行，`next_start` 永远等于同一个 `t`，`has_more` 永远为 true，形成确定性的 livelock。`has_more=true` 无法区分“还有新数据”与“无法前进”。`limit=0` 是同一问题的极端情况：它返回零行，`next_start` 指向首行时间戳，按 Time Cursor 的定义下一步仍停在同一时间戳。

因此，问题不是实现忘了去重，而是**契约把“行位置”压缩成了“时间值”**。只要分页单位是 raw Revision 行，而游标只允许一个 `open_ts`，且 `start` 是闭区间，就不可能对“一个时间戳对应多行”的序列同时保证不重和不漏。

## 5. 现有测试证明什么，以及未证明什么

### 5.1 已证明：唯一 `open_ts` 序列的遍历

`tests/integration/test_bars_pagination.py` 通过同一个真实 REST 接缝证明了：

- 多页遍历在唯一 `open_ts` 上不重不漏，并保持全局 `open_ts` 升序（`test_multi_page_walk_has_no_duplicates_or_gaps_across_a_time_gap`）；
- 存储的 append 顺序不影响分页顺序（`test_pages_are_globally_ordered_when_storage_order_is_not`）；
- `end` 为开区间，边界外行不影响 `has_more` / `next_start`；
- `limit=0` 的探测语义；
- 单页耗尽时 `next_start=null`；
- Arrow、CSV、export 与非分页 JSON 的响应形状不因分页改变；
- 分页 JSON 保持 Decimal 字符串与纳秒时间。

`tests/integration/test_live_dedupe.py` 与 `tests/integration/test_hwm_backfill.py` 证明同一 `open_ts` 会因修正写入 revision 0/1，但它们从不请求分页 REST，因此没有覆盖 Time Cursor 与多 Revision 的组合。

### 5.2 未证明：Revision-heavy 数据下的遍历

现有契约测试没有覆盖（在 ticket 03 之前也没有任何测试覆盖）：

- 一页内包含同一 `open_ts` 的多个 Revisions 时，响应仍返回全部 raw Revisions；
- 页边界切开 Revision 组时，`next_start` 指向已返回的时间戳，下一页重复；
- Revision 组大于剩余页容量时，跟随 `next_start` 会永久停滞，组内剩余行和后续时间戳不可达；
- 上述停滞是确定性的，而不是偶发的排序抖动；
- `limit=0` 在 Revision-heavy 首行上只探测、不前进；
- 末页包含同一时间戳的多个 Revisions 时正常耗尽；
- 组恰好填满页时遍历工作正常（契约何时“碰巧”成立）。

`tests/integration/test_revision_heavy_time_cursor.py` 固定了这些当前行为（包括已知重复与不可达），使后续 DuckDB 迁移不能静默改变它们。测试中标记为 `KNOWN CONTRACT GAP` 的断言是在记录缺陷，不是认可缺陷；没有使用 `xfail` 隐藏当前可通过的行为。

## 6. 可能改变契约的方向（仅分析，不选择、不实现）

以下任一方案都需要独立的契约决策、ADR/GLOSSARY/spec 更新与客户端兼容评估。本文件不推荐其中任何一种，迁移 ticket 也不得顺手实施。

### (a) 隐式最新 Revision 去重

分页读取时按 `(instrument_id, interval, open_ts)` 只保留最大 Revision（或最新 `ingest_ts`）。

- **保持**：`next_start` 仍是单值 `open_ts`；`limit=0` 形状不变；一旦每个时间戳只剩一行，现有遍历逻辑恢复不重不漏。
- **改变**：分页 REST 不再返回全部 raw Revisions，直接违反 spec 的「REST 返回全部已存储 Revisions」与 JSON 上限基于 raw 行数的约束；分页 JSON 与非分页 JSON/Arrow/CSV/export 会对同一查询返回不同行集，除非所有读取表面一起去重，那将改变 Data Contract 的可观察内容；必须定义并列时的 tie-break（最大 revision 还是最新 ingest_ts）；REST 不再能用于审计历史修正；ADR 0012 中“Bar”的含义和 GLOSSARY 的 Revision 读取语义需要重写；迁移的差分测试必须把行数差异标为已批准的契约变化。
- **风险**：这是 spec 明令禁止的“暗修”方式。

### (b) 复合游标

把 `next_start` 扩展为 `(open_ts, tie_break)`，例如 `(open_ts, revision)` 或 `(open_ts, revision, ingest_ts)`。

- **保持**：raw Revisions 不去重；分页单位仍是行。
- **改变**：响应 `page` 形状不再是单值；请求侧需要新的游标参数（或把复合值编码进现有 `start`，但 `start` 当前是 ISO 时间，会破坏现有客户端）；必须定义同一 `open_ts` 内的全序、该全序的稳定性，以及 `limit=0` 时游标的形状；`start`/`end` 的闭/开区间语义需要按分量重新定义；ADR 0012 明确说“不提供复合游标”，GLOSSARY 的 Time Cursor 定义、OpenAPI 文档、客户端解析和测试面都要同步修改；旧客户端把 `next_start` 原样传回 `start` 会失败。
- **注意**：`revision` 在现有数据中是行内不可变值，但同一 `(open_ts, revision)` 理论上仍可能有多行（不同 `ingest_ts`），所以 tie-break 必须选择真正唯一的行标识，否则问题只是被推迟。

### (c) 不透明游标

把 `next_start` 替换为不透明 token（keyset 位置、行标识或偏移的编码）。

- **保持**：raw Revisions；可分页精度达到行级；未来可扩展 tie-break 或更多维度而不暴露形状。
- **改变**：响应形状与请求参数都变化，客户端不能再把游标当 ISO 时间或自行构造；必须定义 token 编码/版本、非法或过期 token 的稳定错误、是否允许跨 Manifest 代际或 Compaction 后继续使用（当前每个分页请求使用新 Snapshot，token 不能假设数据文件不变）、`start`/`end` 与 token 同时出现时的优先级；Time Cursor 这一公共概念被替换，ADR 0012 与 GLOSSARY 需要重写；spec 的 Out of Scope 明确排除了本次迁移引入不透明游标。
- **风险**：与现有“只提供时间游标、保持客户端兼容”的目标冲突最大。

### (d) 扩大页边界

保持单值时间游标，但让一页包含边界时间戳的整个 Revision 组（或至少把 `next_start` 推进到严格大于所有已返回行的 `open_ts`）。

- **保持**：`next_start` 仍是 ISO 时间，客户端传回闭区间 `start` 的方式不变；raw Revisions 不去重。
- **改变**：`limit` 从“最多返回行数”变成“通常返回行数，但边界组会超额”，单页可能返回超过 `limit` 的行，极端情况下同一时间戳的 Revisions 数量决定页大小，无上界；现有测试 `len(page["bars"]) <= limit`、JSON 行数上限的 `effective_total = min(total, limit)` 计算、以及 `limit=0` 的探测形状都需要重新定义；如果只是把 `next_start` 推到下一个时间戳但仍只返回 `limit` 行，会跳过组内剩余行，不构成修复。ADR 0012 的页大小承诺与文档需要修改，但客户端兼容面相对最小。

## 7. 对存储迁移的约束

在契约决策完成前，DuckDB 迁移必须满足：

- 分页 JSON 继续返回 raw Revisions，不做隐式去重；
- `next_start`、`has_more`、`limit=0` 与 JSON 行数上限的形状和行为不变；
- Revision-heavy 数据的当前行为（本文件第 3 节）由测试固定，迁移验收引用本文件而不是隐藏差异；
- 不得通过 `QUALIFY`、`DISTINCT`、复合/不透明游标或改变 REST raw Revisions 来“顺手修复”；
- 若未来独立决策选择上述任一方案，应更新 ADR 0012、GLOSSARY、spec 与客户端文档，并把本文件标记为已解决。

## 8. 复现测试清单

`tests/integration/test_revision_heavy_time_cursor.py`：

| 测试 | 固定的行为 |
| --- | --- |
| `test_a_page_returns_every_raw_revision_sharing_an_open_ts` | 页内同一 `open_ts` 的全部 Revisions 都返回；非分页 JSON 与分页都不去重 |
| `test_page_boundary_inside_a_revision_group_repeats_rows_and_hides_the_rest` | `limit=1` 时下一页与上一页完全相同；组内剩余行与后续时间戳不可达 |
| `test_a_revision_group_larger_than_the_page_never_reaches_the_remainder` | 组大于页容量时确定性 livelock |
| `test_walk_is_complete_when_the_page_ends_on_a_revision_group_boundary` | 组恰好填满页时遍历不重不漏 |
| `test_last_page_with_same_timestamp_revisions_reports_exhaustion` | 末页多 Revisions 正常耗尽 |
| `test_limit_zero_probes_the_first_timestamp_and_never_advances` | `limit=0` 只探测不前进；切换到实际 limit 后从组头完整返回 |

结果：`6 passed`（离线套件，见 ticket 03 的 Comments）。

## 9. 留给契约决策的问题

1. 分页单位究竟是 raw Revision 行，还是每个 `(instrument, interval, open_ts)` 的最新 Bar？
2. 若保留 raw 行，同一 `open_ts` 内的稳定全序由什么字段定义，是否对所有历史数据唯一？
3. 若游标携带行标识，它在 Manifest 切换、Compaction/GC 与 Retention 之后是否仍然有效？
4. `limit` 是最大行数还是最大时间戳数？JSON ceiling 以哪一种计数？
5. 非分页 JSON、Arrow、CSV 与 export 是否必须与分页返回相同的行集？
6. 变更是否需要新的查询参数/响应版本，旧客户端如何失败与迁移？
