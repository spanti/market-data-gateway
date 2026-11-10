# Revision-safe 历史分页使用不透明游标（Data Contract v2）

Status: accepted（2026-11-10）

单 Instrument 的 JSON Bar 历史新增 `/api/v2/bars`，以**行级 keyset 游标**分页并返回每一条 raw Revision；`/api/v1/bars` 的 Time Cursor、Arrow、CSV、export、非分页 JSON 与 Replay 语义全部保持不变。v1 在 Revision-heavy 数据上的页边界缺口（同一 `open_ts` 多个 Revision 时跨页重复、不可达；见 [`docs/revision-heavy-time-cursor.md`](../revision-heavy-time-cursor.md)）**不在 v1 上修复**，而是由 v2 提供正确的新契约；ADR-0012 仍是 v1 的契约，不被本 ADR supersede。

## 决策 A：新端点 + 不透明 keyset 游标

`GET /api/v2/bars` 是 JSON-only、单 Instrument、始终分页的历史读取面。查询参数：

- `interval`（必填，Data Contract Interval）、`instrument`（必填，InstrumentId）、`limit`（必填整数 1..100000）。
- 首页可带 `start`（闭区间 `open_ts`）与 `end`（开区间 `open_ts`）。
- 后续页只带上一页返回的不透明 `cursor`；`start` 与 `cursor` 同时出现是 400。`instrument`/`interval` 必须与 cursor 一致，显式给出的 `end` 必须等于 cursor 记录的值，否则 400 并在 `details.field` 指向出错参数。
- `format` 不是 v2 的概念；显式提供时只能是 `json`，其他值返回 400 `invalid_format`。
- `limit=0` 不是 v2 探针：v2 要求真实页大小，返回 400 `invalid_parameter`。100000 是单响应 raw 行上限。

响应固定为：

```json
{"bars": ["<Message Envelope>", "..."],
 "page": {"has_more": true, "next_cursor": "<opaque>"}}
```

`has_more=false` 时 `next_cursor=null`。`bars` 返回**每一条 raw Revision**，不做 LATEST/最大 Revision 去重，JSON ceiling 以 raw 行计数。

## 全序与行指纹

v2 的总序（也是游标键）是：

```
(open_ts ASC, revision ASC, ingest_ts ASC, row_fingerprint ASC)
```

`row_fingerprint` 是每行物理字段的规范化 SHA-256，在 DuckDB SQL 中计算，因此不依赖 Manifest 文件顺序、文件内行号或 Python 内存顺序：

1. 按 `PHYSICAL_SCHEMA.names` 的 schema 顺序取全部列（即 `BAR_SCHEMA` 去掉 Hive 分区列 `interval`）。
2. 每列生成 `coalesce(CAST(col AS VARCHAR), '\x00')`；NULL 与真实文本因此可区分。
3. 以 `\x1f` 连接字段，并在最前面加上版本标记 `mdgw-row-fingerprint-v1`。
4. 对 UTF-8 字节做 SHA-256，得到 64 位小写十六进制字符串。

DuckDB 连接固定 `timezone=UTC`，因此时间戳列的文本 cast 与机器时区无关。SQL 以内部列 `_row_fingerprint` 参与 `WHERE`/`ORDER BY`，在渲染 JSON 前被剥离，不进入 Data Contract 行。改变列集合、列顺序、连接符、NULL 表示、标记、cast 规则或哈希函数都属于**算法版本变化**：必须同时提升 cursor 版本，使旧 cursor 以 400 `invalid_parameter` / `details.reason="invalid_cursor"` 失败，而不是在新物理行集上错位。

同一组物理列的完全重复行（所有列都相同）拥有相同指纹，因此在该组内不可区分；这是 raw 行去重之外的边界，raw Revision 之间总有 `revision` 区分。

## 游标编码与签名

游标载荷是紧凑、键排序的 JSON：

```json
{"v":1,"i":"binance:BTC/USDT","n":"1m","e":null,"o":1704067200000000000,"r":3,"g":1704067260123456789,"f":"<64 hex>"}
```

- `e` 是请求窗口的排他上界（ns，或 `null`）；`o`/`r`/`g`/`f` 是上一页最后一条返回行的总序键。
- token = `base64url(payload).base64url(HMAC-SHA256(payload))`，均无 padding；签名以常量时间 `hmac.compare_digest` 校验。
- HMAC key 来自 `[server].cursor_key_env` 命名的环境变量：命名但缺失/为空时进程**启动失败**（fail closed）；未命名时生成 32 字节随机进程 key 并记录一次警告，此时游标不跨重启、不跨实例。key 本身不进配置、不写日志、不进入 token。
- 游标是**位置令牌，不是授权凭证**；不得放入秘密，也不能替代服务端鉴权。客户端必须把它当作不透明字符串，不能解析、修改或当作 `start` 时间复用。

## 快照、Compaction 与游标有效期

每个 v2 请求像 v1 一样独立打开 Manifest Snapshot 与 Dataset Read Lease；游标不 pin 任何 Snapshot。下一页在**当时**的 Snapshot 上按同一全序继续，因此：

- 游标在 Manifest 切换、Compaction 与 GC 后仍然有效；被删掉的旧 Data File 与游标无关。
- 游标之后插入/修正的新行会在后续页按总序出现；游标之前的行不会重复。
- 游标不承诺“某一时刻的一致快照”，它承诺“按当前可见 raw 行的总序继续，不重不漏”。

## 被拒绝的方案

- **B：复合游标（如 `(open_ts, revision)`）。** 仍会暴露内部排序键并需要客户端理解复合值；`(open_ts, revision)` 本身也不唯一（不同 `ingest_ts`），且未来扩展指纹/编码会再次破坏客户端。public 响应形状与请求参数都需要重新教学。
- **C：复用 v1 端点并替换 `next_start`。** 会直接破坏现有 v1 客户端（把游标当 ISO 时间传回 `start`），违反“v1 不变”的兼容承诺。
- **D：扩大页边界以包含完整 Revision 组。** 页大小失去上界（同一 `open_ts` 的 Revision 数无界），`limit`、JSON ceiling 与 `limit=0` 语义都要重定义，且仍无法表达组内进度。
- **E：v2 隐式只返回最大 Revision。** 改变 REST 的 raw Revision 行集，破坏审计/历史读取，并使分页 JSON 与非分页 JSON/Arrow/CSV/export 对同一查询返回不同行集。v2 保持 raw Revision。
- **Offset 分页。** offset 依赖稳定总数，写入与 Compaction 会让结果漂移；keyset 游标没有该问题。
- **服务端分页状态。** 需要过期、内存与多实例一致性；v2 的钥匙在 token 里，服务端无状态。

## 后果

- 新增公开面 `/api/v2/bars`、`docs/data-contract.md` 的 v2 章节、OpenAPI 组件与 GLOSSARY 的 Revision-safe Cursor；v1 文档保留并链接本 ADR 与既有缺口记录。
- v2 的排序使用 DuckDB 全序 + `LIMIT limit+1`，仍受 Store 的 DuckDB 内存/临时目录预算约束；超预算按既有 `internal_error` 失败，绝不返回短页。
- 多 Instrument 分页仍不提供：v2 与 v1 一样限定单 Instrument，需要多 Instrument 的消费方逐 Instrument 查询或使用 Arrow/CSV/export。
- 行指纹让 cursor 在 Compaction 后保持稳定，但精确重复的 raw 行在该指纹下不可区分；这不是 Revision 去重，raw Revision 仍逐条返回。
