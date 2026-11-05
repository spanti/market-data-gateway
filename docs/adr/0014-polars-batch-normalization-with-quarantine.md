# Polars 批量 Normalization 与独立 Quarantine

Normalization 与 Validation 仍发生在 Collector 已有的 Raw Bars 批量边界上，但规则表达式改由 Polars 执行：一次 `ingest()` 排空 Feed Handler 后得到一个 RawBar 批次，`normalize_batch` 为整批计算 Instrument、Interval 与 closed-Bar 规则，不为每条实时消息构造单行 DataFrame。Live ingest、Backfill 与 archive import 共用这一个规则来源，原先分散在标量 Python 中的重复实现已删除。Collector 实时批次、CCXT 行解析与 archive 文件的无效输入不再只留在日志里，而是进入独立、版本化的 Quarantine Dataset；Quarantine 不属于 Bars Dataset，REST、Replay 与 Bars Store 都不会读取它。

## Collector 批量边界

Polars 只看到规则实际需要的窄 Arrow 列：`instrument_id`、`interval` 和 `open_ns`。Decimal 字段在任何 Arrow/Polars 转换之前用 Python `Decimal` 精确校验并拒绝 float；Arrow 与 Polars 都会对未带类型的超 scale decimal 静默重缩放，因此不能把 decimal 文本或参数交给它们做规范化。校验通过的值保持 `decimal128(38,18)`，时间保持 UTC 纳秒，`trade_count`、`revision` 等整数字段按契约类型逐行检查。Polars 计算出的批次标记再与逐行的类型问题合并，生成规范 Bar 或带稳定 Reason Code 的 rejection；输出不经过 `to_dicts()` 之类的逐行回转，批量路径才是生产路径。

Feed Handler 继续负责 Venue 协议、原始报文、闭合识别、重连和来源分页；archive 模块继续负责 checksum、ZIP/CSV 结构与来源时间单位；Bar Aggregation、HWM、Backfill 的 Revision merge 和队列恢复都没有改写成 Polars。Polars 是批量表格规则引擎，不是协议状态机或存储引擎。

## 整批与整文件失败原子性

实时批次中只要有一行 Normalization 或 Instrument Validation rejection，Collector 先为该批所有被拒行写 Quarantine，再抛出批次错误：Aggregation 状态、HWM 和 freshness 都不推进，已接受的行也不会部分落盘；修复参考数据后同一批可以重试。Archive 保持整文件原子性：任何一行 Normalization 失败都会使整个文件失败，不写入未经批准的部分归档，同时为整个文件记录一条带来源定位的 Quarantine Record。CCXT 单行解析失败是唯一允许逐行跳过的来源：该行先成功写入 Quarantine，之后才跳过并继续处理来源；如果 Quarantine 写入失败，handler 进入 fail-closed 终止状态，不重连越过该行，也不确认消费。Backfill 复用同一批量规则，但它的 rejection 仍以 `BatchNormalizationError` 直接让整批失败且不写 Quarantine；它不伪造审计记录。队列溢出仍走 HWM 缺口与 Backfill，不会被伪装成 Validation Quarantine。

## 稳定的 Reason Code 与 Source Stage

Reason Code 与 Source Stage 是面向运维的稳定、低基数契约，不是异常文本的分类。Reason Code 固定为 `unclosed_bar`、`float_value`、`invalid_interval`、`decimal_scale_exceeded`、`invalid_instrument`、`unknown_instrument`、`malformed_row`、`archive_checksum_mismatch`、`archive_format_error`、`archive_time_unit_unsupported`、`archive_field_error`；Source Stage 固定为 `live_normalize`、`live_instrument_validate`、`ccxt_live_row`、`ccxt_history_row`、`archive_file`。异常类型和消息只写入经过截断的 `details` 诊断字段，不能作为程序分支或指标标签；Collector 指标只按 `source_stage` 与 `reason_code` 聚合。

Quarantine Record 的字段为 `quarantine_schema_version`、`recorded_ts`、`source_stage`、`reason_code`、`rule_version`、`feed`、`instrument_id`、`interval`、`open_ts`、`source_location`、`details`、`raw_record`。`source_location` 是稳定可读的来源定位（实时批次行、CCXT symbol/stream/row 或 archive 文件名），`details` 与 `raw_record` 是以 JSON 文本保存的结构化诊断和可获得的原始记录表示，写入前按固定深度和长度截断。Quarantine 拥有独立的 `schema_version`、descriptor 与 Manifest，并按 `year=YYYY/month=MM` 分区；它从 schema 到存储都不属于 Bar Data Contract。

## Rule Version

`NORMALIZATION_RULE_VERSION` 当前为 `polars-1`，是产生数据或拒绝输入时实际运行的规则集合标识。新 Data File 的 Manifest 条目记录 `rule_versions` 集合，Quarantine Record 记录单值 `rule_version`；Compaction 只做来源集合的并集，不重新 Normalization。缺失或空集合表示 legacy/unknown，读者必须把它当作空集，永不伪造历史版本，也不会因此拒绝读取。

Rule Version 只是审计元数据，Data Contract Version 仍是 Schema 兼容性的唯一权威。只改变实现细节、字段值和 Bar Schema 完全不变的规则变化可以只升级 Rule Version；任何会改变公共 Bar 语义的变化必须升级 Data Contract Version，而不是指望 Rule Version 阻止旧数据被读取。

## Fail-closed 的 Quarantine 写入

Quarantine 默认位于 `<storage.root>/quarantine`，可通过 `[storage.quarantine].root` 覆盖；配置校验拒绝把它放进 Bars Dataset。写入流程先为每个 UTC 月份生成 Data File（临时文件 + 原子替换），再追加 `_quarantine_manifest.json`；任何一步失败都会删除本次新建的 Data File、累加 `write_failures` 指标并抛出 `QuarantineWriteError`。调用方不得在该错误后确认消费、跳过、发布或落盘对应输入；单行来源也不能以“先跳过、稍后补审计”的方式继续。审计系统故障因此表现为可用性故障，而不是静默数据丢失。

Quarantine 是只追加的审计集合，`QuarantineStore` 提供读取与统计供运维使用，但正常 Bars 查询、REST、export、Compaction 与 Replay 不注册、不扫描、不导出它。Instrument Snapshot、HWM 和 Backfill 也都不会把 Quarantine 当作缺口或修正来源。

## Retention 与 GC

Quarantine 默认保留 30 天，`[storage.quarantine].retention_days = 0` 表示永久保留。清理永远是显式运维命令，不新增后台任务：`mdgw quarantine-gc` 默认 dry-run，只清理已经结束且整月都早于截止时间的 UTC 月份，当前 UTC 月永远跳过；`--apply` 才切换 Quarantine Manifest 并删除旧 Data File，Manifest 切换先于 unlink，删除失败保留 inactive 记录并报告。`--retention-days` 可以临时覆盖配置。Collector 的 `/metrics` 暴露 `mdgw_collector_quarantine_records_total{source_stage,reason_code}`、`mdgw_collector_quarantine_write_failures_total`、活跃文件数/字节数与保留天数，容量增长因此可被观察，但不会自动触发删除。

## 被拒绝的方案

**继续使用逐行标量规则。** 同一组 closed-Bar、Decimal、UTC 和 Instrument 规则在实时、Backfill、archive 中重复实现，任何修正都容易只改一处；逐行 Python 校验和 `Bar` 构造也明显慢于一次批次表达式。保留标量路径还会让 Reason Code 与 Source Stage 继续散落，无法形成统一审计。

**为每条实时消息构造单行 DataFrame。** Polars 的批次开销会落在每根 Bar 上，实时吞吐由消息数而不是批次大小决定，也违背 Collector 已有的批次边界。批量规则必须在一次 `ingest()` 的 RawBar 集合上执行。

**只记录日志，不写 Quarantine。** 日志不是可查询、可计数的持久审计，进程重启即丢失；没有 Reason Code、Rule Version 和来源定位，也无法在修复后重放输入。纯日志策略还会让“拒绝”与“丢弃”在行为上无法区分。

**Quarantine 写入失败时回退到继续处理。** 这会把审计故障变成静默数据丢失，并让不同部署对同一输入产生不同的消费结果。正确行为是 fail-closed：对应输入不得被确认、跳过或发布，直到 Quarantine 恢复可用。

## 代价与后果

Polars（`polars==2.1.0`，由类型 Spike 验证）成为正式运行时依赖；缺少它时 Normalization 模块无法导入，Collector、Backfill 与 archive 的相应路径直接失败，没有标量后备。Decimal 的精确校验仍保留在 Python 侧，因为 Arrow/Polars 的未类型化输入会静默重缩放；这意味着批量规则并非全部由 Polars 完成，但赢在规则集中与批次原子性，而不是把所有类型系统交给一个引擎。实时批次是整批拒绝而不是逐行部分接受，坏行会连同同批好行一起延迟到修复后重试，这是既有可用性语义，Quarantine 只是把拒绝变成可审计。Quarantine 会持续占用磁盘，默认 30 天保留需要运维显式执行 GC；fail-closed 写入在审计存储故障时会停止消费，这是用可用性换数据完整性。旧的逐行标量实现已删除，回退只能通过回退软件版本完成。
