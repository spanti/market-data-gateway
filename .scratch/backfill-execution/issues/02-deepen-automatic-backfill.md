# 02: 让自动 HWM Backfill 穿过 deep execution module

**What to build:** 建立深的 Backfill execution module，并让自动 HWM Backfill 从公共入口到 Dataset/HWM 的完整路径率先穿过它。自动行为保持原样，尚未迁移的 Backfill Job 继续作为跨入口 differential 对照。

**Blocked by:** 01《用公共入口固定 Backfill 行为基线》

**Status:** ready-for-agent

- [ ] 新 execution module 以一个 InstrumentId 和一个实际 fetch range 为粒度，拥有 fetch、closed-window、Normalization、Revision/parent merge、持久化和执行结果收集的共同顺序。
- [ ] module 通过显式注入使用 Feed Handler、Aggregator、clock、Normalization、merge 和持久化能力，不接受整个 CollectorCore 或 AdminContext，也不依赖 Admin Job/Gap stores。
- [ ] 自动执行 adapter 保留按 HWM/backfill_hours/now 选窗、inclusive HWM 重抓、history unsupported 与 backfill limit gap，以及逐 Instrument 全窗口 materialization。
- [ ] 自动 adapter 继续使用 Core 持有的 Aggregator 和同一个 now 作为批 ingest 时间，不引入 Job 的日 chunk、500 行批或逐批墙钟。
- [ ] 自动 Normalization rejection 继续整批抛出 BatchNormalizationError，当前批不写、不写 Quarantine，其他 Instrument 已完成的写入不回滚。
- [ ] 自动持久化 adapter 继续直接写 Dataset 后推进 HWM；无写入不推进，不新增 Core bars_written、逐 Bar health 或 expected-key 副作用。
- [ ] run_backfill 的现有关键字 interface、BackfillReport 字段和计数含义保持不变，GatewayRuntime 与 Collector 的启动/显式 Backfill 行为不变。
- [ ] 现有共享 merge helper 保持独立且语义不变，live ingest、archive import 和 buffered sink existing-reader 行为不受影响。
- [ ] 自动入口的旧重复 history orchestration 被移除，不增加新旧实现开关，也不要求尚未迁移的 Job 调用新 module。
- [ ] 自动 Backfill、HWM、Revision、freshness、Normalization 聚焦测试通过，01 的 differential 在“自动新实现 + Job 旧实现”组合下通过。
- [ ] live ingest 与 archive 相关测试保持通过，完整结果如实记录，不以跳过测试替代兼容验证。
