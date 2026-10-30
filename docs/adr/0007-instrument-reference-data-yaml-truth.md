# Instrument Reference Data 以 YAML 为真相源，Parquet 仅作 Instrument Snapshot

Instrument 元数据（资产类别、最小价位、合约乘数、时区、交易时段、退市时间）的真相源是版本控制的 `config/instruments/*.yaml`；网关定期将其导出为 `instruments/` 下的 Parquet Instrument Snapshot，回放进程与外部消费者只读该快照。理由：元数据是"人编辑、机器消费"，配置可以 review 与 diff；反向（机器写、人读）会让纠错极慢。被历史数据引用过的 Instrument 永不删除，只标记 `delisted_ts`。
