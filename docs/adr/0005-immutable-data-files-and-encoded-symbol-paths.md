# Parquet 使用不可变 Data File 与百分号编码的标的目录

Dataset 路径为 `data/<data_type>/asset_class=<...>/venue=<...>/symbol=<file_symbol>/year=YYYY/month=MM/`，Bar 额外插入 `interval=`；`file_symbol` 是 URL 百分号编码后的 symbol（`BTC%2FUSDT`），真实 symbol 只存在于行内与 Instrument Reference Data。Data File 命名 `data-<start_ns>-<end_ns>-<uuid>.parquet`，一经写入永不修改。补采与源修正只会新增文件，读者按 `(instrument_id, interval, open_ts, revision)` 取最新。按 Data Type 物理隔离、拒绝重写分区，是为了让 Parquet 目录本身成为可信的真相源；symbol 中的 `/` 是路径稳定性的经典事故来源，必须编码。
