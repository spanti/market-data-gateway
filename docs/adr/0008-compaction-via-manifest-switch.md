# 手动 compaction 通过清单切换替代原地重写

Data File 的内容仍然一经写入不可变；`mdgw compact` 把同一分区内的小文件合并成 64–256MB 的新 Data File，更新 `_manifest.json` 把旧文件标记为 inactive，然后删除旧文件。读取方必须先读 Manifest 再打开文件，不能直接 glob 目录。理由：即使按小时 flush，低成交量标的仍会在数月内积累大量小文件，而 Parquet 不支持追加；"新文件 + 原子 Manifest 切换"同时保住了不可变性与文件大小。代价是读取路径多了一层 Manifest 解析，且 compaction 必须与 GC、活跃 Replay Session 协调（见 ADR-0005 与 §5.4）。
