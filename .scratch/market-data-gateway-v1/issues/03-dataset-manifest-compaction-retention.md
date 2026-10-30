# 03: Dataset 布局、Manifest、Compaction、Retention/GC

**What to build:** 运维人员可以信任历史数据：分区稳定、文件不可变、读取先看 Manifest；可以手动压缩小文件、按保留期删除整月分区，并且 `/status` 能看到每个 Dataset 的最后写入时间。

**Blocked by:** 02

**Status:** ready-for-agent

- [ ] Hive key=value 分区；symbol 百分号编码；Data File 命名与不可变性。
- [ ] `_dataset.json` 与 `_manifest.json`；读取路径先读 Manifest。
- [ ] `mdgw compact --older-than` 合并小文件并原子切换 Manifest。
- [ ] `mdgw gc --dry-run/--apply` 只删除整月分区，跳过当前月与活跃 Replay Session。
- [ ] `/api/v1/status` 暴露 last_flushed_ts、Data File 数量与中位大小。
- [ ] 测试覆盖路径编码、Manifest 切换、Compaction、GC。
