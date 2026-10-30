# 13: Archive Backfill — data.binance.vision

**What to build:** 用户可以把数月/数年的 Binance K 线归档导入 Dataset，并立即通过 API/SDK 查询；2025 年前后的 ms/µs 时间戳被正确处理。

**Blocked by:** 03, 06

**Status:** ready-for-agent

- [ ] `mdgw backfill --archive` 读取日/月 K 线归档。
- [ ] 时间戳 ms/µs 判定。
- [ ] 写入遵循 Data Contract、Manifest、HWM。
- [ ] 与 Compaction/Retention 协同。
- [ ] 测试使用本地 fixture，不依赖网络。
