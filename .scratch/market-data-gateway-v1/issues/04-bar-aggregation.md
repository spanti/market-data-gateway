# 04: Bar Aggregation — 1m → 5m/15m/1h/1d

**What to build:** 用户查询任意受支持 interval 时，得到由同一份已收盘 1m Bar 聚合出的、可复算的 Bar；未完成周期不会出现。

**Blocked by:** 02

**Status:** ready-for-agent

- [ ] 1m→5m/15m/1h/1d 聚合规则正确：open=首根、high/low=极值、close=末根、volume/trade_count=求和。
- [ ] 只发布整段 1m 已到齐且已收盘的父周期。
- [ ] 1m 为 external、高周期为 internal 的 aggregation_source 正确。
- [ ] 1m revision 增加时父周期同步 revision。
- [ ] API 可按 interval 查询；测试覆盖 UTC 日界、缺口与修正。
