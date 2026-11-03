# 06: High-Water Mark + Backfill

**What to build:** 进程重启或断线后，Bar 流从中断处继续，缺口被补上；无法补的缺口显式报 gap_unrecoverable，静默停更会触发 stale。

**Blocked by:** 04, 05

**Status:** resolved

- [x] SQLite HWM 按 `(instrument_id, interval)` 持久化。
- [x] 启动/重连从 HWM 补采；无 HWM 时回看 backfill_hours。
- [x] 补采使用 FeedHandler 历史接口；缺口超限记 gap_unrecoverable。
- [x] stale 检测按 `(feed, instrument, interval)`。
- [x] 测试覆盖重启、断线、缺口与源修正。

## Comments

- Resolved in `integration/market-data-gateway-v1` after the v1 delivery code review and follow-up fixes.
