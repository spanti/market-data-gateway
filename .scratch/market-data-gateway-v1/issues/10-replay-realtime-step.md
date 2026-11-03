# 10: Replay Protocol — realtime/step + 控制

**What to build:** 用户可以暂停、恢复、定位、调速、逐事件步进，多个会话互不影响。

**Blocked by:** 09

**Status:** resolved

- [x] Replay Clock 与 Speed。
- [x] `pause` / `resume` / `seek` / `set_speed` / `step`。
- [x] Step Credit：`step(n)` 最多推送 n 条。
- [x] 多 Replay Session 隔离。
- [x] 测试覆盖状态机与非法操作（例如未 pause 就 seek）。

## Comments

- Resolved in `integration/market-data-gateway-v1` after the v1 delivery code review and follow-up fixes.
