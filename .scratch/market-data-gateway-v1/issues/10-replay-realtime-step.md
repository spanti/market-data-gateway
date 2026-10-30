# 10: Replay Protocol — realtime/step + 控制

**What to build:** 用户可以暂停、恢复、定位、调速、逐事件步进，多个会话互不影响。

**Blocked by:** 09

**Status:** ready-for-agent

- [ ] Replay Clock 与 Speed。
- [ ] `pause` / `resume` / `seek` / `set_speed` / `step`。
- [ ] Step Credit：`step(n)` 最多推送 n 条。
- [ ] 多 Replay Session 隔离。
- [ ] 测试覆盖状态机与非法操作（例如未 pause 就 seek）。
