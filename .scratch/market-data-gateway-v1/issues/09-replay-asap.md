# 09: Replay Protocol — asap

**What to build:** 用户可以从历史 Data File 创建一个 asap 回放会话，按 close_ts 有序收到 Bar，结束时收到 End of Session；同一会话两次运行结果一致。

**Blocked by:** 03, 04

**Status:** resolved

- [x] replay Service Role 读取 Parquet（经 Manifest）。
- [x] `create_session` / `stop` / End of Session。
- [x] 多标的按 close_ts、instrument_id、seq 定序。
- [x] asap 模式不节流。
- [x] 确定性测试：同一 Session 两次运行事件序列逐字节一致。

## Comments

- Resolved in `integration/market-data-gateway-v1` after the v1 delivery code review and follow-up fixes.
