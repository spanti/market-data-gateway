# 14: 交付有界 realtime Replay 与 seek 查询代际

**What to build:** 让 realtime Replay 在 asap 的有界读取基础设施上支持 pause、resume、set_speed、seek 和 stop。seek 在同一 Manifest Snapshot 上启动新查询代际，旧代际结果不能发布，I/O 时间仍不消耗 Replay Clock。

**Blocked by:** 13 交付有界 asap Replay。

**Status:** ready-for-agent

- [ ] realtime 不再保存完整 Bars 和 close-time 数组，按有界批次等待 Replay Clock 并发布。
- [ ] pause/resume/set_speed/stop 的合法状态、幂等和竞态行为保持，Speed 变化继续影响后续等待。
- [ ] seek 只在既有合法暂停状态生效，从同一 Snapshot 查询第一条 `close_ts >= ts` 的 Bar，不纳入新 Data Files。
- [ ] 每次初始读取和 seek 都有查询代际，旧代际即使稍后返回结果，也不能发布或修改当前 cursor、Clock 和会话状态。
- [ ] seek 和 stop 使用已验证的取消机制释放旧 Reader、连接和执行资源，随后才能安全复用或替换。
- [ ] Replay Clock 在首个可读批次准备好后启动；初始读取或 seek 的 I/O 时间不消耗虚拟时间。
- [ ] 初始暂停或 seek 读取期间只允许有界预取，pause 不会导致整个范围提前读入内存。
- [ ] 覆盖空结果、seek 到首条之前、seek 到两条之间、seek 越过末尾、连续 seek、读取期间 pause/stop 和 set_speed。
- [ ] 只有当前代际耗尽且预取记录全部发布后发送 exhausted，seek 越过末尾和 stop 的 EOS 行为与契约一致。
- [ ] asap 与尚未流式化的 step 不发生行为回退，realtime 的内存在不同结果规模下保持有界。
