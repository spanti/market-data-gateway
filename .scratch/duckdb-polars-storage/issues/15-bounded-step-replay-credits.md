# 15: 交付有界 step Replay 与 Step Credit 竞态保证

**What to build:** 让 step Replay 使用与其他模式相同的有界查询和 seek 代际机制，同时精确保持 Step Credit 的消费、丢弃和读取期间授予语义。客户端能逐事件控制大范围回放而不触发全量物化。

**Blocked by:** 14 交付有界 realtime Replay 与 seek 查询代际。

**Status:** ready-for-agent

- [ ] step Session 启动时保持 paused，只有可用 Step Credit 才允许逐条发布，1 credit 严格对应 1 条事件。
- [ ] step 不再保存完整 Bars 和 close-time 数组，跨批次消费不会重发、漏发或提前消耗 Credits。
- [ ] pause 继续是 no-op，resume/set_speed 的合法性不变，stop 在有无 Credits 时都能终止并释放资源。
- [ ] seek 只在新代际建立时丢弃它观察到的既有 Credits，seek 之后授予的 Credits 即使读取尚未完成也必须保留。
- [ ] 旧查询代际不能消费新 Credits、发布 Bars 或修改当前 cursor；连续 seek 和 step/stop 竞态有确定性结果。
- [ ] seek 后从同一 Snapshot 第一条 `close_ts >= ts` 的 Bar 继续，并保持 Revision 与五键排序语义。
- [ ] Credits 为 0、恰好耗尽、超过剩余行数、读取期间授予和查询耗尽等情况都有明确 End of Session 行为。
- [ ] 只有当前查询耗尽且已预取记录全部发布后才发送 exhausted，不会因暂时无 Credits 而误判结束。
- [ ] 测试覆盖单条、大于单批次、空结果、seek 越过末尾、连续 seek、读取期间 step/stop 和重复控制命令。
- [ ] asap/realtime 保持回归通过，三种 Replay Mode 都不再依赖完整 Bars 或 close-time 列表。
