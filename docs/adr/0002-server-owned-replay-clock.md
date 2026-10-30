# Replay Clock 归服务端，`asap` 模式关闭节流

回放服务持有 Replay Clock 并以 Replay Session 为单位推进；客户端通过 REQ/REP 控制 `pause/resume/seek/set_speed/stop`，数据经 PUB 推送。`replay_mode=asap` 时不产生任何等待，回测框架可把它当作按 `event_ts` 有序的批量流，自行驱动策略时钟；`realtime` 与 `step` 才使用 Replay Clock 与 Step Credit。不提供客户端定时拉取（pull）模式，因为它迫使服务端为每个客户端保存游标，并且与 ZMQ 推送语义冲突；逐条驱动由 `step` 覆盖。
