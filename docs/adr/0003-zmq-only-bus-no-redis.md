# 进程间总线只用 ZeroMQ，v1 不引入 Redis

Status: accepted；其中“控制走 REQ/REP”的条款仅对 Admin 控制通道被 [ADR-0017](0017-admin-control-loopback-tcp.md) 取代。行情 PUB/SUB、HWM/SQLite 本地状态与“不引入 Redis”的结论继续有效。

collector、api、replay 三个 Service Role 之间用 ZMQ 通信：行情走 PUB/SUB，控制走 REQ/REP，同一套抽象在进程内用 `inproc://`、跨进程用 `ipc://`/`tcp://`。High-Water Mark（HWM）与回放会话状态存放在各进程自己的本地 SQLite 中，Redis 不参与。理由：多进程已经引入一次复杂度，再引入一个有状态的外部服务会制造第二个真相源与额外的运维负担；ZMQ 不提供持久化，但网关需要的持久化本来就在 Parquet 与 SQLite 里。PUB/SUB 的慢加入问题由启动顺序（collector → 就绪 → api）解决。
