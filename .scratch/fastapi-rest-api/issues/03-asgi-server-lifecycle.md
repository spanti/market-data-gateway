# 03: 新增 AsgiServer 生命周期模块

**What to build:** 提供一个替换旧 `HttpServer` 的 ASGI 生命周期接缝，但暂不迁移任何调用方。调用方只学习 `start`、`stop`、`call_soon_threadsafe`、`bound_port`、`base_url` 五个公共成员；线程、事件循环、uvicorn 启动、端口解析与有序拆除全部留在内部。`start` 幂等，`stop` 幂等，启动失败完整回滚，从服务器自身线程调用 `stop` 抛 `RuntimeError`，通配地址在 `base_url` 中改写为 `127.0.0.1`。

**Blocked by:** None (can start immediately)

**Status:** ready-for-agent

- [ ] 新模块提供 `start(app, *, host, port, thread_name, on_shutdown=None) -> int`、`stop()`、`call_soon_threadsafe(fn, *args)`、`bound_port`、`base_url`
- [ ] uvicorn 在 daemon 线程运行，使用 `log_config=None` 与 `access_log=False`
- [ ] 真实 socket + 端口 0：`bound_port` 与 `base_url` 反映实际端口
- [ ] 重复 `start()` 返回同一端口且只有一个线程；重复 `stop()` 为 no-op
- [ ] 启动失败后无线程/循环/端口残留，并重新抛出原始异常
- [ ] `on_shutdown` 在关闭阶段执行；其异常不阻断拆除
- [ ] `call_soon_threadsafe` 可从另一线程派发；停止后抛 `RuntimeError`
- [ ] 从服务器线程调用 `stop()` 抛 `RuntimeError` 而不是死锁
- [ ] 旧 `HttpServer` 与现有调用方保持不变
