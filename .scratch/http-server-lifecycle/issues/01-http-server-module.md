# 01: 新增 HttpServer 生命周期模块

**What to build:** 一个新的深模块，把“在后台线程上服务一个已构建的 aiohttp 应用”的全部实现收进一个小接口：线程、事件循环、`AppRunner`、`TCPSite`、端口解析与有序拆除。接口只有 `start`、`stop`、`call_soon_threadsafe`、`bound_port`、`base_url`。模块不拥有 `WebSocketHub`，不感知 ZMQ、Replay Protocol 或任何 Service Role。

**Blocked by:** None (can start immediately)

**Status:** resolved

- [x] 模块公开 `start(app, *, host, port, thread_name, on_shutdown=None) -> int`，返回解析后的实际端口；`port=0` 由操作系统分配。
- [x] `start()` 幂等：已启动时返回既有端口，不创建新线程。
- [x] `stop()` 幂等：未启动时为 no-op。
- [x] `start()` 失败时完整清理线程、事件循环与 socket，并重新抛出原始异常。
- [x] `on_shutdown` 只在 `stop()` 时执行，在 `runner.cleanup()` 之前、在事件循环上执行；其异常被记录并吞掉，后续拆除继续。
- [x] `call_soon_threadsafe` 可从其他线程派发到 HTTP 事件循环；服务器未启动时抛 `RuntimeError`。
- [x] `bound_port` 与 `base_url` 在未启动时抛 `RuntimeError`；`base_url` 把 `0.0.0.0` / `::` 报告为 `127.0.0.1`。
- [x] 从服务器自身线程调用 `stop()` 抛 `RuntimeError`，而不是死锁。
- [x] 超时保持模块常量（启动 10 秒、关闭 future 10 秒、线程 join 5 秒）；后台线程 `daemon=True`；线程名由调用方传入。
- [x] 十个公共接口测试覆盖：端口 0、通配地址、真实请求、重复 start、重复 stop、端口复用、绑定失败回滚、关闭钩子顺序与异常、跨线程派发、服务器线程内 stop。
- [x] 测试不断言私有字段；不新增配置字段；不修改 `GLOSSARY.md` 或 ADR。

## Comments

- Resolved on `integration/http-server-lifecycle` (module merge `41a0c62`, fix pass `f281c7c`). Full offline suite: 528 passed, 5 known Typer/Click baseline failures, no new failures.
