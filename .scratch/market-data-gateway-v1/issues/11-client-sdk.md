# 11: 客户端 SDK

**What to build:** 使用者用 `mdgw-client` 就能查历史、订阅实时、控制回放，不需要 import 网关内部代码。

**Blocked by:** 08, 10

**Status:** ready-for-agent

- [ ] `history()` 返回 `pyarrow.Table`，并提供 `to_pandas()`。
- [ ] 实时订阅封装 ZMQ/WS 与重连。
- [ ] Replay Session 控制封装。
- [ ] decimal 在 pandas 中为 `Decimal` 的文档与测试。
- [ ] SDK 只依赖 pyarrow、pyzmq 与 schema 定义。
