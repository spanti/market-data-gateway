# 02: 为当前 HTTP/WS 行为建立 characterization 测试

**What to build:** 在删除任何 aiohttp 实现之前，用 `GatewayRuntime` 端到端冻结当前用户可见行为，作为后续迁移的对等性 oracle。覆盖 JSON Bars 的 envelope、decimal 字符串与纳秒时间；Arrow 响应的表语义相等；CSV 文本；各格式的 Content-Type 与状态码；统一错误 envelope 与 `X-Request-ID`；以及 WebSocket 的订阅、事件与 `lag_notice`。测试只断言外部行为，不做 Arrow 字节比较。

**Blocked by:** None (can start immediately)

**Status:** resolved

- [x] JSON Bars 的 envelope、decimal 字符串、纳秒时间与响应头被语义化冻结
- [x] Arrow 响应以表语义相等断言，CSV 以文本断言
- [x] 错误响应冻结为 `{"error": {code, message, details, request_id}}`，并验证 `X-Request-ID`
- [x] WebSocket 订阅、事件与 `lag_notice` 行为被冻结
- [x] 测试在当前 aiohttp 实现上全绿，且不修改生产代码

## Comments

- Resolved on `integration/fastapi-rest-api` (merge `c9d2827`; code-review fixes `c067310`). `tests/integration/test_api_characterization.py` 以 11 个测试冻结 JSON/Arrow/CSV/export、错误 envelope、X-Request-ID 与 WebSocket/lag_notice 语义；迁移前后均通过。 Full offline suite: 698 passed, 5 known Typer/Click baseline failures, no new failures.
