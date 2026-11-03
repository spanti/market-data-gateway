# 08: Streaming Interface + WebSocket

**What to build:** 消费者可以订阅 `market.bar.<interval>.<instrument_id>`，通过 ZMQ 收到 Arrow IPC，或通过 WebSocket 收到 JSON；慢消费者收到 lag_notice 而不断线。

**Blocked by:** 04

**Status:** resolved

- [x] collector 以 ZMQ PUB 发布多帧 `[topic][Arrow IPC stream]`。
- [x] api 订阅并 fan-out；WS 支持 subscribe/unsubscribe/ping。
- [x] Message Envelope 与 DESIGN §3.3 一致。
- [x] 慢消费者发送 lag_notice，不主动断开。
- [x] 前缀订阅与多标的测试通过。

## Comments

- Resolved in `integration/market-data-gateway-v1` after the v1 delivery code review and follow-up fixes.
