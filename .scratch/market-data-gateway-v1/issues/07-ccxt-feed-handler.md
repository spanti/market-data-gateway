# 07: ccxt Feed Handler — Binance 现货 1m

**What to build:** 不写任何交易所代码的用户，配置 Binance 现货后就能持续收到真实 1m Bar，并在 API 中查到。

**Blocked by:** 06

**Status:** ready-for-agent

- [ ] 使用 `ccxt.pro` 的 `watch_ohlcv` / `watch_ohlcv_for_symbols`；能力判断与回退。
- [ ] 只用 `x` 标志确认收盘 Bar；REST 历史过滤未完成 K 线。
- [ ] `enable_rate_limit` + 按 Venue 令牌桶；自动重连后继续补采。
- [ ] 配置驱动 symbols / Venue，不按交易所名硬编码。
- [ ] network 测试默认跳过；离线测试用假 exchange。
