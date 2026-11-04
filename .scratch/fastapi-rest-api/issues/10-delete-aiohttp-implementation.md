# 10: Contract：删除旧 aiohttp 服务器与应用

**What to build:** 在 09 切换完成、且旧实现没有调用方之后，删除旧的 aiohttp `HttpServer` 与 aiohttp 应用代码，确保源码中不再有 aiohttp import，并从直接依赖中移除 aiohttp。生命周期测试改为指向 `AsgiServer`。aiohttp 仍可能作为 ccxt 的传递依赖存在于环境中，不声称从环境移除。

**Blocked by:** 09 (切换生产到 FastAPI 应用 + AsgiServer)

**Status:** ready-for-agent

- [ ] 旧 `HttpServer` 与 aiohttp 应用代码被删除
- [ ] 源码中不再有 aiohttp import
- [ ] `pyproject.toml` 不再直接声明 aiohttp
- [ ] 生命周期测试只针对 `AsgiServer`
- [ ] 全部测试通过，且无新增失败
- [ ] 不声称 aiohttp 已从运行环境移除
