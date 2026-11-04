# Spec: 将 api Service Role 的 HTTP 面迁移到 FastAPI + ASGI

Status: ready-for-agent

## Problem Statement

api Service Role 目前用 aiohttp 手写请求分发与 HTTP 生命周期。每个新行为（参数校验、统一错误映射、限流、分页元数据、OpenAPI 文档）都要在多个 handler 或中间件里重复实现，且没有一份机器可读的接口描述。运维侧也没有可直接启动的 Prometheus/Grafana 形态，只有 `/metrics` 端点；历史查询只有 `limit` 上限，没有 `has_more` / `next_start`，消费方必须自己猜下一页边界。

与此同时，迁移不能破坏 Data Contract：JSON 中 decimal 必须是字符串、时间必须是 ISO-8601 纳秒字符串，Arrow/CSV 必须逐批流式且绝不经过浮点，错误必须是 `{"error": {code, message, details, request_id}}`，WebSocket 的 Message Envelope 与 `lag_notice` 语义必须不变。现有 `HttpServer` 是 aiohttp 专用，`WebSocketHub` 又绑定了 aiohttp 类型，collector / replay 的健康与指标端点复用了同一套生命周期；如果只把 api 换成 FastAPI，仓库会长期保留两套服务器栈。

## Solution

把全部 HTTP 面（api 的 REST / WebSocket / 运维端点，以及 collector / replay 的健康与指标端点）统一迁移到 FastAPI + ASGI，用 uvicorn 在现有后台线程模型中运行。旧 `HttpServer` 退役，新建 `AsgiServer` 作为唯一的生命周期接缝，保留原有五个公共成员与幂等/回滚语义。

REST 路由由 FastAPI 承担；Pydantic 只用于入口校验与 OpenAPI 描述，响应仍手工构造。新增统一错误映射、每 IP / 每路由限流、单 Instrument JSON 查询的分页元数据、始终开启的 `/openapi.json` 与 `/docs`，以及 HTTP 延迟 histogram 和可选的 Prometheus/Grafana compose 部署。

Data Contract、Streaming Interface、Replay Protocol、Parquet Dataset 布局与 WebSocket 消息形状均不变。

## User Stories

1. 作为现有 REST 消费方，我希望迁移后 JSON Bars 响应的 envelope、decimal 字符串和纳秒时间字符串保持不变，这样我的解析器不需要改。
2. 作为现有 REST 消费方，我希望 Arrow 与 CSV 响应的 schema、Content-Type 和流式行为保持不变，这样大范围历史下载仍然可用。
3. 作为现有 REST 消费方，我希望错误仍然是 `{"error": {code, message, details, request_id}}`，并继续返回 `X-Request-ID`，这样我的错误处理与日志关联不需要改。
4. 作为现有 REST 消费方，我希望 404、405 与参数校验失败返回稳定的网关错误码，而不是 FastAPI 默认的 422 `{"detail": ...}`，这样我可以继续按 `code` 分支。
5. 作为现有 REST 消费方，我希望新增的 `rate_limited`、`not_found`、`method_not_allowed` 被写入稳定错误码表，这样我不需要猜测新的失败形态。
6. 作为历史数据消费方，我希望单 Instrument 的 JSON 查询返回 `has_more` 与 `next_start`，这样我可以确定性地逐页读取。
7. 作为历史数据消费方，我希望 `next_start` 是第一条未返回 Bar 的 `open_ts`，并且下一次作为闭区间 `start` 使用，这样页与页之间不重不漏。
8. 作为历史数据消费方，我希望 `limit=0` 返回空 `bars`，同时告诉我是否存在匹配行以及首行 `open_ts`，这样我可以先探测再决定页大小。
9. 作为历史数据消费方，我希望 `has_more` 只统计 `end` 以内的行，这样最后一页不会被范围外的数据误判。
10. 作为历史数据消费方，我希望 Arrow、CSV 与 export 完全不受分页改动影响，这样大文件下载路径保持稳定。
11. 作为历史数据消费方，我希望分页元数据只出现在同时给出 `instrument` 与 `limit` 的查询里，这样多 Instrument 查询不会出现静默跳过或重复。
12. 作为历史数据消费方，我希望 `/api/v1/instruments` 不引入分页，这样参考数据的读取方式不变。
13. 作为 API 消费方，我希望 `/openapi.json` 与 `/docs` 始终可用，这样我可以在不读源码的情况下发现接口。
14. 作为 API 消费方，我希望 OpenAPI 描述统一错误 envelope、Arrow/CSV/Parquet 的 Content-Type、decimal 字符串与纳秒时间字符串，这样生成的客户端不会把价格误当浮点。
15. 作为 API 消费方，我希望 `/redoc` 不暴露，这样公开面保持最小。
16. 作为 API 消费方，我希望路径末尾斜杠不会触发隐式重定向，这样我的请求行为与现在一致。
17. 作为 API 消费方，我希望 HEAD 行为被明确记录为不在契约内，这样我不会依赖未承诺的语义。
18. 作为运维人员，我希望限流默认关闭，这样升级不会意外改变现有部署行为。
19. 作为运维人员，我希望配置每秒请求数、突发容量与每 IP WebSocket 会话上限，这样我可以按部署环境调整。
20. 作为运维人员，我希望 `/healthz`、`/readyz`、`/metrics`、`/openapi.json`、`/docs` 固定豁免限流，这样健康检查、Prometheus 抓取与文档不会被误伤。
21. 作为运维人员，我希望超限返回 429、`Retry-After` 与 `rate_limited` 错误码，这样客户端可以退避。
22. 作为运维人员，我希望 WebSocket 在 `accept()` 之前按每 IP 会话上限拒绝，这样我不会建立注定要关闭的连接。
23. 作为运维人员，我希望限流状态在进程内、单 worker 运行，这样行为可解释，不引入外部状态。
24. 作为运维人员，我希望看到 HTTP 请求延迟 histogram，这样我可以区分慢端点与慢存储。
25. 作为运维人员，我希望看到限流命中计数，这样我可以判断阈值是否合理。
26. 作为运维人员，我希望有一套可选的 Prometheus + Grafana compose 部署，这样我不需要自己拼监控栈。
27. 作为运维人员，我希望 Prometheus 同时抓取 collector、api、replay 三个 Service Role，这样我能在一个面板看到全链路。
28. 作为运维人员，我希望默认 `docker compose up` 不启动 Prometheus/Grafana，这样核心栈保持轻量。
29. 作为维护者，我希望只有一个 ASGI 生命周期模块，并保留 `start`、`stop`、`call_soon_threadsafe`、`bound_port`、`base_url` 五个公共成员，这样调用方不需要重新学习生命周期。
30. 作为维护者，我希望 `start()` 幂等、`stop()` 幂等、启动失败完整回滚，这样端口占用不会留下孤儿线程。
31. 作为维护者，我希望端口 0 能解析出实际绑定端口并反映在 `base_url` 中，这样测试与本地脚本不需要固定端口。
32. 作为维护者，我希望 `call_soon_threadsafe` 能从 ZMQ 订阅线程把消息送到 WebSocket hub，这样跨线程派发仍然安全。
33. 作为维护者，我希望从服务器自身线程调用 `stop()` 抛 `RuntimeError` 而不是死锁，这样线程约束可被发现。
34. 作为维护者，我希望所有 HTTP 面只使用一套服务器栈，这样不需要维护两套中间件、指标与关停逻辑。
35. 作为维护者，我希望 `ServiceHealthServer` 迁移到同一套 ASGI 栈，这样 collector / replay 的 `/healthz`、`/readyz`、`/metrics`、`/status` 行为保持不变。
36. 作为维护者，我希望 `WebSocketHub` 只依赖 `send_json` 与 `close` 这样的传输中性协议，这样它不再绑定 aiohttp 类型。
37. 作为维护者，我希望迁移完成后源码中不再有 aiohttp import，这样直接依赖可以被移除。
38. 作为维护者，我希望 `create_app(...)` 的名称与参数形状保持不变，这样 `ApiService` 与 `GatewayRuntime` 的调用点不需要重写。
39. 作为维护者，我希望路由按资源拆分，这样 Bars、Instruments、运维端点和 WebSocket 不再挤在一个文件里。
40. 作为测试作者，我希望在删除 aiohttp 实现之前先建立语义 characterization 测试，这样迁移期间有真实的回归保护。
41. 作为测试作者，我希望所有用户可见的 REST/WS/运维行为都从 `GatewayRuntime` 端到端进入，这样我不需要为 FastAPI 应用再开一个测试入口。
42. 作为测试作者，我希望 `AsgiServer` 用真实 socket 与端口 0 验证启停、幂等、回滚与跨线程派发，这样生命周期接缝仍然可独立测试。
43. 作为测试作者，我希望令牌桶用注入假时钟的单元测试验证，这样限流行为不依赖真实等待。
44. 作为测试作者，我希望验收标准是“相对改动前无新增失败”，这样我不会顺手修无关问题。
45. 作为文档读者，我希望 `DESIGN.md`、`README.md` 与 `docs/data-contract.md` 同步更新，这样接口、依赖与错误码没有第二套说法。
46. 作为文档读者，我希望 `GLOSSARY.md` 新增 **REST API** 与 **Time Cursor**，并明确不再使用“服务层”这个词。
47. 作为文档读者，我希望有两份 ADR：FastAPI/ASGI 取代 aiohttp，以及分页要求单 Instrument。
48. 作为维护者，我希望迁移拆成可独立验证的提交，这样每个提交都能单独 review 与回退。
49. 作为实现代理，我希望保留工作区已有的 `Dockerfile` / `pyproject.toml` 未提交改动，在其上追加依赖变更，这样不会覆盖用户正在做的事。
50. 作为契约维护者，我希望 OpenAPI 只是生成的文档，规范真相仍是 Data Contract，这样不会出现第二个真相源。

## Implementation Decisions

### HTTP runtime and lifecycle

- 旧 aiohttp `HttpServer` 退役；新建模块 `mdgw.api.asgi_server`，公开类 `AsgiServer`。
- `AsgiServer` 保留原五个公共成员：`start(app, *, host, port, thread_name, on_shutdown=None) -> int`、`stop()`、`call_soon_threadsafe(fn, *args)`、`bound_port`、`base_url`。
- 内部用 `uvicorn.Server` 在 daemon 线程运行，保留原后台线程模型；`log_config=None`、`access_log=False`，继续使用网关自己的结构化日志。
- 超时保持模块常量：启动 10 秒、关闭 future 10 秒、线程 join 5 秒。
- `port=0` 时从实际绑定的 socket 解析端口；`base_url` 在通配地址（`0.0.0.0` / `::`）下报告 `127.0.0.1`。
- `on_shutdown` 在 ASGI lifespan shutdown 阶段执行；其异常被记录并吞掉，后续拆除继续。
- `start()` 幂等；`stop()` 幂等；启动失败时先清理线程、事件循环与 socket，再重新抛出原始异常；从服务器自身线程调用 `stop()` 抛 `RuntimeError`。
- `ApiService`、`GatewayRuntime`、`ServiceHealthServer` 全部迁移到 `AsgiServer`；`GatewayRuntime` 的 `base_url`、`ApiService` 的 `base_url` / `http_port`、`ServiceHealthServer` 的 `base_url` / `bound_port` 名称与语义保持不变。
- `create_app(...)` 的名称与参数形状保持不变，内部改为构建 FastAPI 应用。
- 路由按资源拆分：Bars、Instruments、运维端点、WebSocket 各自独立；REST 路由使用 FastAPI `APIRouter`，运维与 WebSocket 路由使用 Starlette 风格并标 `include_in_schema=False`。
- `redirect_slashes=False`；不注册 HEAD 路由，HEAD 不在契约内。
- 依赖：新增 `fastapi` 与纯 `uvicorn`（不使用 `[standard]` extra）；最后一个 aiohttp import 删除后，从直接依赖中移除 aiohttp。aiohttp 仍可能作为 ccxt 的传递依赖留在环境中，不声称从环境移除。
- 出站 HTTP 客户端不在本次范围：不引入 `aiohttp.ClientSession`，不创建空 session。

### REST behavior and error contract

- 现有端点与路径保持不变；不新增、不删除业务端点（`/openapi.json`、`/docs` 除外）。
- 所有错误统一为 `{"error": {code, message, details, request_id}}`；`X-Request-ID` 继续由中间件生成或透传，并在响应头返回。
- 新增稳定错误码：`rate_limited`（429）、`not_found`（404）、`method_not_allowed`（405）。
- FastAPI / Starlette 的参数校验错误映射到既有稳定码：`invalid_interval`、`invalid_range`、`invalid_parameter`、`invalid_instrument`、`invalid_format`；不向客户端暴露 422 与 `{"detail": ...}`。
- 未处理异常映射为 `internal_error`（500），并记录结构化日志。
- Content-Type 保持：JSON 为 `application/json`；Arrow 流为 `application/vnd.apache.arrow.stream`；Arrow 文件为 `application/vnd.apache.arrow.file`；Parquet 为 `application/vnd.apache.parquet`；CSV 为 `text/csv; charset=utf-8`；Metrics 为 `text/plain; version=0.0.4`。
- Pydantic 只用于入口校验与 OpenAPI 描述；JSON 响应仍手工构造；Arrow/CSV/Parquet 完全不经过 Pydantic。
- `/openapi.json` 与 `/docs` 始终开启，不提供配置开关；`/redoc` 关闭。
- OpenAPI 描述统一错误 envelope、各 `format` 的 Content-Type、decimal 为字符串、时间为 ISO-8601 纳秒字符串；但不作为规范真相源。
- `/openapi.json` 与 `/docs` 加入限流固定豁免列表。

### Rate limiting

- 新增 `[rate_limit]` 配置段：

```toml
[rate_limit]
enabled = false              # 默认关闭
requests_per_second = 50     # 每 (IP, 路由模板) 的稳态速率
burst = 100                  # 令牌桶容量
max_ws_sessions_per_ip = 16  # 0 表示不限制
```

- 键为源 IP + 路由模板（不是原始 path），避免高基数；不信任 `X-Forwarded-For`。
- 固定豁免：`/healthz`、`/readyz`、`/metrics`、`/openapi.json`、`/docs`；该列表不可配置。
- 异步令牌桶放在 `mdgw.core.rate_limit`，不复用 ingest 模块；只限制请求建立，不限制 Arrow/CSV 流式响应体。
- 超限返回 429，带 `Retry-After`（秒，向上取整）与 `rate_limited`；`details` 至少包含 `retry_after_s`。
- WebSocket 在 `accept()` 之前检查每 IP 会话上限，超限直接拒绝；常规限流同样作用于握手请求。
- 单 worker、进程内状态；不引入跨进程共享配额。
- 新增 `mdgw_api_rate_limited_total{path}` 计数器。

### Pagination

- 分页元数据只对同时带 `instrument` 与 `limit` 的 JSON 查询生效；其余查询保持 `{"bars": [...]}`。
- 响应形状：

```json
{
  "bars": ["<Message Envelope>", "..."],
  "page": {"has_more": true, "next_start": "2026-10-30T12:35:00.000000000Z"}
}
```

- 处理端读取 `limit + 1` 行，并按 `open_ts` 升序物化排序后返回前 `limit` 行。
- `next_start` 是第一条未返回 Bar 的 `open_ts`，ISO-8601 纳秒字符串；下一次请求把它作为 `start`（闭区间）使用。
- `has_more` 只统计 `end` 以内仍然存在的行；没有更多行时 `next_start` 为 `null`。
- `limit=0` 时返回空 `bars`；`has_more` 表示是否存在匹配行；`next_start` 为首行 `open_ts`（若存在）。
- Arrow、CSV、export 完全不变；`/api/v1/instruments` 不引入分页。
- 该决定定义了一个显式的排序契约：分页 JSON 查询按 `open_ts` 升序；非分页的流式格式继续按存储顺序，不承诺全序。

### Observability

- 保留自研 `MetricsRegistry`，为其增加 histogram 能力，输出 Prometheus 文本格式的 `_bucket`、`_sum`、`_count` 与 `le` 标签。
- 新增 `mdgw_api_http_request_duration_seconds` histogram，标签为 `method`、`path`（路由模板）、`status`；桶为 `0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, +Inf` 秒。
- 新增 `mdgw_api_rate_limited_total` 计数器，标签为 `path`（路由模板）。
- 保留现有 `mdgw_up`、`mdgw_ready`、`mdgw_api_ws_sessions`、`mdgw_api_ws_dropped_total`、消息计数与 Dataset 指标。
- 新增一个仓库根目录的 observability 部署目录，包含 Prometheus 抓取配置、Grafana datasource 与一个 dashboard JSON。
- 用独立的 `docker-compose.observability.yml` 覆盖文件启动 Prometheus（9090）与 Grafana（3000）；默认 `docker compose up` 不启动它们。
- Prometheus 抓取 collector、api、replay 三个 Service Role 的 `/metrics`；抓取间隔默认 15 秒。
- 不引入 `prometheus_client`，不引入 OpenTelemetry / tracing。

### WebSocket

- `WebSocketHub` 改为面向传输中性 Protocol（`send_json` 与 `close`），不再 import aiohttp 类型；FastAPI/Starlette 路由把 `WebSocket` 传入。
- 订阅、取消订阅、`ping` / `pong`、Message Envelope、`lag_notice`、慢消费者不断连的语义全部不变。
- 每 IP 会话上限在 `accept()` 之前检查；每会话有界队列与丢最旧行为不变。

### Config and documentation

- `GatewayConfig` 新增 `[rate_limit]` 段；校验：`requests_per_second > 0`、`burst >= 0`、`max_ws_sessions_per_ip >= 0`（0 表示不限制）。
- 写两份 ADR：FastAPI/ASGI 取代 aiohttp 作为 HTTP 面（推翻 `DESIGN.md` §12 的“FastAPI 等 Web 框架”条目），以及分页要求单 Instrument（排序契约）。
- 更新 `DESIGN.md` 的 HTTP API、依赖、测试接缝、风险与已拒绝方案；更新 `README.md` 的入口、验证命令与已知限制；更新 `docs/data-contract.md` 的 JSON Bars 形状、新错误码与分页语义。
- `GLOSSARY.md` 新增 **REST API**（api Service Role 对外提供的历史查询 HTTP 接口；避免“服务层”）与 **Time Cursor**（闭区间 `start`、开区间 `end` 与 `next_start` 组成的读取游标）。
- 旧 `http-server-lifecycle` spec 标记 superseded，并指向本 spec；不实现其 aiohttp 版 `HttpServer` 迁移。
- 不修改 Bar / Instrument Arrow schema、Message Envelope、ZMQ topic、Replay Protocol、Parquet Dataset 布局或 WebSocket 消息形状。

### Migration order

1. 新增 `AsgiServer` 与健康端点迁移；既有生命周期测试改写并通过。
2. FastAPI 应用全量端口对齐；`WebSocketHub` 改为传输中性；`ApiService` 与 `GatewayRuntime` 迁移；删除旧 aiohttp 服务器实现；characterization 测试通过。
3. 统一错误映射与 OpenAPI / docs。
4. 限流。
5. 分页元数据。
6. histogram 与 Prometheus/Grafana 部署。
7. 文档、ADR、glossary 与依赖清理。

每个提交都必须可独立验证、测试全绿。

## Testing Decisions

### 好测试的标准

- 只断言外部行为：HTTP 状态码、响应头、JSON 语义、Arrow 表语义相等、CSV 文本、错误码与 `request_id`、线程是否存活、端口是否可复用。
- 不断言私有字段，不引入测试专用的生产垫片。
- 使用端口 0 与真实 HTTP 请求；测试结束必须停止服务器并验证线程退出。
- Arrow 比较使用表语义相等，不做字节比较，避免元数据差异造成假失败。

### 接缝

1. **主行为接缝：`GatewayRuntime` 端到端（现有）**。所有 REST / WebSocket / 运维行为从这里进入，使用假 Feed Handler 与临时数据目录；覆盖 JSON/Arrow/CSV/export 语义、错误 envelope、分页、限流与豁免、`/docs`、`/openapi.json`、`/metrics`、`/healthz`、`/readyz`、`/status`。
2. **生命周期接缝：`AsgiServer` 公共接口（替换现有 `HttpServer` 接缝）**。真实 socket + 端口 0，验证启动、停止、幂等、失败回滚、`base_url`、`bound_port`、`call_soon_threadsafe`、自身线程调用 `stop()` 抛错；`ApiService` 的进程内启停沿用现有接缝。
3. **Data Contract 接缝：线上字节（现有）**。迁移前建立 characterization：JSON envelope、Arrow 表语义、CSV 文本、decimal 字符串、纳秒时间、错误体；迁移后必须原样通过。

不为 `create_app` 新增测试入口，也不引入 `httpx.ASGITransport` 路由级接缝；路由行为统一经 `GatewayRuntime` 验证。令牌桶、histogram 渲染与配置校验是纯单元测试，不构成新的功能接缝。

### 模块与先例

- `GatewayRuntime` 端到端：沿用 `tests/integration` 中 Bars、status、instruments、client history、client live 的写法。
- `AsgiServer`：沿用现有 `HttpServer` 生命周期测试的十类用例，断言公共接口。
- `ApiService`：沿用现有进程内启停测试，验证 `/healthz`、`/readyz`、`/status`、`/api/v1/status` 与干净停止。
- `ServiceHealthServer`：由 collector / replay 多进程测试与 `AsgiServer` 测试共同覆盖。
- 令牌桶：注入假时钟的单元测试；histogram：`MetricsRegistry.render()` 的文本断言。
- characterization：在删除 aiohttp 实现前，从当前实现捕获语义黄金样本，作为迁移期的回归保护。

### 新增覆盖

- 迁移对等性：同一组 Bars 在 JSON / Arrow / CSV / export 下的语义与响应头。
- 错误映射：404、405、参数校验错误、500 都返回统一 envelope；无 422、无 `{"detail": ...}`。
- 分页：单 Instrument、时间缺口、`end` 边界、`limit=0`、缺少 `instrument`、`limit` 省略。
- 限流：默认关闭；`burst=0` 时首个请求即 429；`Retry-After`；豁免路径不受影响；每 IP WebSocket 会话上限。
- 文档：`/openapi.json` 可解析；`/docs` 200；`/redoc` 404。
- 指标：histogram 的 `_bucket` / `_sum` / `_count` 与 `le` 标签；限流计数。
- 路由细节：末尾斜杠不重定向；HEAD 行为符合“不在契约内”的决定。

### 基线

- 记录改动前的失败集合；验收标准是“无新增失败”，不修复与本 feature 无关的既有失败。
- 真实 Binance 仍只出现在 `@pytest.mark.network` 测试中，默认跳过。

## Out of Scope

- 出站 HTTP 客户端、共享 `aiohttp.ClientSession`、归档下载异步化。
- 鉴权、多租户、TLS、公网暴露。
- offset 分页、不透明 cursor、多 Instrument 分页。
- HEAD 支持、隐式末尾斜杠重定向、CORS。
- `/docs` 的配置开关（始终开启）。
- 引入 `prometheus_client` 或 OpenTelemetry / tracing。
- 修改 Bar / Instrument Arrow schema、Message Envelope、ZMQ topic、Replay Protocol、Parquet Dataset 布局或 WebSocket 消息形状。
- 多 worker 限流共享、分布式限流。
- 扩展 Grafana 仪表盘数量或告警规则。
- 实现旧的 aiohttp `HttpServer` 生命周期迁移；该 spec 标记为 superseded。
- 修复与本 feature 无关的既有测试失败。

## Further Notes

- 两份 ADR 是本次契约变更的正式记录；`DESIGN.md` §12 的“FastAPI 等 Web 框架”条目必须被显式取代，而不是静默删除。
- `GLOSSARY.md` 目前没有 **REST API** 与 **Time Cursor**；它们应在实现前写入，并使用“api Service Role”而不是“服务层”。
- 迁移期间的主要风险：uvicorn 在后台线程中的端口 0 解析、lifespan 事件循环捕获、`on_shutdown` 与 hub 关闭顺序、ASGI 下的流式响应、以及 uvicorn 默认日志配置覆盖网关日志。每一项都要由 `AsgiServer` 或 `GatewayRuntime` 测试覆盖。
- aiohttp 的 `add_get` 默认注册 HEAD，Starlette 不会；本 feature 明确不补 HEAD。characterization 测试用于确认现有测试没有依赖 HEAD。
- 工作区当前有未提交的 `Dockerfile` 与 `pyproject.toml` 改动；依赖变更只在其上追加，不覆盖或回退这些改动。
- 下一步是 `/to-tickets`：把七个迁移步骤拆成编号 issue，按 `docs/agents/issue-tracker.md` 的约定放在本 feature 目录下。
