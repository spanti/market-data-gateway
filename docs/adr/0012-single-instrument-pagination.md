# 时间游标分页只对单 Instrument 的 JSON 查询生效

当 JSON Bars 查询同时给出 `instrument` 与 `limit` 时，响应在 `bars` 之外返回 `page: {has_more, next_start}`；缺少任一参数，以及 Arrow、CSV、export 与 `/api/v1/instruments`，都保持原响应形状。处理端多读一行（`limit + 1`），按 `open_ts` 升序物化后返回前 `limit` 行；`next_start` 是第一条未返回 Bar 的 `open_ts`（ISO-8601 纳秒字符串），下一次请求把它作为闭区间 `start` 使用，因此页与页之间既不重复也不遗漏。`has_more` 只统计 `end` 以内仍然存在的行；没有更多行时 `next_start` 为 `null`；`limit=0` 返回空 `bars`，但仍报告是否存在匹配行以及首行 `open_ts`。这也为分页 JSON 定义了一条显式排序契约：按 `open_ts` 升序；非分页的流式格式继续按存储顺序读取，不承诺全序。

多 Instrument 的时间游标分页不提供。多个 Instrument 的 Bar 需要按时间归并成一条有序流，而单值 `next_start` 无法同时表达每个 Instrument 的读取进度：若取所有 Instrument 中最小或最大的 `open_ts`，同一时间戳上的行会被重复返回或被跳过；若取某一个 Instrument 的边界，其余 Instrument 必然漏读。要让多 Instrument 分页正确，就必须引入 `(open_ts, instrument_id)` 复合游标或每 Instrument 一份游标，并额外定义同一时间戳的稳定次序、去重规则与客户端状态管理——这会让响应形状、客户端实现和测试面显著复杂化，也偏离「只提供时间游标、不提供不透明 cursor」的范围约束。消费方应逐 Instrument 查询并各自翻页，或使用 Arrow / CSV 一次性下载后自行排序。代价是 REST 分页读取始终限制在单个 Instrument 上。
