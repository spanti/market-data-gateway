# 06: 迁移非分页 JSON 与 count

**What to build:** 让非分页 JSON 历史响应和 100,000 行检查通过同一 Manifest Snapshot 的 DuckDB 查询完成，保持 raw Revisions、LIMIT、精确 JSON 表示和稳定错误行为，不再经过旧手工 Arrow 过滤。

**Blocked by:** 05 打通 DuckDB → Store → REST Arrow 的 tracer bullet。

**Status:** ready-for-agent

- [ ] JSON count 和实际读取共享请求级 Manifest Snapshot 与 Read Lease，不会分别看到不同 Manifest 代际。
- [ ] count 和响应都保留全部 raw Revisions，100,000 行 ceiling 及错误详情中的行数不得因隐式去重改变。
- [ ] LIMIT 继续只限制返回行数，不新增全局 ORDER BY，也不承诺复刻旧引擎的偶然扫描顺序。
- [ ] 首版在同一 Snapshot 内分别执行精确 count 和读取，不为了合并扫描引入改变语义的窗口 SQL。
- [ ] JSON Message Envelope、Decimal 字符串、九位纳秒时间、null 和响应形状与旧实现一致。
- [ ] `range_too_large`、参数校验、Schema Version 不匹配和内部错误继续使用既有稳定错误码与 envelope。
- [ ] 覆盖空结果、边界行数、LIMIT 与 ceiling 交互、多文件和 Revision-heavy 数据的旧新差分测试。
- [ ] Reader、连接和 Lease 在响应成功、验证失败和读取异常后均正确释放。
- [ ] 非分页 JSON 的典型延迟和内存相对基线没有未经批准的回退。
