# 08: 迁移 CSV、export 与流式失败行为

**What to build:** 让 CSV 历史响应和 Parquet/Arrow export 使用 DuckDB Manifest Snapshot 读取，并定义完整的流式失败行为：响应开始前返回稳定错误，响应开始后必须被客户端识别为不完整传输，而不是正常截断。

**Blocked by:** 05 打通 DuckDB → Store → REST Arrow 的 tracer bullet。

**Status:** ready-for-agent

- [ ] CSV 逐批读取并输出，保持精确 Decimal 文本、九位纳秒时间、null 表示、字段顺序和 Content-Type。
- [ ] CSV 不增加全局排序，保留全部 raw Revisions，并对空结果、多文件和大结果进行旧新差分验证。
- [ ] Parquet/Arrow export 在同一 Manifest Snapshot 中读取全部匹配数据，先完整生成临时文件再发送，保持既有文件名、Content-Type 和清理行为。
- [ ] 响应开始前的失败使用现有稳定 HTTP 状态、错误 envelope 和 request ID，不泄漏 DuckDB/Polars 异常或路径。
- [ ] Arrow/CSV 响应开始后的失败会异常终止传输，不拼接 JSON 错误、不吞异常，也不产生貌似成功的正常 EOF。
- [ ] 客户端能把过早 EOF、缺失 Footer 或不完整分块识别为失败，而不是完整空结果或完整文件。
- [ ] 客户端断开、生成失败和 export 发送失败都会关闭 Reader、释放连接与 Read Lease，并清理临时文件。
- [ ] 资源耗尽和外部文件错误不会被静默截断；正常维护由 Read Lease 阻止删除使用中的 Data Files。
- [ ] 大型流式响应不默认物化到进程内存，峰值内存和临时磁盘相对基线没有未经批准的增长。
