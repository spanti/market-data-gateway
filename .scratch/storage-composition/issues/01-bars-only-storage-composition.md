# 01: 用 Bars-only Storage Composition 打通只读读取路径

**What to build:** 网关维护者可以通过一个深的 Storage Composition 模块，从已验证的 GatewayConfig 和可选 root override 获得 Bars Store。api 与 replay Service Role 使用该组合获得各自独立的读取对象，不创建 QuarantineStore；实际 root 的独立性校验在任何文件系统副作用之前完成，现有 REST API、Replay Protocol、Dataset 布局和角色生命周期保持不变。

**Blocked by:** None (can start immediately)

**Status:** ready-for-agent

- [ ] Bars-only 组合接受已验证的 GatewayConfig 与可选 root override，每次返回新的 ParquetBarStore，不创建或缓存 QuarantineStore，不引入全局 singleton。
- [ ] 实际 storage root 保持“显式 override 优先于配置值”的语义；Bars Dataset、Instrument Snapshot、High-Water Mark 与 DuckDB 临时目录的派生位置保持当前契约。
- [ ] 默认 Quarantine root 跟随实际 storage root，显式 Quarantine root 继续原值优先且不跟随 override；相对显式路径的现有锚点不变。
- [ ] Bars-only 组合也会在构造 Store 前校验本次实际 root 组合；Quarantine 等于或位于实际 Bars Dataset 内时，在任何目录、descriptor、Manifest 或 DuckDB 临时目录创建前失败。
- [ ] 校验覆盖父目录片段、相对路径、root override 和绕过模型校验构造的配置；比较不要求路径预先存在，也不新增反向包含禁令。
- [ ] api 与 replay 的默认生产构造路径全部迁移到 Bars-only 组合，原位置的重复 Store 参数映射被删除，不增加切换开关或旧路径后备。
- [ ] api 与 replay 仍只拥有 Bars 读取依赖，其公共构造方式、启动、状态、就绪、指标、关闭、REST API 和 Replay 行为不变。
- [ ] DuckDB 资源配置与当前转换结果一致；未配置临时目录时仍位于实际 Store root 下，DuckDBConfig 与 DuckDBOptions 不合并，配置导入的懒加载方向不倒退。
- [ ] ParquetBarStore 的公共构造接口、子类行为和直接构造方式不变；不为原本没有注入参数的 Service Role 扩张公共接口。
- [ ] 新增测试穿过 Storage Composition 的 interface，使用真实临时目录与真实 DuckDB 行为，覆盖按需创建、新实例、root 优先级、路径布局、允许布局和失败前零写入。
- [ ] 现有 api、replay、REST、Replay、Store contract 与多进程读取测试保持通过；测试不断言私有字段或内部调用次数。
- [ ] 组合模块不创建 Feed Handler、不接管对象生命周期、不引入存储 adapter，也不修改 GLOSSARY 或 ADR。
