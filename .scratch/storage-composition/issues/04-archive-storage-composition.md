# 04: 将 archive backfill CLI 接入统一组合并保持直接调用兼容

**What to build:** archive backfill CLI 通过 Bars + Quarantine Storage Composition 获得存储对象，并把同一有效 Normalization Rule Version 传给 archive import，使 Manifest 与 Quarantine 血缘一致。直接调用 archive import 的调用方仍可省略 GatewayConfig 和 Quarantine，并继续得到 Store root 下的默认 Quarantine；显式注入与整文件失败语义保持不变。

**Blocked by:** 02: 用 Bars + Quarantine 组合打通采集写入与血缘；03: 将 Compaction 与 GC 运维命令接入按需组合

**Status:** ready-for-agent

- [ ] archive backfill CLI 使用联合组合创建 Bars Store 与 QuarantineStore，不再独立重复两者的配置映射或 Rule Version 默认逻辑。
- [ ] CLI 将同一次组合的有效 Rule Version 传给 archive import；默认与自定义值均同时到达新 Bars Manifest 条目和 Quarantine Record。
- [ ] CLI 继续使用配置化 Quarantine root、独立 Schema Version 与 Retention，root override 和显式 Quarantine root 的既有优先级不变。
- [ ] archive import 的公共接口继续接受可选 Quarantine；直接调用者未注入时仍使用 Store root 下的 quarantine，不被要求提供 GatewayConfig。
- [ ] 直接调用的默认 Quarantine Schema Version、Retention、物理格式和 Rule Version 默认行为保持兼容，不因 CLI 组合统一而被静默修改。
- [ ] 显式注入 Quarantine 的直接调用与测试保持有效，Quarantine 写入失败的 fail-closed 行为不被吞掉或改写。
- [ ] archive checksum、ZIP/CSV 结构、时间单位、整文件原子性、稳定 Reason Code、来源定位和 sanitized 错误行为不变。
- [ ] archive backfill 的 High-Water Mark、Bar Aggregation、Revision merge 与实际 root 派生路径不变。
- [ ] CLI 参数错误继续先于存储组合发生并产生零目录副作用；实际 root 组合无效时也在任何写入前失败。
- [ ] 现有直接 archive 调用的默认 Quarantine 测试保持原样通过，CLI 配置化 Quarantine、自定义 Rule Version 和注入失败测试继续覆盖各自公共行为。
- [ ] 新增测试证明 CLI 使用同一次联合组合的对象与 Rule Version，而不是通过私有字段或内部调用次数推断组合方式。
- [ ] 不改变 archive 直接调用、CLI、Store、GLOSSARY 或 ADR 的公共契约，不引入新的 adapter 或全局生命周期管理者。
