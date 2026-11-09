# 08: 实现有配额、有期限的大文件 Export Job

**What to build:** 为 31–366 天单 Instrument 原始 Bar CSV 提供持久 Export Job、受限执行器、Manifest Snapshot/Read Lease、Dataset 外临时文件、原子发布、SHA-256、24h 过期、所有权、Range 下载和清理。

**Blocked by:** 01, 02, 03

**Status:** ready-for-agent

- [ ] 状态、真实/未知进度、计数、取消和中断恢复符合 spec。
- [ ] 每用户一个运行中任务及全局并发/存储配额可配置并执行。
- [ ] CSV 保留所有 Revision、BAR_SCHEMA 列顺序和精确十进制文本，Dataset 不变。
- [ ] 临时文件在 Dataset 外，完成原子发布并记录大小/SHA-256/过期时间。
- [ ] 不透明 Job ID、资源所有权、安全 attachment 文件名、Content-Length 和 Range 正确。
- [ ] 过期清理只删除 Export 文件和元数据，不触碰 Bars/Quarantine。
- [ ] 大文件、断点、取消、重启、配额、越权和 Lease 集成测试完整。

## Comments
