# 04: 打通 Bar 图、Revision 表格和双路径 CSV 下载

**What to build:** 实现单 InstrumentId 的 Bar 历史/实时图、最大 Revision 视图、原始 Revision 表格、URL 共享查询、小文件 GET Blob 下载和 Admin Export Job 界面。当前已知分页与 WS 不连续必须显式暴露，Admin 异步能力未联调时保持清晰边界。

**Blocked by:** 02

**Status:** ready-for-agent

- [ ] 支持五个规范 Bar Interval、UTC/本地切换、start 闭/end 开、默认 24h 和最大 31d 图表范围。
- [ ] 最大 Revision 去重与原始表格分离，精确字符串不被导出或详情改写。
- [ ] WS 实时追加标记断线接收段，不宣称无缝恢复；Time Cursor 不前进时停止并报错。
- [ ] Blob 文件名支持 filename*/filename/fallback，进度区分已知/未知总量，支持取消和 Blob 错误解析。
- [ ] 小文件最多 31d；31–366d 只创建 Export Job，不自动退回超大 Blob。
- [ ] Export Job 状态、所有权、配额、SHA-256、24h 过期和原生 Range 下载符合 spec。
- [ ] Playwright 验证真实小文件下载内容与文件名；Admin 大文件仅报告实际联调程度。

## Comments
