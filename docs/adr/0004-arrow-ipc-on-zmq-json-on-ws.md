# ZMQ 上传输 Arrow IPC，WebSocket 上传输 JSON

ZMQ 的行情与回放数据使用 Arrow IPC RecordBatch，WebSocket 使用 JSON。理由：ZMQ 面向高频行情流与批量回放，Arrow IPC 让"存储格式 = 传输格式"成立，避免 JSON 解析与二次转换；WebSocket 面向调试、浏览器与低频消费，JSON 的可读性优先。只保留这两种线上格式，不提供 msgpack 或 protobuf 变体，避免接入方为同一份数据实现多套解码。
