# 网关只交付公开接口，不内置任何回测/交易框架适配

网关对外只提供四组中性接口：Data Contract（版本化 Arrow schema + Parquet Dataset 布局）、Streaming Interface（ZMQ/WS topic 与 Message Envelope）、Replay Protocol、HTTP & WebSocket API。NautilusTrader、vnpy 或任何自研框架的接入代码由接入方自己编写，网关内核禁止 import 任何框架包。这样"支持多种回测和交易框架"通过协议成立，而不是通过适配器数量成立：新增一个框架不需要修改或重新发布网关。代价是接入方需要理解 Arrow IPC 与 Replay Protocol。
