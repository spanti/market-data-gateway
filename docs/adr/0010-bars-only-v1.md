# v1 只做 Bar，不采集逐笔与盘口

网关 v1 的规范数据只有 Instrument 与 Bar，不实现 TradeTick、QuoteTick、Depth 与交易状态流。理由：按已确定的规模（单机、<50 标的、日增 <10GB），保留全量逐笔很可能直接超出存储预算；而 Bar 足以支撑中低频策略、Bar 级回放与大多数框架的初筛。代价是明确的：无法还原周期内价格路径、点差、深度与排队位置，依赖精确止损/止盈或紧贴盘口成交的策略不能在 v1 上做可信回测；Nautilus 的 bar execution 只是把 OHLC 拆成 O→H→L→C 的启发式重建。若未来需要逐笔，新增 Data Type 与 Feed Handler 能力声明即可，不改动现有 Bar 契约。
