import { useVirtualizer } from "@tanstack/react-virtual";
import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  useSyncExternalStore,
} from "react";
import { DisabledReason } from "@/components/disabled-reason";
import { EmptyState } from "@/components/states";
import { StatusBadge, type StatusTone } from "@/components/status-badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Panel,
  PanelDescription,
  PanelHeader,
  PanelTitle,
} from "@/components/ui/panel";
import { Select } from "@/components/ui/select";
import { Textarea } from "@/components/ui/textarea";
import { BAR_INTERVALS } from "@/lib/time-range";
import { filterMessages, WS_DEBUG_MESSAGE_LIMIT } from "@/pages/console/message-buffer";
import { resolveWsTarget } from "@/api/ws/ws-target";
import { useRuntimeMode } from "@/runtime/runtime-mode-context";
import { useActionReason } from "@/runtime/use-permission";
import {
  buildControlledTopic,
  TOPIC_PREFIX_HINTS,
  validateControlledTopic,
  type TopicScope,
} from "@/pages/console/topics";
import {
  WsDebugSession,
  type WsDebugSnapshot,
} from "@/pages/console/ws-debug-session";

/**
 * WebSocket Debug console.
 *
 * Fully real in Current Contract: it talks to the gateway `/ws` through
 * GatewayWsTransport. There is no subscribe ACK; pong is an ordering barrier
 * only. Each connection is a new receive segment with no resume position.
 */

export interface WebSocketDebugViewProps {
  /**
   * Injectable for tests; defaults to a real transport-backed session. The
   * resolved target URL is passed for assertion/documentation; test factories
   * may ignore it and inject their own transport.
   */
  sessionFactory?: (targetUrl: string | null) => WsDebugSession;
}

function formatClock(value: string | null): string {
  if (!value) {
    return "—";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return `${date.toISOString().slice(11, 23)}Z`;
}

function connectionLabel(snapshot: WsDebugSnapshot): string {
  switch (snapshot.connection) {
    case "idle":
      return "未连接";
    case "connecting":
      return "连接中…";
    case "open":
      return `已连接 · 接收段 ${snapshot.segment}`;
    case "closed":
      return snapshot.segmentInterrupted
        ? `接收段 ${snapshot.segment} 中断`
        : "连接失败";
  }
}

function connectionTone(snapshot: WsDebugSnapshot): StatusTone {
  switch (snapshot.connection) {
    case "open":
      return "good";
    case "connecting":
      return "warning";
    case "closed":
      return snapshot.segmentInterrupted ? "critical" : "unknown";
    case "idle":
      return "unknown";
  }
}

function syncLabel(snapshot: WsDebugSnapshot): string {
  switch (snapshot.syncState) {
    case "synced":
      return "已同步";
    case "waiting":
      return "等待 pong…";
    case "idle":
      return "未同步";
  }
}

export function WebSocketDebugView({
  sessionFactory,
}: WebSocketDebugViewProps) {
  const { capability } = useRuntimeMode();
  const wsTarget = useMemo(() => resolveWsTarget(capability), [capability]);
  const session = useMemo<WsDebugSession>(() => {
    if (sessionFactory) {
      return sessionFactory(wsTarget.url);
    }
    return new WsDebugSession(
      wsTarget.url !== null ? { url: wsTarget.url } : {},
    );
  }, [sessionFactory, wsTarget.url]);

  const subscribeToStore = useCallback(
    (listener: () => void) => session.subscribeToChanges(listener),
    [session],
  );
  const getSnapshot = useCallback(() => session.getSnapshot(), [session]);
  const snapshot = useSyncExternalStore(subscribeToStore, getSnapshot);

  useEffect(() => () => session.disconnect(), [session]);

  const subscribeActionReason = useActionReason("ws.session.subscribe");
  const freeformActionReason = useActionReason("ws.debug.freeform");
  const capabilityReason = wsTarget.reason;
  const connectionBlockReason = capabilityReason;
  const subscribeBlockReason = subscribeActionReason ?? capabilityReason;
  const freeformBlockReason = freeformActionReason ?? capabilityReason;

  const [scope, setScope] = useState<TopicScope>("1m");
  const [instrument, setInstrument] = useState("");
  const [topicError, setTopicError] = useState<string | null>(null);
  const [protocolNotice, setProtocolNotice] = useState<string | null>(null);
  const [freeformText, setFreeformText] = useState('{"op":"ping"}');
  const [freeformError, setFreeformError] = useState<string | null>(null);
  const [freeformNotice, setFreeformNotice] = useState<string | null>(null);
  const [filter, setFilter] = useState("");
  const [paused, setPaused] = useState(false);

  const isOpen = snapshot.connection === "open";
  const topic = buildControlledTopic(scope, instrument);
  const topicValidation = validateControlledTopic(topic);
  const canSendTopic =
    isOpen && topicValidation.valid && subscribeBlockReason === null;

  const scrollRef = useRef<HTMLDivElement | null>(null);
  const filtered = useMemo(
    () => filterMessages(snapshot.messages, filter),
    [snapshot.messages, filter],
  );
  const virtualizer = useVirtualizer({
    count: filtered.length,
    getScrollElement: () => scrollRef.current,
    estimateSize: () => 72,
    overscan: 8,
    getItemKey: (index) => filtered[index]?.id ?? index,
    initialRect: { width: 800, height: 420 },
  });

  useEffect(() => {
    if (paused) {
      return;
    }
    const element = scrollRef.current;
    if (element) {
      element.scrollTop = element.scrollHeight;
    }
  }, [paused, snapshot.messages.length]);

  const handleTopicAction = (unsubscribe: boolean) => {
    setTopicError(null);
    setProtocolNotice(null);
    if (!topicValidation.valid) {
      setTopicError(topicValidation.error);
      return;
    }
    const sent = unsubscribe
      ? session.unsubscribe(topic)
      : session.subscribe(topic);
    if (!sent) {
      setTopicError("无法发送：WebSocket 未连接或 Topic 无效。");
      return;
    }
    setProtocolNotice(
      `${unsubscribe ? "取消订阅" : "订阅"}已发送（没有订阅 ACK）；可用 Ping 确认操作顺序。`,
    );
  };

  const handleFreeformSend = () => {
    setFreeformError(null);
    setFreeformNotice(null);
    const result = session.sendFreeform(freeformText);
    if (!result.ok) {
      setFreeformError(result.error);
      return;
    }
    setFreeformNotice("已通过白名单并发送（没有订阅 ACK）。");
  };

  return (
    <Panel className="mt-6 p-5" aria-label="WebSocket Debug">
      <PanelHeader>
        <div>
          <PanelTitle>WebSocket Debug</PanelTitle>
          <PanelDescription>
            通过 GatewayWsTransport 连接网关 WebSocket。每个连接是新的接收段；断线后没有恢复位点，不宣称无缝恢复。
            Current Contract 使用公开 /ws；Admin API 模式只连接能力声明中的
            /api/v1/admin/ws，不降级到公开 /ws。
          </PanelDescription>
        </div>
        <div className="flex flex-col items-end gap-1">
          <StatusBadge tone={connectionTone(snapshot)} live>
            {connectionLabel(snapshot)}
          </StatusBadge>
          <span className="text-[11px] text-faint-foreground">
            目标 {wsTarget.url ?? `—（${wsTarget.reason?.message ?? "不可用"}）`}{" "}
            · 连接时间 {formatClock(snapshot.connectedAt)}
          </span>
        </div>
      </PanelHeader>

      <div className="mt-3 flex flex-wrap items-center gap-2">
        <Button
          size="sm"
          onClick={() => session.connect()}
          disabled={
            connectionBlockReason !== null ||
            snapshot.connection === "open" ||
            snapshot.connection === "connecting"
          }
          aria-describedby={
            connectionBlockReason ? "ws-connection-reason" : undefined
          }
        >
          连接
        </Button>
        <Button
          size="sm"
          variant="outline"
          onClick={() => session.disconnect()}
          disabled={!isOpen && snapshot.connection !== "connecting"}
        >
          断开
        </Button>
        <DisabledReason
          id="ws-connection-reason"
          reason={connectionBlockReason}
        />
        <span className="text-[11px] text-faint-foreground">
          接收段 {snapshot.segment} · 新连接即新接收段
        </span>
      </div>

      <div className="mt-4 grid grid-cols-1 gap-4 xl:grid-cols-[minmax(0,1fr)_400px]">
        <div>
          <div className="flex flex-wrap items-center gap-3 text-[11px] text-muted-foreground">
            <span>界面保留 {snapshot.messages.length} / {WS_DEBUG_MESSAGE_LIMIT.toLocaleString("en-US")}</span>
            <span data-testid="ws-local-dropped">
              界面丢弃数 {snapshot.localDropped}
            </span>
            <span data-testid="ws-server-lag">
              服务端丢弃（lag_notice） {snapshot.serverLagDropped}
            </span>
            <span>总接收 {snapshot.totalReceived}</span>
            <span>筛选 {filtered.length} / 总计 {snapshot.messages.length}</span>
          </div>
          <p className="mt-1 text-[11px] text-faint-foreground">
            最多保留 {WS_DEBUG_MESSAGE_LIMIT.toLocaleString("en-US")} 条；本地溢出丢弃最旧项并单独计数，与服务端 lag_notice
            分开。消息只保存在内存，不持久化。
          </p>

          <div className="mt-2 flex flex-wrap items-end gap-2">
            <div className="flex min-w-52 flex-1 flex-col gap-1">
              <Label htmlFor="ws-filter">筛选消息</Label>
              <Input
                id="ws-filter"
                value={filter}
                onChange={(event) => setFilter(event.target.value)}
                placeholder="按 kind、Topic、摘要或原始 JSON 过滤"
              />
            </div>
            <Button
              size="sm"
              variant="outline"
              onClick={() => session.clear()}
            >
              清空
            </Button>
            <Button
              size="sm"
              variant="outline"
              aria-pressed={paused}
              onClick={() => setPaused((value) => !value)}
            >
              {paused ? "恢复自动滚动" : "暂停自动滚动"}
            </Button>
            <span className="text-[11px] text-faint-foreground">
              自动滚动：{paused ? "已暂停" : "开启"}
            </span>
          </div>

          <div
            ref={scrollRef}
            data-testid="ws-message-list"
            className="mt-2 h-[420px] overflow-auto border border-border bg-card"
          >
            <div
              style={{
                height: `${virtualizer.getTotalSize()}px`,
                position: "relative",
                width: "100%",
              }}
            >
              {virtualizer.getVirtualItems().map((virtualItem) => {
                const message = filtered[virtualItem.index];
                if (!message) {
                  return null;
                }
                return (
                  <div
                    key={virtualItem.key}
                    data-testid="ws-message"
                    style={{
                      position: "absolute",
                      top: 0,
                      left: 0,
                      width: "100%",
                      transform: `translateY(${virtualItem.start}px)`,
                    }}
                    className="border-b border-border px-3 py-2"
                  >
                    <div className="flex flex-wrap items-center gap-2 text-[11px] text-faint-foreground">
                      <time dateTime={message.receivedAt}>
                        {formatClock(message.receivedAt)}
                      </time>
                      <span className="font-mono">{message.kind}</span>
                      <span>接收段 {message.segment}</span>
                      {message.topic ? (
                        <span className="font-mono">{message.topic}</span>
                      ) : null}
                    </div>
                    <div className="mt-0.5 text-xs text-foreground">
                      {message.summary}
                    </div>
                    <details className="mt-1">
                      <summary className="cursor-pointer text-[11px] text-muted-foreground">
                        原始 JSON
                      </summary>
                      <pre
                        data-testid="ws-message-detail"
                        className="mt-1 whitespace-pre-wrap break-all font-mono text-[11px] text-muted-foreground"
                      >
                        {message.detailText}
                      </pre>
                    </details>
                  </div>
                );
              })}
            </div>
            {filtered.length === 0 ? (
              <EmptyState
                title={snapshot.messages.length === 0 ? "暂无报文" : "没有匹配的报文"}
                description="连接并订阅后才会收到未来事件；没有初始快照或历史回放。"
              />
            ) : null}
          </div>

          {snapshot.segmentInterrupted ? (
            <p className="mt-2 text-[11px] text-status-critical">
              接收段 {snapshot.segment} 已中断；没有恢复位点，重连不会补齐缺失报文。
            </p>
          ) : null}
        </div>

        <div className="flex flex-col gap-4">
          <section className="border border-border p-3" aria-label="受控 Topic 表单">
            <div className="text-sm font-semibold">受控 Topic 表单</div>
            <p className="mt-0.5 text-[11px] text-faint-foreground">
              Topic 是前缀匹配；订阅没有 ACK。replay.* 不在 Console 范围内。
            </p>

            <div className="mt-2 flex flex-col gap-2">
              <div className="flex flex-col gap-1">
                <Label htmlFor="ws-scope">Topic 范围</Label>
                <Select
                  id="ws-scope"
                  value={scope}
                  disabled={subscribeBlockReason !== null}
                  onChange={(event) =>
                    setScope(event.target.value as TopicScope)
                  }
                >
                  <option value="all">全部 Bar（market.bar.）</option>
                  {BAR_INTERVALS.map((interval) => (
                    <option key={interval} value={interval}>
                      {interval}
                    </option>
                  ))}
                </Select>
              </div>

              {scope !== "all" ? (
                <div className="flex flex-col gap-1">
                  <Label htmlFor="ws-instrument">InstrumentId（可选）</Label>
                  <Input
                    id="ws-instrument"
                    value={instrument}
                    disabled={subscribeBlockReason !== null}
                    onChange={(event) => setInstrument(event.target.value)}
                    placeholder="例如 binance:BTC/USDT；留空表示该 Interval 全部"
                    autoComplete="off"
                    spellCheck={false}
                    aria-invalid={!topicValidation.valid}
                  />
                </div>
              ) : null}

              <div className="text-[11px] text-muted-foreground">
                将订阅：{" "}
                <code data-testid="ws-topic-preview" className="font-mono">
                  {topic}
                </code>
              </div>
              {!topicValidation.valid ? (
                <p className="text-[11px] text-status-critical">
                  {topicValidation.error}
                </p>
              ) : null}

              <div className="flex flex-wrap items-center gap-2">
                <Button
                  size="sm"
                  disabled={!canSendTopic}
                  aria-describedby={
                    subscribeBlockReason ? "ws-subscribe-reason" : undefined
                  }
                  onClick={() => handleTopicAction(false)}
                >
                  订阅
                </Button>
                <Button
                  size="sm"
                  variant="outline"
                  disabled={!canSendTopic}
                  onClick={() => handleTopicAction(true)}
                >
                  取消订阅
                </Button>
                <DisabledReason
                  id="ws-subscribe-reason"
                  reason={subscribeBlockReason}
                />
              </div>
              {topicError ? (
                <p className="text-[11px] text-status-critical">{topicError}</p>
              ) : null}
              {protocolNotice ? (
                <p className="text-[11px] text-muted-foreground">
                  {protocolNotice}
                </p>
              ) : null}

              <ul className="flex flex-col gap-0.5 text-[11px] text-faint-foreground">
                {TOPIC_PREFIX_HINTS.map((hint) => (
                  <li key={hint.topic}>
                    <code className="font-mono text-muted-foreground">
                      {hint.topic}
                    </code>{" "}
                    · {hint.label}
                  </li>
                ))}
              </ul>

              <div className="text-[11px] text-muted-foreground">
                当前会话 Topic 模式：
                {snapshot.topicPatterns.length === 0
                  ? "无"
                  : snapshot.topicPatterns.join("、")}
              </div>
            </div>
          </section>

          <section className="border border-border p-3" aria-label="Ping 顺序屏障">
            <div className="text-sm font-semibold">Ping / Pong 顺序屏障</div>
            <p className="mt-0.5 text-[11px] text-faint-foreground">
              pong 仅证明操作顺序，不是订阅 ACK。
            </p>
            <div className="mt-2 flex flex-wrap items-center gap-2">
              <Button
                size="sm"
                variant="outline"
                disabled={!isOpen}
                onClick={() => session.ping()}
              >
                Ping
              </Button>
              <span data-testid="ws-sync-state" className="text-xs">
                {syncLabel(snapshot)}
              </span>
              <span className="text-[11px] text-faint-foreground">
                最近 pong {formatClock(snapshot.lastPongAt)}
              </span>
            </div>
          </section>

          <section className="border border-border p-3" aria-label="自由 JSON 调试">
            <div className="text-sm font-semibold">自由 JSON 调试</div>
            <p className="mt-0.5 text-[11px] text-faint-foreground">
              白名单仅 subscribe / unsubscribe / ping；没有订阅 ACK。
            </p>
            <Textarea
              aria-label="自由 JSON"
              className="mt-2 font-mono"
              value={freeformText}
              disabled={freeformBlockReason !== null}
              onChange={(event) => setFreeformText(event.target.value)}
              placeholder='{"op":"subscribe","topics":["market.bar.1m.binance:BTC/USDT"]}'
            />
            <div className="mt-2 flex flex-wrap items-center gap-2">
              <Button
                size="sm"
                variant="outline"
                disabled={freeformBlockReason !== null || !isOpen}
                aria-describedby={
                  freeformBlockReason ? "ws-freeform-reason" : undefined
                }
                onClick={handleFreeformSend}
              >
                校验并发送
              </Button>
              <DisabledReason
                id="ws-freeform-reason"
                reason={freeformBlockReason}
              />
            </div>
            {freeformError ? (
              <p className="mt-1 text-[11px] text-status-critical">
                {freeformError}
              </p>
            ) : null}
            {freeformNotice ? (
              <p className="mt-1 text-[11px] text-muted-foreground">
                {freeformNotice}
              </p>
            ) : null}
          </section>
        </div>
      </div>
    </Panel>
  );
}
