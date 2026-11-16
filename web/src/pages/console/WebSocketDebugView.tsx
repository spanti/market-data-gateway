import { useVirtualizer } from "@tanstack/react-virtual";
import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  useSyncExternalStore,
  type ReactNode,
} from "react";
import { DisabledReason } from "@/components/disabled-reason";
import { HelpTrigger } from "@/components/help-trigger";
import {
  EmptyState,
  ErrorState,
  LoadingState,
  UnknownState,
} from "@/components/states";
import { StatusBadge, type StatusTone } from "@/components/status-badge";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
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
 *
 * WebSocket Session Subscription only filters this debug session's topics. It
 * must never be confused with Feed Subscription, which changes the actual
 * collector acquisition scope in the Admin API contract.
 */

export interface WebSocketDebugViewProps {
  /**
   * Injectable for tests; defaults to a real transport-backed session. The
   * resolved target URL is passed for assertion/documentation; test factories
   * may ignore it and inject their own transport.
   */
  sessionFactory?: (targetUrl: string | null) => WsDebugSession;
}

const WEBSOCKET_DEBUG_HELP =
  "WebSocket Debug 使用网关 WebSocket。WebSocket Session Subscription 只筛选当前调试会话的 Topic，不改变 Feed Subscription 的采集配置与采集范围。订阅没有 ACK，pong 只是顺序屏障。每个成功连接都会开始新的接收段，断线后没有恢复位点。报文只保存在内存，最多保留 10,000 条。原始 JSON 原样展示。";
const SESSION_SUBSCRIPTION_HELP =
  "WebSocket Session Subscription 只影响当前浏览器调试会话接收哪些 Topic。它不会修改 Collector 的 Feed Subscription，也不会扩大或缩小实际采集范围。Topic 按前缀匹配，订阅没有 ACK。replay.* 不在 Console 范围内。";
const MESSAGE_BUFFER_HELP =
  "界面最多保留 10,000 条报文，溢出时丢弃最旧项并计入界面丢弃数。服务端 lag_notice 的 dropped 是服务端队列丢弃，单独累计，两者不能互相替代。报文只保存在内存，不持久化。";
const PING_HELP =
  "Ping 发送客户端 ping，服务端以 pong 回应。pong 只证明此前操作已被按序处理，是顺序屏障，不是订阅 ACK。";
const FREEFORM_HELP =
  "自由 JSON 仅允许 subscribe / unsubscribe / ping，Topic 仍需符合受控语法。没有订阅 ACK。编辑器按服务端 Access Role 开放，界面禁用不替代服务端拒绝。";

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

interface ConnectionStatus {
  tone: StatusTone;
  label: string;
}

function connectionStatus(snapshot: WsDebugSnapshot): ConnectionStatus {
  switch (snapshot.connection) {
    case "idle":
      return { tone: "unknown", label: "未连接" };
    case "connecting":
      return { tone: "warning", label: "连接中…" };
    case "open":
      return { tone: "good", label: "已连接" };
    case "closed":
      return snapshot.segmentInterrupted
        ? { tone: "critical", label: `接收段 ${snapshot.segment} 中断` }
        : { tone: "unknown", label: "连接失败" };
  }
}

function syncStatus(snapshot: WsDebugSnapshot): ConnectionStatus {
  switch (snapshot.syncState) {
    case "synced":
      return { tone: "good", label: "已同步" };
    case "waiting":
      return { tone: "warning", label: "等待 pong…" };
    case "idle":
      return { tone: "unknown", label: "未同步" };
  }
}

function GroupTitleWithHelp({
  title,
  helpLabel,
  help,
}: {
  title: string;
  helpLabel: string;
  help: string;
}) {
  return (
    <div className="flex min-w-0 items-center gap-2">
      <h3 className="text-sm font-semibold text-foreground">{title}</h3>
      <HelpTrigger label={helpLabel} content={help} side="top" />
    </div>
  );
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
  const connectionFailure =
    snapshot.connection === "closed" &&
    !snapshot.segmentInterrupted &&
    snapshot.lastClose !== null &&
    !snapshot.lastClose.wasClean;

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
      setTopicError("无法发送。WebSocket 未连接或 Topic 无效。");
      return;
    }
    setProtocolNotice(
      `${unsubscribe ? "取消订阅" : "订阅"}已发送。没有订阅 ACK，可用 Ping 确认操作顺序。`,
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
    setFreeformNotice("已通过白名单并发送。没有订阅 ACK。");
  };

  const connection = connectionStatus(snapshot);
  const sync = syncStatus(snapshot);
  const hasMessages = snapshot.messages.length > 0;

  let messageListState: ReactNode = null;
  if (hasMessages && filtered.length === 0) {
    messageListState = (
      <EmptyState
        className="m-3"
        title="没有匹配的报文"
        description="调整筛选条件后重试。"
      />
    );
  } else if (!hasMessages) {
    if (capability === null) {
      messageListState = (
        <LoadingState className="m-3" label="正在发现运行时能力…" />
      );
    } else if (snapshot.connection === "connecting") {
      messageListState = (
        <LoadingState className="m-3" label="正在连接 WebSocket…" />
      );
    } else if (connectionFailure) {
      // The explicit ErrorState above the grid carries the raw close reason.
      messageListState = null;
    } else if (
      connectionBlockReason?.code === "capabilities-unknown"
    ) {
      messageListState = (
        <UnknownState
          className="m-3"
          cause="failed"
          reason="运行时能力发现失败，无法确认 WebSocket 目标。"
        />
      );
    } else {
      messageListState = (
        <EmptyState
          className="m-3"
          title={isOpen ? "暂无报文" : "尚未连接"}
          description={
            isOpen
              ? "已连接，等待订阅后的新事件，没有初始快照或历史回放。"
              : "连接并订阅后才会收到未来事件，没有初始快照或历史回放。"
          }
        />
      );
    }
  }

  return (
    <Card role="region" aria-label="WebSocket Debug" className="mt-5">
      <CardHeader className="p-5 pb-0">
        <div className="min-w-0">
          <div className="flex min-w-0 items-center gap-2">
            <CardTitle>WebSocket Debug</CardTitle>
            <HelpTrigger
              label="WebSocket Debug 说明"
              content={WEBSOCKET_DEBUG_HELP}
              side="bottom"
            />
          </div>
          <p className="mt-1 max-w-3xl text-xs leading-relaxed text-muted-foreground">
            WebSocket Session Subscription 只筛选当前调试会话的 Topic，不改变
            Feed Subscription 的采集范围。
          </p>
        </div>
        <div className="flex flex-col items-start gap-1 text-[11px] text-faint-foreground sm:items-end">
          <StatusBadge
            data-testid="ws-connection-state"
            tone={connection.tone}
            live
          >
            {connection.label}
          </StatusBadge>
          <span data-testid="ws-segment-state">接收段 {snapshot.segment}</span>
          <span className="max-w-full break-all">
            目标 {wsTarget.url ?? "不可用"}
          </span>
          <span>连接时间 {formatClock(snapshot.connectedAt)}</span>
        </div>
      </CardHeader>

      <CardContent className="pt-4">
        <div className="flex flex-wrap items-center gap-2">
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
        </div>

        {snapshot.segmentInterrupted ? (
          <p
            data-testid="ws-interruption"
            role="alert"
            className="mt-3 rounded-lg border border-status-critical bg-status-critical-bg px-3 py-2 text-xs font-medium text-status-critical-fg"
          >
            接收段 {snapshot.segment} 已中断。没有恢复位点，重连不会补齐缺失报文。
          </p>
        ) : null}

        {connectionFailure && snapshot.lastClose ? (
          <div data-testid="ws-connection-error" className="mt-3">
            <ErrorState
              title="连接失败"
              description={
                <span
                  data-raw-evidence="true"
                  className="break-all font-mono"
                >
                  code {snapshot.lastClose.code}{" "}
                  {snapshot.lastClose.reason || "没有服务端原因"}
                </span>
              }
              onRetry={
                connectionBlockReason === null
                  ? () => session.connect()
                  : undefined
              }
            />
          </div>
        ) : null}

        <div className="mt-4 grid grid-cols-1 gap-4 xl:grid-cols-[minmax(0,1fr)_390px]">
          <Card
            role="group"
            aria-label="报文缓冲区"
            className="min-w-0 bg-surface-sunken shadow-none"
          >
            <CardContent className="p-4">
              <GroupTitleWithHelp
                title="报文缓冲区"
                helpLabel="报文缓冲区说明"
                help={MESSAGE_BUFFER_HELP}
              />
              <div className="mt-2 flex flex-wrap items-center gap-x-4 gap-y-1 text-[11px] text-muted-foreground">
                <span data-testid="ws-retained">
                  界面保留 {snapshot.messages.length} /{" "}
                  {WS_DEBUG_MESSAGE_LIMIT.toLocaleString("en-US")}
                </span>
                <span data-testid="ws-local-dropped">
                  界面丢弃数 {snapshot.localDropped}
                </span>
                <span data-testid="ws-server-lag">
                  服务端丢弃（lag_notice） {snapshot.serverLagDropped}
                </span>
                <span>总接收 {snapshot.totalReceived}</span>
                <span>
                  筛选 {filtered.length} / 总计 {snapshot.messages.length}
                </span>
              </div>
              <p className="mt-1 text-[11px] leading-relaxed text-faint-foreground">
                最多保留 {WS_DEBUG_MESSAGE_LIMIT.toLocaleString("en-US")}{" "}
                条，仅保存在内存，不持久化。界面丢弃与服务端 lag_notice
                分开计数。
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
                <StatusBadge
                  data-testid="ws-autoscroll-state"
                  tone={paused ? "warning" : "info"}
                >
                  {paused ? "自动滚动已暂停" : "自动滚动开启"}
                </StatusBadge>
              </div>

              <div
                ref={scrollRef}
                data-testid="ws-message-list"
                className="mt-3 h-[420px] overflow-auto rounded-lg border border-card-border bg-card"
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
                        className="border-b border-card-border px-3 py-2"
                      >
                        <div className="flex flex-wrap items-center gap-2 text-[11px] text-faint-foreground">
                          <time dateTime={message.receivedAt}>
                            {formatClock(message.receivedAt)}
                          </time>
                          <span className="font-mono">{message.kind}</span>
                          <span>接收段 {message.segment}</span>
                          {message.topic ? (
                            <span className="break-all font-mono">
                              {message.topic}
                            </span>
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
                            data-raw-evidence="true"
                            className="mt-1 max-h-60 overflow-auto whitespace-pre-wrap break-all rounded-md bg-surface-sunken p-2 font-mono text-[11px] text-muted-foreground"
                          >
                            {message.detailText}
                          </pre>
                        </details>
                      </div>
                    );
                  })}
                </div>
                {messageListState}
              </div>
            </CardContent>
          </Card>

          <div className="flex min-w-0 flex-col gap-4">
            <Card
              role="group"
              aria-label="WebSocket Session Subscription"
              className="min-w-0 bg-surface-sunken shadow-none"
            >
              <CardContent className="p-4">
                <GroupTitleWithHelp
                  title="WebSocket Session Subscription"
                  helpLabel="WebSocket Session Subscription 说明"
                  help={SESSION_SUBSCRIPTION_HELP}
                />
                <p className="mt-1 text-[11px] leading-relaxed text-muted-foreground">
                  只筛选当前调试会话的 Topic，不改变 Feed Subscription 的采集范围。
                </p>

                <div className="mt-3 flex flex-col gap-2">
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
                      <Label htmlFor="ws-instrument">
                        InstrumentId（可选）
                      </Label>
                      <Input
                        id="ws-instrument"
                        value={instrument}
                        disabled={subscribeBlockReason !== null}
                        onChange={(event) => setInstrument(event.target.value)}
                        placeholder="例如 binance:BTC/USDT，留空表示该 Interval 全部"
                        autoComplete="off"
                        spellCheck={false}
                        aria-invalid={!topicValidation.valid}
                      />
                    </div>
                  ) : null}

                  <div className="flex flex-wrap items-baseline gap-x-1.5 text-[11px] text-muted-foreground">
                    <span>将订阅</span>
                    <code
                      data-testid="ws-topic-preview"
                      className="min-w-0 break-all font-mono"
                    >
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
                    <p className="text-[11px] text-status-critical">
                      {topicError}
                    </p>
                  ) : null}
                  {protocolNotice ? (
                    <p className="text-[11px] text-muted-foreground">
                      {protocolNotice}
                    </p>
                  ) : null}

                  <ul className="flex flex-col gap-1 text-[11px] text-faint-foreground">
                    {TOPIC_PREFIX_HINTS.map((hint) => (
                      <li
                        key={hint.topic}
                        className="flex min-w-0 flex-col gap-0.5"
                      >
                        <code className="break-all font-mono text-muted-foreground">
                          {hint.topic}
                        </code>
                        <span>{hint.label}</span>
                      </li>
                    ))}
                  </ul>

                  <div className="flex flex-wrap items-baseline gap-x-1.5 text-[11px] text-muted-foreground">
                    <span>当前会话 Topic 模式</span>
                    <span
                      data-testid="ws-topic-patterns"
                      className="min-w-0 break-all font-mono"
                    >
                      {snapshot.topicPatterns.length === 0
                        ? "无"
                        : snapshot.topicPatterns.join("、")}
                    </span>
                  </div>
                </div>
              </CardContent>
            </Card>

            <Card
              role="group"
              aria-label="Ping 顺序屏障"
              className="min-w-0 bg-surface-sunken shadow-none"
            >
              <CardContent className="p-4">
                <GroupTitleWithHelp
                  title="Ping / Pong"
                  helpLabel="Ping 顺序屏障说明"
                  help={PING_HELP}
                />
                <p className="mt-1 text-[11px] leading-relaxed text-muted-foreground">
                  pong 仅证明操作顺序，不是订阅 ACK。
                </p>
                <div className="mt-3 flex flex-wrap items-center gap-2">
                  <Button
                    size="sm"
                    variant="outline"
                    disabled={!isOpen}
                    onClick={() => session.ping()}
                  >
                    Ping
                  </Button>
                  <StatusBadge data-testid="ws-sync-state" tone={sync.tone} live>
                    {sync.label}
                  </StatusBadge>
                  <span className="text-[11px] text-faint-foreground">
                    最近 pong {formatClock(snapshot.lastPongAt)}
                  </span>
                </div>
              </CardContent>
            </Card>

            <Card
              role="group"
              aria-label="自由 JSON 调试"
              className="min-w-0 bg-surface-sunken shadow-none"
            >
              <CardContent className="p-4">
                <GroupTitleWithHelp
                  title="自由 JSON"
                  helpLabel="自由 JSON 调试说明"
                  help={FREEFORM_HELP}
                />
                <p className="mt-1 text-[11px] leading-relaxed text-muted-foreground">
                  白名单仅 subscribe / unsubscribe / ping，没有订阅 ACK。
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
              </CardContent>
            </Card>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
