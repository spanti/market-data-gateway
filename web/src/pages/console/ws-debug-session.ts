import {
  GatewayWsTransport,
  type WebSocketFactory,
  type WsCloseInfo,
  type WsEventMessage,
  type WsLagNoticeInfo,
  type WsOpenInfo,
  type WsProtocolIssue,
  type WsPongInfo,
} from "@/api/ws/transport";
import {
  appendMessage,
  clearMessages,
  createMessageBuffer,
  type DebugMessage,
  type DebugMessageKind,
  type MessageBufferState,
} from "@/pages/console/message-buffer";
import {
  parseFreeformFrame,
  validateControlledTopic,
} from "@/pages/console/topics";

/**
 * WebSocket Debug session.
 *
 * Wraps GatewayWsTransport with the console-specific state: bounded message
 * buffer, local drop counter, server lag accumulator, ordering-barrier sync
 * state and receive segments. It never persists messages and never claims a
 * subscription ACK; `pong` only proves op ordering.
 */

export type WsConnectionState = "idle" | "connecting" | "open" | "closed";

export type WsSyncState = "idle" | "waiting" | "synced";

export interface WsDebugSessionOptions {
  url?: string;
  createSocket?: WebSocketFactory;
  limit?: number;
  now?: () => Date;
}

export interface WsCloseSummary {
  code: number;
  reason: string;
  at: string;
}

export interface WsDebugSnapshot {
  connection: WsConnectionState;
  segment: number;
  /** True when a previously open receive segment ended without recovery. */
  segmentInterrupted: boolean;
  targetUrl: string;
  connectedAt: string | null;
  lastClose: WsCloseSummary | null;
  messages: readonly DebugMessage[];
  localDropped: number;
  totalReceived: number;
  /** Sum of server lag_notice.dropped values (server-side queue drops). */
  serverLagDropped: number;
  serverLagLastAt: string | null;
  lastPongAt: string | null;
  syncState: WsSyncState;
  topicPatterns: readonly string[];
}

export type FreeformSendResult = { ok: true } | { ok: false; error: string };

function describeEvent(message: WsEventMessage): string {
  const { data } = message;
  return `${data.instrument_id} · ${data.payload.interval} · close ${data.payload.close}`;
}

export class WsDebugSession {
  private readonly transport: GatewayWsTransport;
  private readonly now: () => Date;
  private readonly listeners = new Set<() => void>();

  private buffer: MessageBufferState;
  private connection: WsConnectionState = "idle";
  private segment = 0;
  private segmentInterrupted = false;
  private connectedAt: string | null = null;
  private lastClose: WsCloseSummary | null = null;
  private serverLagDropped = 0;
  private serverLagLastAt: string | null = null;
  private lastPongAt: string | null = null;
  private syncState: WsSyncState = "idle";
  private topicPatterns: string[] = [];
  private snapshot: WsDebugSnapshot;

  constructor(options: WsDebugSessionOptions = {}) {
    this.now = options.now ?? (() => new Date());
    this.buffer = createMessageBuffer(options.limit);
    this.transport = new GatewayWsTransport({
      ...(options.url !== undefined ? { url: options.url } : {}),
      ...(options.createSocket !== undefined
        ? { createSocket: options.createSocket }
        : {}),
      onOpen: (info) => this.handleOpen(info),
      onClose: (info) => this.handleClose(info),
      onEvent: (message) => this.handleEvent(message),
      onPong: (info) => this.handlePong(info),
      onLagNotice: (info) => this.handleLagNotice(info),
      onProtocolIssue: (issue) => this.handleProtocolIssue(issue),
    });
    this.snapshot = this.buildSnapshot();
  }

  getSnapshot(): WsDebugSnapshot {
    return this.snapshot;
  }

  subscribeToChanges(listener: () => void): () => void {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }

  connect(): void {
    if (this.connection === "connecting" || this.connection === "open") {
      return;
    }
    this.connection = "connecting";
    this.emit();
    this.transport.open();
  }

  disconnect(): void {
    this.transport.close();
  }

  subscribe(topic: string): boolean {
    const validation = validateControlledTopic(topic);
    if (!validation.valid) {
      return false;
    }
    if (this.topicPatterns.includes(validation.topic)) {
      // The server subscription container is a set; do not resend a
      // duplicate frame just because the form was submitted twice.
      return this.transport.status.state === "open";
    }
    if (!this.transport.subscribe([validation.topic])) {
      return false;
    }
    this.topicPatterns = [...this.topicPatterns, validation.topic];
    this.emit();
    return true;
  }

  unsubscribe(topic: string): boolean {
    const validation = validateControlledTopic(topic);
    if (!validation.valid) {
      return false;
    }
    if (!this.transport.unsubscribe([validation.topic])) {
      return false;
    }
    this.topicPatterns = this.topicPatterns.filter(
      (item) => item !== validation.topic,
    );
    this.emit();
    return true;
  }

  /**
   * `pong` is only an ordering barrier. Waiting for it never means a
   * subscription was acknowledged.
   */
  ping(): boolean {
    if (!this.transport.ping()) {
      return false;
    }
    this.syncState = "waiting";
    this.emit();
    return true;
  }

  sendFreeform(text: string): FreeformSendResult {
    const parsed = parseFreeformFrame(text);
    if (!parsed.ok) {
      return parsed;
    }

    const frame = parsed.frame;
    if (frame.op === "ping") {
      return this.ping()
        ? { ok: true }
        : { ok: false, error: "WebSocket 未连接" };
    }

    const sent =
      frame.op === "subscribe"
        ? this.transport.subscribe(frame.topics)
        : this.transport.unsubscribe(frame.topics);
    if (!sent) {
      return { ok: false, error: "WebSocket 未连接" };
    }

    if (frame.op === "subscribe") {
      this.topicPatterns = [
        ...this.topicPatterns,
        ...frame.topics.filter((topic) => !this.topicPatterns.includes(topic)),
      ];
    } else {
      this.topicPatterns = this.topicPatterns.filter(
        (topic) => !frame.topics.includes(topic),
      );
    }
    this.emit();
    return { ok: true };
  }

  clear(): void {
    this.buffer = clearMessages(this.buffer);
    this.emit();
  }

  private append(input: {
    kind: DebugMessageKind;
    segment: number;
    summary: string;
    detailText: string;
    topic?: string;
  }): void {
    const receivedAt = this.now().toISOString();
    this.buffer = appendMessage(this.buffer, {
      ...input,
      receivedAt,
    });
  }

  private handleOpen(info: WsOpenInfo): void {
    this.connection = "open";
    this.segment = info.segment;
    this.segmentInterrupted = false;
    this.connectedAt = this.now().toISOString();
    this.lastClose = null;
    this.append({
      kind: "connection",
      segment: info.segment,
      summary: `接收段 ${info.segment} 已连接`,
      detailText: JSON.stringify(
        { op: "connection", state: "open", segment: info.segment, url: info.url },
        null,
        2,
      ),
    });
    this.emit();
  }

  private handleClose(info: WsCloseInfo): void {
    this.connection = "closed";
    this.segment = info.segment;
    this.lastClose = {
      code: info.code,
      reason: info.reason,
      at: this.now().toISOString(),
    };
    this.syncState = "idle";
    if (info.segment > 0) {
      this.segmentInterrupted = true;
      this.append({
        kind: "connection",
        segment: info.segment,
        summary: `接收段 ${info.segment} 中断（code ${info.code}）；无恢复位点`,
        detailText: JSON.stringify(
          {
            op: "connection",
            state: "closed",
            segment: info.segment,
            code: info.code,
            reason: info.reason,
            was_clean: info.wasClean,
          },
          null,
          2,
        ),
      });
    }
    this.emit();
  }

  private handleEvent(message: WsEventMessage): void {
    this.append({
      kind: "event",
      segment: message.segment,
      topic: message.topic,
      summary: describeEvent(message),
      detailText: JSON.stringify(message.data, null, 2),
    });
    this.emit();
  }

  private handlePong(info: WsPongInfo): void {
    this.syncState = "synced";
    this.lastPongAt = this.now().toISOString();
    this.append({
      kind: "pong",
      segment: info.segment,
      summary: "pong · 已同步（顺序屏障，不是订阅 ACK）",
      detailText: JSON.stringify({ op: "pong" }, null, 2),
    });
    this.emit();
  }

  private handleLagNotice(info: WsLagNoticeInfo): void {
    this.serverLagDropped += info.dropped;
    this.serverLagLastAt = this.now().toISOString();
    this.append({
      kind: "lag_notice",
      segment: info.segment,
      summary: `lag_notice · 服务端丢弃 ${info.dropped} 条`,
      detailText: JSON.stringify(
        { op: "lag_notice", dropped: info.dropped, since: info.since },
        null,
        2,
      ),
    });
    this.emit();
  }

  private handleProtocolIssue(issue: WsProtocolIssue): void {
    this.append({
      kind: "protocol_issue",
      segment: issue.segment,
      summary: `协议问题：${issue.code}`,
      detailText: JSON.stringify(
        { code: issue.code, message: issue.message },
        null,
        2,
      ),
    });
    this.emit();
  }

  private buildSnapshot(): WsDebugSnapshot {
    return {
      connection: this.connection,
      segment: this.segment,
      segmentInterrupted: this.segmentInterrupted,
      targetUrl: this.transport.targetUrl,
      connectedAt: this.connectedAt,
      lastClose: this.lastClose,
      messages: this.buffer.messages,
      localDropped: this.buffer.localDropped,
      totalReceived: this.buffer.totalReceived,
      serverLagDropped: this.serverLagDropped,
      serverLagLastAt: this.serverLagLastAt,
      lastPongAt: this.lastPongAt,
      syncState: this.syncState,
      topicPatterns: this.topicPatterns,
    };
  }

  private emit(): void {
    this.snapshot = this.buildSnapshot();
    for (const listener of this.listeners) {
      listener();
    }
  }
}
