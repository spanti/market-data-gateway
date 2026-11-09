import type { BarMessageEnvelope } from "@/api/endpoints/bars";

/**
 * Reusable browser WebSocket transport for the gateway Streaming Interface.
 *
 * Contract (api-contract.md §5):
 * - Client ops are exactly subscribe / unsubscribe / ping.
 * - There is no subscribe ACK. `pong` is only an ordering barrier.
 * - Server messages are `{"op":"event",topic,data}`, `{"op":"pong"}` and
 *   `{"op":"lag_notice",dropped,since}`. Invalid JSON, unknown ops and
 *   malformed shapes are ignored and reported through `onProtocolIssue`.
 * - Each successful open starts a NEW receive segment; close/error ends it.
 *   There is no resume position and no automatic reconnect.
 *
 * This module deliberately carries no local message buffer or pause state;
 * those belong to the screens that consume it.
 */

export type WsTransportState = "idle" | "connecting" | "open" | "closed";

export interface WsTransportStatus {
  state: WsTransportState;
  /** 0 before the first successful open; increments on every successful open. */
  segment: number;
}

export interface WebSocketLike {
  readyState: number;
  send(data: string): void;
  close(code?: number, reason?: string): void;
  onopen: ((event: Event) => void) | null;
  onmessage: ((event: MessageEvent) => void) | null;
  onclose: ((event: CloseEvent) => void) | null;
  onerror: ((event: Event) => void) | null;
}

export type WebSocketFactory = (url: string) => WebSocketLike;

export interface WsOpenInfo {
  segment: number;
  url: string;
}

export interface WsCloseInfo {
  segment: number;
  code: number;
  reason: string;
  wasClean: boolean;
}

export interface WsEventMessage {
  segment: number;
  topic: string;
  data: BarMessageEnvelope;
}

export interface WsPongInfo {
  segment: number;
}

export interface WsLagNoticeInfo {
  segment: number;
  dropped: number;
  since: string;
}

export type WsProtocolIssueCode =
  | "invalid_json"
  | "malformed_message"
  | "unknown_op"
  | "malformed_event"
  | "malformed_lag_notice"
  | "constructor_error"
  | "socket_error"
  | "send_error";

export interface WsProtocolIssue {
  segment: number;
  code: WsProtocolIssueCode;
  message: string;
  raw?: unknown;
}

export interface WsTransportCallbacks {
  onOpen?: (info: WsOpenInfo) => void;
  onClose?: (info: WsCloseInfo) => void;
  onEvent?: (message: WsEventMessage) => void;
  onPong?: (info: WsPongInfo) => void;
  onLagNotice?: (info: WsLagNoticeInfo) => void;
  onProtocolIssue?: (issue: WsProtocolIssue) => void;
}

export interface WsTransportOptions extends WsTransportCallbacks {
  /** Defaults to VITE_WS_URL or same-origin `ws(s)://<location.host>/ws`. */
  url?: string;
  /** Injectable for tests; defaults to the browser WebSocket constructor. */
  createSocket?: WebSocketFactory;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isMessageEnvelope(value: unknown): value is BarMessageEnvelope {
  if (!isRecord(value)) {
    return false;
  }
  if (
    typeof value.schema_version !== "number" ||
    value.data_type !== "bar" ||
    typeof value.instrument_id !== "string" ||
    typeof value.event_ts !== "string" ||
    typeof value.ingest_ts !== "string" ||
    typeof value.feed !== "string" ||
    !(value.seq === null || typeof value.seq === "number")
  ) {
    return false;
  }

  const payload = value.payload;
  if (!isRecord(payload)) {
    return false;
  }
  return (
    typeof payload.interval === "string" &&
    typeof payload.open_ts === "string" &&
    typeof payload.close_ts === "string" &&
    typeof payload.aggregation_source === "string" &&
    typeof payload.open === "string" &&
    typeof payload.high === "string" &&
    typeof payload.low === "string" &&
    typeof payload.close === "string" &&
    typeof payload.volume === "string" &&
    (payload.trade_count === null ||
      typeof payload.trade_count === "number") &&
    typeof payload.revision === "number"
  );
}

export interface WsLocationLike {
  protocol: string;
  host: string;
}

export function deriveWsUrl(
  locationLike: WsLocationLike = window.location,
  configuredUrl: string | undefined = import.meta.env.VITE_WS_URL,
): string {
  if (configuredUrl && configuredUrl.trim().length > 0) {
    return configuredUrl;
  }
  const protocol = locationLike.protocol === "https:" ? "wss:" : "ws:";
  return `${protocol}//${locationLike.host}/ws`;
}

function defaultCreateSocket(url: string): WebSocketLike {
  return new WebSocket(url) as WebSocketLike;
}

export class GatewayWsTransport {
  private readonly url: string;
  private readonly createSocket: WebSocketFactory;
  private readonly callbacks: WsTransportCallbacks;
  private socket: WebSocketLike | null = null;
  private state: WsTransportState = "idle";
  private segment = 0;

  constructor(options: WsTransportOptions = {}) {
    this.url = options.url ?? deriveWsUrl();
    this.createSocket = options.createSocket ?? defaultCreateSocket;
    this.callbacks = {
      onOpen: options.onOpen,
      onClose: options.onClose,
      onEvent: options.onEvent,
      onPong: options.onPong,
      onLagNotice: options.onLagNotice,
      onProtocolIssue: options.onProtocolIssue,
    };
  }

  get status(): WsTransportStatus {
    return { state: this.state, segment: this.segment };
  }

  get targetUrl(): string {
    return this.url;
  }

  open(): void {
    if (this.state === "connecting" || this.state === "open") {
      return;
    }

    let socket: WebSocketLike;
    try {
      socket = this.createSocket(this.url);
    } catch (error) {
      const message =
        error instanceof Error
          ? error.message
          : "WebSocket constructor failed";
      this.state = "closed";
      this.issue("constructor_error", message, error);
      this.callbacks.onClose?.({
        segment: this.segment,
        code: 1006,
        reason: message,
        wasClean: false,
      });
      return;
    }

    this.socket = socket;
    this.state = "connecting";

    socket.onopen = () => {
      if (this.socket !== socket) {
        return;
      }
      this.state = "open";
      this.segment += 1;
      this.callbacks.onOpen?.({ segment: this.segment, url: this.url });
    };

    socket.onmessage = (event) => {
      if (this.socket !== socket) {
        return;
      }
      this.handleMessage(event.data);
    };

    socket.onclose = (event) => {
      if (this.socket !== socket) {
        return;
      }
      this.socket = null;
      this.state = "closed";
      this.callbacks.onClose?.({
        segment: this.segment,
        code: event.code,
        reason: event.reason,
        wasClean: event.wasClean,
      });
    };

    socket.onerror = () => {
      if (this.socket !== socket) {
        return;
      }
      this.finishWithError(socket, "WebSocket transport error");
    };
  }

  /**
   * Client-initiated close. It ends the current receive segment; any later
   * events from the stale socket are ignored. It never reopens.
   */
  close(code = 1000, reason = "client closed"): void {
    const socket = this.socket;
    if (!socket) {
      this.state = "closed";
      return;
    }

    this.socket = null;
    this.state = "closed";
    try {
      socket.close(code, reason);
    } catch {
      // A socket that is already gone has nothing left to close.
    }
    this.callbacks.onClose?.({
      segment: this.segment,
      code,
      reason,
      wasClean: true,
    });
  }

  subscribe(topics: readonly string[]): boolean {
    return this.sendJson({ op: "subscribe", topics });
  }

  unsubscribe(topics: readonly string[]): boolean {
    return this.sendJson({ op: "unsubscribe", topics });
  }

  ping(): boolean {
    return this.sendJson({ op: "ping" });
  }

  private sendJson(payload: unknown): boolean {
    if (this.state !== "open" || this.socket === null) {
      return false;
    }
    try {
      this.socket.send(JSON.stringify(payload));
      return true;
    } catch (error) {
      this.issue(
        "send_error",
        error instanceof Error ? error.message : "WebSocket send failed",
        error,
      );
      return false;
    }
  }

  private finishWithError(socket: WebSocketLike, message: string): void {
    this.socket = null;
    this.state = "closed";
    this.issue("socket_error", message);
    try {
      socket.close();
    } catch {
      // Ignore close failures on a broken socket.
    }
    this.callbacks.onClose?.({
      segment: this.segment,
      code: 1006,
      reason: message,
      wasClean: false,
    });
  }

  private issue(
    code: WsProtocolIssueCode,
    message: string,
    raw?: unknown,
  ): void {
    this.callbacks.onProtocolIssue?.({
      segment: this.segment,
      code,
      message,
      ...(raw !== undefined ? { raw } : {}),
    });
  }

  private handleMessage(data: unknown): void {
    if (typeof data !== "string") {
      this.issue(
        "invalid_json",
        "Non-text WebSocket frame is ignored",
        data,
      );
      return;
    }

    let parsed: unknown;
    try {
      parsed = JSON.parse(data);
    } catch {
      this.issue("invalid_json", "WebSocket message is not valid JSON", data);
      return;
    }

    if (!isRecord(parsed)) {
      this.issue(
        "malformed_message",
        "WebSocket message is not a JSON object",
        parsed,
      );
      return;
    }

    switch (parsed.op) {
      case "pong":
        this.callbacks.onPong?.({ segment: this.segment });
        return;

      case "event":
        if (
          typeof parsed.topic !== "string" ||
          parsed.topic.length === 0 ||
          !isMessageEnvelope(parsed.data)
        ) {
          this.issue(
            "malformed_event",
            "WebSocket event does not match topic + MessageEnvelope",
            parsed,
          );
          return;
        }
        this.callbacks.onEvent?.({
          segment: this.segment,
          topic: parsed.topic,
          data: parsed.data,
        });
        return;

      case "lag_notice":
        if (
          typeof parsed.dropped !== "number" ||
          !Number.isFinite(parsed.dropped) ||
          parsed.dropped < 0 ||
          typeof parsed.since !== "string" ||
          parsed.since.length === 0
        ) {
          this.issue(
            "malformed_lag_notice",
            "WebSocket lag_notice requires dropped >= 0 and since",
            parsed,
          );
          return;
        }
        this.callbacks.onLagNotice?.({
          segment: this.segment,
          dropped: parsed.dropped,
          since: parsed.since,
        });
        return;

      default:
        this.issue(
          "unknown_op",
          `Unknown WebSocket op: ${String(parsed.op)}`,
          parsed,
        );
    }
  }
}
