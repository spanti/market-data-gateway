import { describe, expect, it, vi } from "vitest";
import {
  deriveWsUrl,
  GatewayWsTransport,
  type WebSocketLike,
  type WsTransportOptions,
} from "@/api/ws/transport";

const envelope = {
  schema_version: 1,
  data_type: "bar",
  instrument_id: "binance:BTC/USDT",
  event_ts: "2024-01-01T00:01:00.000000000Z",
  ingest_ts: "2026-11-09T04:24:42.946489000Z",
  feed: "fake",
  seq: null,
  payload: {
    interval: "1m",
    open_ts: "2024-01-01T00:00:00.000000000Z",
    close_ts: "2024-01-01T00:01:00.000000000Z",
    aggregation_source: "external",
    open: "65000.1",
    high: "65010.0",
    low: "64990.0",
    close: "65005.5",
    volume: "12.34",
    trade_count: 128,
    revision: 0,
  },
};

class FakeWebSocket implements WebSocketLike {
  readyState = 0;
  onopen: WebSocketLike["onopen"] = null;
  onmessage: WebSocketLike["onmessage"] = null;
  onclose: WebSocketLike["onclose"] = null;
  onerror: WebSocketLike["onerror"] = null;
  readonly sent: string[] = [];
  closed: { code: number | undefined; reason: string | undefined } | null = null;

  send(data: string): void {
    if (this.readyState !== 1) {
      throw new Error("fake send while not open");
    }
    this.sent.push(data);
  }

  close(code?: number, reason?: string): void {
    this.readyState = 3;
    this.closed = { code, reason };
    this.onclose?.({
      code: code ?? 1000,
      reason: reason ?? "",
      wasClean: true,
    } as CloseEvent);
  }

  fireOpen(): void {
    this.readyState = 1;
    this.onopen?.(new Event("open"));
  }

  fireMessage(payload: unknown): void {
    this.onmessage?.({
      data:
        typeof payload === "string" ? payload : JSON.stringify(payload),
    } as MessageEvent);
  }

  fireServerClose(
    code = 1006,
    reason = "server gone",
    wasClean = false,
  ): void {
    this.readyState = 3;
    this.onclose?.({ code, reason, wasClean } as CloseEvent);
  }

  fireError(): void {
    this.onerror?.(new Event("error"));
  }
}

interface Harness {
  transport: GatewayWsTransport;
  sockets: FakeWebSocket[];
  factory: ReturnType<typeof vi.fn>;
  callbacks: {
    onOpen: ReturnType<typeof vi.fn>;
    onClose: ReturnType<typeof vi.fn>;
    onEvent: ReturnType<typeof vi.fn>;
    onPong: ReturnType<typeof vi.fn>;
    onLagNotice: ReturnType<typeof vi.fn>;
    onProtocolIssue: ReturnType<typeof vi.fn>;
  };
}

function setup(overrides: Partial<WsTransportOptions> = {}): Harness {
  const sockets: FakeWebSocket[] = [];
  const callbacks = {
    onOpen: vi.fn(),
    onClose: vi.fn(),
    onEvent: vi.fn(),
    onPong: vi.fn(),
    onLagNotice: vi.fn(),
    onProtocolIssue: vi.fn(),
  };
  const factory = vi.fn(() => {
    const socket = new FakeWebSocket();
    sockets.push(socket);
    return socket;
  });
  const transport = new GatewayWsTransport({
    url: "ws://test.invalid/ws",
    createSocket: factory,
    ...callbacks,
    ...overrides,
  });
  return { transport, sockets, factory, callbacks };
}

describe("client payloads", () => {
  it("sends exact subscribe, unsubscribe and ping JSON frames", () => {
    const { transport, sockets, callbacks } = setup();
    transport.open();
    sockets[0]?.fireOpen();

    expect(
      transport.subscribe(["market.bar.1m.binance:BTC/USDT"]),
    ).toBe(true);
    expect(transport.unsubscribe(["market.bar."])).toBe(true);
    expect(transport.ping()).toBe(true);

    expect(sockets[0]?.sent).toEqual([
      '{"op":"subscribe","topics":["market.bar.1m.binance:BTC/USDT"]}',
      '{"op":"unsubscribe","topics":["market.bar."]}',
      '{"op":"ping"}',
    ]);
    expect(callbacks.onOpen).toHaveBeenCalledWith({
      segment: 1,
      url: "ws://test.invalid/ws",
    });
  });
});

describe("server message parsing", () => {
  it("parses pong, event and lag_notice into typed callbacks", () => {
    const { transport, sockets, callbacks } = setup();
    transport.open();
    sockets[0]?.fireOpen();

    sockets[0]?.fireMessage({ op: "pong" });
    sockets[0]?.fireMessage({
      op: "event",
      topic: "market.bar.1m.binance:BTC/USDT",
      data: envelope,
    });
    sockets[0]?.fireMessage({
      op: "lag_notice",
      dropped: 12,
      since: "2026-11-09T12:00:00.000000000Z",
    });

    expect(callbacks.onPong).toHaveBeenCalledWith({ segment: 1 });
    expect(callbacks.onEvent).toHaveBeenCalledWith({
      segment: 1,
      topic: "market.bar.1m.binance:BTC/USDT",
      data: envelope,
    });
    expect(callbacks.onLagNotice).toHaveBeenCalledWith({
      segment: 1,
      dropped: 12,
      since: "2026-11-09T12:00:00.000000000Z",
    });
    expect(callbacks.onProtocolIssue).not.toHaveBeenCalled();
  });

  it("accepts a raw JSON string frame", () => {
    const { transport, sockets, callbacks } = setup();
    transport.open();
    sockets[0]?.fireOpen();

    sockets[0]?.fireMessage(JSON.stringify({ op: "pong" }));

    expect(callbacks.onPong).toHaveBeenCalledTimes(1);
  });

  it("ignores invalid JSON, unknown ops and malformed shapes without throwing", () => {
    const { transport, sockets, callbacks } = setup();
    transport.open();
    sockets[0]?.fireOpen();

    expect(() => {
      sockets[0]?.fireMessage("{not-json");
      sockets[0]?.fireMessage({ op: "future-op" });
      sockets[0]?.fireMessage({});
      sockets[0]?.fireMessage(42);
      sockets[0]?.fireMessage({
        op: "event",
        topic: "",
        data: envelope,
      });
      sockets[0]?.fireMessage({
        op: "event",
        topic: "market.bar.1m.binance:BTC/USDT",
        data: { bogus: true },
      });
      sockets[0]?.fireMessage({
        op: "lag_notice",
        dropped: "12",
        since: "2026-11-09T12:00:00Z",
      });
      sockets[0]?.fireMessage({ op: "lag_notice", dropped: 2 });
    }).not.toThrow();

    expect(callbacks.onEvent).not.toHaveBeenCalled();
    expect(callbacks.onPong).not.toHaveBeenCalled();
    expect(callbacks.onLagNotice).not.toHaveBeenCalled();
    expect(
      callbacks.onProtocolIssue.mock.calls.map(
        (call) => (call[0] as { code: string }).code,
      ),
    ).toEqual([
      "invalid_json",
      "unknown_op",
      "unknown_op",
      "malformed_message",
      "malformed_event",
      "malformed_event",
      "malformed_lag_notice",
      "malformed_lag_notice",
    ]);
  });
});

describe("receive segments", () => {
  it("starts a new segment on every successful open and ends it on close", () => {
    const { transport, sockets, callbacks } = setup();

    transport.open();
    expect(transport.status).toEqual({ state: "connecting", segment: 0 });
    sockets[0]?.fireOpen();
    expect(transport.status).toEqual({ state: "open", segment: 1 });

    transport.close();
    expect(transport.status).toEqual({ state: "closed", segment: 1 });
    expect(callbacks.onClose).toHaveBeenLastCalledWith({
      segment: 1,
      code: 1000,
      reason: "client closed",
      wasClean: true,
    });

    transport.open();
    sockets[1]?.fireOpen();

    expect(transport.status).toEqual({ state: "open", segment: 2 });
    expect(callbacks.onOpen).toHaveBeenLastCalledWith({
      segment: 2,
      url: "ws://test.invalid/ws",
    });
  });

  it("ends the segment on a socket error and does not auto-reconnect", () => {
    const { transport, sockets, callbacks, factory } = setup();
    transport.open();
    sockets[0]?.fireOpen();
    const segmentBefore = transport.status.segment;

    sockets[0]?.fireError();

    expect(transport.status.state).toBe("closed");
    expect(callbacks.onClose).toHaveBeenCalledWith({
      segment: segmentBefore,
      code: 1006,
      reason: "WebSocket transport error",
      wasClean: false,
    });
    expect(
      callbacks.onProtocolIssue.mock.calls.map(
        (call) => (call[0] as { code: string }).code,
      ),
    ).toContain("socket_error");

    // A later server close event from the stale socket is ignored.
    sockets[0]?.fireServerClose();
    expect(callbacks.onClose).toHaveBeenCalledTimes(1);
    expect(factory).toHaveBeenCalledTimes(1);
  });

  it("does not open a second socket while connecting or open", () => {
    const { transport, sockets, factory } = setup();
    transport.open();
    transport.open();
    sockets[0]?.fireOpen();
    transport.open();

    expect(factory).toHaveBeenCalledTimes(1);
  });
});

describe("closed-state send safety", () => {
  it("refuses to send before open and after close", () => {
    const { transport, sockets } = setup();

    expect(transport.subscribe(["market.bar."])).toBe(false);
    expect(transport.unsubscribe(["market.bar."])).toBe(false);
    expect(transport.ping()).toBe(false);
    expect(sockets).toHaveLength(0);

    transport.open();
    sockets[0]?.fireOpen();
    transport.close();

    expect(transport.ping()).toBe(false);
    expect(transport.subscribe(["market.bar."])).toBe(false);
    expect(sockets[0]?.sent).toEqual([]);
  });
});

describe("URL derivation and construction failures", () => {
  it("derives ws/wss same-origin URLs and honours the configured override", () => {
    expect(
      deriveWsUrl({ protocol: "http:", host: "127.0.0.1:5173" }, ""),
    ).toBe("ws://127.0.0.1:5173/ws");
    expect(
      deriveWsUrl({ protocol: "https:", host: "console.example" }, ""),
    ).toBe("wss://console.example/ws");
    expect(
      deriveWsUrl({ protocol: "http:", host: "x" }, "wss://override/ws"),
    ).toBe("wss://override/ws");
  });

  it("passes the injected URL to the socket factory", () => {
    const factory = vi.fn(() => new FakeWebSocket());
    const transport = new GatewayWsTransport({
      url: "ws://injected/ws",
      createSocket: factory,
    });

    transport.open();

    expect(factory).toHaveBeenCalledWith("ws://injected/ws");
  });

  it("handles a throwing WebSocket factory without crashing", () => {
    const onClose = vi.fn();
    const onProtocolIssue = vi.fn();
    const transport = new GatewayWsTransport({
      url: "ws://bad/ws",
      createSocket: () => {
        throw new Error("bad websocket url");
      },
      onClose,
      onProtocolIssue,
    });

    expect(() => transport.open()).not.toThrow();
    expect(transport.status).toEqual({ state: "closed", segment: 0 });
    expect(onProtocolIssue).toHaveBeenCalledWith(
      expect.objectContaining({
        code: "constructor_error",
        message: "bad websocket url",
      }),
    );
    expect(onClose).toHaveBeenCalledWith(
      expect.objectContaining({
        segment: 0,
        code: 1006,
        wasClean: false,
      }),
    );
  });
});
