import { describe, expect, it } from "vitest";
import type { WebSocketLike } from "@/api/ws/transport";
import { WsDebugSession } from "@/pages/console/ws-debug-session";

let clock = 0;
const now = () => new Date(Date.UTC(2026, 10, 9, 12, 0, clock++));

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
    open: "65000.100000000000000001",
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

  send(data: string): void {
    this.sent.push(data);
  }

  close(code = 1000, reason = ""): void {
    this.readyState = 3;
    this.onclose?.({ code, reason, wasClean: true } as CloseEvent);
  }

  fireOpen(): void {
    this.readyState = 1;
    this.onopen?.(new Event("open"));
  }

  fireMessage(payload: unknown): void {
    this.onmessage?.({
      data: typeof payload === "string" ? payload : JSON.stringify(payload),
    } as MessageEvent);
  }

  fireServerClose(): void {
    this.readyState = 3;
    this.onclose?.({ code: 1006, reason: "gone", wasClean: false } as CloseEvent);
  }
}

function setup(limit?: number) {
  const sockets: FakeWebSocket[] = [];
  const session = new WsDebugSession({
    url: "ws://test.invalid/ws",
    createSocket: () => {
      const socket = new FakeWebSocket();
      sockets.push(socket);
      return socket;
    },
    ...(limit !== undefined ? { limit } : {}),
    now,
  });

  return { session, sockets };
}

function open(session: WsDebugSession, sockets: FakeWebSocket[]) {
  session.connect();
  sockets.at(-1)?.fireOpen();
}

describe("WS debug session", () => {
  it("starts a receive segment on connect and records its start time", () => {
    const { session, sockets } = setup();
    expect(session.getSnapshot().connection).toBe("idle");

    open(session, sockets);
    const snapshot = session.getSnapshot();

    expect(snapshot.connection).toBe("open");
    expect(snapshot.segment).toBe(1);
    expect(snapshot.connectedAt).not.toBeNull();
    expect(snapshot.segmentInterrupted).toBe(false);
  });

  it("records events with topic, client receive time and exact JSON detail", () => {
    const { session, sockets } = setup();
    open(session, sockets);

    sockets[0]?.fireMessage({
      op: "event",
      topic: "market.bar.1m.binance:BTC/USDT",
      data: envelope,
    });

    const snapshot = session.getSnapshot();
    const message = snapshot.messages.at(-1);

    expect(message).toMatchObject({
      kind: "event",
      topic: "market.bar.1m.binance:BTC/USDT",
      segment: 1,
    });
    expect(message?.receivedAt).toMatch(/^2026-11-09T12:00:/);
    // Exact decimal strings must survive the raw detail view unchanged.
    expect(message?.detailText).toContain("65000.100000000000000001");
    expect(message?.detailText).toContain("65005.5");
    expect(message?.summary).toContain("binance:BTC/USDT");
  });

  it("treats pong as an ordering barrier and shows 已同步, never a subscription ACK", () => {
    const { session, sockets } = setup();
    open(session, sockets);

    expect(session.ping()).toBe(true);
    expect(session.getSnapshot().syncState).toBe("waiting");

    sockets[0]?.fireMessage({ op: "pong" });
    const snapshot = session.getSnapshot();

    expect(snapshot.syncState).toBe("synced");
    expect(snapshot.messages.at(-1)?.summary).toContain("已同步");
    expect(snapshot.messages.at(-1)?.summary).not.toContain("订阅已确认");
    expect(snapshot.lastPongAt).not.toBeNull();
  });

  it("keeps server lag and local buffer drops as separate counters", () => {
    const { session, sockets } = setup(2);
    open(session, sockets);

    sockets[0]?.fireMessage({
      op: "lag_notice",
      dropped: 12,
      since: "2026-11-09T12:00:00.000000000Z",
    });
    for (let index = 0; index < 3; index += 1) {
      sockets[0]?.fireMessage({
        op: "event",
        topic: `market.bar.1m.binance:SYM${index}`,
        data: { ...envelope, instrument_id: `binance:SYM${index}` },
      });
    }

    const snapshot = session.getSnapshot();
    expect(snapshot.serverLagDropped).toBe(12);
    // limit 2, with the connection marker and lag notice included: 5 appended,
    // 3 oldest entries were dropped locally.
    expect(snapshot.localDropped).toBe(3);
    expect(snapshot.serverLagDropped).not.toBe(snapshot.localDropped);
  });

  it("marks a closed segment as interrupted and never implies seamless recovery", () => {
    const { session, sockets } = setup();
    open(session, sockets);
    sockets[0]?.fireMessage({ op: "pong" });

    sockets[0]?.fireServerClose();
    const interrupted = session.getSnapshot();
    expect(interrupted.connection).toBe("closed");
    expect(interrupted.segmentInterrupted).toBe(true);
    expect(interrupted.messages.at(-1)?.summary).toContain("中断");

    open(session, sockets);
    const resumed = session.getSnapshot();
    expect(resumed.segment).toBe(2);
    expect(resumed.segmentInterrupted).toBe(false);
    expect(resumed.messages.at(-1)?.segment).toBe(2);
    expect(resumed.messages.at(-1)?.summary).toContain("接收段 2");
  });

  it("clears messages without clearing drop accounting", () => {
    const { session, sockets } = setup(1);
    open(session, sockets);
    sockets[0]?.fireMessage({ op: "lag_notice", dropped: 4, since: "2026-11-09T12:00:00Z" });
    sockets[0]?.fireMessage({ op: "pong" });
    sockets[0]?.fireMessage({ op: "pong" });

    // open marker + lag notice + 2 pongs with limit 1 => 3 local drops.
    expect(session.getSnapshot().localDropped).toBe(3);
    session.clear();
    const cleared = session.getSnapshot();

    expect(cleared.messages).toEqual([]);
    expect(cleared.localDropped).toBe(3);
    expect(cleared.serverLagDropped).toBe(4);
  });

  it("sends controlled Topic patterns, deduplicates them and unsubscribes", () => {
    const { session, sockets } = setup();
    open(session, sockets);

    expect(session.subscribe("market.bar.1m.binance:BTC/USDT")).toBe(true);
    expect(session.subscribe("market.bar.1m.binance:BTC/USDT")).toBe(true);
    expect(session.getSnapshot().topicPatterns).toEqual([
      "market.bar.1m.binance:BTC/USDT",
    ]);
    expect(session.unsubscribe("market.bar.1m.binance:BTC/USDT")).toBe(true);
    expect(session.getSnapshot().topicPatterns).toEqual([]);

    expect(sockets[0]?.sent).toEqual([
      '{"op":"subscribe","topics":["market.bar.1m.binance:BTC/USDT"]}',
      '{"op":"unsubscribe","topics":["market.bar.1m.binance:BTC/USDT"]}',
    ]);
  });

  it("rejects an invalid controlled topic without sending it", () => {
    const { session, sockets } = setup();
    open(session, sockets);

    expect(session.subscribe("replay.rpl-1.control")).toBe(false);
    expect(sockets[0]?.sent).toEqual([]);
  });

  it("validates free-form JSON against the whitelist before sending", () => {
    const { session, sockets } = setup();
    open(session, sockets);

    expect(session.sendFreeform('{"op":"event"}')).toMatchObject({ ok: false });
    expect(session.sendFreeform("{broken")).toMatchObject({ ok: false });
    expect(sockets[0]?.sent).toEqual([]);

    expect(
      session.sendFreeform(
        '{"op":"subscribe","topics":["market.bar.5m."]}',
      ),
    ).toEqual({ ok: true });
    expect(
      session.sendFreeform('{"op":"unsubscribe","topics":["market.bar.5m."]}'),
    ).toEqual({ ok: true });
    expect(session.sendFreeform('{"op":"ping"}')).toEqual({ ok: true });
    expect(sockets[0]?.sent).toEqual([
      '{"op":"subscribe","topics":["market.bar.5m."]}',
      '{"op":"unsubscribe","topics":["market.bar.5m."]}',
      '{"op":"ping"}',
    ]);
  });

  it("never persists messages", () => {
    const { session, sockets } = setup();
    open(session, sockets);
    sockets[0]?.fireMessage({
      op: "event",
      topic: "market.bar.1m.binance:BTC/USDT",
      data: envelope,
    });

    expect(window.localStorage.length).toBe(0);
    expect(window.sessionStorage.length).toBe(0);
  });
});
