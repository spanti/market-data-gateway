import { describe, expect, it } from "vitest";
import {
  appendMessage,
  clearMessages,
  createMessageBuffer,
  filterMessages,
  WS_DEBUG_MESSAGE_LIMIT,
  type DebugMessageInput,
} from "@/pages/console/message-buffer";

function input(index: number): DebugMessageInput {
  return {
    segment: 1,
    kind: "event",
    receivedAt: `2026-11-09T12:00:${String(index % 60).padStart(2, "0")}.000Z`,
    topic: `market.bar.1m.binance:SYM${index}`,
    summary: `event ${index}`,
    detailText: `{"instrument_id":"binance:SYM${index}","close":"65000.${index}"}`,
  };
}

describe("bounded WebSocket debug buffer", () => {
  it("defaults to the product limit of 10,000 messages", () => {
    expect(WS_DEBUG_MESSAGE_LIMIT).toBe(10_000);
    expect(createMessageBuffer().limit).toBe(10_000);
  });

  it("keeps every message up to the limit", () => {
    let buffer = createMessageBuffer();
    for (let index = 1; index <= WS_DEBUG_MESSAGE_LIMIT; index += 1) {
      buffer = appendMessage(buffer, input(index));
    }

    expect(buffer.messages).toHaveLength(WS_DEBUG_MESSAGE_LIMIT);
    expect(buffer.localDropped).toBe(0);
    expect(buffer.totalReceived).toBe(WS_DEBUG_MESSAGE_LIMIT);
    expect(buffer.messages[0]?.summary).toBe("event 1");
    expect(buffer.messages.at(-1)?.summary).toBe("event 10000");
  });

  it("drops the oldest message locally when the buffer overflows", () => {
    let buffer = createMessageBuffer();
    for (let index = 1; index <= WS_DEBUG_MESSAGE_LIMIT + 1; index += 1) {
      buffer = appendMessage(buffer, input(index));
    }

    expect(buffer.messages).toHaveLength(WS_DEBUG_MESSAGE_LIMIT);
    expect(buffer.messages[0]?.summary).toBe("event 2");
    expect(buffer.messages.at(-1)?.summary).toBe("event 10001");
    expect(buffer.localDropped).toBe(1);
    expect(buffer.totalReceived).toBe(WS_DEBUG_MESSAGE_LIMIT + 1);
  });

  it("counts every locally dropped message, not only the first", () => {
    let buffer = createMessageBuffer(3);
    for (let index = 1; index <= 8; index += 1) {
      buffer = appendMessage(buffer, input(index));
    }

    expect(buffer.messages.map((message) => message.summary)).toEqual([
      "event 6",
      "event 7",
      "event 8",
    ]);
    expect(buffer.localDropped).toBe(5);
    expect(buffer.totalReceived).toBe(8);
  });

  it("assigns stable monotonic IDs across appends", () => {
    let buffer = createMessageBuffer(2);
    buffer = appendMessage(buffer, input(1));
    buffer = appendMessage(buffer, input(2));
    buffer = appendMessage(buffer, input(3));

    expect(buffer.messages.map((message) => message.id)).toEqual([2, 3]);
  });

  it("clears messages without rewriting drop accounting", () => {
    let buffer = createMessageBuffer(2);
    buffer = appendMessage(buffer, input(1));
    buffer = appendMessage(buffer, input(2));
    buffer = appendMessage(buffer, input(3));

    const cleared = clearMessages(buffer);
    expect(cleared.messages).toEqual([]);
    expect(cleared.localDropped).toBe(1);
    expect(cleared.totalReceived).toBe(3);
  });
});

describe("message filtering", () => {
  const messages = [
    {
      id: 1,
      segment: 1,
      kind: "event" as const,
      receivedAt: "2026-11-09T12:00:00.000Z",
      topic: "market.bar.1m.binance:BTC/USDT",
      summary: "BTC 1m close 65005.5",
      detailText: '{"close":"65005.5"}',
    },
    {
      id: 2,
      segment: 1,
      kind: "lag_notice" as const,
      receivedAt: "2026-11-09T12:00:01.000Z",
      summary: "服务端丢弃 12 条",
      detailText: '{"op":"lag_notice","dropped":12}',
    },
    {
      id: 3,
      segment: 2,
      kind: "pong" as const,
      receivedAt: "2026-11-09T12:00:02.000Z",
      summary: "pong · 已同步",
      detailText: '{"op":"pong"}',
    },
  ];

  it("returns every message for a blank filter", () => {
    expect(filterMessages(messages, "")).toHaveLength(3);
    expect(filterMessages(messages, "   ")).toHaveLength(3);
  });

  it("matches topic, kind, summary and raw detail case-insensitively", () => {
    expect(filterMessages(messages, "btc/usdt").map((m) => m.id)).toEqual([1]);
    expect(filterMessages(messages, "LAG_NOTICE").map((m) => m.id)).toEqual([
      2,
    ]);
    expect(filterMessages(messages, "已同步").map((m) => m.id)).toEqual([3]);
    expect(filterMessages(messages, '"dropped":12').map((m) => m.id)).toEqual([
      2,
    ]);
  });

  it("returns an empty list when nothing matches", () => {
    expect(filterMessages(messages, "no-such-value")).toEqual([]);
  });
});
