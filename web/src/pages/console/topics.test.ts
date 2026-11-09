import { describe, expect, it } from "vitest";
import {
  buildControlledTopic,
  parseFreeformFrame,
  TOPIC_PREFIX_HINTS,
  validateControlledTopic,
} from "@/pages/console/topics";

describe("controlled Topic grammar", () => {
  it("accepts all-bars, interval-prefix and exact topics", () => {
    for (const topic of [
      "market.bar.",
      "market.bar.1m.",
      "market.bar.5m.",
      "market.bar.1h.",
      "market.bar.1m.binance:BTC/USDT",
      "market.bar.1d.binance:ETH/USDT",
    ]) {
      expect(validateControlledTopic(topic), topic).toEqual({
        valid: true,
        topic,
      });
    }
  });

  it("trims surrounding whitespace before validating", () => {
    expect(validateControlledTopic("  market.bar.1m.  ")).toEqual({
      valid: true,
      topic: "market.bar.1m.",
    });
  });

  it("rejects replay topics and non-market topics", () => {
    for (const topic of [
      "replay.rpl-1.market.bar.1m.binance:BTC/USDT",
      "replay.rpl-1.control",
      "_internal.status",
      "market.",
    ]) {
      const result = validateControlledTopic(topic);
      expect(result.valid, topic).toBe(false);
      expect(result.valid ? "" : result.error).toContain("market.bar");
    }
  });

  it("rejects a missing trailing dot, unknown intervals and malformed InstrumentIds", () => {
    for (const topic of [
      "market.bar.1m",
      "market.bar.2m.",
      "market.bar.1m.BTCUSDT",
      "market.bar.1m.binance:",
      "market.bar.1m.:BTC",
      "market.bar.1m.binance:BT C/USDT",
    ]) {
      expect(validateControlledTopic(topic).valid, topic).toBe(false);
    }
  });

  it("explains the required form in the error", () => {
    const result = validateControlledTopic("market.bar.1m");
    expect(result).toMatchObject({ valid: false });
    if (!result.valid) {
      expect(result.error).toContain("market.bar.<interval>");
    }
  });
});

describe("controlled Topic builder and hints", () => {
  it("builds the all-bars prefix regardless of instrument", () => {
    expect(buildControlledTopic("all")).toBe("market.bar.");
    expect(buildControlledTopic("all", "binance:BTC/USDT")).toBe(
      "market.bar.",
    );
  });

  it("builds interval prefixes and exact topics", () => {
    expect(buildControlledTopic("5m")).toBe("market.bar.5m.");
    expect(buildControlledTopic("1m", "binance:BTC/USDT")).toBe(
      "market.bar.1m.binance:BTC/USDT",
    );
    expect(buildControlledTopic("1m", "   ")).toBe("market.bar.1m.");
  });

  it("exposes the three prefix hints with canonical examples", () => {
    expect(TOPIC_PREFIX_HINTS.map((hint) => hint.topic)).toEqual([
      "market.bar.",
      "market.bar.1m.",
      "market.bar.1m.binance:BTC/USDT",
    ]);
    expect(TOPIC_PREFIX_HINTS.every((hint) => hint.label.length > 0)).toBe(
      true,
    );
  });
});

describe("free-form JSON whitelist", () => {
  it("accepts subscribe and unsubscribe with whitelisted market topics", () => {
    expect(
      parseFreeformFrame(
        '{"op":"subscribe","topics":["market.bar.1m.binance:BTC/USDT","market.bar."]}',
      ),
    ).toEqual({
      ok: true,
      frame: {
        op: "subscribe",
        topics: ["market.bar.1m.binance:BTC/USDT", "market.bar."],
      },
    });

    expect(
      parseFreeformFrame('{"op":"unsubscribe","topics":["market.bar.5m."]}'),
    ).toEqual({
      ok: true,
      frame: { op: "unsubscribe", topics: ["market.bar.5m."] },
    });
  });

  it("accepts ping and ignores extra fields", () => {
    expect(parseFreeformFrame('{"op":"ping","extra":true}')).toEqual({
      ok: true,
      frame: { op: "ping" },
    });
  });

  it("rejects unknown ops and missing ops", () => {
    for (const text of [
      '{"op":"event"}',
      '{"op":"close"}',
      '{"topics":["market.bar."]}',
    ]) {
      const result = parseFreeformFrame(text);
      expect(result.ok, text).toBe(false);
      if (!result.ok) {
        expect(result.error).toContain("subscribe / unsubscribe / ping");
      }
    }
  });

  it("rejects non-object JSON", () => {
    for (const text of ["[]", '"ping"', "42", "null"]) {
      const result = parseFreeformFrame(text);
      expect(result.ok, text).toBe(false);
      if (!result.ok) {
        expect(result.error).toContain("对象");
      }
    }
  });

  it("rejects invalid JSON with an actionable error", () => {
    const result = parseFreeformFrame("{not-json");
    expect(result).toMatchObject({ ok: false });
    if (!result.ok) {
      expect(result.error).toContain("JSON");
    }
  });

  it("rejects a non-array, empty or non-string topics list", () => {
    for (const text of [
      '{"op":"subscribe","topics":"market.bar."}',
      '{"op":"subscribe","topics":[]}',
      '{"op":"subscribe","topics":[42]}',
      '{"op":"subscribe","topics":[""]}',
      '{"op":"subscribe"}',
    ]) {
      expect(parseFreeformFrame(text).ok, text).toBe(false);
    }
  });

  it("rejects topics outside the controlled grammar", () => {
    const result = parseFreeformFrame(
      '{"op":"subscribe","topics":["replay.rpl-1.control"]}',
    );
    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.error).toContain("Topic");
    }
  });

  it("enforces the topic count and length limits", () => {
    const tooMany = JSON.stringify({
      op: "subscribe",
      topics: Array.from({ length: 21 }, (_, index) => `market.bar.${index}`),
    });
    expect(parseFreeformFrame(tooMany).ok).toBe(false);

    const tooLong = JSON.stringify({
      op: "subscribe",
      topics: [`market.bar.1m.binance:${"X".repeat(600)}`],
    });
    expect(parseFreeformFrame(tooLong).ok).toBe(false);
  });
});
