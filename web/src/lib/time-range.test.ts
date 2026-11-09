import { describe, expect, it } from "vitest";
import {
  containsInstant,
  DEFAULT_RANGE_MS,
  formatDateTimeInput,
  MAX_CHART_RANGE_MS,
  parseBarQuery,
  parseDateTimeInput,
  parseIsoInstant,
  serializeBarQuery,
  toIsoWithOffset,
  toQueryInstant,
  validateChartRange,
} from "@/lib/time-range";

const now = new Date("2026-11-09T12:00:00.000Z");

describe("Bar query URL state", () => {
  it("defaults to the most recent 24 hours", () => {
    const state = parseBarQuery(new URLSearchParams(), { now });

    expect(state.isDefaultRange).toBe(true);
    expect(state.start.toISOString()).toBe("2026-11-08T12:00:00.000Z");
    expect(state.end.toISOString()).toBe("2026-11-09T12:00:00.000Z");
    expect(state.start.getTime() - state.end.getTime()).toBe(-DEFAULT_RANGE_MS);
  });

  it("parses instrument_id, interval, start and end from the URL", () => {
    const state = parseBarQuery(
      "instrument_id=binance%3ABTC%2FUSDT&interval=1h&start=2026-11-01T00%3A00%3A00Z&end=2026-11-02T00%3A00%3A00Z",
      { now },
    );

    expect(state.instrumentId).toBe("binance:BTC/USDT");
    expect(state.interval).toBe("1h");
    expect(state.start.toISOString()).toBe("2026-11-01T00:00:00.000Z");
    expect(state.end.toISOString()).toBe("2026-11-02T00:00:00.000Z");
    expect(state.invalidParams).toEqual([]);
    expect(state.isDefaultRange).toBe(false);
  });

  it("treats start as closed and end as open", () => {
    const state = parseBarQuery(
      "start=2026-11-01T00:00:00Z&end=2026-11-02T00:00:00Z",
      { now },
    );

    expect(containsInstant(state.start, state.end, state.start)).toBe(true);
    expect(
      containsInstant(
        state.start,
        state.end,
        new Date("2026-11-01T23:59:59.999Z"),
      ),
    ).toBe(true);
    expect(containsInstant(state.start, state.end, state.end)).toBe(false);
  });

  it("accepts offsets and treats a value without an offset as UTC", () => {
    const withOffset = parseBarQuery(
      "start=2026-11-01T08:00:00%2B08:00&end=2026-11-02T08:00:00%2B08:00",
      { now },
    );
    const withoutOffset = parseBarQuery(
      "start=2026-11-01T00:00:00&end=2026-11-02T00:00:00",
      { now },
    );

    expect(withOffset.start.toISOString()).toBe("2026-11-01T00:00:00.000Z");
    expect(withoutOffset.start.toISOString()).toBe(
      "2026-11-01T00:00:00.000Z",
    );
  });

  it("accepts a space separator and nanosecond fractions", () => {
    expect(parseIsoInstant("2026-11-01 00:00:00.123456789Z")?.toISOString())
      .toBe("2026-11-01T00:00:00.123Z");
  });

  it("reports invalid parameters instead of silently inventing values", () => {
    const state = parseBarQuery(
      "instrument_id=&interval=2m&start=not-a-time&end=2026-11-02T00:00:00Z",
      { now },
    );

    expect(state.instrumentId).toBeNull();
    expect(state.interval).toBeNull();
    expect(state.invalidParams).toEqual(
      expect.arrayContaining(["interval", "start"]),
    );
    // Invalid start falls back to the default range around the valid end.
    expect(state.start.toISOString()).toBe("2026-11-01T00:00:00.000Z");
    expect(state.end.toISOString()).toBe("2026-11-02T00:00:00.000Z");
  });

  it("derives the missing bound from the present one", () => {
    const endOnly = parseBarQuery("end=2026-11-02T00:00:00Z", { now });
    const startOnly = parseBarQuery("start=2026-11-01T00:00:00Z", { now });

    expect(endOnly.start.toISOString()).toBe("2026-11-01T00:00:00.000Z");
    expect(startOnly.end.toISOString()).toBe("2026-11-02T00:00:00.000Z");
  });

  it("serializes UTC instants and survives a round trip", () => {
    const state = parseBarQuery(
      "instrument_id=binance%3ABTC%2FUSDT&interval=5m&start=2026-11-01T08:00:00%2B08:00&end=2026-11-02T08:00:00%2B08:00",
      { now, displayZone: "local" },
    );
    const serialized = serializeBarQuery(state);

    expect(serialized.get("instrument_id")).toBe("binance:BTC/USDT");
    expect(serialized.get("interval")).toBe("5m");
    expect(serialized.get("start")).toBe("2026-11-01T00:00:00.000Z");
    expect(serialized.get("end")).toBe("2026-11-02T00:00:00.000Z");

    const roundTrip = parseBarQuery(serialized, { now });
    expect(roundTrip.start.getTime()).toBe(state.start.getTime());
    expect(roundTrip.end.getTime()).toBe(state.end.getTime());
  });
});

describe("chart range bounds", () => {
  const start = new Date("2026-11-01T00:00:00.000Z");

  it("allows exactly 31 UTC days", () => {
    const end = new Date(start.getTime() + MAX_CHART_RANGE_MS);
    expect(validateChartRange(start, end)).toBeNull();
  });

  it("rejects more than 31 UTC days with a chart-range error", () => {
    const end = new Date(start.getTime() + MAX_CHART_RANGE_MS + 1);
    expect(validateChartRange(start, end)).toMatchObject({
      code: "chart_range_too_large",
    });
  });

  it("rejects an inverted range and allows an empty range", () => {
    expect(
      validateChartRange(new Date("2026-11-02T00:00:00Z"), start),
    ).toMatchObject({ code: "reversed" });
    expect(validateChartRange(start, start)).toBeNull();
  });
});

describe("UTC and local display preference", () => {
  const instant = new Date("2026-11-09T04:30:00.000Z");

  it("formats UTC requests with a Z offset", () => {
    expect(toIsoWithOffset(instant, "utc")).toBe("2026-11-09T04:30:00.000Z");
    expect(toQueryInstant(instant)).toBe("2026-11-09T04:30:00.000Z");
  });

  it("formats local time with an explicit numeric offset", () => {
    const local = toIsoWithOffset(instant, "local");

    expect(local).toMatch(/[+-]\d{2}:\d{2}$/);
    expect(parseIsoInstant(local)?.getTime()).toBe(instant.getTime());
  });

  it("round-trips datetime-local input in both zones", () => {
    for (const zone of ["utc", "local"] as const) {
      const input = formatDateTimeInput(instant, zone);
      expect(parseDateTimeInput(input, zone)?.getTime()).toBe(
        instant.getTime(),
      );
    }
  });

  it("rejects an impossible calendar date", () => {
    expect(parseIsoInstant("2026-02-30T00:00:00Z")).toBeNull();
    expect(parseDateTimeInput("2026-02-30T00:00", "utc")).toBeNull();
  });
});
