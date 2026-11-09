/**
 * Bar query URL/time semantics.
 *
 * - `start` is inclusive; `end` is exclusive, matching the API Time Cursor.
 * - Requests are canonical UTC ISO-8601 (`Z`), which carries an explicit offset.
 * - A value without an offset is interpreted as UTC, matching the backend.
 * - Default range is the most recent 24 hours; charts are capped at 31 UTC days
 *   (a frontend/product policy, not a server-enforced limit).
 */

export const BAR_INTERVALS = ["1m", "5m", "15m", "1h", "1d"] as const;
export type BarInterval = (typeof BAR_INTERVALS)[number];

export type TimeDisplayZone = "utc" | "local";

export const DEFAULT_RANGE_MS = 24 * 60 * 60 * 1000;
export const MAX_CHART_RANGE_MS = 31 * 24 * 60 * 60 * 1000;

export interface BarQueryState {
  instrumentId: string | null;
  interval: BarInterval | null;
  start: Date;
  end: Date;
  displayZone: TimeDisplayZone;
  isDefaultRange: boolean;
  invalidParams: string[];
}

export interface ParseBarQueryOptions {
  now?: Date;
  displayZone?: TimeDisplayZone;
}

export type RangeViolationCode =
  | "invalid_range"
  | "reversed"
  | "chart_range_too_large";

export interface RangeViolation {
  code: RangeViolationCode;
  message: string;
}

export function isBarInterval(value: string): value is BarInterval {
  return (BAR_INTERVALS as readonly string[]).includes(value);
}

const INSTANT_PATTERN =
  /^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:?\d{2})?$/;

function parseOffsetMinutes(offset: string | undefined): number | null {
  if (offset === undefined || offset === "Z") {
    return 0;
  }
  const match = /^([+-])(\d{2}):?(\d{2})$/.exec(offset);
  if (!match) {
    return null;
  }
  const sign = match[1] === "-" ? -1 : 1;
  const hours = Number(match[2]);
  const minutes = Number(match[3]);
  if (hours > 23 || minutes > 59) {
    return null;
  }
  return sign * (hours * 60 + minutes);
}

/**
 * Parses the timestamp forms accepted by the gateway: date and time separated
 * by `T` or a space, 1-9 fractional digits, and `Z`, `+HH:MM`, `+HHMM` or no
 * offset (UTC). Precision is truncated to milliseconds for display/range use;
 * raw strings are never rewritten for data/CSV.
 */
export function parseIsoInstant(value: string): Date | null {
  const match = INSTANT_PATTERN.exec(value.trim());
  if (!match) {
    return null;
  }
  const [, yearText, monthText, dayText, hourText, minuteText, secondText] =
    match;
  if (
    yearText === undefined ||
    monthText === undefined ||
    dayText === undefined ||
    hourText === undefined ||
    minuteText === undefined ||
    secondText === undefined
  ) {
    return null;
  }

  const offsetMinutes = parseOffsetMinutes(match[8]);
  if (offsetMinutes === null) {
    return null;
  }

  const year = Number(yearText);
  const month = Number(monthText);
  const day = Number(dayText);
  const hour = Number(hourText);
  const minute = Number(minuteText);
  const second = Number(secondText);
  if (hour > 23 || minute > 59 || second > 59) {
    return null;
  }

  const fraction = match[7] ?? "";
  const millis = Number(fraction.padEnd(3, "0").slice(0, 3));

  const utcMs =
    Date.UTC(year, month - 1, day, hour, minute, second, millis) -
    offsetMinutes * 60 * 1000;
  const date = new Date(utcMs);
  if (Number.isNaN(date.getTime())) {
    return null;
  }

  // Reject impossible calendar values (e.g. 2026-02-30) that Date.UTC rolls over.
  const shifted = new Date(utcMs + offsetMinutes * 60 * 1000);
  if (
    shifted.getUTCFullYear() !== year ||
    shifted.getUTCMonth() !== month - 1 ||
    shifted.getUTCDate() !== day ||
    shifted.getUTCHours() !== hour ||
    shifted.getUTCMinutes() !== minute ||
    shifted.getUTCSeconds() !== second
  ) {
    return null;
  }

  return date;
}

function pad(value: number, length = 2): string {
  return String(value).padStart(length, "0");
}

/**
 * Formats an instant with an explicit offset. UTC uses `Z`; local uses the
 * browser's numeric offset so the request remains unambiguous.
 */
export function toIsoWithOffset(date: Date, zone: TimeDisplayZone): string {
  if (zone === "utc") {
    return date.toISOString();
  }
  const offsetMinutes = -date.getTimezoneOffset();
  const shifted = new Date(date.getTime() + offsetMinutes * 60 * 1000);
  const sign = offsetMinutes >= 0 ? "+" : "-";
  const absolute = Math.abs(offsetMinutes);
  return [
    `${shifted.getUTCFullYear()}-${pad(shifted.getUTCMonth() + 1)}-${pad(
      shifted.getUTCDate(),
    )}`,
    `T${pad(shifted.getUTCHours())}:${pad(shifted.getUTCMinutes())}:${pad(
      shifted.getUTCSeconds(),
    )}.${pad(shifted.getUTCMilliseconds(), 3)}`,
    `${sign}${pad(Math.floor(absolute / 60))}:${pad(absolute % 60)}`,
  ].join("");
}

/** Canonical, shareable request form: UTC ISO-8601 with `Z`. */
export function toQueryInstant(date: Date): string {
  return date.toISOString();
}

export function containsInstant(start: Date, end: Date, instant: Date): boolean {
  const value = instant.getTime();
  return value >= start.getTime() && value < end.getTime();
}

export function validateChartRange(
  start: Date,
  end: Date,
): RangeViolation | null {
  if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime())) {
    return { code: "invalid_range", message: "时间范围无效" };
  }
  const span = end.getTime() - start.getTime();
  if (span < 0) {
    return { code: "reversed", message: "开始时间必须早于结束时间" };
  }
  if (span > MAX_CHART_RANGE_MS) {
    return {
      code: "chart_range_too_large",
      message: "单次图表最多查询 31 个 UTC 天，请缩小范围",
    };
  }
  return null;
}

export function parseBarQuery(
  input: URLSearchParams | string,
  options: ParseBarQueryOptions = {},
): BarQueryState {
  const params =
    typeof input === "string" ? new URLSearchParams(input) : input;
  const now = options.now ?? new Date();
  const displayZone = options.displayZone ?? "utc";
  const invalidParams: string[] = [];

  const instrumentRaw = params.get("instrument_id")?.trim() ?? "";
  const instrumentId = instrumentRaw.length > 0 ? instrumentRaw : null;

  const intervalRaw = params.get("interval")?.trim() ?? "";
  let interval: BarInterval | null = null;
  if (intervalRaw.length > 0) {
    if (isBarInterval(intervalRaw)) {
      interval = intervalRaw;
    } else {
      invalidParams.push("interval");
    }
  }

  const startRaw = params.get("start");
  const endRaw = params.get("end");
  const parsedStart = startRaw ? parseIsoInstant(startRaw) : null;
  const parsedEnd = endRaw ? parseIsoInstant(endRaw) : null;

  if (startRaw !== null && parsedStart === null) {
    invalidParams.push("start");
  }
  if (endRaw !== null && parsedEnd === null) {
    invalidParams.push("end");
  }

  let start: Date;
  let end: Date;
  if (parsedStart && parsedEnd) {
    start = parsedStart;
    end = parsedEnd;
  } else if (parsedStart) {
    start = parsedStart;
    end = new Date(start.getTime() + DEFAULT_RANGE_MS);
  } else if (parsedEnd) {
    end = parsedEnd;
    start = new Date(end.getTime() - DEFAULT_RANGE_MS);
  } else {
    end = now;
    start = new Date(now.getTime() - DEFAULT_RANGE_MS);
  }

  return {
    instrumentId,
    interval,
    start,
    end,
    displayZone,
    isDefaultRange: startRaw === null && endRaw === null,
    invalidParams,
  };
}

export function serializeBarQuery(state: BarQueryState): URLSearchParams {
  const params = new URLSearchParams();
  if (state.instrumentId) {
    params.set("instrument_id", state.instrumentId);
  }
  if (state.interval) {
    params.set("interval", state.interval);
  }
  params.set("start", toQueryInstant(state.start));
  params.set("end", toQueryInstant(state.end));
  return params;
}

const INPUT_PATTERN =
  /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::(\d{2}))?$/;

/** Formats a `datetime-local` control value in the chosen display zone. */
export function formatDateTimeInput(
  date: Date,
  zone: TimeDisplayZone,
): string {
  if (zone === "utc") {
    return date.toISOString().slice(0, 16);
  }
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(
    date.getDate(),
  )}T${pad(date.getHours())}:${pad(date.getMinutes())}`;
}

export function parseDateTimeInput(
  value: string,
  zone: TimeDisplayZone,
): Date | null {
  const match = INPUT_PATTERN.exec(value.trim());
  if (!match) {
    return null;
  }
  const [, yearText, monthText, dayText, hourText, minuteText, secondText] =
    match;
  if (
    yearText === undefined ||
    monthText === undefined ||
    dayText === undefined ||
    hourText === undefined ||
    minuteText === undefined
  ) {
    return null;
  }
  const year = Number(yearText);
  const month = Number(monthText);
  const day = Number(dayText);
  const hour = Number(hourText);
  const minute = Number(minuteText);
  const second = secondText === undefined ? 0 : Number(secondText);

  if (zone === "utc") {
    const seconds = secondText === undefined ? "00" : pad(second);
    return parseIsoInstant(
      `${yearText}-${monthText}-${dayText}T${hourText}:${minuteText}:${seconds}Z`,
    );
  }

  const date = new Date(year, month - 1, day, hour, minute, second, 0);
  if (
    Number.isNaN(date.getTime()) ||
    date.getFullYear() !== year ||
    date.getMonth() !== month - 1 ||
    date.getDate() !== day ||
    date.getHours() !== hour ||
    date.getMinutes() !== minute ||
    date.getSeconds() !== second
  ) {
    return null;
  }
  return date;
}
