/**
 * Display formatters. Unknown values are rendered as an em dash, never zero.
 * Timestamps are always explicit UTC so the UI cannot be mistaken for local
 * time; raw API strings are never rewritten for data or export paths.
 */

const UNKNOWN = "—";

function toDate(value: string | number | Date | null): Date | null {
  if (value === null) {
    return null;
  }
  const date = value instanceof Date ? value : new Date(value);
  return Number.isNaN(date.getTime()) ? null : date;
}

function pad(value: number, length = 2): string {
  return String(value).padStart(length, "0");
}

export function formatUtcDateTime(
  value: string | number | Date | null,
): string {
  const date = toDate(value);
  if (!date) {
    return UNKNOWN;
  }
  return `${date.getUTCFullYear()}-${pad(date.getUTCMonth() + 1)}-${pad(
    date.getUTCDate(),
  )} ${pad(date.getUTCHours())}:${pad(date.getUTCMinutes())}:${pad(
    date.getUTCSeconds(),
  )} UTC`;
}

export function formatUtcTime(value: string | number | Date | null): string {
  const date = toDate(value);
  if (!date) {
    return UNKNOWN;
  }
  return `${pad(date.getUTCHours())}:${pad(date.getUTCMinutes())}:${pad(
    date.getUTCSeconds(),
  )} UTC`;
}

export function formatNumber(
  value: number | null,
  maximumFractionDigits = 2,
): string {
  if (value === null || !Number.isFinite(value)) {
    return UNKNOWN;
  }
  return new Intl.NumberFormat("zh-CN", {
    maximumFractionDigits,
  }).format(value);
}

export function formatDurationSeconds(value: number | null): string {
  if (value === null || !Number.isFinite(value)) {
    return UNKNOWN;
  }
  const sign = value < 0 ? "-" : "";
  const total = Math.round(Math.abs(value));
  if (total < 60) {
    return `${sign}${total}s`;
  }
  if (total < 3600) {
    return `${sign}${Math.floor(total / 60)}m ${pad(total % 60)}s`;
  }
  if (total < 86400) {
    return `${sign}${Math.floor(total / 3600)}h ${pad(
      Math.floor((total % 3600) / 60),
    )}m`;
  }
  return `${sign}${Math.floor(total / 86400)}d ${pad(
    Math.floor((total % 86400) / 3600),
  )}h`;
}

/** Histogram observations are seconds; the dashboard displays milliseconds. */
export function formatLatencyMs(seconds: number | null): string {
  if (seconds === null || !Number.isFinite(seconds)) {
    return UNKNOWN;
  }
  return `${formatNumber(seconds * 1000, 2)} ms`;
}

export function formatBytes(value: number | null): string {
  if (value === null || !Number.isFinite(value)) {
    return UNKNOWN;
  }
  if (Math.abs(value) < 1024) {
    return `${formatNumber(value, 0)} B`;
  }
  const units = ["KB", "MB", "GB", "TB"];
  let scaled = value / 1024;
  let index = 0;
  while (Math.abs(scaled) >= 1024 && index < units.length - 1) {
    scaled /= 1024;
    index += 1;
  }
  return `${formatNumber(scaled, 2)} ${units[index]}`;
}
