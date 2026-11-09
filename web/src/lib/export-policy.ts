import { MAX_CHART_RANGE_MS } from "@/lib/time-range";

/**
 * Frontend/product export policy. The current API does not enforce any of
 * these limits; the console must.
 *
 * - `<= 31` UTC days: direct CSV Blob (`GET /api/v1/bars?format=csv`).
 * - `> 31` and `<= 366` UTC days: Admin Export Job only. Never fall back to a
 *   huge synchronous Blob.
 * - `> 366` UTC days: split into multiple jobs. No multi-instrument ZIP.
 */

export const DAY_MS = 24 * 60 * 60 * 1000;
export const MAX_DIRECT_RANGE_MS = MAX_CHART_RANGE_MS;
export const MAX_JOB_RANGE_MS = 366 * DAY_MS;

export type ExportPathKind = "invalid" | "direct" | "job" | "split";

export interface ExportRangeChunk {
  start: Date;
  end: Date;
}

export interface ExportPathDecision {
  kind: ExportPathKind;
  start: Date;
  end: Date;
  rangeMs: number;
  rangeDays: number;
  chunkCount: number;
  chunks: ExportRangeChunk[];
  message: string;
}

function invalidDecision(
  start: Date,
  end: Date,
  message: string,
): ExportPathDecision {
  return {
    kind: "invalid",
    start,
    end,
    rangeMs: 0,
    rangeDays: 0,
    chunkCount: 0,
    chunks: [],
    message,
  };
}

function splitRange(start: Date, end: Date): ExportRangeChunk[] {
  const chunks: ExportRangeChunk[] = [];
  let cursor = start.getTime();
  const endMs = end.getTime();
  while (cursor < endMs) {
    const next = Math.min(cursor + MAX_JOB_RANGE_MS, endMs);
    chunks.push({ start: new Date(cursor), end: new Date(next) });
    cursor = next;
  }
  return chunks;
}

export function classifyExportRange(
  start: Date,
  end: Date,
): ExportPathDecision {
  const startMs = start.getTime();
  const endMs = end.getTime();
  if (Number.isNaN(startMs) || Number.isNaN(endMs)) {
    return invalidDecision(start, end, "时间范围无效");
  }
  if (endMs < startMs) {
    return invalidDecision(start, end, "开始时间必须早于结束时间");
  }

  const rangeMs = endMs - startMs;
  const rangeDays = rangeMs / DAY_MS;
  const base = {
    start,
    end,
    rangeMs,
    rangeDays,
    chunkCount: 1,
    chunks: [{ start, end }],
  };

  if (rangeMs <= MAX_DIRECT_RANGE_MS) {
    return {
      ...base,
      kind: "direct",
      message: "≤31 个 UTC 天：使用小文件 CSV 直接下载（Blob）",
    };
  }

  if (rangeMs <= MAX_JOB_RANGE_MS) {
    return {
      ...base,
      kind: "job",
      message: "31–366 个 UTC 天：只创建 Admin Export Job，不使用超大 Blob",
    };
  }

  const chunks = splitRange(start, end);
  return {
    ...base,
    kind: "split",
    chunkCount: chunks.length,
    chunks,
    message: `超过 366 个 UTC 天，需拆分为 ${chunks.length} 个 Export Job；不支持多 Instrument ZIP`,
  };
}

/** Formats a day count without pretending it is an exact integer. */
export function formatRangeDays(rangeDays: number): string {
  return Number.isInteger(rangeDays)
    ? String(rangeDays)
    : rangeDays.toFixed(2);
}
