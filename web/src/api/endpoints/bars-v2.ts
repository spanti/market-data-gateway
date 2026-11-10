import { apiClient } from "@/api/http/client";
import {
  decideBarsPageStep,
  type BarsPageStop,
} from "@/lib/bar-pagination";
import type { BarInterval } from "@/lib/time-range";
import type { BarMessageEnvelope } from "./bars";

/**
 * Revision-safe JSON Bar wire types (Data Contract v2, `GET /api/v2/bars`).
 *
 * Every `bars` entry is a raw Revision; the client never deduplicates here.
 * `next_cursor` is opaque and signed by the server: it must be passed back
 * unchanged and never parsed as a timestamp.
 */

export interface BarsV2PageInfo {
  has_more: boolean;
  next_cursor: string | null;
}

export interface BarsV2Response {
  bars: BarMessageEnvelope[];
  page: BarsV2PageInfo;
}

export interface BarsV2QueryParams {
  interval: BarInterval;
  instrument: string;
  limit: number;
  start?: string;
  end?: string;
  cursor?: string;
}

/** Default page size for revision-safe JSON history. */
export const BARS_V2_HISTORY_PAGE_LIMIT = 5000;
/**
 * Safety cap: 20 pages x 5000 rows = 100,000 raw JSON rows, matching the
 * server's per-response ceiling. Reaching it surfaces partial results.
 */
export const BARS_V2_HISTORY_MAX_PAGES = 20;

export function buildBarsV2SearchParams(
  query: BarsV2QueryParams,
): URLSearchParams {
  const params = new URLSearchParams();
  params.set("interval", query.interval);
  params.set("instrument", query.instrument);
  params.set("limit", String(query.limit));
  if (query.start) {
    params.set("start", query.start);
  }
  if (query.end) {
    params.set("end", query.end);
  }
  if (query.cursor) {
    params.set("cursor", query.cursor);
  }
  return params;
}

export async function fetchBarsV2Json(
  query: BarsV2QueryParams,
  signal?: AbortSignal,
): Promise<BarsV2Response> {
  const response = await apiClient.get<BarsV2Response>("/api/v2/bars", {
    params: buildBarsV2SearchParams(query),
    signal,
  });
  return response.data;
}

export interface BarsV2HistoryOptions {
  instrument: string;
  interval: BarInterval;
  start: string;
  end: string;
  limit?: number;
  maxPages?: number;
}

export interface BarsV2HistoryResult {
  /** Every raw revision in server response order; never deduped. */
  bars: BarMessageEnvelope[];
  pages: number;
  complete: boolean;
  /** Non-null when the guard stopped early; `bars` is then partial. */
  stop: BarsPageStop | null;
}

/**
 * Paginated revision-safe JSON history.
 *
 * Follows `next_cursor` while `has_more` is true. The guard stops on a
 * missing cursor, an unchanged cursor, a repeated cursor (cycle), an empty
 * page that claims more rows, a non-null cursor on the last page, or the
 * page cap. It never fabricates `start` from a cursor.
 */
export async function fetchBarsV2History(
  options: BarsV2HistoryOptions,
  signal?: AbortSignal,
): Promise<BarsV2HistoryResult> {
  const limit = options.limit ?? BARS_V2_HISTORY_PAGE_LIMIT;
  const maxPages = options.maxPages ?? BARS_V2_HISTORY_MAX_PAGES;
  const bars: BarMessageEnvelope[] = [];
  const seenCursors: string[] = [];
  let cursor: string | null = null;
  let pages = 0;

  for (;;) {
    const page = await fetchBarsV2Json(
      {
        instrument: options.instrument,
        interval: options.interval,
        start: cursor === null ? options.start : undefined,
        end: options.end,
        cursor: cursor ?? undefined,
        limit,
      },
      signal,
    );
    pages += 1;
    bars.push(...page.bars);

    const step = decideBarsPageStep({
      requestCursor: cursor,
      page,
      seenCursors,
      pageCount: pages,
      maxPages,
    });

    if (step.kind === "complete") {
      return { bars, pages, complete: true, stop: null };
    }
    if (step.kind === "stop") {
      return { bars, pages, complete: false, stop: step.stop };
    }
    seenCursors.push(step.nextCursor);
    cursor = step.nextCursor;
  }
}
