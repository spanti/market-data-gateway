import axios from "axios";
import { apiClient, blobClient, normalizeHeaders, toHttpFailure } from "@/api/http/client";
import { normalizeRestFailure, parseBlobFailure } from "@/api/http/errors";
import { HttpError } from "@/api/http/types";
import { maxOpenTs } from "@/lib/bar-data";
import { decideBarsPageStep, type BarsPageStop } from "@/lib/bar-pagination";
import { parseContentDispositionFilename } from "@/lib/content-disposition";
import type { BarInterval } from "@/lib/time-range";

/**
 * Current Contract Bar wire types. Decimal values stay exact strings; only
 * chart coordinates may convert to Number. Raw revisions are never
 * deduplicated in these types.
 */

export type AggregationSource = "external" | "internal";

export interface BarPayload {
  interval: BarInterval;
  open_ts: string;
  close_ts: string;
  aggregation_source: AggregationSource;
  open: string;
  high: string;
  low: string;
  close: string;
  volume: string;
  trade_count: number | null;
  revision: number;
}

export interface BarMessageEnvelope {
  schema_version: number;
  data_type: "bar";
  instrument_id: string;
  event_ts: string;
  ingest_ts: string;
  feed: string;
  seq: number | null;
  payload: BarPayload;
}

export interface BarsPageInfo {
  has_more: boolean;
  next_start: string | null;
}

export interface BarsResponse {
  bars: BarMessageEnvelope[];
  page?: BarsPageInfo;
}

export type BarsFormat = "json" | "arrow" | "csv";

export interface BarsQueryParams {
  interval: BarInterval;
  instrument?: string;
  start?: string;
  end?: string;
  limit?: number;
  format?: BarsFormat;
}

export function buildBarsSearchParams(query: BarsQueryParams): URLSearchParams {
  const params = new URLSearchParams();
  params.set("interval", query.interval);
  if (query.instrument) {
    params.set("instrument", query.instrument);
  }
  if (query.start) {
    params.set("start", query.start);
  }
  if (query.end) {
    params.set("end", query.end);
  }
  if (query.limit !== undefined) {
    params.set("limit", String(query.limit));
  }
  if (query.format) {
    params.set("format", query.format);
  }
  return params;
}

export async function fetchBarsJson(
  query: BarsQueryParams,
  signal?: AbortSignal,
): Promise<BarsResponse> {
  const response = await apiClient.get<BarsResponse>("/api/v1/bars", {
    params: buildBarsSearchParams({ ...query, format: "json" }),
    signal,
  });
  return response.data;
}

/** Default page size for the JSON Time Cursor. */
export const BARS_HISTORY_PAGE_LIMIT = 5000;
/**
 * Safety cap: 20 pages x 5000 rows = 100,000 raw JSON rows, matching the
 * server's JSON ceiling. Reaching it surfaces partial results honestly.
 */
export const BARS_HISTORY_MAX_PAGES = 20;

export interface BarsHistoryOptions {
  instrument: string;
  interval: BarInterval;
  start: string;
  end: string;
  limit?: number;
  maxPages?: number;
}

export interface BarsHistoryResult {
  /** Every raw revision in ascending server response order; never deduped. */
  bars: BarMessageEnvelope[];
  pages: number;
  complete: boolean;
  /** Non-null when the guard stopped early; `bars` is then partial. */
  stop: BarsPageStop | null;
}

/**
 * Paginated single-instrument JSON history with the contract's Time Cursor
 * guard. It never invents `start = next_start + interval`; a non-advancing or
 * revision-group boundary stops the walk and returns partial rows.
 */
export async function fetchBarsHistory(
  options: BarsHistoryOptions,
  signal?: AbortSignal,
): Promise<BarsHistoryResult> {
  const limit = options.limit ?? BARS_HISTORY_PAGE_LIMIT;
  const maxPages = options.maxPages ?? BARS_HISTORY_MAX_PAGES;
  const bars: BarMessageEnvelope[] = [];
  let cursor = options.start;
  let pages = 0;

  for (;;) {
    const page = await fetchBarsJson(
      {
        instrument: options.instrument,
        interval: options.interval,
        start: cursor,
        end: options.end,
        limit,
        format: "json",
      },
      signal,
    );
    pages += 1;
    bars.push(...page.bars);

    const step = decideBarsPageStep({
      requestStart: cursor,
      page,
      lastReturnedOpenTs: maxOpenTs(bars),
      pageCount: pages,
      maxPages,
    });

    if (step.kind === "complete") {
      return { bars, pages, complete: true, stop: null };
    }
    if (step.kind === "stop") {
      return { bars, pages, complete: false, stop: step.stop };
    }
    cursor = step.nextStart;
  }
}

export interface CsvProgress {
  loadedBytes: number;
  /** Trusted Content-Length / progress total; null means unknown. */
  totalBytes: number | null;
  /** Never present when the total is unknown. */
  percent: number | null;
}

export function computeCsvProgress(
  loadedBytes: number,
  totalBytes: number | null,
): CsvProgress {
  const loaded =
    Number.isFinite(loadedBytes) && loadedBytes > 0 ? loadedBytes : 0;
  const total =
    totalBytes !== null && Number.isFinite(totalBytes) && totalBytes > 0
      ? totalBytes
      : null;
  return {
    loadedBytes: loaded,
    totalBytes: total,
    percent:
      total === null
        ? null
        : Math.min(100, Math.round((loaded / total) * 100)),
  };
}

export function defaultCsvFilename(interval: string): string {
  return `${interval}-bars.csv`;
}

export interface CsvDownloadOptions {
  signal?: AbortSignal;
  onProgress?: (progress: CsvProgress) => void;
}

export interface CsvDownloadResult {
  blob: Blob;
  filename: string;
  contentType: string | null;
  contentLength: number | null;
  requestId: string | null;
  disposition: string | null;
}

export class CsvDownloadCancelledError extends Error {
  constructor() {
    super("CSV 下载已取消");
    this.name = "CsvDownloadCancelledError";
  }
}

function parseTrustedLength(value: string | undefined): number | null {
  if (value === undefined) {
    return null;
  }
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
}

function isCancellation(error: unknown, signal?: AbortSignal): boolean {
  if (signal?.aborted === true) {
    return true;
  }
  if (axios.isCancel(error)) {
    return true;
  }
  return (
    typeof error === "object" &&
    error !== null &&
    (error as { code?: unknown }).code === "ERR_CANCELED"
  );
}

/**
 * Direct small-file CSV path: `responseType: "blob"`, percent only with a
 * trusted total, AbortController support and unified Blob error parsing.
 * The returned Blob is passed straight to the download anchor; its bytes are
 * never re-serialized.
 */
export async function fetchBarsCsv(
  query: BarsQueryParams,
  options: CsvDownloadOptions = {},
): Promise<CsvDownloadResult> {
  let progress = computeCsvProgress(0, null);
  try {
    const response = await blobClient.get<Blob>("/api/v1/bars", {
      params: buildBarsSearchParams({ ...query, format: "csv" }),
      responseType: "blob",
      signal: options.signal,
      onDownloadProgress: (event) => {
        const trustedTotal =
          event.lengthComputable === true &&
          typeof event.total === "number" &&
          Number.isFinite(event.total) &&
          event.total > 0
            ? event.total
            : null;
        progress = computeCsvProgress(
          typeof event.loaded === "number" ? event.loaded : 0,
          trustedTotal,
        );
        options.onProgress?.(progress);
      },
    });

    const blob = response.data;
    if (!(blob instanceof Blob)) {
      throw new HttpError({
        kind: "rest",
        status: response.status,
        code: "unexpected_blob_response",
        message: "CSV 响应不是 Blob",
        details: {},
        requestId: null,
      });
    }

    const headers = normalizeHeaders(response.headers);
    const disposition = headers["content-disposition"] ?? null;
    const contentLength = parseTrustedLength(headers["content-length"]);
    const totalBytes = progress.totalBytes ?? contentLength;
    const finalProgress = computeCsvProgress(blob.size, totalBytes);
    options.onProgress?.(finalProgress);

    return {
      blob,
      filename: parseContentDispositionFilename(
        disposition,
        defaultCsvFilename(query.interval),
      ),
      contentType: headers["content-type"] ?? null,
      contentLength,
      requestId: headers["x-request-id"] ?? null,
      disposition,
    };
  } catch (error) {
    if (isCancellation(error, options.signal)) {
      throw new CsvDownloadCancelledError();
    }
    if (error instanceof HttpError) {
      throw error;
    }
    const failure = toHttpFailure(error);
    if (failure.data instanceof Blob) {
      throw await parseBlobFailure(failure.data, {
        status: failure.status,
        headers: failure.headers,
      });
    }
    throw normalizeRestFailure(failure);
  }
}
