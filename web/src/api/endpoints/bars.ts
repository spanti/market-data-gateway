import { apiClient } from "@/api/http/client";
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
