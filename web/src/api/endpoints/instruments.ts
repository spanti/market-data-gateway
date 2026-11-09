import { apiClient } from "@/api/http/client";

/** Instrument Reference Data from the read-only Instrument Snapshot. */
export interface Instrument {
  instrument_id: string;
  venue: string;
  symbol: string;
  asset_class: string;
  instrument_type: string;
  base: string;
  quote: string;
  tick_size: string;
  lot_size: string;
  multiplier: string;
  timezone: string;
  listing_ts: string;
  delisted_ts: string | null;
}

export interface InstrumentsResponse {
  instruments: Instrument[];
}

export interface InstrumentResponse {
  instrument: Instrument;
}

export interface InstrumentsQueryParams {
  venue?: string;
  asset_class?: string;
  include_delisted?: boolean;
}

export async function fetchInstruments(
  query: InstrumentsQueryParams = {},
  signal?: AbortSignal,
): Promise<InstrumentsResponse> {
  const response = await apiClient.get<InstrumentsResponse>(
    "/api/v1/instruments",
    {
      params: {
        ...(query.venue ? { venue: query.venue } : {}),
        ...(query.asset_class ? { asset_class: query.asset_class } : {}),
        ...(query.include_delisted !== undefined
          ? { include_delisted: String(query.include_delisted) }
          : {}),
      },
      signal,
    },
  );
  return response.data;
}

export async function fetchInstrument(
  instrumentId: string,
  signal?: AbortSignal,
): Promise<InstrumentResponse> {
  const response = await apiClient.get<InstrumentResponse>(
    `/api/v1/instruments/${encodeURIComponent(instrumentId)}`,
    { signal },
  );
  return response.data;
}
