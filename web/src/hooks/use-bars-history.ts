import { useQuery } from "@tanstack/react-query";
import {
  fetchBarsV2History,
  type BarsV2HistoryResult,
} from "@/api/endpoints/bars-v2";
import type { BarInterval } from "@/lib/time-range";

export interface UseBarsHistoryOptions {
  instrumentId: string | null;
  interval: BarInterval | null;
  start: Date;
  end: Date;
  enabled?: boolean;
}

export function barsHistoryQueryKey(
  options: Pick<UseBarsHistoryOptions, "instrumentId" | "interval" | "start" | "end">,
) {
  return [
    "bars",
    "history",
    options.instrumentId,
    options.interval,
    options.start.toISOString(),
    options.end.toISOString(),
  ] as const;
}

/**
 * Guarded REST history. The query is disabled without an identity or when the
 * chart range policy rejects the span; nothing auto-expands an empty range.
 */
export function useBarsHistory(options: UseBarsHistoryOptions) {
  const { instrumentId, interval, start, end } = options;
  return useQuery<BarsV2HistoryResult>({
    queryKey: barsHistoryQueryKey(options),
    queryFn: ({ signal }) => {
      if (!instrumentId || !interval) {
        throw new Error("Bars history requires InstrumentId and Bar Interval");
      }
      return fetchBarsV2History(
        {
          instrument: instrumentId,
          interval,
          start: start.toISOString(),
          end: end.toISOString(),
        },
        signal,
      );
    },
    enabled:
      (options.enabled ?? true) &&
      instrumentId !== null &&
      interval !== null,
  });
}
