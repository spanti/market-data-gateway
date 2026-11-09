export interface TrendPoint {
  /** Milliseconds since the Unix epoch, UTC. */
  ts: number;
  /** null means the source had no sample; charts show a gap, never zero. */
  value: number | null;
}

export interface TrendSeries {
  name: string;
  labels?: Record<string, string>;
  points: TrendPoint[];
}

export interface TrendWindow {
  startMs: number;
  endMs: number;
  stepSeconds: number;
}

/** Where a trend came from. Used to label the source in the UI. */
export type TrendSourceKind = "direct-prometheus" | "admin-proxy";

export interface TrendSourceInfo {
  kind: TrendSourceKind;
  label: string;
  windowLabel: string;
}
