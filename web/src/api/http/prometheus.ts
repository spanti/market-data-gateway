/**
 * Prometheus HTTP API and text exposition adapters.
 *
 * Prometheus does not use the gateway REST envelope, so its responses must
 * never be fed through `normalizeRestFailure`. Missing/invalid samples stay
 * explicit; nothing is zero-filled or treated as healthy.
 */

export interface PrometheusPoint {
  ts: number;
  value: number;
}

export interface PrometheusSeries {
  labels: Record<string, string>;
  points: PrometheusPoint[];
}

export type PrometheusResultType = "matrix" | "vector" | "scalar" | "string";

export interface PrometheusQueryResult {
  resultType: PrometheusResultType;
  series: PrometheusSeries[];
}

export interface PrometheusSample {
  name: string;
  labels: Record<string, string>;
  value: number;
}

export type PrometheusErrorCode = "prometheus_error" | "prometheus_malformed";

export class PrometheusContractError extends Error {
  readonly code: PrometheusErrorCode;
  readonly details: Record<string, unknown>;

  constructor(
    code: PrometheusErrorCode,
    message: string,
    details: Record<string, unknown> = {},
  ) {
    super(message);
    this.name = "PrometheusContractError";
    this.code = code;
    this.details = details;
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isResultType(value: unknown): value is PrometheusResultType {
  return (
    value === "matrix" ||
    value === "vector" ||
    value === "scalar" ||
    value === "string"
  );
}

function parseLabels(value: unknown): Record<string, string> {
  if (!isRecord(value)) {
    return {};
  }
  const labels: Record<string, string> = {};
  for (const [key, entry] of Object.entries(value)) {
    labels[key] = String(entry);
  }
  return labels;
}

function parsePoint(value: unknown): PrometheusPoint {
  if (!Array.isArray(value) || value.length < 2) {
    throw new PrometheusContractError(
      "prometheus_malformed",
      "Prometheus sample is not a [timestamp, value] tuple",
    );
  }
  const ts = Number(value[0]);
  const numeric = Number(value[1]);
  if (!Number.isFinite(ts)) {
    throw new PrometheusContractError(
      "prometheus_malformed",
      "Prometheus sample timestamp is not numeric",
    );
  }
  return { ts, value: numeric };
}

function parseSeriesArray(result: unknown): PrometheusSeries[] {
  if (!Array.isArray(result)) {
    throw new PrometheusContractError(
      "prometheus_malformed",
      "Prometheus result is not an array",
    );
  }
  return result.map((entry) => {
    if (!isRecord(entry)) {
      throw new PrometheusContractError(
        "prometheus_malformed",
        "Prometheus series entry is not an object",
      );
    }
    const labels = parseLabels(entry.metric);
    if (Array.isArray(entry.values)) {
      return { labels, points: entry.values.map(parsePoint) };
    }
    if (entry.value !== undefined) {
      return { labels, points: [parsePoint(entry.value)] };
    }
    throw new PrometheusContractError(
      "prometheus_malformed",
      "Prometheus series has neither values nor value",
    );
  });
}

export function parsePrometheusPayload(
  payload: unknown,
): PrometheusQueryResult {
  if (!isRecord(payload)) {
    throw new PrometheusContractError(
      "prometheus_malformed",
      "Prometheus response is not an object",
    );
  }
  if (payload.status === "error") {
    throw new PrometheusContractError(
      "prometheus_error",
      typeof payload.error === "string"
        ? payload.error
        : "Prometheus query failed",
      {
        errorType:
          typeof payload.errorType === "string" ? payload.errorType : null,
      },
    );
  }
  if (payload.status !== "success" || !isRecord(payload.data)) {
    throw new PrometheusContractError(
      "prometheus_malformed",
      "Prometheus response is malformed",
    );
  }
  const resultType = payload.data.resultType;
  if (!isResultType(resultType)) {
    throw new PrometheusContractError(
      "prometheus_malformed",
      "Prometheus resultType is missing or unknown",
    );
  }

  if (resultType === "matrix" || resultType === "vector") {
    return {
      resultType,
      series: parseSeriesArray(payload.data.result),
    };
  }

  // scalar / string results are a single [timestamp, value] tuple.
  const point = parsePoint(payload.data.result);
  return {
    resultType,
    series: [{ labels: {}, points: [point] }],
  };
}

function unescapeLabelValue(value: string): string {
  return value.replace(/\\(["\\n])/g, (_match, char: string) => {
    if (char === "n") {
      return "\n";
    }
    return char;
  });
}

function parseLabelsText(text: string): Record<string, string> {
  const labels: Record<string, string> = {};
  const labelPattern = /([a-zA-Z_][a-zA-Z0-9_]*)="((?:[^"\\]|\\.)*)"/g;
  let match = labelPattern.exec(text);
  while (match) {
    const key = match[1];
    const raw = match[2];
    if (key !== undefined && raw !== undefined) {
      labels[key] = unescapeLabelValue(raw);
    }
    match = labelPattern.exec(text);
  }
  return labels;
}

/**
 * Parses the Prometheus text exposition format used by `/metrics`.
 * HELP/TYPE comments are skipped. Invalid numeric samples are skipped rather
 * than turned into zero.
 */
export function parsePrometheusExposition(text: string): PrometheusSample[] {
  const samples: PrometheusSample[] = [];
  const samplePattern =
    /^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(.*)\})?\s+(\S+)(?:\s+\S+)?$/;

  for (const line of text.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) {
      continue;
    }
    const match = samplePattern.exec(trimmed);
    if (!match) {
      continue;
    }
    const name = match[1];
    const labelsText = match[2] ?? "";
    const valueText = match[3];
    if (name === undefined || valueText === undefined) {
      continue;
    }
    const value = Number(valueText);
    if (!Number.isFinite(value)) {
      continue;
    }
    samples.push({
      name,
      labels: parseLabelsText(labelsText),
      value,
    });
  }
  return samples;
}
