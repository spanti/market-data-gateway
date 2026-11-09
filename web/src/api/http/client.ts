import axios, { type AxiosError } from "axios";
import { normalizeRestFailure } from "./errors";
import { emitHttpError } from "@/runtime/http-error-bus";
import type { HeaderMap, HttpFailure } from "./types";

const BASE_URL = import.meta.env.VITE_API_BASE_URL ?? "";
const DEFAULT_TIMEOUT_MS = 15_000;

/**
 * Same-origin HTTP client. It never reads or writes tokens, never adds
 * credentials to a URL, and relies on the HttpOnly server session cookie
 * (`withCredentials`) in Admin API mode.
 */
export const apiClient = axios.create({
  baseURL: BASE_URL,
  timeout: DEFAULT_TIMEOUT_MS,
  withCredentials: true,
  headers: {
    Accept: "application/json",
  },
});

function normalizeHeaders(headers: unknown): HeaderMap {
  if (!headers) {
    return {};
  }
  let source: Record<string, unknown>;
  if (typeof (headers as { toJSON?: () => unknown }).toJSON === "function") {
    source = (headers as { toJSON: () => Record<string, unknown> }).toJSON();
  } else if (typeof headers === "object") {
    source = headers as Record<string, unknown>;
  } else {
    return {};
  }

  const normalized: HeaderMap = {};
  for (const [key, value] of Object.entries(source)) {
    if (value === undefined || value === null) {
      continue;
    }
    normalized[key.toLowerCase()] = Array.isArray(value)
      ? value.join(", ")
      : String(value);
  }
  return normalized;
}

/**
 * Converts any Axios failure into the adapter input shape. Kept separate from
 * normalization so ops and Prometheus paths can reuse the transport handling
 * with their own response parsers.
 */
export function toHttpFailure(error: unknown): HttpFailure {
  if (axios.isAxiosError(error)) {
    const axiosError = error as AxiosError;
    const response = axiosError.response;
    if (response) {
      return {
        status: response.status,
        headers: normalizeHeaders(response.headers),
        data: response.data,
        transportMessage: axiosError.message,
      };
    }
    const timedOut =
      axiosError.code === "ECONNABORTED" || axiosError.code === "ETIMEDOUT";
    return {
      status: null,
      headers: {},
      data: undefined,
      transportMessage: axiosError.message,
      transportKind: timedOut ? "timeout" : "network",
    };
  }

  return {
    status: null,
    headers: {},
    data: undefined,
    transportMessage:
      error instanceof Error ? error.message : "Unknown network failure",
  };
}

const GLOBAL_NOTICE_STATUSES = new Set([401, 403, 409, 429]);

apiClient.interceptors.response.use(
  (response) => response,
  (error: unknown) => {
    const normalized = normalizeRestFailure(toHttpFailure(error));
    if (
      normalized.status !== null &&
      GLOBAL_NOTICE_STATUSES.has(normalized.status)
    ) {
      emitHttpError(normalized);
    }
    return Promise.reject(normalized);
  },
);

export function getResponseRequestId(headers: unknown): string | null {
  const normalized = normalizeHeaders(headers);
  return normalized["x-request-id"] ?? null;
}
