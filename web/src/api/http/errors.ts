import {
  getHeader,
  HttpError,
  type HeaderMap,
  type HttpErrorDirective,
  type HttpFailure,
  type NormalizedHttpError,
} from "./types";

interface ParsedEnvelope {
  code: string;
  message: string;
  details: Record<string, unknown>;
  requestId: string | null;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function parseEnvelope(data: unknown): ParsedEnvelope | null {
  if (!isRecord(data) || !isRecord(data.error)) {
    return null;
  }
  const body = data.error;
  if (typeof body.code !== "string" || typeof body.message !== "string") {
    return null;
  }
  return {
    code: body.code,
    message: body.message,
    details: isRecord(body.details) ? body.details : {},
    requestId:
      typeof body.request_id === "string" && body.request_id.length > 0
        ? body.request_id
        : null,
  };
}

function envelopeRequestId(
  detail: ParsedEnvelope | null,
  headers: HeaderMap,
): string | null {
  return detail?.requestId ?? getHeader(headers, "x-request-id") ?? null;
}

/**
 * REST failures must use the unified `{error:{code,message,details,request_id}}`
 * envelope. Anything else is reported as a contract failure, never guessed at.
 */
export function normalizeRestFailure(failure: HttpFailure): HttpError {
  if (failure.status === null) {
    const message = failure.transportMessage?.trim()
      ? failure.transportMessage
      : "Network request failed";
    return new HttpError({
      kind: "rest",
      status: null,
      code: failure.transportKind === "timeout" ? "timeout" : "network_error",
      message,
      details: {},
      requestId: getHeader(failure.headers, "x-request-id") ?? null,
      headers: failure.headers,
    });
  }

  const envelope = parseEnvelope(failure.data);
  if (envelope) {
    return new HttpError({
      kind: "rest",
      status: failure.status,
      code: envelope.code,
      message: envelope.message,
      details: envelope.details,
      requestId: envelopeRequestId(envelope, failure.headers),
      headers: failure.headers,
    });
  }

  return new HttpError({
    kind: "rest",
    status: failure.status,
    code: "unexpected_error_response",
    message: `HTTP ${failure.status} response did not match the unified error envelope`,
    details: {},
    requestId: getHeader(failure.headers, "x-request-id") ?? null,
    headers: failure.headers,
  });
}

function opsMessage(body: unknown, status: number | null): string {
  if (isRecord(body)) {
    if (typeof body.reason === "string" && body.reason.trim()) {
      return body.reason;
    }
    if (typeof body.detail === "string" && body.detail.trim()) {
      return body.detail;
    }
    if (typeof body.error === "string" && body.error.trim()) {
      return body.error;
    }
  }
  if (typeof body === "string" && body.trim()) {
    const text = body.trim();
    return text.length > 500 ? `${text.slice(0, 500)}…` : text;
  }
  return status === null
    ? "Ops endpoint unavailable"
    : `Ops endpoint returned HTTP ${status}`;
}

/**
 * Collector/replay health servers use bare Starlette responses: no REST
 * envelope and no X-Request-ID. They therefore get a separate adapter.
 */
export function normalizeOpsFailure(failure: HttpFailure): HttpError {
  const network = failure.status === null;
  return new HttpError({
    kind: "ops",
    status: failure.status,
    code: network ? "ops_unreachable" : "ops_error",
    message: network
      ? failure.transportMessage?.trim() || "Ops endpoint unreachable"
      : opsMessage(failure.data, failure.status),
    details: { status: failure.status },
    requestId: null,
    headers: failure.headers,
  });
}

async function readBlobText(blob: Blob): Promise<string> {
  if (typeof blob.text === "function") {
    try {
      return await blob.text();
    } catch {
      // Fall through to FileReader (older browser / test environments).
    }
  }
  if (typeof FileReader !== "undefined") {
    return new Promise((resolve) => {
      const reader = new FileReader();
      reader.onload = () =>
        resolve(typeof reader.result === "string" ? reader.result : "");
      reader.onerror = () => resolve("");
      reader.readAsText(blob);
    });
  }
  return "";
}

/**
 * Blob responses (CSV/export errors) still carry the REST envelope. Parse it
 * so the user sees the stable code and Request ID instead of opaque binary.
 */
export async function parseBlobFailure(
  blob: Blob,
  init: { status: number | null; headers: HeaderMap },
): Promise<HttpError> {
  const text = await readBlobText(blob);

  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    parsed = undefined;
  }

  const envelope = parseEnvelope(parsed);
  if (envelope) {
    return new HttpError({
      kind: "rest",
      status: init.status,
      code: envelope.code,
      message: envelope.message,
      details: envelope.details,
      requestId: envelopeRequestId(envelope, init.headers),
      headers: init.headers,
    });
  }

  const fallbackMessage = (() => {
    if (isRecord(parsed)) {
      if (typeof parsed.message === "string" && parsed.message.trim()) {
        return parsed.message;
      }
      if (typeof parsed.detail === "string" && parsed.detail.trim()) {
        return parsed.detail;
      }
    }
    const trimmed = text.trim();
    if (trimmed) {
      return trimmed.length > 300 ? `${trimmed.slice(0, 300)}…` : trimmed;
    }
    return init.status === null
      ? "Blob response failed"
      : `Blob response returned HTTP ${init.status}`;
  })();

  return new HttpError({
    kind: "rest",
    status: init.status,
    code: "blob_error_response",
    message: fallbackMessage,
    details: {},
    requestId: getHeader(init.headers, "x-request-id") ?? null,
    headers: init.headers,
  });
}

function parseRetryAfter(
  error: NormalizedHttpError,
): number | undefined {
  const header = getHeader(error.headers, "retry-after");
  if (header) {
    const parsed = Number.parseInt(header, 10);
    if (Number.isFinite(parsed) && parsed >= 0) {
      return parsed;
    }
  }
  const fromDetails = error.details.retry_after_s;
  if (typeof fromDetails === "number" && Number.isFinite(fromDetails)) {
    return fromDetails;
  }
  return undefined;
}

/**
 * Global handling policy for the shell: 401 stops writes and returns to the
 * login entry, 403 names the missing role, 409 demands a manual refresh,
 * 429 respects Retry-After.
 */
export function classifyHttpError(
  error: NormalizedHttpError,
): HttpErrorDirective {
  if (error.status === 401) {
    return { action: "login-required" };
  }
  if (error.status === 403) {
    const requiredRole =
      typeof error.details.required_role === "string"
        ? error.details.required_role
        : undefined;
    return { action: "permission-denied", requiredRole };
  }
  if (error.status === 409) {
    return {
      action: "conflict",
      reason:
        typeof error.details.reason === "string"
          ? error.details.reason
          : undefined,
    };
  }
  if (error.status === 429) {
    return { action: "retry-after", retryAfterS: parseRetryAfter(error) };
  }
  return { action: "show-error" };
}
