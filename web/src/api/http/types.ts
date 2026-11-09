export type HeaderMap = Record<string, string | undefined>;

export interface HttpFailure {
  status: number | null;
  headers: HeaderMap;
  data: unknown;
  transportMessage?: string;
  transportKind?: "network" | "timeout";
}

export type HttpErrorKind = "rest" | "ops";

/**
 * The single error shape every API adapter produces. Components branch on
 * `code`, never on HTTP status or prose, and always surface `requestId`.
 */
export interface NormalizedHttpError {
  kind: HttpErrorKind;
  status: number | null;
  code: string;
  message: string;
  details: Record<string, unknown>;
  requestId: string | null;
  headers?: HeaderMap;
}

export class HttpError extends Error implements NormalizedHttpError {
  readonly kind: HttpErrorKind;
  readonly status: number | null;
  readonly code: string;
  readonly details: Record<string, unknown>;
  readonly requestId: string | null;
  readonly headers?: HeaderMap;

  constructor(init: NormalizedHttpError) {
    super(init.message);
    this.name = "HttpError";
    this.kind = init.kind;
    this.status = init.status;
    this.code = init.code;
    this.details = init.details;
    this.requestId = init.requestId;
    this.headers = init.headers;
  }
}

export interface HttpErrorDirective {
  action:
    | "login-required"
    | "permission-denied"
    | "conflict"
    | "retry-after"
    | "show-error";
  requiredRole?: string;
  reason?: string;
  retryAfterS?: number;
}

export function getHeader(
  headers: HeaderMap | undefined,
  name: string,
): string | undefined {
  if (!headers) {
    return undefined;
  }
  const normalized = name.toLowerCase();
  for (const [key, value] of Object.entries(headers)) {
    if (key.toLowerCase() === normalized) {
      return value;
    }
  }
  return undefined;
}
