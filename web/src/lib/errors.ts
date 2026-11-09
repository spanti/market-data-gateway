import { HttpError } from "@/api/http/types";

/**
 * Single error-text projection for the UI. Network/HttpError messages stay
 * verbatim; anything without a message gets an explicit fallback instead of
 * rendering `undefined`.
 */
export function errorText(error: unknown, fallback = "未知错误"): string {
  if (error instanceof HttpError) {
    return error.message;
  }
  if (error instanceof Error && error.message.trim()) {
    return error.message;
  }
  return fallback;
}

export interface QueryErrorInfo {
  /** Stable code + message, used by Query error states. */
  message: string;
  requestId: string | null;
}

/** Query-error projection that keeps the Request ID available for support. */
export function queryErrorInfo(
  error: unknown,
  fallback = "未知错误",
): QueryErrorInfo {
  if (error instanceof HttpError) {
    return {
      message: `${error.code}: ${error.message}`,
      requestId: error.requestId,
    };
  }
  return { message: errorText(error, fallback), requestId: null };
}
