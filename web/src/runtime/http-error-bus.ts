import type { NormalizedHttpError } from "@/api/http/types";

export type HttpErrorListener = (error: NormalizedHttpError) => void;

const listeners = new Set<HttpErrorListener>();

/**
 * Shell-level notices for 401/403/409/429. The API client emits only these
 * statuses; other failures stay local to the component that made the call.
 */
export function emitHttpError(error: NormalizedHttpError): void {
  for (const listener of listeners) {
    listener(error);
  }
}

export function subscribeHttpErrors(listener: HttpErrorListener): () => void {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}
