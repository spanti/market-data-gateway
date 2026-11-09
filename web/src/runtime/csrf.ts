/**
 * CSRF credentials live in this module's memory only. They are never written
 * to localStorage, sessionStorage, IndexedDB, cookies readable by JS, a URL,
 * a log, or analytics. A page reload drops the credential; the SPA re-probes
 * `GET /auth/session` to obtain a fresh one.
 */
export interface CsrfCredential {
  header_name: string;
  token: string;
}

let credential: CsrfCredential | null = null;

export function setCsrfCredential(next: CsrfCredential | null): void {
  if (!next || !next.token) {
    credential = null;
    return;
  }
  credential = {
    header_name: next.header_name || "X-CSRF-Token",
    token: next.token,
  };
}

export function getCsrfCredential(): CsrfCredential | null {
  return credential;
}

export function clearCsrfCredential(): void {
  credential = null;
}
