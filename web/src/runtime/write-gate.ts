/**
 * After a 401 the console must not issue further mutations until a new
 * server session is established. Reads stay available so the SPA can probe
 * `GET /auth/session` and render the login entry.
 */
let stopped = false;

export function stopWrites(): void {
  stopped = true;
}

export function allowWrites(): void {
  stopped = false;
}

export function writesAreStopped(): boolean {
  return stopped;
}

/** Test helper: restore the module to its boot state. */
export function resetWriteGate(): void {
  stopped = false;
}
