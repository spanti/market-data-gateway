import { execFileSync } from "node:child_process";

/**
 * Removes the Prometheus acceptance container. The container is started
 * detached by .playwright-tmp/prometheus-run.py; Playwright stops that helper
 * process but cannot stop a detached container on Windows.
 */
export default function globalTeardown(): void {
  const port = process.env.MDGW_E2E_PROMETHEUS_PORT ?? "9099";
  const name =
    process.env.MDGW_E2E_PROMETHEUS_CONTAINER ??
    `mdgw-e2e-prometheus-${port}`;
  try {
    execFileSync("docker", ["rm", "-f", name], { stdio: "ignore" });
  } catch {
    // Docker absent or the container already removed; nothing to clean up.
  }
}
