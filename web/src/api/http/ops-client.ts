import axios from "axios";
import { toHttpFailure } from "@/api/http/client";
import { normalizeOpsFailure } from "@/api/http/errors";

/**
 * Collector/replay health servers return bare Starlette responses: no REST
 * error envelope and no X-Request-ID. They therefore use their own client and
 * ops adapter. The paths are same-origin dev/production proxy conventions;
 * the backends themselves serve ops endpoints at their own origins.
 */
export const COLLECTOR_OPS_BASE = "/ops/collector";
export const REPLAY_OPS_BASE = "/ops/replay";

export const opsClient = axios.create({
  timeout: 10_000,
});

opsClient.interceptors.response.use(
  (response) => response,
  (error: unknown) =>
    Promise.reject(normalizeOpsFailure(toHttpFailure(error))),
);
