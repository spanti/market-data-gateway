import { z } from "zod";
import { apiClient } from "@/api/http/client";
import { HttpError } from "@/api/http/types";

/**
 * Capability discovery is the only way the console decides which runtime
 * mode it is in. It must never be inferred from cached data or a previously
 * successful read.
 *
 * Current Contract : GET /api/v1/admin/capabilities is absent (404/405/501).
 * Admin API        : 200 with a valid document, or 401 (Admin exists, auth required).
 * Unknown          : network/timeout/5xx/malformed. Never degrade to Current Contract.
 */

export type RuntimeMode =
  | "current-contract"
  | "admin-api"
  | "capabilities-unknown";

export type CapabilityAvailability =
  | "enabled"
  | "unsupported"
  | "adapter_unavailable";

export interface CapabilityEntry {
  state: CapabilityAvailability;
  [key: string]: unknown;
}

export interface CapabilityVersion {
  admin_api: string;
  contract: number;
  supported_contracts: number[];
  [key: string]: unknown;
}

export interface CapabilityAuth {
  required: boolean;
  mode?: string;
  state?: string;
  roles: string[];
  [key: string]: unknown;
}

export interface CapabilityAdapter {
  name: string;
  kind?: string;
  state?: string;
  reason?: string | null;
  [key: string]: unknown;
}

export interface CapabilityDocument {
  schema_version?: number;
  server_time: string;
  version: CapabilityVersion;
  auth: CapabilityAuth;
  adapters: CapabilityAdapter[];
  capabilities: Record<string, CapabilityEntry>;
  limits?: Record<string, unknown>;
  [key: string]: unknown;
}

export const SUPPORTED_CONTRACT_VERSION = 1;

const documentSchema = z
  .object({
    schema_version: z.number().optional(),
    server_time: z.string(),
    version: z
      .object({
        admin_api: z.string(),
        contract: z.number(),
        supported_contracts: z.array(z.number()),
      })
      .passthrough(),
    auth: z
      .object({
        required: z.boolean(),
        mode: z.string().optional(),
        state: z.string().optional(),
        roles: z.array(z.string()),
      })
      .passthrough(),
    adapters: z
      .array(
        z
          .object({
            name: z.string(),
            kind: z.string().optional(),
            state: z.string().optional(),
            reason: z.string().nullable().optional(),
          })
          .passthrough(),
      )
      .optional()
      .default([]),
    capabilities: z.record(z.string(), z.unknown()),
    limits: z.record(z.string(), z.unknown()).optional(),
  })
  .passthrough();

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function normalizeAvailability(value: unknown): CapabilityAvailability {
  if (
    value === "enabled" ||
    value === "unsupported" ||
    value === "adapter_unavailable"
  ) {
    return value;
  }
  // Missing key or unknown state is treated as unsupported (fail closed).
  return "unsupported";
}

export function parseCapabilityDocument(
  data: unknown,
): CapabilityDocument | null {
  const parsed = documentSchema.safeParse(data);
  if (!parsed.success) {
    return null;
  }

  const capabilities: Record<string, CapabilityEntry> = {};
  for (const [name, value] of Object.entries(parsed.data.capabilities)) {
    const entry = isRecord(value) ? value : {};
    capabilities[name] = {
      ...entry,
      state: normalizeAvailability(entry.state),
    };
  }

  return {
    ...parsed.data,
    capabilities,
  } as CapabilityDocument;
}

export type CapabilityFetchOutcome =
  | { kind: "response"; status: number; data: unknown }
  | { kind: "network-error"; message: string }
  | { kind: "timeout"; message: string };

export interface CapabilityCacheEntry {
  document: CapabilityDocument;
  lastSuccessAt: string;
}

export interface CurrentContractState {
  mode: "current-contract";
  lastSuccessAt: null;
}

export interface AdminApiState {
  mode: "admin-api";
  document: CapabilityDocument | null;
  compatible: boolean | null;
  lastSuccessAt: string | null;
}

export interface CapabilitiesUnknownState {
  mode: "capabilities-unknown";
  lastSuccessAt: string | null;
  errorCode: string;
  message: string;
}

export type CapabilityState =
  | CurrentContractState
  | AdminApiState
  | CapabilitiesUnknownState;

export function resolveCapabilityState(
  outcome: CapabilityFetchOutcome,
  cached: CapabilityCacheEntry | null = null,
  now: () => Date = () => new Date(),
): CapabilityState {
  if (outcome.kind === "network-error" || outcome.kind === "timeout") {
    return {
      mode: "capabilities-unknown",
      lastSuccessAt: cached?.lastSuccessAt ?? null,
      errorCode:
        outcome.kind === "timeout" ? "timeout" : "network_error",
      message: outcome.message,
    };
  }

  if (outcome.status === 200) {
    const document = parseCapabilityDocument(outcome.data);
    if (!document) {
      return {
        mode: "capabilities-unknown",
        lastSuccessAt: cached?.lastSuccessAt ?? null,
        errorCode: "malformed_capabilities",
        message: "Capability document did not match the expected schema",
      };
    }
    const compatible =
      document.version.supported_contracts.includes(
        SUPPORTED_CONTRACT_VERSION,
      ) ?? false;
    return {
      mode: "admin-api",
      document,
      compatible,
      lastSuccessAt: now().toISOString(),
    };
  }

  if (outcome.status === 404 || outcome.status === 405 || outcome.status === 501) {
    return { mode: "current-contract", lastSuccessAt: null };
  }

  if (outcome.status === 401) {
    // 401 proves the Admin API exists; it does not prove the version.
    return {
      mode: "admin-api",
      document: null,
      compatible: null,
      lastSuccessAt: cached?.lastSuccessAt ?? null,
    };
  }

  return {
    mode: "capabilities-unknown",
    lastSuccessAt: cached?.lastSuccessAt ?? null,
    errorCode: `http_${outcome.status}`,
    message: `Capability discovery returned HTTP ${outcome.status}`,
  };
}

let capabilityCache: CapabilityCacheEntry | null = null;

export function resetCapabilityCache(): void {
  capabilityCache = null;
}

export function getCachedCapabilities(): CapabilityCacheEntry | null {
  return capabilityCache;
}

async function fetchCapabilityOutcome(): Promise<CapabilityFetchOutcome> {
  try {
    const response = await apiClient.get<unknown>(
      "/api/v1/admin/capabilities",
    );
    return { kind: "response", status: response.status, data: response.data };
  } catch (error) {
    if (error instanceof HttpError) {
      if (error.status === null) {
        return {
          kind: error.code === "timeout" ? "timeout" : "network-error",
          message: error.message,
        };
      }
      return { kind: "response", status: error.status, data: null };
    }
    return {
      kind: "network-error",
      message:
        error instanceof Error ? error.message : "Unknown capability failure",
    };
  }
}

/**
 * Fetches capability discovery and maintains an in-memory last-success
 * annotation. It is deliberately not persisted to localStorage: a cached
 * capability document must never unlock admin UI across reloads.
 */
export async function discoverCapabilities(): Promise<CapabilityState> {
  const outcome = await fetchCapabilityOutcome();
  const state = resolveCapabilityState(outcome, capabilityCache);

  if (state.mode === "admin-api" && state.document) {
    capabilityCache = {
      document: state.document,
      lastSuccessAt: state.lastSuccessAt ?? new Date().toISOString(),
    };
  }

  return state;
}
