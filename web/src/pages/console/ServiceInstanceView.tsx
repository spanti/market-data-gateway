import { useMutation, useQuery } from "@tanstack/react-query";
import { ServerCog } from "lucide-react";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  fetchAdminOperation,
  newIdempotencyKey,
  postServiceLifecycle,
} from "@/api/endpoints/admin-console";
import {
  adminServiceInstanceToRow,
  fetchAdminServiceInstances,
  type ServiceInstanceRowData,
} from "@/api/endpoints/admin-service-instances";
import {
  SERVICE_ROLES,
  type ServiceLifecycleBody,
  type ServiceRole,
} from "@/api/endpoints/admin";
import {
  fetchApiReady,
  fetchApiStatus,
  fetchCollectorReady,
  fetchCollectorStatus,
  fetchReplayReady,
  fetchReplayStatus,
  type ApiStatus,
  type CollectorStatus,
  type ReadyPayload,
  type ReplayStatus,
} from "@/api/endpoints/ops";
import { HttpError } from "@/api/http/types";
import { errorText } from "@/lib/errors";
import {
  OPERATION_POLL_INTERVAL_MS,
  STATUS_POLL_INTERVAL_MS,
} from "@/lib/monitoring-config";
import { formatUtcTimestamp } from "@/lib/format";
import { ConfirmDialog, type ConfirmDialogInput } from "@/components/confirm-dialog";
import { DisabledReason } from "@/components/disabled-reason";
import { HelpTrigger } from "@/components/help-trigger";
import { EmptyState, ErrorState, LoadingState } from "@/components/states";
import { StatusBadge, type StatusTone } from "@/components/status-badge";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Button } from "@/components/ui/button";
import {
  canQueryOperation,
  createLifecycleOperation,
  LIFECYCLE_ACTION_LABELS,
  LIFECYCLE_ACTIONS,
  lifecycleActionConsequence,
  lifecycleConfirmationRequirement,
  lifecyclePhaseLabel,
  lifecyclePhaseTone,
  reduceLifecycleOperation,
  validateLifecycleConfirmation,
  type LifecycleEvent,
  type LifecycleOperationState,
  type ServiceLifecycleAction,
} from "@/pages/console/lifecycle";
import {
  serviceInstanceSource,
  serviceLifecycleSupervisorState,
} from "@/runtime/service-instance-source";
import {
  adminCapabilityAvailability,
  CAPABILITY_KEYS,
} from "@/runtime/capability-gates";
import {
  disabledReasonFor,
  type DisabledReason as DisabledReasonValue,
  type PermissionSubject,
} from "@/runtime/permissions";
import type { CapabilityState } from "@/api/capabilities";
import { useRuntimeMode } from "@/runtime/runtime-mode-context";
import { usePermissionSubject } from "@/runtime/use-permission";
import { useRetryAfter } from "@/runtime/use-retry-after";
import { IconTile } from "@/components/icon-tile";

/**
 * Service Instance console.
 *
 * Service Role is the process responsibility (collector / api / replay) and
 * Service Instance is one concrete running instance. They are labelled
 * separately; no cluster, leader, worker or sharding semantics are inferred.
 *
 * Current Contract: identity, readiness, uptime and last-observed time come
 * from the real per-role ops endpoints.
 *
 * Admin API: the unauthenticated per-role `/status` and `/ops/*` endpoints are
 * not read at all. The view uses only the authenticated
 * `GET /api/v1/admin/service-instances` seam, gated by `service_lifecycle`.
 * When that capability is missing or unsupported, it shows the explicit gap
 * "需要 Admin API 服务实例接口（未实现）" instead of falling back.
 */

export const SERVICE_INSTANCE_ROLES = SERVICE_ROLES;

const SERVICE_INSTANCE_HELP =
  "Service Role 是进程职责（collector、api、replay）。Service Instance 是一个具体运行实例，由 Admin API 的 instance_id 标识。Current Contract 只按 Service Role 暴露状态，不把 Role 当作 Instance。PID 存在不等于 Ready，Ready 由服务端 readiness 判定。生命周期操作仅 SuperAdmin 可执行，服务端授权才是安全边界。操作会创建可跟踪的 Operation，已接受不等于成功。断连后为状态未知，恢复后只查询 Operation，不自动重试写操作。Reload 是配置校验后的优雅重启，不是热加载，也不承诺零停机。";

interface DetailItem {
  label: string;
  value: string;
}

interface ServiceRow {
  /** Stable operation/dialog key: role in Current Contract, instance_id in Admin. */
  key: string;
  /** Concrete Service Instance id when the contract exposes one. */
  instanceId: string | null;
  role: ServiceRole;
  displayName: string | null;
  ready: boolean | null;
  readyReason: string | null;
  uptimeS: number | null;
  lastObservedAt: number | null;
  details: DetailItem[];
  /** Raw backend/ops error for this row. Rendered verbatim. */
  error: string | null;
  lastErrorRequestId: string | null;
  /** null means every role-permitted action is offered (Current Contract). */
  availableActions: readonly ServiceLifecycleAction[] | null;
}

function formatValue(value: string | number | null | undefined): string {
  if (value === null || value === undefined || value === "") {
    return "—";
  }
  return String(value);
}

function formatUptime(seconds: number | null): string {
  if (seconds === null || !Number.isFinite(seconds) || seconds < 0) {
    return "—";
  }
  const total = Math.floor(seconds);
  const hours = Math.floor(total / 3600);
  const minutes = Math.floor((total % 3600) / 60);
  const remaining = total % 60;
  if (hours > 0) {
    return `${hours}h ${minutes}m`;
  }
  if (minutes > 0) {
    return `${minutes}m ${remaining}s`;
  }
  return `${remaining}s`;
}

function formatObserved(at: number | null): string {
  if (at === null || at === 0) {
    return "—";
  }
  return formatUtcTimestamp(at);
}

function latestUpdatedAt(
  ...updates: Array<number | undefined>
): number | null {
  const values = updates.filter(
    (value): value is number => typeof value === "number" && value > 0,
  );
  return values.length > 0 ? Math.max(...values) : null;
}

function readyFrom(
  ready: ReadyPayload | undefined,
  statusReady: boolean | undefined,
): { ready: boolean | null; reason: string | null } {
  if (ready) {
    return { ready: ready.ready, reason: ready.reason };
  }
  if (statusReady !== undefined) {
    return { ready: statusReady, reason: null };
  }
  return { ready: null, reason: null };
}

function describeError(error: unknown): string | null {
  if (!error) {
    return null;
  }
  return errorText(error, "加载失败");
}

interface RoleRowInput {
  role: ServiceRole;
  status: { uptime_s: number } | undefined;
  ready: ReadyPayload | undefined;
  statusReady?: boolean;
  statusError: unknown;
  readyError: unknown;
  observedAt: number | null;
  details: DetailItem[];
}

/**
 * Current Contract rows share one scaffold: a Service Role identity, one
 * readiness verdict, uptime, last observation and raw error. Only the detail
 * fields differ per role.
 */
function buildRoleRow({
  role,
  status,
  ready,
  statusReady,
  statusError,
  readyError,
  observedAt,
  details,
}: RoleRowInput): ServiceRow {
  const readiness = readyFrom(ready, statusReady);
  return {
    key: role,
    instanceId: null,
    role,
    displayName: null,
    ready: readiness.ready,
    readyReason: readiness.reason,
    uptimeS: status?.uptime_s ?? null,
    lastObservedAt: observedAt,
    details,
    error: describeError(statusError ?? readyError),
    lastErrorRequestId: null,
    availableActions: null,
  };
}

function buildApiRow(
  status: ApiStatus | undefined,
  ready: ReadyPayload | undefined,
  statusError: unknown,
  readyError: unknown,
  observedAt: number | null,
): ServiceRow {
  return buildRoleRow({
    role: "api",
    status,
    ready,
    statusError,
    readyError,
    observedAt,
    details: status
      ? [
          {
            label: "HTTP",
            value: `${status.http.host}:${formatValue(status.http.port)}`,
          },
          {
            label: "ZMQ PUB",
            value: formatValue(status.zmq.publish_endpoint),
          },
          {
            label: "ZMQ Collector",
            value: status.zmq.collector_endpoint,
          },
          {
            label: "Messages Received",
            value: String(status.messages_received),
          },
          {
            label: "Messages Forwarded",
            value: String(status.messages_forwarded),
          },
          {
            label: "Collector Ready",
            value: status.collector.ready ? "true（粘性）" : "false",
          },
          {
            label: "Collector Last Bar",
            value: formatValue(status.collector.last_bar_ts),
          },
        ]
      : [],
  });
}

function buildCollectorRow(
  status: CollectorStatus | undefined,
  ready: ReadyPayload | undefined,
  statusError: unknown,
  readyError: unknown,
  observedAt: number | null,
): ServiceRow {
  return buildRoleRow({
    role: "collector",
    status,
    ready,
    statusReady: status?.ready,
    statusError,
    readyError,
    observedAt,
    details: status
      ? [
          { label: "Feed", value: formatValue(status.feed) },
          { label: "Last Ingest", value: formatValue(status.last_ingest_ts) },
          { label: "Last Bar", value: formatValue(status.last_bar_ts) },
          { label: "Bars Ingested", value: String(status.bars_ingested) },
          { label: "Bars Written", value: String(status.bars_written) },
          {
            label: "Write Queue Depth",
            value: String(status.write_queue_depth),
          },
          { label: "Stale Count", value: String(status.stale_count) },
          {
            label: "Gap Unrecoverable",
            value: String(status.gap_unrecoverable_count),
          },
        ]
      : [],
  });
}

function buildReplayRow(
  status: ReplayStatus | undefined,
  ready: ReadyPayload | undefined,
  statusError: unknown,
  readyError: unknown,
  observedAt: number | null,
): ServiceRow {
  return buildRoleRow({
    role: "replay",
    status,
    ready,
    statusError,
    readyError,
    observedAt,
    details: status
      ? [
          {
            label: "Control Endpoint",
            value: formatValue(status.control_endpoint),
          },
          {
            label: "Publish Endpoint",
            value: formatValue(status.publish_endpoint),
          },
          {
            label: "Active Sessions",
            value: status.active_sessions.length
              ? status.active_sessions.join(", ")
              : "0",
          },
          {
            label: "Sessions",
            value: String(status.sessions.length),
          },
          {
            label: "Instrument Snapshot",
            value: status.instrument_snapshot.status,
          },
        ]
      : [],
  });
}

function buildAdminRow(instance: ServiceInstanceRowData): ServiceRow {
  const observedAt = Date.parse(instance.lastObservedAt);
  return {
    key: instance.instanceId,
    instanceId: instance.instanceId,
    role: instance.role,
    displayName: instance.displayName,
    ready: instance.ready,
    readyReason: instance.readyReason || null,
    uptimeS: null,
    lastObservedAt: Number.isFinite(observedAt) ? observedAt : null,
    details: [
      { label: "Desired", value: instance.desiredState },
      { label: "Observed", value: instance.observedState },
      { label: "Health", value: instance.health },
      { label: "PID", value: formatValue(instance.pid) },
      { label: "Started", value: formatValue(instance.startedAt) },
    ],
    error: instance.lastError
      ? `${instance.lastError.code}: ${instance.lastError.message}`
      : null,
    lastErrorRequestId: instance.lastError?.request_id ?? null,
    availableActions: instance.availableActions,
  };
}

function lifecycleDisabledReason(
  subject: PermissionSubject,
  action: ServiceLifecycleAction,
  capability: CapabilityState | null,
  writesStopped: boolean,
  retryAfterReason: DisabledReasonValue | null,
  row: ServiceRow,
): DisabledReasonValue | null {
  if (writesStopped) {
    return {
      code: "requires-login",
      message: "会话已停止写操作，请重新登录",
    };
  }
  const roleReason = disabledReasonFor(
    subject,
    `service.lifecycle.${action}` as const,
  );
  if (roleReason) {
    return roleReason;
  }
  if (retryAfterReason) {
    return retryAfterReason;
  }
  if (row.availableActions !== null && !row.availableActions.includes(action)) {
    return {
      code: "backend-unsupported",
      message: "该实例不支持此操作",
    };
  }
  const availability = adminCapabilityAvailability(
    capability,
    CAPABILITY_KEYS.serviceLifecycle,
  );
  if (availability.kind === "not-admin") {
    // Current Contract never enables lifecycle writes, even if a local role
    // were somehow present. The server stays the authorization boundary.
    return { code: "requires-admin-api", message: "需要 Admin API" };
  }
  if (availability.kind === "disabled") {
    return availability.reason;
  }
  const supervisor = serviceLifecycleSupervisorState(capability);
  if (supervisor === "unavailable") {
    return { code: "adapter-unavailable", message: "Supervisor 不可用" };
  }
  if (supervisor === "unsupported") {
    return { code: "backend-unsupported", message: "Supervisor 未实现" };
  }
  return null;
}

interface AcceptedOperation {
  operationId: string | null;
  requestId: string | null;
}

function extractAcceptedOperation(data: unknown): AcceptedOperation {
  if (typeof data !== "object" || data === null) {
    return { operationId: null, requestId: null };
  }
  const record = data as Record<string, unknown>;
  const nested =
    typeof record.operation === "object" && record.operation !== null
      ? (record.operation as Record<string, unknown>)
      : null;
  const operationId =
    typeof record.operation_id === "string"
      ? record.operation_id
      : typeof nested?.operation_id === "string"
        ? nested.operation_id
        : null;
  const requestId =
    typeof record.request_id === "string"
      ? record.request_id
      : typeof nested?.request_id === "string"
        ? nested.request_id
        : null;
  return { operationId, requestId };
}

function OperationTracker({
  instanceId,
  operation,
  online,
  queryToken,
  onEvent,
}: {
  instanceId: string;
  operation: LifecycleOperationState;
  online: boolean;
  queryToken: number;
  onEvent: (instanceId: string, event: LifecycleEvent) => void;
}) {
  const operationId = operation.operationId;
  const query = useQuery({
    queryKey: ["console", "operation", instanceId, operationId, queryToken],
    queryFn: ({ signal }) =>
      fetchAdminOperation(operationId as string, signal),
    enabled: online && operationId !== null && canQueryOperation(operation),
    retry: false,
    refetchInterval: OPERATION_POLL_INTERVAL_MS,
  });

  useEffect(() => {
    const data = query.data;
    if (!data) {
      return;
    }
    onEvent(instanceId, {
      type: "operation",
      status: data.status,
      errorCode: data.error?.code ?? null,
      errorMessage: data.error?.message ?? null,
      requestId: data.request_id ?? null,
      at: data.finished_at ?? undefined,
    });
  }, [query.data, instanceId, onEvent]);

  useEffect(() => {
    if (query.error) {
      onEvent(instanceId, { type: "query-failed" });
    }
  }, [query.error, instanceId, onEvent]);

  return null;
}

function OperationSummary({
  operation,
  onRequireQuery,
}: {
  operation: LifecycleOperationState;
  onRequireQuery: () => void;
}) {
  const requestIds = Array.from(
    new Set(
      [operation.requestId, operation.errorRequestId].filter(
        (value): value is string => typeof value === "string" && value !== "",
      ),
    ),
  );
  return (
    <div
      data-testid={`lifecycle-operation-${operation.instanceId}`}
      className="mt-1 flex flex-col gap-1.5 rounded-lg bg-surface-sunken p-2.5 text-[11px] text-muted-foreground"
    >
      <div className="flex flex-wrap items-center gap-2">
        <StatusBadge tone={lifecyclePhaseTone(operation.phase)} live>
          {lifecyclePhaseLabel(operation.phase)}
        </StatusBadge>
        <span>操作 {LIFECYCLE_ACTION_LABELS[operation.action]}</span>
        <span className="font-mono">目标 {operation.instanceId}</span>
      </div>
      {operation.operationId ? (
        <span className="font-mono">Operation {operation.operationId}</span>
      ) : null}
      {requestIds.map((requestId) => (
        <span key={requestId} className="font-mono">
          Request ID {requestId}
        </span>
      ))}
      {operation.errorCode ? (
        <span className="break-words text-status-critical-fg">
          {operation.errorCode}
          {operation.errorMessage ? (
            <>
              {": "}
              <span data-raw-evidence="true">{operation.errorMessage}</span>
            </>
          ) : null}
        </span>
      ) : null}
      <span className="leading-relaxed">
        已接受不等于成功。断连后为状态未知，恢复后只查询 Operation，不自动重试写操作。
      </span>
      {operation.phase === "unknown" ? (
        <Button
          size="sm"
          variant="outline"
          className="self-start"
          onClick={onRequireQuery}
        >
          重新查询 Operation
        </Button>
      ) : null}
    </div>
  );
}

function LifecycleActionButton({
  row,
  action,
  reason,
  onOpen,
}: {
  row: ServiceRow;
  action: ServiceLifecycleAction;
  reason: DisabledReasonValue | null;
  onOpen: () => void;
}) {
  const reasonId = `${row.key}-${action}-reason`;
  return (
    <span className="inline-flex flex-wrap items-center gap-2">
      <Button
        size="sm"
        variant="outline"
        disabled={reason !== null}
        aria-describedby={reason ? reasonId : undefined}
        onClick={onOpen}
      >
        {LIFECYCLE_ACTION_LABELS[action]}
      </Button>
      <DisabledReason id={reasonId} reason={reason} />
    </span>
  );
}

export function ServiceInstanceView() {
  const {
    capability,
    session,
    writesStopped,
    role,
    sessionLoading,
  } = useRuntimeMode();
  const subject = usePermissionSubject();
  const retryAfter = useRetryAfter();
  const csrfToken = session?.csrfToken ?? null;
  const source = serviceInstanceSource(capability);
  const directEnabled = source.kind === "direct";
  const adminEnabled = source.kind === "admin" && role !== null;

  const apiStatus = useQuery({
    queryKey: ["console", "service", "api-status"],
    queryFn: ({ signal }) => fetchApiStatus(signal),
    refetchInterval: STATUS_POLL_INTERVAL_MS,
    enabled: directEnabled,
  });
  const apiReady = useQuery({
    queryKey: ["console", "service", "api-ready"],
    queryFn: ({ signal }) => fetchApiReady(signal),
    refetchInterval: STATUS_POLL_INTERVAL_MS,
    enabled: directEnabled,
  });
  const collectorStatus = useQuery({
    queryKey: ["console", "service", "collector-status"],
    queryFn: ({ signal }) => fetchCollectorStatus(signal),
    refetchInterval: STATUS_POLL_INTERVAL_MS,
    enabled: directEnabled,
  });
  const collectorReady = useQuery({
    queryKey: ["console", "service", "collector-ready"],
    queryFn: ({ signal }) => fetchCollectorReady(signal),
    refetchInterval: STATUS_POLL_INTERVAL_MS,
    enabled: directEnabled,
  });
  const replayStatus = useQuery({
    queryKey: ["console", "service", "replay-status"],
    queryFn: ({ signal }) => fetchReplayStatus(signal),
    refetchInterval: STATUS_POLL_INTERVAL_MS,
    enabled: directEnabled,
  });
  const replayReady = useQuery({
    queryKey: ["console", "service", "replay-ready"],
    queryFn: ({ signal }) => fetchReplayReady(signal),
    refetchInterval: STATUS_POLL_INTERVAL_MS,
    enabled: directEnabled,
  });
  const adminInstances = useQuery({
    queryKey: ["console", "service", "admin-instances"],
    queryFn: ({ signal }) => fetchAdminServiceInstances(signal),
    refetchInterval: STATUS_POLL_INTERVAL_MS,
    enabled: adminEnabled,
    retry: false,
  });

  const gapReason =
    source.kind === "gap"
      ? source.reason
      : source.kind === "admin" && !role
        ? { code: "requires-login" as const, message: "需要登录" }
        : null;

  const rows = useMemo<ServiceRow[]>(() => {
    if (source.kind === "admin") {
      return (adminInstances.data ?? []).map((instance) =>
        buildAdminRow(adminServiceInstanceToRow(instance)),
      );
    }
    return [
      buildCollectorRow(
        collectorStatus.data,
        collectorReady.data,
        collectorStatus.error,
        collectorReady.error,
        latestUpdatedAt(
          collectorStatus.dataUpdatedAt,
          collectorReady.dataUpdatedAt,
        ),
      ),
      buildApiRow(
        apiStatus.data,
        apiReady.data,
        apiStatus.error,
        apiReady.error,
        latestUpdatedAt(apiStatus.dataUpdatedAt, apiReady.dataUpdatedAt),
      ),
      buildReplayRow(
        replayStatus.data,
        replayReady.data,
        replayStatus.error,
        replayReady.error,
        latestUpdatedAt(replayStatus.dataUpdatedAt, replayReady.dataUpdatedAt),
      ),
    ];
  }, [
    source.kind,
    adminInstances.data,
    collectorStatus.data,
    collectorReady.data,
    collectorStatus.error,
    collectorReady.error,
    collectorStatus.dataUpdatedAt,
    collectorReady.dataUpdatedAt,
    apiStatus.data,
    apiReady.data,
    apiStatus.error,
    apiReady.error,
    apiStatus.dataUpdatedAt,
    apiReady.dataUpdatedAt,
    replayStatus.data,
    replayReady.data,
    replayStatus.error,
    replayReady.error,
    replayStatus.dataUpdatedAt,
    replayReady.dataUpdatedAt,
  ]);

  const [operations, setOperations] = useState<
    Record<string, LifecycleOperationState>
  >({});
  const [pending, setPending] = useState<{
    action: ServiceLifecycleAction;
    instanceId: string;
  } | null>(null);
  const [dialogError, setDialogError] = useState<string | null>(null);
  const [online, setOnline] = useState(
    () => (typeof navigator === "undefined" ? true : navigator.onLine),
  );
  const [queryToken, setQueryToken] = useState(0);
  const submitLock = useRef(false);

  const updateOperation = useCallback(
    (instanceId: string, event: LifecycleEvent) => {
      setOperations((previous) => {
        const current = previous[instanceId];
        if (!current) {
          return previous;
        }
        const next = reduceLifecycleOperation(current, event);
        if (next === current) {
          return previous;
        }
        return { ...previous, [instanceId]: next };
      });
    },
    [],
  );

  useEffect(() => {
    const handleOffline = () => {
      setOnline(false);
      setOperations((previous) => {
        const next: Record<string, LifecycleOperationState> = {};
        for (const [instanceId, operation] of Object.entries(previous)) {
          next[instanceId] = reduceLifecycleOperation(operation, {
            type: "disconnect",
          });
        }
        return next;
      });
    };
    const handleOnline = () => {
      setOnline(true);
      setQueryToken((token) => token + 1);
    };
    window.addEventListener("offline", handleOffline);
    window.addEventListener("online", handleOnline);
    return () => {
      window.removeEventListener("offline", handleOffline);
      window.removeEventListener("online", handleOnline);
    };
  }, []);

  const mutation = useMutation({
    mutationFn: (input: {
      action: ServiceLifecycleAction;
      instanceId: string;
      body: ServiceLifecycleBody;
    }) =>
      postServiceLifecycle(
        input.instanceId,
        input.action,
        input.body,
        csrfToken,
        newIdempotencyKey(),
      ),
    onSuccess: (data, input) => {
      submitLock.current = false;
      const accepted = extractAcceptedOperation(data);
      updateOperation(input.instanceId, {
        type: "accepted",
        operationId: accepted.operationId,
        requestId: accepted.requestId,
      });
    },
    onError: (error, input) => {
      submitLock.current = false;
      if (error instanceof HttpError && error.status === null) {
        // The write may have reached the server; never infer failure.
        updateOperation(input.instanceId, { type: "disconnect" });
        return;
      }
      updateOperation(input.instanceId, {
        type: "submit-failed",
        errorCode: error instanceof HttpError ? error.code : "network_error",
        errorMessage:
          error instanceof Error ? error.message : "生命周期操作提交失败",
        requestId: error instanceof HttpError ? error.requestId : null,
      });
    },
  });

  const openDialog = (instanceId: string, action: ServiceLifecycleAction) => {
    const current = operations[instanceId];
    if (
      current &&
      current.phase !== "pending" &&
      current.phase !== "failed"
    ) {
      return;
    }
    setDialogError(null);
    setPending({ action, instanceId });
  };

  const handleConfirm = (input: ConfirmDialogInput) => {
    if (!pending || submitLock.current) {
      return;
    }
    if (retryAfter.reason) {
      setDialogError(retryAfter.reason.message);
      return;
    }
    const validation = validateLifecycleConfirmation({
      action: pending.action,
      instanceId: pending.instanceId,
      reason: input.reason,
      ...(input.confirmInstanceName !== undefined
        ? { confirmInstanceName: input.confirmInstanceName }
        : {}),
    });
    if (!validation.valid) {
      setDialogError(validation.error);
      return;
    }

    const current = operations[pending.instanceId];
    if (current && current.phase !== "pending" && current.phase !== "failed") {
      setDialogError("该实例已有进行中的 Operation，不能重复提交。");
      return;
    }

    submitLock.current = true;
    setDialogError(null);
    setOperations((previous) => ({
      ...previous,
      [pending.instanceId]: reduceLifecycleOperation(
        createLifecycleOperation(pending.action, pending.instanceId),
        { type: "submit" },
      ),
    }));
    mutation.mutate({
      action: pending.action,
      instanceId: pending.instanceId,
      body: validation.body,
    });
    setPending(null);
  };

  const sourceLabel =
    source.kind === "direct"
      ? "Current Contract"
      : source.kind === "admin"
        ? "Admin API"
        : "生命周期不可用";
  const sourceTone: StatusTone =
    source.kind === "direct"
      ? "unknown"
      : source.kind === "admin"
        ? "info"
        : "warning";
  const roleBlocked =
    disabledReasonFor(subject, "service.lifecycle.start") !== null;
  const lifecycleReadOnly =
    writesStopped || roleBlocked || source.kind !== "admin";
  const refreshing =
    adminEnabled && adminInstances.isFetching && !adminInstances.isPending;
  const adminErrorRequestId =
    adminInstances.error instanceof HttpError
      ? adminInstances.error.requestId
      : null;

  return (
    <Card
      role="region"
      aria-label="Service Instance"
      data-testid="service-instance-card"
      className="mt-5"
    >
      <CardHeader className="p-5 pb-0">
        <div className="min-w-0">
          <div className="flex min-w-0 flex-wrap items-center gap-2">
            <IconTile icon={ServerCog} />
            <CardTitle>Service Instance</CardTitle>
            <HelpTrigger
              label="Service Instance 说明"
              content={SERVICE_INSTANCE_HELP}
              side="bottom"
            />
          </div>
          <p className="mt-2 text-xs leading-relaxed text-muted-foreground">
            Service Role 是进程职责，Service Instance 是具体运行实例。PID
            存在不等于 Ready。
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <StatusBadge tone={sourceTone}>{sourceLabel}</StatusBadge>
          {source.kind === "admin" && role ? (
            <StatusBadge tone="unknown">Access Role {role}</StatusBadge>
          ) : null}
          <StatusBadge tone={lifecycleReadOnly ? "unknown" : "good"}>
            {lifecycleReadOnly ? "当前只读" : "生命周期可执行"}
          </StatusBadge>
          {refreshing ? (
            <StatusBadge tone="info" live>
              正在刷新
            </StatusBadge>
          ) : null}
        </div>
      </CardHeader>

      <CardContent className="pt-4">
        {gapReason ? (
          <div
            data-testid="service-instance-gap"
            role="note"
            className="rounded-lg border border-card-border bg-status-warning-bg p-4"
          >
            <StatusBadge tone="warning">{gapReason.message}</StatusBadge>
            <p className="mt-2 text-xs leading-relaxed text-muted-foreground">
              {source.kind === "gap" ? (
                <>
                  Admin API 模式不读取无认证 <code>/status</code>、
                  <code>/ops/*</code> 或 <code>/prometheus</code>。请由后端提供{" "}
                  <code>GET /api/v1/admin/service-instances</code> 并声明{" "}
                  <code>service_lifecycle</code> 能力后重试。
                </>
              ) : (
                <>
                  写操作已停止。请回到登录入口重新登录后再试。服务端授权仍是唯一安全边界。
                </>
              )}
            </p>
          </div>
        ) : sessionLoading && source.kind === "admin" ? (
          <LoadingState label="正在确认 Admin 会话…" />
        ) : adminInstances.isError ? (
          <ErrorState
            title="Service Instance 加载失败"
            description={
              <span data-raw-evidence="true" className="break-words">
                {errorText(
                  adminInstances.error,
                  "Admin API service-instances 请求失败",
                )}
              </span>
            }
            requestId={adminErrorRequestId}
            onRetry={() => void adminInstances.refetch()}
          />
        ) : adminInstances.isPending && source.kind === "admin" ? (
          <LoadingState
            label="正在读取 Admin service-instances…"
            showSkeleton
          />
        ) : source.kind === "admin" && rows.length === 0 ? (
          <EmptyState
            title="没有 Service Instance"
            description="Admin API 返回了空实例列表，不生成示例实例。"
          />
        ) : (
          <div className="mt-1">
            <Table className="min-w-[1080px] text-xs">
              <caption className="sr-only">
                Service Instance 与 Service Role 状态及生命周期操作
              </caption>
              <TableHeader>
                <TableRow>
                  <TableHead>Service Instance</TableHead>
                  <TableHead>Service Role</TableHead>
                  <TableHead>Ready</TableHead>
                  <TableHead>Uptime</TableHead>
                  <TableHead>Last Observed</TableHead>
                  <TableHead>Details</TableHead>
                  <TableHead>Lifecycle</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {rows.map((row) => {
                  const operation = operations[row.key];
                  const operationActive =
                    operation !== undefined &&
                    operation.phase !== "pending" &&
                    operation.phase !== "failed";
                  const readyLabel =
                    row.ready === null ? "未知" : row.ready ? "Ready" : "未就绪";
                  const readyTone: StatusTone =
                    row.ready === null
                      ? "unknown"
                      : row.ready
                        ? "good"
                        : "critical";
                  return (
                    <TableRow
                      key={row.key}
                      data-testid={`service-instance-${row.key}`}
                      data-instance-id={row.key}
                      data-service-role={row.role}
                    >
                      <TableCell className="align-top">
                        {row.instanceId ? (
                          <div className="min-w-0">
                            <span className="font-mono text-xs">
                              {row.instanceId}
                            </span>
                            {row.displayName ? (
                              <div className="mt-0.5 break-words text-[11px] text-faint-foreground">
                                {row.displayName}
                              </div>
                            ) : null}
                          </div>
                        ) : (
                          <div className="min-w-0">
                            <StatusBadge tone="unknown">未暴露</StatusBadge>
                            <div className="mt-1 text-[11px] text-faint-foreground">
                              Current Contract 仅暴露 Service Role
                            </div>
                          </div>
                        )}
                      </TableCell>
                      <TableCell className="align-top">
                        <span className="font-mono text-xs">{row.role}</span>
                        <div className="mt-0.5 text-[10px] uppercase tracking-wide text-faint-foreground">
                          Service Role
                        </div>
                      </TableCell>
                      <TableCell className="align-top">
                        <StatusBadge tone={readyTone} live>
                          {readyLabel}
                        </StatusBadge>
                        {row.readyReason ? (
                          <div className="mt-1 max-w-48 break-words text-[11px] text-muted-foreground">
                            {row.readyReason}
                          </div>
                        ) : null}
                      </TableCell>
                      <TableCell className="align-top tabular-nums">
                        {formatUptime(row.uptimeS)}
                      </TableCell>
                      <TableCell className="align-top">
                        <span
                          data-testid={`last-observed-${row.key}`}
                          className="tabular-nums"
                        >
                          {formatObserved(row.lastObservedAt)}
                        </span>
                        {row.error ? (
                          <div className="mt-1 max-w-64 break-words text-[11px] text-status-critical-fg">
                            <span data-raw-evidence="true">{row.error}</span>
                          </div>
                        ) : null}
                        {row.lastErrorRequestId ? (
                          <div className="mt-1 font-mono text-[11px] text-faint-foreground">
                            Request ID {row.lastErrorRequestId}
                          </div>
                        ) : null}
                      </TableCell>
                      <TableCell className="align-top">
                        <dl className="grid grid-cols-1 gap-x-4 gap-y-0.5">
                          {row.details.map((detail) => (
                            <div key={detail.label} className="flex gap-2">
                              <dt className="text-faint-foreground">
                                {detail.label}
                              </dt>
                              <dd className="font-mono">{detail.value}</dd>
                            </div>
                          ))}
                          {row.details.length === 0 ? (
                            <div className="text-faint-foreground">
                              没有可确认的状态数据。
                            </div>
                          ) : null}
                        </dl>
                      </TableCell>
                      <TableCell className="align-top">
                        <div className="flex flex-col gap-1.5">
                          {LIFECYCLE_ACTIONS.map((action) => {
                            const reason = lifecycleDisabledReason(
                              subject,
                              action,
                              capability,
                              writesStopped,
                              retryAfter.reason,
                              row,
                            );
                            const lockedReason: DisabledReasonValue | null =
                              operationActive
                                ? {
                                    code: "backend-unsupported",
                                    message: "已有进行中的 Operation",
                                  }
                                : null;
                            return (
                              <LifecycleActionButton
                                key={action}
                                row={row}
                                action={action}
                                reason={reason ?? lockedReason}
                                onOpen={() => openDialog(row.key, action)}
                              />
                            );
                          })}
                          {pending?.instanceId === row.key ? (
                            <div className="flex flex-wrap items-center gap-2 rounded-lg bg-surface-sunken p-2 text-[11px] text-muted-foreground">
                              <StatusBadge tone="unknown">待提交</StatusBadge>
                              <span>
                                操作 {LIFECYCLE_ACTION_LABELS[pending.action]}
                              </span>
                              <span className="font-mono">
                                目标 {row.instanceId ?? row.role}
                              </span>
                            </div>
                          ) : null}
                          {operation ? (
                            <OperationSummary
                              operation={operation}
                              onRequireQuery={() =>
                                setQueryToken((token) => token + 1)
                              }
                            />
                          ) : null}
                        </div>
                      </TableCell>
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
          </div>
        )}

        {Object.entries(operations).map(([instanceId, operation]) => (
          <OperationTracker
            key={instanceId}
            instanceId={instanceId}
            operation={operation}
            online={online}
            queryToken={queryToken}
            onEvent={updateOperation}
          />
        ))}

        <ConfirmDialog
          open={pending !== null}
          onOpenChange={(open) => {
            if (!open) {
              setPending(null);
              setDialogError(null);
            }
          }}
          title={
            pending
              ? `确认 ${LIFECYCLE_ACTION_LABELS[pending.action]} ${pending.instanceId}`
              : "确认生命周期操作"
          }
          description={
            pending ? (
              <span className="block">
                <span className="flex flex-wrap items-center gap-2">
                  <StatusBadge tone="warning">
                    {LIFECYCLE_ACTION_LABELS[pending.action]}
                  </StatusBadge>
                  <span>
                    目标 Service Instance{" "}
                    <span className="font-mono">{pending.instanceId}</span>
                  </span>
                </span>
                <span className="mt-2 block">
                  {lifecycleActionConsequence(pending.action)}
                </span>
                <span className="mt-2 block">
                  {lifecycleConfirmationRequirement(
                    pending.action,
                    pending.instanceId,
                  )}
                </span>
                <span className="mt-2 block">
                  操作会创建可跟踪的 Operation，结果就地显示。
                </span>
                {dialogError ? (
                  <span
                    role="alert"
                    className="mt-2 block text-status-critical-fg"
                  >
                    {dialogError}
                  </span>
                ) : null}
              </span>
            ) : undefined
          }
          confirmLabel="确认执行"
          requireReason
          requireInstanceName={
            pending && pending.action !== "start"
              ? pending.instanceId
              : undefined
          }
          submitting={
            pending !== null &&
            operations[pending.instanceId]?.phase === "submitting"
          }
          onConfirm={handleConfirm}
        />
      </CardContent>
    </Card>
  );
}
