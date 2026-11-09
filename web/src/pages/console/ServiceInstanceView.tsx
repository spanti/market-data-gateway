import { useMutation, useQuery } from "@tanstack/react-query";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  fetchAdminOperation,
  newIdempotencyKey,
  postServiceLifecycle,
} from "@/api/endpoints/admin-console";
import {
  adminServiceInstanceToRow,
  fetchAdminServiceInstances,
  type AdminServiceInstanceRow,
} from "@/api/endpoints/admin-service-instances";
import type { ServiceLifecycleBody } from "@/api/endpoints/admin";
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
import { ConfirmDialog, type ConfirmDialogInput } from "@/components/confirm-dialog";
import { DisabledReason } from "@/components/disabled-reason";
import { ErrorState, LoadingState } from "@/components/states";
import { StatusBadge } from "@/components/status-badge";
import {
  Panel,
  PanelDescription,
  PanelHeader,
  PanelTitle,
} from "@/components/ui/panel";
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
  lifecyclePhaseLabel,
  lifecyclePhaseTone,
  reduceLifecycleOperation,
  validateLifecycleConfirmation,
  type LifecycleEvent,
  type LifecycleOperationState,
  type ServiceLifecycleAction,
} from "@/pages/console/lifecycle";
import { serviceInstanceSource } from "@/runtime/service-instance-source";
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

/**
 * Service Instance console.
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

export const SERVICE_INSTANCE_ROLES = ["collector", "api", "replay"] as const;
type ServiceInstanceRole = (typeof SERVICE_INSTANCE_ROLES)[number];

const ACTION_LABELS: Record<ServiceLifecycleAction, string> = {
  start: "Start",
  stop: "Graceful Stop",
  reload: "Reload",
};

const LIFECYCLE_ACTIONS: readonly ServiceLifecycleAction[] = [
  "start",
  "stop",
  "reload",
];

interface DetailItem {
  label: string;
  value: string;
}

interface ServiceRow {
  /** Stable operation/dialog key: role in Current Contract, instance_id in Admin. */
  key: string;
  role: ServiceInstanceRole;
  displayName: string | null;
  ready: boolean | null;
  readyReason: string | null;
  uptimeS: number | null;
  lastObservedAt: number | null;
  details: DetailItem[];
  error: string | null;
  /** null means every role-permitted action is offered (Current Contract). */
  availableActions: readonly ServiceLifecycleAction[] | null;
  admin: boolean;
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
  const date = new Date(at);
  if (Number.isNaN(date.getTime())) {
    return "—";
  }
  return `${date.toISOString().replace("T", " ").slice(0, 19)} UTC`;
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
  if (error instanceof Error) {
    return error.message;
  }
  return "加载失败";
}

function buildApiRow(
  status: ApiStatus | undefined,
  ready: ReadyPayload | undefined,
  statusError: unknown,
  readyError: unknown,
  observedAt: number | null,
): ServiceRow {
  const readiness = readyFrom(ready, undefined);
  return {
    key: "api",
    role: "api",
    displayName: null,
    ready: readiness.ready,
    readyReason: readiness.reason,
    uptimeS: status?.uptime_s ?? null,
    lastObservedAt: observedAt,
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
    error: describeError(statusError ?? readyError),
    availableActions: null,
    admin: false,
  };
}

function buildCollectorRow(
  status: CollectorStatus | undefined,
  ready: ReadyPayload | undefined,
  statusError: unknown,
  readyError: unknown,
  observedAt: number | null,
): ServiceRow {
  const readiness = readyFrom(ready, status?.ready);
  return {
    key: "collector",
    role: "collector",
    displayName: null,
    ready: readiness.ready,
    readyReason: readiness.reason,
    uptimeS: status?.uptime_s ?? null,
    lastObservedAt: observedAt,
    details: status
      ? [
          { label: "Feed", value: formatValue(status.feed) },
          { label: "Last Ingest", value: formatValue(status.last_ingest_ts) },
          { label: "Last Bar", value: formatValue(status.last_bar_ts) },
          { label: "Bars Ingested", value: String(status.bars_ingested) },
          { label: "Bars Written", value: String(status.bars_written) },
          { label: "Write Queue Depth", value: String(status.write_queue_depth) },
          { label: "Stale Count", value: String(status.stale_count) },
          {
            label: "Gap Unrecoverable",
            value: String(status.gap_unrecoverable_count),
          },
        ]
      : [],
    error: describeError(statusError ?? readyError),
    availableActions: null,
    admin: false,
  };
}

function buildReplayRow(
  status: ReplayStatus | undefined,
  ready: ReadyPayload | undefined,
  statusError: unknown,
  readyError: unknown,
  observedAt: number | null,
): ServiceRow {
  const readiness = readyFrom(ready, undefined);
  return {
    key: "replay",
    role: "replay",
    displayName: null,
    ready: readiness.ready,
    readyReason: readiness.reason,
    uptimeS: status?.uptime_s ?? null,
    lastObservedAt: observedAt,
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
    error: describeError(statusError ?? readyError),
    availableActions: null,
    admin: false,
  };
}

function buildAdminRow(instance: AdminServiceInstanceRow): ServiceRow {
  const observedAt = Date.parse(instance.lastObservedAt);
  const details: DetailItem[] = [
    { label: "Instance ID", value: instance.instanceId },
    { label: "Display Name", value: instance.displayName },
    { label: "Desired", value: instance.desiredState },
    { label: "Observed", value: instance.observedState },
    { label: "Health", value: instance.health },
    { label: "PID", value: formatValue(instance.pid) },
    { label: "Started", value: formatValue(instance.startedAt) },
  ];
  if (instance.lastError) {
    details.push({
      label: "Last Error",
      value: `${instance.lastError.code}: ${instance.lastError.message}`,
    });
  }
  return {
    key: instance.instanceId,
    role: instance.role,
    displayName: instance.displayName,
    ready: instance.ready,
    readyReason: instance.readyReason || null,
    uptimeS: null,
    lastObservedAt: Number.isFinite(observedAt) ? observedAt : null,
    details,
    error: instance.lastError?.message ?? null,
    availableActions: instance.availableActions,
    admin: true,
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
    // Current Contract never reaches here with a role (the role gate above
    // already returns); lifecycle remains an Admin-only contract target.
    return null;
  }
  if (availability.kind === "disabled") {
    return availability.reason;
  }
  const supervisor =
    capability?.mode === "admin-api"
      ? capability.document?.adapters?.find(
          (adapter) =>
            adapter.name === "supervisor" &&
            adapter.kind === "service_lifecycle",
        )
      : undefined;
  if (supervisor?.state === "unavailable") {
    return { code: "adapter-unavailable", message: "Supervisor 不可用" };
  }
  if (supervisor?.state === "unsupported") {
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
    refetchInterval: 3000,
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
  return (
    <div
      data-testid={`lifecycle-operation-${operation.instanceId}`}
      className="mt-2 flex flex-col gap-1 border-t border-border pt-2 text-[11px] text-muted-foreground"
    >
      <div className="flex flex-wrap items-center gap-2">
        <StatusBadge tone={lifecyclePhaseTone(operation.phase)} live>
          {lifecyclePhaseLabel(operation.phase)}
        </StatusBadge>
        {operation.operationId ? (
          <span className="font-mono">Operation {operation.operationId}</span>
        ) : null}
        {operation.requestId ? (
          <span className="font-mono">Request ID {operation.requestId}</span>
        ) : null}
      </div>
      {operation.errorCode ? (
        <span className="text-status-critical">
          {operation.errorCode}: {operation.errorMessage ?? ""}
        </span>
      ) : null}
      <span>
        已接受 ≠ 成功；断连后为状态未知，恢复后只查询 Operation，不自动重试写操作。
      </span>
      {operation.phase === "unknown" ? (
        <Button size="sm" variant="outline" onClick={onRequireQuery}>
          重新查询 Operation
        </Button>
      ) : null}
    </div>
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
    refetchInterval: 5_000,
    enabled: directEnabled,
  });
  const apiReady = useQuery({
    queryKey: ["console", "service", "api-ready"],
    queryFn: ({ signal }) => fetchApiReady(signal),
    refetchInterval: 5_000,
    enabled: directEnabled,
  });
  const collectorStatus = useQuery({
    queryKey: ["console", "service", "collector-status"],
    queryFn: ({ signal }) => fetchCollectorStatus(signal),
    refetchInterval: 5_000,
    enabled: directEnabled,
  });
  const collectorReady = useQuery({
    queryKey: ["console", "service", "collector-ready"],
    queryFn: ({ signal }) => fetchCollectorReady(signal),
    refetchInterval: 5_000,
    enabled: directEnabled,
  });
  const replayStatus = useQuery({
    queryKey: ["console", "service", "replay-status"],
    queryFn: ({ signal }) => fetchReplayStatus(signal),
    refetchInterval: 5_000,
    enabled: directEnabled,
  });
  const replayReady = useQuery({
    queryKey: ["console", "service", "replay-ready"],
    queryFn: ({ signal }) => fetchReplayReady(signal),
    refetchInterval: 5_000,
    enabled: directEnabled,
  });
  const adminInstances = useQuery({
    queryKey: ["console", "service", "admin-instances"],
    queryFn: ({ signal }) => fetchAdminServiceInstances(signal),
    refetchInterval: 5_000,
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

  const sourceBadge =
    source.kind === "direct"
      ? "结果来自各角色 /status 与 /readyz；生命周期为 Admin API 契约目标"
      : source.kind === "admin"
        ? "数据来自 Admin API service-instances；不读取 Current Contract /status 或 /ops"
        : "Admin 模式不暴露无认证 ops 资源；需要 Admin API 服务实例接口";

  return (
    <Panel className="mt-6 p-5" aria-label="Service Instance 状态">
      <PanelHeader>
        <div>
          <PanelTitle>Service Instance</PanelTitle>
          <PanelDescription>
            仅展示 collector / api / replay 三个真实 Service Role，不推断
            Master/Worker、Leader 或分片。PID 存在不等于 Ready。生命周期状态严格区分：待提交 /
            提交中 / 已接受 / 执行中 / 成功 / 失败 / 状态未知。
          </PanelDescription>
        </div>
        <StatusBadge tone={source.kind === "gap" ? "warning" : "unknown"}>
          {sourceBadge}
        </StatusBadge>
      </PanelHeader>

      {gapReason ? (
        <div
          data-testid="service-instance-gap"
          role="note"
          className="mt-4 border-l-2 border-status-warning pl-3 text-sm"
        >
          <p className="font-medium">{gapReason.message}</p>
          <p className="mt-1 text-xs text-muted-foreground">
            Admin API 模式不读取无认证 <code>/status</code>、<code>/ops/*</code>{" "}
            或 <code>/prometheus</code>。请由后端提供
            <code> GET /api/v1/admin/service-instances</code> 并声明{" "}
            <code>service_lifecycle</code> 能力后重试。
          </p>
        </div>
      ) : sessionLoading && source.kind === "admin" ? (
        <LoadingState className="mt-4" label="正在确认 Admin 会话…" />
      ) : adminInstances.isError ? (
        <ErrorState
          className="mt-4"
          title="Service Instance 加载失败"
          description={
            adminInstances.error instanceof Error
              ? adminInstances.error.message
              : "Admin API service-instances 请求失败"
          }
          onRetry={() => void adminInstances.refetch()}
        />
      ) : adminInstances.isPending && source.kind === "admin" ? (
        <LoadingState className="mt-4" label="正在读取 Admin service-instances…" showSkeleton />
      ) : (
        <div className="mt-3">
          <Table>
            <caption className="sr-only">
              Service Instance real status and lifecycle controls
            </caption>
            <TableHeader>
              <TableRow>
                <TableHead>Instance</TableHead>
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
                const readyTone =
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
                  >
                    <TableCell className="align-top">
                      <span className="font-mono text-sm">{row.role}</span>
                      <div className="text-[11px] text-faint-foreground">
                        {row.admin ? "Admin instance" : "Service Role"}
                      </div>
                    </TableCell>
                    <TableCell className="align-top">
                      <StatusBadge tone={readyTone} live>
                        {readyLabel}
                      </StatusBadge>
                      {row.readyReason ? (
                        <div className="mt-1 text-[11px] text-faint-foreground">
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
                        <div className="mt-1 text-[11px] text-status-critical">
                          {row.error}
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
                      <div className="flex flex-col gap-1">
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
                          const effectiveReason = reason ?? lockedReason;
                          const reasonId = `${row.key}-${action}-reason`;
                          return (
                            <span
                              key={action}
                              className="inline-flex flex-wrap items-center gap-2"
                            >
                              <Button
                                size="sm"
                                variant="outline"
                                disabled={effectiveReason !== null}
                                aria-describedby={
                                  effectiveReason ? reasonId : undefined
                                }
                                onClick={() => openDialog(row.key, action)}
                              >
                                {ACTION_LABELS[action]}
                              </Button>
                              <DisabledReason
                                id={reasonId}
                                reason={effectiveReason}
                              />
                            </span>
                          );
                        })}
                      </div>
                      {operation ? (
                        <OperationSummary
                          operation={operation}
                          onRequireQuery={() =>
                            setQueryToken((token) => token + 1)
                          }
                        />
                      ) : null}
                    </TableCell>
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>
          {source.kind === "admin" && rows.length === 0 && !adminInstances.isPending ? (
            <p className="mt-3 text-xs text-muted-foreground">
              Admin API 返回了空实例列表。
            </p>
          ) : null}
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
            ? `确认 ${ACTION_LABELS[pending.action]} ${pending.instanceId}`
            : "确认生命周期操作"
        }
        description={
          <span>
            该操作会创建可跟踪的 Operation。Start 等待真实 Ready；Graceful Stop
            会先停止接流量并排空；Reload 是配置校验后的优雅重启，不是热加载。
            {dialogError ? (
              <span className="mt-2 block text-status-critical">
                {dialogError}
              </span>
            ) : null}
          </span>
        }
        confirmLabel="确认执行"
        requireReason
        requireInstanceName={
          pending && pending.action !== "start"
            ? pending.instanceId
            : undefined
        }
        submitting={
          pending !== null && operations[pending.instanceId]?.phase === "submitting"
        }
        onConfirm={handleConfirm}
      />
    </Panel>
  );
}
