import { useMutation, useQuery } from "@tanstack/react-query";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  fetchAdminOperation,
  newIdempotencyKey,
  postServiceLifecycle,
} from "@/api/endpoints/admin-console";
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
import {
  capabilityDisabledReason,
  disabledReasonFor,
  type DisabledReason as DisabledReasonValue,
  type PermissionSubject,
} from "@/runtime/permissions";
import type { CapabilityState } from "@/api/capabilities";
import { useRuntimeMode } from "@/runtime/runtime-mode-context";
import { usePermissionSubject } from "@/runtime/use-permission";

/**
 * Service Instance console.
 *
 * Current Contract: identity, readiness, uptime and last-observed time come
 * from the real per-role ops endpoints. Lifecycle controls are Admin API
 * contract targets and stay disabled with a visible reason.
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
  role: ServiceInstanceRole;
  ready: boolean | null;
  readyReason: string | null;
  uptimeS: number | null;
  lastObservedAt: number | null;
  details: DetailItem[];
  error: string | null;
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
    role: "api",
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
    role: "collector",
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
    role: "replay",
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
  };
}

function lifecycleDisabledReason(
  subject: PermissionSubject,
  action: ServiceLifecycleAction,
  capability: CapabilityState | null,
): DisabledReasonValue | null {
  const roleReason = disabledReasonFor(
    subject,
    `service.lifecycle.${action}` as const,
  );
  if (roleReason) {
    return roleReason;
  }
  if (capability?.mode !== "admin-api") {
    return null;
  }
  if (capability.compatible === false) {
    return { code: "backend-unsupported", message: "Admin API 版本不兼容" };
  }
  const capabilityReason = capabilityDisabledReason(
    capability.document?.capabilities.service_lifecycle,
  );
  if (capabilityReason) {
    return capabilityReason;
  }
  const supervisor = capability.document?.adapters?.find(
    (adapter) =>
      adapter.name === "supervisor" && adapter.kind === "service_lifecycle",
  );
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
  const { capability, session } = useRuntimeMode();
  const subject = usePermissionSubject();
  const csrfToken = session?.csrfToken ?? null;

  const apiStatus = useQuery({
    queryKey: ["console", "service", "api-status"],
    queryFn: ({ signal }) => fetchApiStatus(signal),
    refetchInterval: 5_000,
  });
  const apiReady = useQuery({
    queryKey: ["console", "service", "api-ready"],
    queryFn: ({ signal }) => fetchApiReady(signal),
    refetchInterval: 5_000,
  });
  const collectorStatus = useQuery({
    queryKey: ["console", "service", "collector-status"],
    queryFn: ({ signal }) => fetchCollectorStatus(signal),
    refetchInterval: 5_000,
  });
  const collectorReady = useQuery({
    queryKey: ["console", "service", "collector-ready"],
    queryFn: ({ signal }) => fetchCollectorReady(signal),
    refetchInterval: 5_000,
  });
  const replayStatus = useQuery({
    queryKey: ["console", "service", "replay-status"],
    queryFn: ({ signal }) => fetchReplayStatus(signal),
    refetchInterval: 5_000,
  });
  const replayReady = useQuery({
    queryKey: ["console", "service", "replay-ready"],
    queryFn: ({ signal }) => fetchReplayReady(signal),
    refetchInterval: 5_000,
  });

  const rows = useMemo<ServiceRow[]>(
    () => [
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
    ],
    [
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
    ],
  );

  const [operations, setOperations] = useState<
    Record<string, LifecycleOperationState>
  >({});
  const [pending, setPending] = useState<{
    action: ServiceLifecycleAction;
    instanceId: ServiceInstanceRole;
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
      instanceId: ServiceInstanceRole;
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

  const openDialog = (
    instanceId: ServiceInstanceRole,
    action: ServiceLifecycleAction,
  ) => {
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
        <StatusBadge tone="unknown">
          结果来自各角色 /status 与 /readyz；生命周期为 Admin API 契约目标
        </StatusBadge>
      </PanelHeader>

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
              const operation = operations[row.role];
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
                  key={row.role}
                  data-testid={`service-instance-${row.role}`}
                >
                  <TableCell className="align-top">
                    <span className="font-mono text-sm">{row.role}</span>
                    <div className="text-[11px] text-faint-foreground">
                      Service Role
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
                      data-testid={`last-observed-${row.role}`}
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
                        );
                        const lockedReason: DisabledReasonValue | null =
                          operationActive
                            ? {
                                code: "backend-unsupported",
                                message: "已有进行中的 Operation",
                              }
                            : null;
                        const effectiveReason = reason ?? lockedReason;
                        const reasonId = `${row.role}-${action}-reason`;
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
                              onClick={() => openDialog(row.role, action)}
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
      </div>

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
