import { zodResolver } from "@hookform/resolvers/zod";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useMemo, type ReactNode } from "react";
import {
  useForm,
  type DefaultValues,
  type FieldValues,
  type Path,
} from "react-hook-form";
import { z } from "zod";
import {
  fetchFlowControl,
  updateApiRateLimit,
  updateWsSlowConsumer,
} from "@/api/endpoints/admin-console";
import type { FlowControlResponse } from "@/api/endpoints/admin";
import { DisabledReason as DisabledReasonView } from "@/components/disabled-reason";
import { HelpTrigger } from "@/components/help-trigger";
import { LoadingState, UnknownState } from "@/components/states";
import { StatusBadge } from "@/components/status-badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { queryErrorInfo } from "@/lib/errors";
import { describeFlowControl } from "@/pages/console/feed-subscription";
import {
  CardTitleWithHelp,
  QueryFailure,
  UnavailableActions,
} from "@/pages/console/console-parts";
import {
  adminCapabilityReason,
  CAPABILITY_KEYS,
} from "@/runtime/capability-gates";
import { capabilityModeKind } from "@/runtime/mode-presentation";
import type { DisabledReason } from "@/runtime/permissions";
import { useRuntimeMode } from "@/runtime/runtime-mode-context";
import { useActionReason } from "@/runtime/use-permission";
import { useRetryAfter } from "@/runtime/use-retry-after";

/**
 * Flow-control console: API Rate Limit and WebSocket Slow Consumer.
 *
 * CONTRACT TARGET: these Admin API resources do not exist in the current
 * backend. Current Contract renders disabled editors with visible reasons.
 * The two resources are versioned, audited and written independently.
 */

const FLOW_QUERY_KEY = ["console", "flow-control"] as const;

const FLOW_HELP =
  "API Rate Limit 与 WebSocket Slow Consumer 是两个分别版本化、分别审计的资源。没有通用阈值，也不控制 Venue、Collector 写队列或存储背压。";

const API_RATE_LIMIT_HELP =
  "enabled 开启或关闭 API 限流。requests_per_second 是每秒请求数。burst 是允许的突发请求数。max_ws_sessions_per_ip 是每个 IP 允许的最大 WebSocket 会话数。写入需要 Admin API 与 Operator 或 SuperAdmin 角色。";

const WS_SLOW_CONSUMER_HELP =
  "enabled 开启或关闭慢消费者保护。queue_size 是每个会话保留的最大待发送消息数。lag_notice_interval_ms 是发送 lag_notice 的间隔，单位毫秒。需要重启 Service Instance 后生效。";

function FlowResourceUnavailable({
  title,
  help,
  readReason,
  writeReason,
}: {
  title: string;
  help: ReactNode;
  readReason: DisabledReason | null;
  writeReason: DisabledReason | null;
}) {
  return (
    <div className="rounded-lg border border-card-border bg-surface-sunken p-3">
      <div className="flex min-w-0 items-center gap-2">
        <h3 className="text-sm font-semibold text-foreground">{title}</h3>
        <HelpTrigger label={`${title} 说明`} content={help} side="top" />
      </div>
      <p className="mt-1 text-[11px] leading-relaxed text-muted-foreground">
        独立版本化与审计，不与其他流控设置共用阈值。
      </p>
      <UnavailableActions
        label={title}
        readReason={readReason}
        writeReason={writeReason}
      />
    </div>
  );
}

const apiRateLimitSchema = z.object({
  enabled: z.boolean(),
  requests_per_second: z
    .number({ invalid_type_error: "请输入数字" })
    .positive("requests_per_second 必须大于 0"),
  burst: z
    .number({ invalid_type_error: "请输入数字" })
    .int("burst 必须是整数")
    .min(0, "burst 不能小于 0"),
  max_ws_sessions_per_ip: z
    .number({ invalid_type_error: "请输入数字" })
    .int("max_ws_sessions_per_ip 必须是整数")
    .min(1, "max_ws_sessions_per_ip 至少为 1"),
});

const wsSlowConsumerSchema = z.object({
  enabled: z.boolean(),
  queue_size: z
    .number({ invalid_type_error: "请输入数字" })
    .int("queue_size 必须是整数")
    .min(1, "queue_size 至少为 1"),
  lag_notice_interval_ms: z
    .number({ invalid_type_error: "请输入数字" })
    .int("lag_notice_interval_ms 必须是整数")
    .min(1, "lag_notice_interval_ms 至少为 1"),
});

type ApiRateLimitForm = z.infer<typeof apiRateLimitSchema>;
type WsSlowConsumerForm = z.infer<typeof wsSlowConsumerSchema>;

interface FlowField {
  name: string;
  label: string;
  unit: string;
  id: string;
  step: string;
  wide?: boolean;
}

const API_RATE_LIMIT_FIELDS: FlowField[] = [
  {
    name: "requests_per_second",
    label: "requests_per_second",
    unit: "req/s",
    id: "api-rps",
    step: "any",
  },
  {
    name: "burst",
    label: "burst",
    unit: "requests",
    id: "api-burst",
    step: "1",
  },
  {
    name: "max_ws_sessions_per_ip",
    label: "max_ws_sessions_per_ip",
    unit: "sessions",
    id: "api-ws-sessions",
    step: "1",
    wide: true,
  },
];

const WS_SLOW_CONSUMER_FIELDS: FlowField[] = [
  {
    name: "queue_size",
    label: "queue_size",
    unit: "messages",
    id: "ws-queue-size",
    step: "1",
  },
  {
    name: "lag_notice_interval_ms",
    label: "lag_notice_interval_ms",
    unit: "ms",
    id: "ws-lag-ms",
    step: "1",
  },
];

function FieldError({
  id,
  message,
}: {
  id: string;
  message: string | undefined;
}) {
  if (!message) {
    return null;
  }
  return (
    <p id={id} role="alert" className="text-[11px] text-status-critical-fg">
      {message}
    </p>
  );
}

function InlineMutationResult({
  saving,
  success,
  error,
}: {
  saving: boolean;
  success: boolean;
  error: unknown;
}) {
  if (saving) {
    return (
      <span role="status" className="text-[11px] text-muted-foreground">
        正在保存…
      </span>
    );
  }
  if (error !== null) {
    const info = queryErrorInfo(error, "保存失败");
    return (
      <div
        role="alert"
        className="mt-2 w-full rounded-md bg-status-critical-bg px-3 py-2 text-[11px] text-muted-foreground"
      >
        <div className="font-medium text-status-critical-fg">保存失败</div>
        <span data-raw-evidence="true" className="break-words">
          {info.message}
        </span>
        {info.requestId ? (
          <div className="mt-1 font-mono">Request ID {info.requestId}</div>
        ) : null}
      </div>
    );
  }
  if (success) {
    return (
      <span role="status" className="text-[11px] text-status-good-fg">
        保存请求已提交，等待 Observed 确认。
      </span>
    );
  }
  return null;
}

function LabelWithUnit({
  htmlFor,
  label,
  unit,
}: {
  htmlFor: string;
  label: string;
  unit: string;
}) {
  return (
    <div className="flex items-baseline justify-between gap-2">
      <Label htmlFor={htmlFor}>{label}</Label>
      <span className="text-[11px] text-faint-foreground">{unit}</span>
    </div>
  );
}

interface ResourceEditorProps<TValues extends FieldValues> {
  fields: FlowField[];
  schema: z.ZodType<TValues>;
  defaultValues: DefaultValues<TValues>;
  saveLabel: string;
  resource: "api_rate_limit" | "ws_slow_consumer";
  version: number;
  csrfToken: string | null;
  readOnlyReason: DisabledReason | null;
  save: (
    values: TValues,
    version: number,
    csrfToken: string | null,
  ) => Promise<unknown>;
  onSaved: () => void;
}

/**
 * One flow-resource editor: form lifecycle, validation, units, versioned write
 * and inline results. Both resources differ only by fields, schema and save
 * target, so the markup and mutation lifecycle live here once.
 */
function ResourceEditor<TValues extends FieldValues>({
  fields,
  schema,
  defaultValues,
  saveLabel,
  resource,
  version,
  csrfToken,
  readOnlyReason,
  save,
  onSaved,
}: ResourceEditorProps<TValues>) {
  const {
    register,
    handleSubmit,
    reset,
    formState: { errors },
  } = useForm<TValues>({
    resolver: zodResolver(schema),
    mode: "onSubmit",
    defaultValues,
  });

  useEffect(() => {
    reset(defaultValues);
  }, [defaultValues, reset]);

  const mutation = useMutation<unknown, Error, TValues>({
    mutationFn: (values) => save(values, version, csrfToken),
    onSuccess: onSaved,
  });
  const readOnly = readOnlyReason !== null;
  const saving = mutation.isPending;
  const reasonId = `flow-${resource}-reason`;

  return (
    <form
      onSubmit={handleSubmit((values) => {
        if (!readOnly) {
          mutation.mutate(values);
        }
      })}
      noValidate
    >
      <div className="mt-3 grid grid-cols-1 gap-3 sm:grid-cols-2">
        <label className="col-span-full inline-flex items-center gap-2 text-xs">
          <input
            type="checkbox"
            disabled={readOnly}
            {...register("enabled" as Path<TValues>)}
          />
          启用此资源
        </label>
        {fields.map((field) => {
          const message = errors[field.name as keyof TValues]?.message;
          const fieldError =
            typeof message === "string" ? message : undefined;
          return (
            <div
              key={field.name}
              className={
                field.wide
                  ? "col-span-full flex flex-col gap-1"
                  : "flex flex-col gap-1"
              }
            >
              <LabelWithUnit
                htmlFor={field.id}
                label={field.label}
                unit={field.unit}
              />
              <Input
                id={field.id}
                type="number"
                step={field.step}
                disabled={readOnly}
                aria-invalid={fieldError !== undefined}
                aria-describedby={
                  fieldError ? `${field.id}-error` : undefined
                }
                {...register(field.name as Path<TValues>, {
                  valueAsNumber: true,
                })}
              />
              <FieldError
                id={`${field.id}-error`}
                message={fieldError}
              />
            </div>
          );
        })}
      </div>

      <div className="mt-3 flex flex-wrap items-center gap-2">
        <Button
          type="submit"
          size="sm"
          variant="outline"
          disabled={readOnly || saving}
          aria-describedby={readOnlyReason ? reasonId : undefined}
        >
          {saveLabel}
        </Button>
        <DisabledReasonView id={reasonId} reason={readOnlyReason} />
        <InlineMutationResult
          saving={saving}
          success={mutation.isSuccess}
          error={mutation.isError ? mutation.error : null}
        />
      </div>
    </form>
  );
}

function ApiRateLimitEditor({
  version,
  csrfToken,
  initial,
  readOnlyReason,
  onSaved,
}: {
  version: number;
  csrfToken: string | null;
  initial: FlowControlResponse["api_rate_limit"];
  readOnlyReason: DisabledReason | null;
  onSaved: () => void;
}) {
  const defaultValues = useMemo<ApiRateLimitForm>(
    () => ({
      enabled: initial.enabled,
      requests_per_second: initial.requests_per_second,
      burst: initial.burst,
      max_ws_sessions_per_ip: initial.max_ws_sessions_per_ip,
    }),
    [initial],
  );
  return (
    <ResourceEditor
      fields={API_RATE_LIMIT_FIELDS}
      schema={apiRateLimitSchema}
      defaultValues={defaultValues}
      saveLabel="保存 API Rate Limit"
      resource="api_rate_limit"
      version={version}
      csrfToken={csrfToken}
      readOnlyReason={readOnlyReason}
      save={(values, writeVersion, token) =>
        updateApiRateLimit(values, writeVersion, token)
      }
      onSaved={onSaved}
    />
  );
}

function WsSlowConsumerEditor({
  version,
  csrfToken,
  initial,
  readOnlyReason,
  onSaved,
}: {
  version: number;
  csrfToken: string | null;
  initial: FlowControlResponse["ws_slow_consumer"];
  readOnlyReason: DisabledReason | null;
  onSaved: () => void;
}) {
  const defaultValues = useMemo<WsSlowConsumerForm>(
    () => ({
      enabled: initial.enabled,
      queue_size: initial.queue_size,
      lag_notice_interval_ms: initial.lag_notice_interval_ms,
    }),
    [initial],
  );
  return (
    <ResourceEditor
      fields={WS_SLOW_CONSUMER_FIELDS}
      schema={wsSlowConsumerSchema}
      defaultValues={defaultValues}
      saveLabel="保存 WebSocket Slow Consumer"
      resource="ws_slow_consumer"
      version={version}
      csrfToken={csrfToken}
      readOnlyReason={readOnlyReason}
      save={(values, writeVersion, token) =>
        updateWsSlowConsumer(values, writeVersion, token)
      }
      onSaved={onSaved}
    />
  );
}

function FlowControlEditor({
  resource,
  title,
  help,
  version,
  applyNote,
  observedVersion,
  inSync,
  writeReason,
  retryAfterReason,
  csrfToken,
  initial,
  onSaved,
}: {
  resource: "api_rate_limit" | "ws_slow_consumer";
  title: string;
  help: ReactNode;
  version: number;
  applyNote: string;
  observedVersion: number;
  inSync: boolean;
  writeReason: DisabledReason | null;
  retryAfterReason: DisabledReason | null;
  csrfToken: string | null;
  initial:
    | FlowControlResponse["api_rate_limit"]
    | FlowControlResponse["ws_slow_consumer"];
  onSaved: () => void;
}) {
  const readOnlyReason = writeReason ?? retryAfterReason;

  return (
    <Card
      role="group"
      aria-label={title}
      data-testid={`flow-${resource.replaceAll("_", "-")}`}
      className="bg-surface-sunken shadow-none"
    >
      <div className="flex flex-wrap items-start justify-between gap-2 p-4 pb-0">
        <div className="min-w-0">
          <div className="flex min-w-0 items-center gap-2">
            <h3 className="text-sm font-semibold text-foreground">{title}</h3>
            <HelpTrigger label={`${title} 说明`} content={help} side="top" />
          </div>
          <div className="mt-1 flex flex-wrap items-center gap-2 text-[11px] text-muted-foreground">
            <StatusBadge tone={inSync ? "good" : "warning"}>
              {inSync ? "已同步" : "存在漂移"}
            </StatusBadge>
            <span>Desired 版本 {version}</span>
            <span>Observed 版本 {observedVersion}</span>
          </div>
          <p className="mt-1 text-[11px] leading-relaxed text-faint-foreground">
            {applyNote}
          </p>
        </div>
      </div>

      <div className="p-4 pt-3">
        {resource === "api_rate_limit" && "requests_per_second" in initial ? (
          <ApiRateLimitEditor
            version={version}
            csrfToken={csrfToken}
            initial={initial}
            readOnlyReason={readOnlyReason}
            onSaved={onSaved}
          />
        ) : null}

        {resource === "ws_slow_consumer" && "queue_size" in initial ? (
          <WsSlowConsumerEditor
            version={version}
            csrfToken={csrfToken}
            initial={initial}
            readOnlyReason={readOnlyReason}
            onSaved={onSaved}
          />
        ) : null}
      </div>
    </Card>
  );
}

/**
 * The Flow Control card. It intentionally lives beside, not inside, the Feed
 * Subscription view: both are Console resources but neither owns the other.
 */
export function FlowControlSection() {
  const { capability, session, writesStopped } = useRuntimeMode();
  const queryClient = useQueryClient();
  const retryAfter = useRetryAfter();
  const csrfToken = session?.csrfToken ?? null;

  const modeKind = capabilityModeKind(capability);
  const discovering = modeKind === "discovering";
  const capabilityUnknown = modeKind === "capabilities-unknown";
  const isAdmin = modeKind === "admin-api";

  const flowReadReason = useActionReason("flow.control.view");
  const flowWriteReason = useActionReason("flow.control.write");
  const flowCapabilityReason = adminCapabilityReason(
    capability,
    CAPABILITY_KEYS.feedFlowControl,
  );
  const flowReadBlockReason = flowReadReason ?? flowCapabilityReason;
  const flowRoleReason = flowWriteReason ?? flowCapabilityReason;
  const stoppedWriteReason: DisabledReason | null = writesStopped
    ? {
        code: "requires-login",
        message: "会话已停止写操作，请重新登录",
      }
    : null;
  const flowWriteBlockReason =
    stoppedWriteReason ??
    flowWriteReason ??
    retryAfter.reason ??
    flowCapabilityReason;
  const canReadFlow = isAdmin && flowReadBlockReason === null;

  const flowQuery = useQuery({
    queryKey: FLOW_QUERY_KEY,
    queryFn: ({ signal }) => fetchFlowControl(signal),
    enabled: canReadFlow,
  });
  const flow = flowQuery.data ?? null;
  const flowView = flow ? describeFlowControl(flow) : null;

  const flowContent = !canReadFlow ? (
    discovering ? (
      <LoadingState label="正在发现运行时能力…" />
    ) : (
      <>
        {capabilityUnknown ? (
          <UnknownState
            cause="failed"
            reason="运行时能力发现失败，无法确认 API Rate Limit 与 WebSocket Slow Consumer 的读写能力。"
          />
        ) : (
          <p className="text-xs leading-relaxed text-muted-foreground">
            API Rate Limit 与 WebSocket Slow Consumer 分别依赖 Admin API 能力与对应的服务端
            Access Role。没有通用阈值。
          </p>
        )}
        <div className="mt-3 grid grid-cols-1 gap-3 sm:grid-cols-2">
          <FlowResourceUnavailable
            title="API Rate Limit"
            help={API_RATE_LIMIT_HELP}
            readReason={flowReadBlockReason}
            writeReason={flowWriteBlockReason}
          />
          <FlowResourceUnavailable
            title="WebSocket Slow Consumer"
            help={WS_SLOW_CONSUMER_HELP}
            readReason={flowReadBlockReason}
            writeReason={flowWriteBlockReason}
          />
        </div>
      </>
    )
  ) : flowQuery.isPending ? (
    <LoadingState label="正在加载流控配置…" />
  ) : flowQuery.isError || !flowView || !flow ? (
    <QueryFailure
      error={flowQuery.error}
      fallback="无法读取流控配置"
      onRetry={() => void flowQuery.refetch()}
    />
  ) : (
    <div className="grid grid-cols-1 gap-4 xl:grid-cols-2">
      <FlowControlEditor
        resource="api_rate_limit"
        title="API Rate Limit"
        help={API_RATE_LIMIT_HELP}
        version={flowView.apiRateLimit.version}
        applyNote={flowView.apiRateLimit.applyNote}
        observedVersion={flowView.apiRateLimit.observedVersion}
        inSync={flowView.apiRateLimit.inSync}
        writeReason={flowWriteBlockReason}
        retryAfterReason={retryAfter.reason}
        csrfToken={csrfToken}
        initial={flow.api_rate_limit}
        onSaved={() =>
          void queryClient.invalidateQueries({ queryKey: FLOW_QUERY_KEY })
        }
      />
      <FlowControlEditor
        resource="ws_slow_consumer"
        title="WebSocket Slow Consumer"
        help={WS_SLOW_CONSUMER_HELP}
        version={flowView.wsSlowConsumer.version}
        applyNote={flowView.wsSlowConsumer.applyNote}
        observedVersion={flowView.wsSlowConsumer.observedVersion}
        inSync={flowView.wsSlowConsumer.inSync}
        writeReason={flowWriteBlockReason}
        retryAfterReason={retryAfter.reason}
        csrfToken={csrfToken}
        initial={flow.ws_slow_consumer}
        onSaved={() =>
          void queryClient.invalidateQueries({ queryKey: FLOW_QUERY_KEY })
        }
      />
    </div>
  );

  return (
    <Card role="region" aria-label="Flow Control" className="mt-4">
      <CardHeader className="p-5 pb-0">
        <div className="min-w-0">
          <CardTitleWithHelp
            title="Flow Control"
            helpLabel="Flow Control 说明"
            help={FLOW_HELP}
          />
          <p className="mt-1 text-xs leading-relaxed text-muted-foreground">
            API Rate Limit 与 WebSocket Slow Consumer 独立配置，分别版本化与审计。
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          {discovering ? (
            <StatusBadge tone="unknown">发现能力中</StatusBadge>
          ) : canReadFlow ? (
            <StatusBadge tone={flowRoleReason ? "unknown" : "good"}>
              {flowRoleReason ? "当前只读" : "当前可写"}
            </StatusBadge>
          ) : (
            <StatusBadge tone={capabilityUnknown ? "warning" : "unknown"}>
              {capabilityUnknown ? "能力未知" : "需要 Admin API"}
            </StatusBadge>
          )}
          {canReadFlow && flowQuery.isFetching && !flowQuery.isPending ? (
            <StatusBadge tone="info" live>
              正在刷新
            </StatusBadge>
          ) : null}
        </div>
      </CardHeader>
      <CardContent className="pt-4">{flowContent}</CardContent>
    </Card>
  );
}
