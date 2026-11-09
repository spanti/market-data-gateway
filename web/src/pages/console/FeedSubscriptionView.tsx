import { zodResolver } from "@hookform/resolvers/zod";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useEffect, useMemo, useState, type ReactNode } from "react";
import { useForm } from "react-hook-form";
import { z } from "zod";
import type { CapabilityState } from "@/api/capabilities";
import {
  deleteFeedSubscription,
  fetchFeedSubscriptions,
  fetchFlowControl,
  updateApiRateLimit,
  updateFeedSubscription,
  updateWsSlowConsumer,
} from "@/api/endpoints/admin-console";
import type {
  FeedSubscription,
  FlowControlResponse,
} from "@/api/endpoints/admin";
import { HttpError } from "@/api/http/types";
import { ConfirmDialog } from "@/components/confirm-dialog";
import { DisabledAction, DisabledReason } from "@/components/disabled-reason";
import { EmptyState, ErrorState, LoadingState } from "@/components/states";
import { StatusBadge } from "@/components/status-badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
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
import { formatUtcTimestamp } from "@/lib/format";
import {
  buildManualMergeDraft,
  conflictFromError,
  describeFeedSubscription,
  describeFlowControl,
  FEED_DELETION_HISTORY_NOTE,
  feedSubscriptionUpdateBody,
  type FeedConflictState,
} from "@/pages/console/feed-subscription";
import {
  adminCapabilityAvailability,
  CAPABILITY_KEYS,
  type CapabilityKey,
} from "@/runtime/capability-gates";
import type { DisabledReason as DisabledReasonValue } from "@/runtime/permissions";
import { useRuntimeMode } from "@/runtime/runtime-mode-context";
import { useActionReason } from "@/runtime/use-permission";
import { useRetryAfter } from "@/runtime/use-retry-after";

/**
 * Feed Subscription + flow control console.
 *
 * CONTRACT TARGET: these Admin API resources do not exist in the current
 * backend. Current Contract renders a disabled area with visible reasons and
 * never a fabricated subscription table.
 */

const FEED_QUERY_KEY = ["console", "feed-subscriptions"] as const;
const FLOW_QUERY_KEY = ["console", "flow-control"] as const;

function adminCapabilityReason(
  capability: CapabilityState | null,
  key: CapabilityKey,
): DisabledReasonValue | null {
  const availability = adminCapabilityAvailability(capability, key);
  return availability.kind === "disabled" ? availability.reason : null;
}

function DisabledArea({
  title,
  description,
  readReason,
  writeReason,
  children,
}: {
  title: string;
  description: string;
  readReason: DisabledReasonValue | null;
  writeReason: DisabledReasonValue | null;
  children?: ReactNode;
}) {
  return (
    <Panel className="mt-6 p-5" aria-label={title}>
      <PanelHeader>
        <div>
          <PanelTitle>{title}</PanelTitle>
          <PanelDescription>{description}</PanelDescription>
        </div>
        <DisabledAction label="读取" reason={readReason} />
      </PanelHeader>
      <div className="mt-3 flex flex-wrap items-center gap-3">
        <DisabledAction label="修改" reason={writeReason} />
      </div>
      {children}
    </Panel>
  );
}

export function FeedSubscriptionView() {
  const { capability, session, writesStopped } = useRuntimeMode();
  const queryClient = useQueryClient();
  const retryAfter = useRetryAfter();
  const csrfToken = session?.csrfToken ?? null;

  const readReason = useActionReason("feed.subscription.view");
  const writeReason = useActionReason("feed.subscription.write");
  const flowReadReason = useActionReason("flow.control.view");
  const flowWriteReason = useActionReason("flow.control.write");

  const feedCapabilityReason = adminCapabilityReason(
    capability,
    CAPABILITY_KEYS.feedSubscription,
  );
  const flowCapabilityReason = adminCapabilityReason(
    capability,
    CAPABILITY_KEYS.feedFlowControl,
  );

  const isAdmin = capability?.mode === "admin-api";
  const feedReadReason = readReason ?? feedCapabilityReason;
  const flowReadBlockReason = flowReadReason ?? flowCapabilityReason;
  const canReadFeed = isAdmin && feedReadReason === null;
  const canReadFlow = isAdmin && flowReadBlockReason === null;
  const stoppedWriteReason: DisabledReasonValue | null = writesStopped
    ? {
        code: "requires-login",
        message: "会话已停止写操作，请重新登录",
      }
    : null;
  const feedWriteBlockReason =
    stoppedWriteReason ?? writeReason ?? retryAfter.reason ?? feedCapabilityReason;
  const flowWriteBlockReason =
    stoppedWriteReason ?? flowWriteReason ?? retryAfter.reason ?? flowCapabilityReason;

  const subscriptionsQuery = useQuery({
    queryKey: FEED_QUERY_KEY,
    queryFn: ({ signal }) => fetchFeedSubscriptions(signal),
    enabled: canReadFeed,
  });

  const flowQuery = useQuery({
    queryKey: FLOW_QUERY_KEY,
    queryFn: ({ signal }) => fetchFlowControl(signal),
    enabled: canReadFlow,
  });

  const [conflict, setConflict] = useState<{
    state: FeedConflictState;
    enabled: boolean;
  } | null>(null);
  const [merge, setMerge] = useState<{
    subscription: FeedSubscription;
    enabled: boolean;
  } | null>(null);
  const [mergeInfo, setMergeInfo] = useState<string | null>(null);
  const [submittedInfo, setSubmittedInfo] = useState<string | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<FeedSubscription | null>(
    null,
  );

  const updateMutation = useMutation({
    mutationFn: (input: {
      subscription: FeedSubscription;
      enabled: boolean;
      version: number;
    }) =>
      updateFeedSubscription(
        input.subscription.subscription_id,
        feedSubscriptionUpdateBody(input.subscription, input.enabled),
        input.version,
        csrfToken,
      ),
    onSuccess: async () => {
      setConflict(null);
      setMerge(null);
      setMergeInfo(null);
      setSubmittedInfo("已提交；effective 状态以 Observed 与 Drift 为准。");
      await queryClient.invalidateQueries({ queryKey: FEED_QUERY_KEY });
    },
    onError: (error, input) => {
      if (!(error instanceof HttpError)) {
        return;
      }
      const conflictState = conflictFromError(
        input.subscription.subscription_id,
        input.version,
        error,
      );
      if (conflictState) {
        setConflict({ state: conflictState, enabled: input.enabled });
        setMerge(null);
        setMergeInfo(null);
      }
    },
  });

  const deleteMutation = useMutation({
    mutationFn: (input: { subscription: FeedSubscription }) =>
      deleteFeedSubscription(
        input.subscription.subscription_id,
        input.subscription.desired.version,
        csrfToken,
      ),
    onSuccess: async () => {
      setDeleteTarget(null);
      setSubmittedInfo(
        "删除请求已接受：仅停止后续采集，历史 Bars 不变；以 Observed 确认删除结果。",
      );
      await queryClient.invalidateQueries({ queryKey: FEED_QUERY_KEY });
    },
    onError: (error, input) => {
      setDeleteTarget(null);
      if (!(error instanceof HttpError)) {
        return;
      }
      const conflictState = conflictFromError(
        input.subscription.subscription_id,
        input.subscription.desired.version,
        error,
      );
      if (conflictState) {
        setConflict({
          state: conflictState,
          enabled: input.subscription.desired.enabled,
        });
        setMerge(null);
        setMergeInfo(null);
      }
    },
  });

  const handleRefreshForMerge = async () => {
    if (!conflict) {
      return;
    }
    const result = await subscriptionsQuery.refetch();
    const fresh = result.data?.items.find(
      (item) => item.subscription_id === conflict.state.subscriptionId,
    );
    if (!fresh) {
      setMergeInfo("未在刷新结果中找到该订阅，请稍后重试。");
      return;
    }
    const draft = buildManualMergeDraft({
      ...conflict.state,
      current: fresh,
      currentVersion: fresh.desired.version,
    });
    if (!draft) {
      setMergeInfo("刷新结果缺少可合并的 Desired 版本。");
      return;
    }
    setMerge({ subscription: fresh, enabled: conflict.enabled });
    setMergeInfo(
      `已刷新到版本 ${fresh.desired.version}；请确认合并后显式提交。`,
    );
  };

  if (!canReadFeed && !canReadFlow) {
    return (
      <DisabledArea
        title="Feed Subscription"
        description="Feed Subscription 是 Admin API 契约目标；Current Contract 不伪造订阅表，Desired / Observed / Drift 必须来自真实服务端。"
        readReason={feedReadReason}
        writeReason={feedWriteBlockReason}
      >
        <div className="mt-4 border-t border-border pt-3">
          <PanelTitle>流控（分开配置）</PanelTitle>
          <PanelDescription>
            API Rate Limit 与 WebSocket Slow Consumer 是两个分别版本化、分别审计的资源；没有通用阈值。
          </PanelDescription>
          <div className="mt-2 flex flex-wrap items-center gap-3">
            <DisabledAction label="读取流控" reason={flowReadBlockReason} />
            <DisabledAction label="修改流控" reason={flowWriteBlockReason} />
          </div>
        </div>
      </DisabledArea>
    );
  }

  const flow = flowQuery.data ?? null;
  const flowView = flow ? describeFlowControl(flow) : null;

  return (
    <>
      <Panel className="mt-6 p-5" aria-label="Feed Subscription">
        <PanelHeader>
          <div>
            <PanelTitle>Feed Subscription</PanelTitle>
            <PanelDescription>
              Desired、Observed、Drift 与 Last Error 分开呈现；删除仅停止后续采集，历史 Bars 不受影响。
            </PanelDescription>
          </div>
          <StatusBadge tone="unknown">Admin API 契约目标 · MSW 契约测试</StatusBadge>
        </PanelHeader>

        {!canReadFeed ? (
          <div className="mt-3 flex flex-wrap items-center gap-3">
            <DisabledAction
              label="读取 Feed Subscription"
              reason={feedReadReason}
            />
            <DisabledAction
              label="修改 Feed Subscription"
              reason={feedWriteBlockReason}
            />
          </div>
        ) : (
          <>
            {conflict ? (
          <div
            data-testid="feed-conflict"
            role="alert"
            className="mt-3 border border-status-warning bg-surface-sunken p-3 text-xs"
          >
            <div className="font-medium text-status-critical">
              409 版本冲突：已停止自动提交
            </div>
            <p className="mt-1 text-muted-foreground">
              版本冲突：请刷新后人工合并，不会自动覆盖他人修改，也不会自动重试。
            </p>
            <p className="mt-1 font-mono text-[11px]">
              提交版本 {conflict.state.attemptedVersion} · 当前版本{" "}
              {conflict.state.currentVersion ?? "未知"}
            </p>
            <Button
              size="sm"
              variant="outline"
              className="mt-2"
              onClick={() => void handleRefreshForMerge()}
            >
              刷新并人工合并
            </Button>
            {mergeInfo ? (
              <p data-testid="feed-merge-info" className="mt-2">
                {mergeInfo}
              </p>
            ) : null}
            {merge ? (
              <div className="mt-2 flex flex-wrap items-center gap-3">
                <span>
                  当前 Desired：
                  {merge.subscription.desired.enabled ? "启用" : "停用"} · v
                  {merge.subscription.desired.version}
                </span>
                <label className="inline-flex items-center gap-1">
                  <input
                    type="checkbox"
                    checked={merge.enabled}
                    onChange={(event) =>
                      setMerge({ ...merge, enabled: event.target.checked })
                    }
                  />
                  合并后启用
                </label>
                <Button
                  size="sm"
                  onClick={() =>
                    updateMutation.mutate({
                      subscription: merge.subscription,
                      enabled: merge.enabled,
                      version: merge.subscription.desired.version,
                    })
                  }
                  disabled={updateMutation.isPending || retryAfter.blocked}
                >
                  应用合并
                </Button>
                {retryAfter.reason ? (
                  <span className="text-[11px] text-status-warning">
                    {retryAfter.reason.message}
                  </span>
                ) : null}
              </div>
            ) : null}
          </div>
        ) : null}

        {submittedInfo ? (
          <p role="status" className="mt-3 text-xs text-status-good">
            {submittedInfo}
          </p>
        ) : null}

        <div className="mt-3">
          {subscriptionsQuery.isPending ? (
            <LoadingState label="正在加载 Feed Subscription…" showSkeleton />
          ) : subscriptionsQuery.isError ? (
            <ErrorState
              description={
                subscriptionsQuery.error instanceof Error
                  ? subscriptionsQuery.error.message
                  : "无法读取 Feed Subscription"
              }
              requestId={
                subscriptionsQuery.error instanceof HttpError
                  ? subscriptionsQuery.error.requestId
                  : null
              }
              onRetry={() => void subscriptionsQuery.refetch()}
            />
          ) : (subscriptionsQuery.data?.items.length ?? 0) === 0 ? (
            <EmptyState
              title="暂无 Feed Subscription"
              description="没有真实订阅记录时保持空白，不生成示例订阅。"
            />
          ) : (
            <Table>
              <caption className="sr-only">
                Feed Subscription，Desired、Observed、Drift 与 Last Error 分开展示
              </caption>
              <TableHeader>
                <TableRow>
                  <TableHead>Subscription</TableHead>
                  <TableHead>Desired</TableHead>
                  <TableHead>Observed</TableHead>
                  <TableHead>Drift</TableHead>
                  <TableHead>Last Error</TableHead>
                  <TableHead>Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {subscriptionsQuery.data?.items.map((subscription) => {
                  const view = describeFeedSubscription(subscription);
                  const actionReason = feedWriteBlockReason;
                  const busy =
                    updateMutation.isPending || deleteMutation.isPending;
                  return (
                    <TableRow
                      key={subscription.subscription_id}
                      data-testid={`feed-subscription-${subscription.subscription_id}`}
                    >
                      <TableCell className="align-top">
                        <div className="font-mono text-xs">
                          {view.identity.instrumentId}
                        </div>
                        <div className="text-[11px] text-faint-foreground">
                          {view.identity.feed} · {view.identity.venue} ·{" "}
                          {view.identity.interval}
                        </div>
                      </TableCell>
                      <TableCell className="align-top">
                        <div>
                          {view.desired.label} · v{view.desired.version}
                        </div>
                        <div className="text-[11px] text-faint-foreground">
                          {formatUtcTimestamp(view.desired.updatedAt)} ·{" "}
                          {view.desired.updatedBy}
                        </div>
                      </TableCell>
                      <TableCell className="align-top">
                        <div>
                          {view.observed.label} · v{view.observed.version}
                        </div>
                        <div className="text-[11px] text-faint-foreground">
                          {formatUtcTimestamp(view.observed.observedAt)} ·{" "}
                          {view.observed.collectorInstanceId}
                        </div>
                      </TableCell>
                      <TableCell className="align-top">
                        <div
                          className={
                            view.drift.inSync
                              ? "text-status-good"
                              : "text-status-warning"
                          }
                        >
                          {view.drift.label}
                        </div>
                        <div className="text-[11px] text-faint-foreground">
                          desired v{view.drift.desiredVersion} / observed v
                          {view.drift.observedVersion}
                        </div>
                      </TableCell>
                      <TableCell className="align-top">
                        {view.lastError ? (
                          <>
                            <div className="font-mono text-status-critical">
                              {view.lastError.code}
                            </div>
                            <div className="text-[11px] text-muted-foreground">
                              {view.lastError.message}
                            </div>
                            <div className="text-[11px] text-faint-foreground">
                              {formatUtcTimestamp(view.lastError.at)} · Request ID{" "}
                              {view.lastError.requestId}
                            </div>
                          </>
                        ) : (
                          <span className="text-faint-foreground">—</span>
                        )}
                      </TableCell>
                      <TableCell className="align-top">
                        <div className="flex flex-col gap-1">
                          <span className="inline-flex flex-wrap items-center gap-2">
                            <Button
                              size="sm"
                              variant="outline"
                              disabled={actionReason !== null || busy}
                              aria-describedby={
                                actionReason
                                  ? `feed-${subscription.subscription_id}-reason`
                                  : undefined
                              }
                              onClick={() =>
                                updateMutation.mutate({
                                  subscription,
                                  enabled: !subscription.desired.enabled,
                                  version: subscription.desired.version,
                                })
                              }
                            >
                              {subscription.desired.enabled ? "停用" : "启用"}
                            </Button>
                            <DisabledReason
                              id={`feed-${subscription.subscription_id}-reason`}
                              reason={actionReason}
                            />
                          </span>
                          <span className="inline-flex flex-wrap items-center gap-2">
                            <Button
                              size="sm"
                              variant="destructive"
                              disabled={actionReason !== null || busy}
                              aria-describedby={
                                actionReason
                                  ? `feed-${subscription.subscription_id}-reason`
                                  : undefined
                              }
                              onClick={() => setDeleteTarget(subscription)}
                            >
                              删除
                            </Button>
                          </span>
                        </div>
                      </TableCell>
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
          )}
        </div>
            <p className="mt-2 text-[11px] text-faint-foreground">
              {FEED_DELETION_HISTORY_NOTE}
            </p>
          </>
        )}
      </Panel>

      <Panel className="mt-6 p-5" aria-label="Flow Control">
        <PanelHeader>
          <div>
            <PanelTitle>流控（分开配置）</PanelTitle>
            <PanelDescription>
              API Rate Limit 与 WebSocket Slow Consumer
              是两个分别版本化、分别审计的资源（flow_control.api_rate_limit.update /
              flow_control.ws_slow_consumer.update）；没有通用阈值，也不控制
              Venue、Collector 写队列或存储背压。
            </PanelDescription>
          </div>
        </PanelHeader>

        {!canReadFlow ? (
          <div className="mt-3 flex flex-wrap items-center gap-3">
            <DisabledAction label="读取流控" reason={flowReadBlockReason} />
            <DisabledAction label="修改流控" reason={flowWriteBlockReason} />
          </div>
        ) : flowQuery.isPending ? (
          <LoadingState label="正在加载流控配置…" />
        ) : flowQuery.isError || !flowView || !flow ? (
          <ErrorState
            description={
              flowQuery.error instanceof Error
                ? flowQuery.error.message
                : "无法读取流控配置"
            }
            requestId={
              flowQuery.error instanceof HttpError
                ? flowQuery.error.requestId
                : null
            }
            onRetry={() => void flowQuery.refetch()}
          />
        ) : (
          <div className="mt-3 grid grid-cols-1 gap-4 xl:grid-cols-2">
            <FlowControlEditor
              resource="api_rate_limit"
              title="API Rate Limit"
              version={flowView.apiRateLimit.version}
              applyNote={flowView.apiRateLimit.applyNote}
              observedVersion={flowView.apiRateLimit.observedVersion}
              inSync={flowView.apiRateLimit.inSync}
              writeReason={flowWriteBlockReason}
              retryAfterReason={retryAfter.reason}
              csrfToken={csrfToken}
              initial={flow.api_rate_limit}
              onSaved={() => void queryClient.invalidateQueries({ queryKey: FLOW_QUERY_KEY })}
            />
            <FlowControlEditor
              resource="ws_slow_consumer"
              title="WebSocket Slow Consumer"
              version={flowView.wsSlowConsumer.version}
              applyNote={flowView.wsSlowConsumer.applyNote}
              observedVersion={flowView.wsSlowConsumer.observedVersion}
              inSync={flowView.wsSlowConsumer.inSync}
              writeReason={flowWriteBlockReason}
              retryAfterReason={retryAfter.reason}
              csrfToken={csrfToken}
              initial={flow.ws_slow_consumer}
              onSaved={() => void queryClient.invalidateQueries({ queryKey: FLOW_QUERY_KEY })}
            />
          </div>
        )}
      </Panel>

      <ConfirmDialog
        open={deleteTarget !== null}
        onOpenChange={(open) => {
          if (!open) {
            setDeleteTarget(null);
          }
        }}
        title="确认删除 Feed Subscription"
        description={FEED_DELETION_HISTORY_NOTE}
        confirmLabel="提交删除"
        requireReason={false}
        submitting={deleteMutation.isPending}
        onConfirm={() => {
          if (retryAfter.blocked) {
            return;
          }
          if (deleteTarget) {
            deleteMutation.mutate({ subscription: deleteTarget });
          }
        }}
      />
    </>
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
    <p id={id} role="alert" className="text-[11px] text-status-critical">
      {message}
    </p>
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
  readOnlyReason: DisabledReasonValue | null;
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
  const {
    register,
    handleSubmit,
    reset,
    formState: { errors },
  } = useForm<ApiRateLimitForm>({
    resolver: zodResolver(apiRateLimitSchema),
    mode: "onSubmit",
    defaultValues,
  });

  useEffect(() => {
    reset(defaultValues);
  }, [defaultValues, reset]);

  const mutation = useMutation({
    mutationFn: (form: ApiRateLimitForm) =>
      updateApiRateLimit(form, version, csrfToken),
    onSuccess: onSaved,
  });
  const readOnly = readOnlyReason !== null;
  const saving = mutation.isPending;

  return (
    <form
      onSubmit={handleSubmit((values) => {
        if (!readOnly) {
          mutation.mutate(values);
        }
      })}
      noValidate
    >
      <div className="mt-3 grid grid-cols-2 gap-2">
        <label className="col-span-2 inline-flex items-center gap-2 text-xs">
          <input
            type="checkbox"
            disabled={readOnly}
            {...register("enabled")}
          />
          enabled
        </label>
        <div className="flex flex-col gap-1">
          <Label htmlFor="api-rps">requests_per_second</Label>
          <Input
            id="api-rps"
            type="number"
            step="any"
            disabled={readOnly}
            aria-invalid={Boolean(errors.requests_per_second)}
            aria-describedby={
              errors.requests_per_second ? "api-rps-error" : undefined
            }
            {...register("requests_per_second", { valueAsNumber: true })}
          />
          <FieldError
            id="api-rps-error"
            message={errors.requests_per_second?.message}
          />
        </div>
        <div className="flex flex-col gap-1">
          <Label htmlFor="api-burst">burst</Label>
          <Input
            id="api-burst"
            type="number"
            step="1"
            disabled={readOnly}
            aria-invalid={Boolean(errors.burst)}
            aria-describedby={errors.burst ? "api-burst-error" : undefined}
            {...register("burst", { valueAsNumber: true })}
          />
          <FieldError id="api-burst-error" message={errors.burst?.message} />
        </div>
        <div className="col-span-2 flex flex-col gap-1">
          <Label htmlFor="api-ws-sessions">max_ws_sessions_per_ip</Label>
          <Input
            id="api-ws-sessions"
            type="number"
            step="1"
            disabled={readOnly}
            aria-invalid={Boolean(errors.max_ws_sessions_per_ip)}
            aria-describedby={
              errors.max_ws_sessions_per_ip
                ? "api-ws-sessions-error"
                : undefined
            }
            {...register("max_ws_sessions_per_ip", { valueAsNumber: true })}
          />
          <FieldError
            id="api-ws-sessions-error"
            message={errors.max_ws_sessions_per_ip?.message}
          />
        </div>
      </div>

      <div className="mt-3 flex flex-wrap items-center gap-2">
        <Button
          type="submit"
          size="sm"
          variant="outline"
          disabled={readOnly || saving}
          aria-describedby={
            readOnlyReason ? "flow-api_rate_limit-reason" : undefined
          }
        >
          保存 API Rate Limit
        </Button>
        <DisabledReason
          id="flow-api_rate_limit-reason"
          reason={readOnlyReason}
        />
        {mutation.isError ? (
          <span className="text-xs text-status-critical">
            保存失败，请查看错误与 Request ID。
          </span>
        ) : null}
      </div>
    </form>
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
  readOnlyReason: DisabledReasonValue | null;
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
  const {
    register,
    handleSubmit,
    reset,
    formState: { errors },
  } = useForm<WsSlowConsumerForm>({
    resolver: zodResolver(wsSlowConsumerSchema),
    mode: "onSubmit",
    defaultValues,
  });

  useEffect(() => {
    reset(defaultValues);
  }, [defaultValues, reset]);

  const mutation = useMutation({
    mutationFn: (form: WsSlowConsumerForm) =>
      updateWsSlowConsumer(form, version, csrfToken),
    onSuccess: onSaved,
  });
  const readOnly = readOnlyReason !== null;
  const saving = mutation.isPending;

  return (
    <form
      onSubmit={handleSubmit((values) => {
        if (!readOnly) {
          mutation.mutate(values);
        }
      })}
      noValidate
    >
      <div className="mt-3 grid grid-cols-2 gap-2">
        <label className="col-span-2 inline-flex items-center gap-2 text-xs">
          <input
            type="checkbox"
            disabled={readOnly}
            {...register("enabled")}
          />
          enabled
        </label>
        <div className="flex flex-col gap-1">
          <Label htmlFor="ws-queue-size">queue_size</Label>
          <Input
            id="ws-queue-size"
            type="number"
            step="1"
            disabled={readOnly}
            aria-invalid={Boolean(errors.queue_size)}
            aria-describedby={
              errors.queue_size ? "ws-queue-size-error" : undefined
            }
            {...register("queue_size", { valueAsNumber: true })}
          />
          <FieldError
            id="ws-queue-size-error"
            message={errors.queue_size?.message}
          />
        </div>
        <div className="flex flex-col gap-1">
          <Label htmlFor="ws-lag-ms">lag_notice_interval_ms</Label>
          <Input
            id="ws-lag-ms"
            type="number"
            step="1"
            disabled={readOnly}
            aria-invalid={Boolean(errors.lag_notice_interval_ms)}
            aria-describedby={
              errors.lag_notice_interval_ms
                ? "ws-lag-ms-error"
                : undefined
            }
            {...register("lag_notice_interval_ms", { valueAsNumber: true })}
          />
          <FieldError
            id="ws-lag-ms-error"
            message={errors.lag_notice_interval_ms?.message}
          />
        </div>
      </div>

      <div className="mt-3 flex flex-wrap items-center gap-2">
        <Button
          type="submit"
          size="sm"
          variant="outline"
          disabled={readOnly || saving}
          aria-describedby={
            readOnlyReason ? "flow-ws_slow_consumer-reason" : undefined
          }
        >
          保存 WebSocket Slow Consumer
        </Button>
        <DisabledReason
          id="flow-ws_slow_consumer-reason"
          reason={readOnlyReason}
        />
        {mutation.isError ? (
          <span className="text-xs text-status-critical">
            保存失败，请查看错误与 Request ID。
          </span>
        ) : null}
      </div>
    </form>
  );
}

function FlowControlEditor({
  resource,
  title,
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
  version: number;
  applyNote: string;
  observedVersion: number;
  inSync: boolean;
  writeReason: DisabledReasonValue | null;
  retryAfterReason: DisabledReasonValue | null;
  csrfToken: string | null;
  initial: FlowControlResponse["api_rate_limit"] | FlowControlResponse["ws_slow_consumer"];
  onSaved: () => void;
}) {
  const readOnlyReason = writeReason ?? retryAfterReason;

  return (
    <div
      data-testid={`flow-${resource.replaceAll("_", "-")}`}
      className="border border-border p-3"
    >
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div>
          <div className="text-sm font-semibold">{title}</div>
          <div className="text-[11px] text-faint-foreground">
            版本 {version} · Observed v{observedVersion} ·{" "}
            {inSync ? "同步" : "存在漂移"}
          </div>
        </div>
        <StatusBadge tone={inSync ? "good" : "warning"}>
          {applyNote}
        </StatusBadge>
      </div>

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
  );
}
