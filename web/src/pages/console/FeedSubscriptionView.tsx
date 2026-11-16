import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { AlertTriangle } from "lucide-react";
import { useState, type ReactNode } from "react";
import {
  deleteFeedSubscription,
  fetchFeedSubscriptions,
  updateFeedSubscription,
} from "@/api/endpoints/admin-console";
import type { FeedSubscription } from "@/api/endpoints/admin";
import { HttpError } from "@/api/http/types";
import { ConfirmDialog } from "@/components/confirm-dialog";
import { DisabledReason } from "@/components/disabled-reason";
import { HelpTrigger } from "@/components/help-trigger";
import {
  EmptyState,
  LoadingState,
  UnknownState,
} from "@/components/states";
import { StatusBadge, type StatusTone } from "@/components/status-badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { formatUtcTimestamp } from "@/lib/format";
import {
  CardTitleWithHelp,
  QueryFailure,
  UnavailableActions,
} from "@/pages/console/console-parts";
import {
  buildManualMergeDraft,
  conflictFromError,
  describeFeedSubscription,
  FEED_DELETION_HISTORY_NOTE,
  feedSubscriptionUpdateBody,
  type FeedConflictState,
  type FeedSubscriptionView as FeedSubscriptionPresentation,
} from "@/pages/console/feed-subscription";
import {
  adminCapabilityReason,
  CAPABILITY_KEYS,
} from "@/runtime/capability-gates";
import {
  capabilityModeKind,
  capabilityModePresentation,
} from "@/runtime/mode-presentation";
import type { DisabledReason as DisabledReasonValue } from "@/runtime/permissions";
import { useRuntimeMode } from "@/runtime/runtime-mode-context";
import { useActionReason } from "@/runtime/use-permission";
import { useRetryAfter } from "@/runtime/use-retry-after";

/**
 * Feed Subscription console.
 *
 * CONTRACT TARGET: this Admin API resource does not exist in the current
 * backend. Current Contract renders a disabled area with visible reasons and
 * never a fabricated subscription table.
 *
 * Desired, Observed, Drift and Last Error stay separate groups in every state.
 * A 409 stops the write and requires a manual merge from a refreshed version;
 * nothing is ever overwritten automatically.
 *
 * Flow-control editing is a sibling resource and lives in
 * `flow-control-section.tsx`.
 */

const FEED_QUERY_KEY = ["console", "feed-subscriptions"] as const;

const FEED_HELP =
  "Feed Subscription 是采集配置。Desired 是已接受的目标，Observed 是 Collector 最近确认的状态，Drift 比较两者，Last Error 保留最近失败的原始 code、message、时间与 Request ID。遇到 409 必须刷新后人工合并，不会自动覆盖他人修改。删除只停止后续采集。";

const GROUP_HELP = {
  desired:
    "Desired 是服务端已接受的配置目标与版本。修改需要 Admin API 与 Operator 或 SuperAdmin 角色。",
  observed:
    "Observed 是 Collector 最近应用的状态与版本。它可能落后于 Desired 的更新时间。",
  drift:
    "Drift 分开比较 Desired 与 Observed。存在漂移时以 Observed 为准，刷新后由 Operator 人工处理。",
  lastError:
    "Last Error 保留最近一次应用失败的原始 code、message、时间与 Request ID。message 原样展示。",
} as const;

function desiredTone(view: FeedSubscriptionPresentation): StatusTone {
  if (view.desired.deleted) {
    return "warning";
  }
  return view.desired.enabled ? "good" : "info";
}

function observedTone(state: string): StatusTone {
  switch (state) {
    case "applied":
      return "good";
    case "pending":
      return "warning";
    case "applying":
      return "info";
    case "rejected":
      return "critical";
    default:
      return "unknown";
  }
}

function FeedGroup({
  label,
  help,
  children,
}: {
  label: string;
  help: ReactNode;
  children: ReactNode;
}) {
  return (
    <div
      role="group"
      aria-label={label}
      data-slot="feed-group"
      className="min-w-0 rounded-lg border border-card-border bg-surface-sunken p-3"
    >
      <div className="flex items-center gap-1.5">
        <span className="text-[11px] font-medium uppercase tracking-wide text-faint-foreground">
          {label}
        </span>
        <HelpTrigger label={`${label} 说明`} content={help} side="top" />
      </div>
      <div className="mt-2 min-w-0">{children}</div>
    </div>
  );
}

function MetadataRow({
  label,
  children,
}: {
  label: string;
  children: ReactNode;
}) {
  return (
    <div className="flex flex-wrap items-baseline gap-x-1.5">
      <dt className="text-faint-foreground">{label}</dt>
      <dd className="min-w-0 break-words">{children}</dd>
    </div>
  );
}

function FeedSubscriptionCard({
  view,
  actionReason,
  busy,
  onToggle,
  onDelete,
}: {
  view: FeedSubscriptionPresentation;
  actionReason: DisabledReasonValue | null;
  busy: boolean;
  onToggle: () => void;
  onDelete: () => void;
}) {
  const reasonId = `feed-${view.subscriptionId}-reason`;
  return (
    <Card
      role="group"
      aria-label={`${view.identity.instrumentId} Feed Subscription`}
      data-testid={`feed-subscription-${view.subscriptionId}`}
      className="bg-surface-sunken shadow-none"
    >
      <div className="flex flex-wrap items-start justify-between gap-x-3 gap-y-2 p-4 pb-0">
        <div className="min-w-0">
          <div className="break-all font-mono text-sm font-medium text-foreground">
            {view.identity.instrumentId}
          </div>
          <div className="mt-1 flex flex-wrap gap-x-3 gap-y-1 text-[11px] text-muted-foreground">
            <span>
              Feed <span className="font-mono">{view.identity.feed}</span>
            </span>
            <span>
              Venue <span className="font-mono">{view.identity.venue}</span>
            </span>
            <span>
              Bar Interval{" "}
              <span className="font-mono">{view.identity.interval}</span>
            </span>
          </div>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <Button
            size="sm"
            variant="outline"
            disabled={actionReason !== null || busy}
            aria-describedby={actionReason ? reasonId : undefined}
            onClick={onToggle}
          >
            {view.desired.enabled ? "停用" : "启用"}
          </Button>
          <Button
            size="sm"
            variant="destructive"
            disabled={actionReason !== null || busy}
            aria-describedby={actionReason ? reasonId : undefined}
            onClick={onDelete}
          >
            删除
          </Button>
          <DisabledReason id={reasonId} reason={actionReason} />
        </div>
      </div>

      <div className="grid grid-cols-1 gap-3 p-4 sm:grid-cols-2 xl:grid-cols-4">
        <FeedGroup label="Desired" help={GROUP_HELP.desired}>
          <div className="flex flex-wrap items-center gap-2">
            <StatusBadge tone={desiredTone(view)}>
              {view.desired.label}
            </StatusBadge>
            <span className="font-mono text-xs">v{view.desired.version}</span>
          </div>
          <dl className="mt-2 space-y-1 text-[11px] text-muted-foreground">
            <MetadataRow label="更新于">
              <span className="font-mono">
                {formatUtcTimestamp(view.desired.updatedAt)}
              </span>
            </MetadataRow>
            <MetadataRow label="更新者">
              <span className="font-mono">{view.desired.updatedBy}</span>
            </MetadataRow>
          </dl>
        </FeedGroup>

        <FeedGroup label="Observed" help={GROUP_HELP.observed}>
          <div className="flex flex-wrap items-center gap-2">
            <StatusBadge tone={observedTone(view.observed.state)}>
              {view.observed.label}
            </StatusBadge>
            <span className="font-mono text-xs">v{view.observed.version}</span>
          </div>
          <dl className="mt-2 space-y-1 text-[11px] text-muted-foreground">
            <MetadataRow label="观测于">
              <span className="font-mono">
                {formatUtcTimestamp(view.observed.observedAt)}
              </span>
            </MetadataRow>
            <MetadataRow label="Collector Instance">
              <span className="font-mono">
                {view.observed.collectorInstanceId}
              </span>
            </MetadataRow>
          </dl>
        </FeedGroup>

        <FeedGroup label="Drift" help={GROUP_HELP.drift}>
          <StatusBadge tone={view.drift.inSync ? "good" : "warning"}>
            {view.drift.label}
          </StatusBadge>
          <dl className="mt-2 space-y-1 text-[11px] text-muted-foreground">
            {!view.drift.inSync ? (
              <MetadataRow label="原因">
                <span>{view.drift.reasonLabel}</span>
              </MetadataRow>
            ) : null}
            <MetadataRow label="版本">
              <span className="font-mono">
                Desired v{view.drift.desiredVersion}
              </span>
              <span className="ml-2 font-mono">
                Observed v{view.drift.observedVersion}
              </span>
            </MetadataRow>
            <MetadataRow label="自">
              <span className="font-mono">
                {formatUtcTimestamp(view.drift.since)}
              </span>
            </MetadataRow>
          </dl>
        </FeedGroup>

        <FeedGroup label="Last Error" help={GROUP_HELP.lastError}>
          {view.lastError ? (
            <>
              <StatusBadge tone="critical">{view.lastError.code}</StatusBadge>
              <p
                data-raw-evidence="true"
                className="mt-2 break-words text-[11px] leading-relaxed text-muted-foreground"
              >
                {view.lastError.message}
              </p>
              <dl className="mt-2 space-y-1 text-[11px] text-muted-foreground">
                <MetadataRow label="发生于">
                  <span className="font-mono">
                    {formatUtcTimestamp(view.lastError.at)}
                  </span>
                </MetadataRow>
                <MetadataRow label="Request ID">
                  <span className="font-mono">{view.lastError.requestId}</span>
                </MetadataRow>
              </dl>
            </>
          ) : (
            <p className="text-[11px] text-muted-foreground">无 Last Error</p>
          )}
        </FeedGroup>
      </div>
    </Card>
  );
}

export function FeedSubscriptionView() {
  const { capability, session, role, writesStopped } = useRuntimeMode();
  const queryClient = useQueryClient();
  const retryAfter = useRetryAfter();
  const csrfToken = session?.csrfToken ?? null;

  const readReason = useActionReason("feed.subscription.view");
  const writeReason = useActionReason("feed.subscription.write");

  const modeKind = capabilityModeKind(capability);
  const isAdmin = modeKind === "admin-api";
  const discovering = modeKind === "discovering";
  const capabilityUnknown = modeKind === "capabilities-unknown";

  const feedCapabilityReason = adminCapabilityReason(
    capability,
    CAPABILITY_KEYS.feedSubscription,
  );
  const feedReadReason = readReason ?? feedCapabilityReason;
  const canReadFeed = isAdmin && feedReadReason === null;
  const stoppedWriteReason: DisabledReasonValue | null = writesStopped
    ? {
        code: "requires-login",
        message: "会话已停止写操作，请重新登录",
      }
    : null;
  const feedWriteBlockReason =
    stoppedWriteReason ?? writeReason ?? retryAfter.reason ?? feedCapabilityReason;
  const feedRoleReason = writeReason ?? feedCapabilityReason;

  const subscriptionsQuery = useQuery({
    queryKey: FEED_QUERY_KEY,
    queryFn: ({ signal }) => fetchFeedSubscriptions(signal),
    enabled: canReadFeed,
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
        "删除请求已接受。仅停止后续采集，历史 Bars 不变。以 Observed 确认删除结果。",
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

  const feedMode = capabilityModePresentation(capability, {
    "current-contract": { label: "需要 Admin API", tone: "unknown" },
    "admin-api": { label: "Admin API 契约目标", tone: "info" },
    "capabilities-unknown": { label: "能力未知", tone: "warning" },
  });
  const feedStatusLabel = feedMode.label;
  const feedStatusTone: StatusTone = feedMode.tone;

  const conflictPanel = conflict ? (
    <Card
      data-testid="feed-conflict"
      role="alert"
      className="mb-3 border-status-warning bg-status-warning-bg shadow-none"
    >
      <div className="flex items-start gap-2 p-3">
        <AlertTriangle
          className="mt-0.5 size-4 shrink-0 text-status-warning-fg"
          aria-hidden="true"
        />
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-2">
            <StatusBadge tone="warning">409 版本冲突</StatusBadge>
            <span className="text-xs font-medium text-foreground">
              已停止自动提交
            </span>
          </div>
          <p className="mt-1 text-xs leading-relaxed text-muted-foreground">
            请刷新后人工合并，不会自动覆盖他人修改，也不会自动重试。
          </p>
          <dl className="mt-2 flex flex-wrap gap-x-4 gap-y-1 text-[11px]">
            <MetadataRow label="提交版本">
              <span className="font-mono">
                {conflict.state.attemptedVersion}
              </span>
            </MetadataRow>
            <MetadataRow label="当前版本">
              <span className="font-mono">
                {conflict.state.currentVersion ?? "未知"}
              </span>
            </MetadataRow>
          </dl>
          <Button
            size="sm"
            variant="outline"
            className="mt-2"
            onClick={() => void handleRefreshForMerge()}
          >
            刷新并人工合并
          </Button>
          {mergeInfo ? (
            <p data-testid="feed-merge-info" role="status" className="mt-2 text-xs">
              {mergeInfo}
            </p>
          ) : null}
          {merge ? (
            <div className="mt-2 flex flex-wrap items-center gap-3 rounded-lg bg-card p-2">
              <span className="flex flex-wrap items-center gap-1.5 text-xs">
                当前 Desired
                <StatusBadge
                  tone={merge.subscription.desired.enabled ? "good" : "info"}
                >
                  {merge.subscription.desired.enabled ? "启用" : "停用"}
                </StatusBadge>
                版本
                <span className="font-mono">
                  {merge.subscription.desired.version}
                </span>
              </span>
              <label className="inline-flex items-center gap-1 text-xs">
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
                <span className="text-[11px] text-status-warning-fg">
                  {retryAfter.reason.message}
                </span>
              ) : null}
            </div>
          ) : null}
        </div>
      </div>
    </Card>
  ) : null;

  const feedDependencyCopy = isAdmin
    ? "Admin API 已连接，但当前服务端能力或 Access Role 不允许读取该资源。Desired、Observed、Drift 与 Last Error 只来自真实服务端。"
    : "依赖 Admin API feed_subscription 能力与对应的服务端 Access Role。Current Contract 不伪造订阅表，Desired、Observed、Drift 与 Last Error 必须来自真实服务端。";

  const feedBody = !canReadFeed ? (
    discovering ? (
      <LoadingState label="正在发现运行时能力…" showSkeleton />
    ) : (
      <>
        {capabilityUnknown ? (
          <UnknownState
            cause="failed"
            reason="运行时能力发现失败，无法确认 Feed Subscription 的读写能力。"
          />
        ) : (
          <p className="text-xs leading-relaxed text-muted-foreground">
            {feedDependencyCopy}
          </p>
        )}
        <UnavailableActions
          label="Feed Subscription"
          readReason={feedReadReason}
          writeReason={feedWriteBlockReason}
        />
      </>
    )
  ) : subscriptionsQuery.isPending ? (
    <LoadingState label="正在加载 Feed Subscription…" showSkeleton />
  ) : subscriptionsQuery.isError ? (
    <QueryFailure
      error={subscriptionsQuery.error}
      fallback="无法读取 Feed Subscription"
      onRetry={() => void subscriptionsQuery.refetch()}
    />
  ) : (subscriptionsQuery.data?.items.length ?? 0) === 0 ? (
    <EmptyState
      title="暂无 Feed Subscription"
      description="没有真实订阅记录时保持空白，不生成示例订阅。"
    />
  ) : (
    <div className="flex flex-col gap-3">
      {subscriptionsQuery.data?.items.map((subscription) => (
        <FeedSubscriptionCard
          key={subscription.subscription_id}
          view={describeFeedSubscription(subscription)}
          actionReason={feedWriteBlockReason}
          busy={updateMutation.isPending || deleteMutation.isPending}
          onToggle={() =>
            updateMutation.mutate({
              subscription,
              enabled: !subscription.desired.enabled,
              version: subscription.desired.version,
            })
          }
          onDelete={() => setDeleteTarget(subscription)}
        />
      ))}
    </div>
  );

  return (
    <>
      <Card
        role="region"
        aria-label="Feed Subscription"
        className="mt-5"
      >
        <CardHeader className="p-5 pb-0">
          <div className="min-w-0">
            <CardTitleWithHelp
              title="Feed Subscription"
              helpLabel="Feed Subscription 说明"
              help={FEED_HELP}
            />
            <p className="mt-1 text-xs leading-relaxed text-muted-foreground">
              Desired、Observed、Drift 与 Last Error 分开展示。
            </p>
          </div>
          <div className="flex flex-wrap items-center gap-2">
            <StatusBadge tone={feedStatusTone}>{feedStatusLabel}</StatusBadge>
            {isAdmin && role ? (
              <StatusBadge tone="unknown">Access Role {role}</StatusBadge>
            ) : null}
            {canReadFeed ? (
              <StatusBadge tone={feedRoleReason ? "unknown" : "good"}>
                {feedRoleReason ? "当前只读" : "当前可写"}
              </StatusBadge>
            ) : null}
            {canReadFeed &&
            subscriptionsQuery.isFetching &&
            !subscriptionsQuery.isPending ? (
              <StatusBadge tone="info" live>
                正在刷新
              </StatusBadge>
            ) : null}
          </div>
        </CardHeader>

        <CardContent className="pt-4">
          {conflictPanel}
          {submittedInfo ? (
            <p role="status" className="mb-3 text-xs text-status-good-fg">
              {submittedInfo}
            </p>
          ) : null}
          {feedBody}
          {canReadFeed ? (
            <p className="mt-2 text-[11px] text-faint-foreground">
              {FEED_DELETION_HISTORY_NOTE}
            </p>
          ) : null}
        </CardContent>
      </Card>

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
