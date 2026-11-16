import { formatUtcDateTime } from "@/lib/format";

/**
 * In-page alerts are derived only from real loaded conditions and real query
 * errors. Nothing here invents times, severities or health records: callers
 * pass stale server entries, failed sources, not-ready service instances and
 * explicit capability gaps.
 */

export type MonitoringAlertTone = "critical" | "warning" | "info";

export interface MonitoringAlert {
  id: string;
  tone: MonitoringAlertTone;
  /** UI-generated title; must not contain `·` or `：`. */
  title: string;
  /** UI-generated detail; must not contain `·` or `：`. */
  detail?: string;
  /** Backend raw error, rendered verbatim as evidence. */
  rawError?: string | null;
  /** Real time evidence such as an event time or last successful update. */
  evidence?: string;
}

export interface StaleFeedAlertInput {
  instrumentId: string;
  feed: string;
  interval: string;
  ageS: number | null;
  thresholdS: number | null;
  lastEventTs: string | null;
}

export interface SourceFailureInput {
  label: string;
  error: string | null;
  lastSuccessAt: number | null;
}

export interface MonitoringAlertInput {
  directOpsEnabled: boolean;
  opsGapReason: string | null;
  staleFeeds: StaleFeedAlertInput[];
  staleVenues: Array<{ venue: string; staleCount: number }>;
  notReadyServices: Array<{
    role: string;
    instanceId?: string | null;
    reason: string | null;
  }>;
  sourceFailures: SourceFailureInput[];
  capabilityGaps: Array<{ title: string; reason: string }>;
}

function ageThreshold(ageS: number | null, thresholdS: number | null): string {
  const age = ageS === null ? "age 未知" : `age ${Math.round(ageS)}s`;
  const threshold =
    thresholdS === null ? "threshold 未知" : `threshold ${Math.round(thresholdS)}s`;
  return `${age}，${threshold}`;
}

function sourceEvidence(lastSuccessAt: number | null): string {
  return lastSuccessAt === null
    ? "最后成功 未成功过"
    : `最后成功 ${formatUtcDateTime(lastSuccessAt)}`;
}

export function deriveMonitoringAlerts(
  input: MonitoringAlertInput,
): MonitoringAlert[] {
  const alerts: MonitoringAlert[] = [];

  for (const feed of input.staleFeeds) {
    alerts.push({
      id: `feed-stale:${feed.feed}:${feed.instrumentId}:${feed.interval}`,
      tone: "critical",
      title: "Feed Freshness 失活",
      detail: `${feed.instrumentId}，${feed.feed}，Bar Interval ${feed.interval}，${ageThreshold(
        feed.ageS,
        feed.thresholdS,
      )}`,
      evidence:
        feed.lastEventTs === null
          ? "最后事件 未暴露"
          : `最后事件 ${formatUtcDateTime(feed.lastEventTs)}`,
    });
  }

  for (const venue of input.staleVenues) {
    alerts.push({
      id: `venue-stale:${venue.venue}`,
      tone: "warning",
      title: `上游 ${venue.venue} 有 Feed 失活`,
      detail: `${venue.staleCount} 项 Feed Freshness 超过服务端阈值`,
    });
  }

  for (const service of input.notReadyServices) {
    alerts.push({
      id: `service-not-ready:${service.instanceId ?? service.role}:${service.role}`,
      tone: "warning",
      title: `Service Instance ${service.role} 未就绪`,
      detail: service.reason ?? "服务端未给出原因",
    });
  }

  for (const source of input.sourceFailures) {
    alerts.push({
      id: `source-failure:${source.label}`,
      tone: "warning",
      title: `${source.label} 加载失败`,
      detail: "当前值保持未知，旧快照不会继续标为健康。",
      rawError: source.error,
      evidence: sourceEvidence(source.lastSuccessAt),
    });
  }

  if (!input.directOpsEnabled && input.opsGapReason) {
    alerts.push({
      id: "capability:ops",
      tone: "warning",
      title: "Feed Freshness 与 Service Instance 能力未知",
      detail: input.opsGapReason,
    });
  }

  for (const gap of input.capabilityGaps) {
    alerts.push({
      id: `capability:${gap.title}`,
      tone: "info",
      title: gap.title,
      detail: gap.reason,
    });
  }

  return alerts;
}
