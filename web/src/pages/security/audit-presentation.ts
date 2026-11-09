import type { AuditResult } from "@/api/endpoints/admin";
import type { StatusTone } from "@/components/status-badge";

export const AUDIT_RESULT_TONE: Record<AuditResult, StatusTone> = {
  requested: "info",
  succeeded: "good",
  failed: "critical",
  denied: "warning",
};

export const AUDIT_RESULT_LABEL: Record<AuditResult, string> = {
  requested: "requested · 已请求",
  succeeded: "succeeded · 成功",
  failed: "failed · 失败",
  denied: "denied · 拒绝",
};

export function formatAuditTimestamp(value: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return `${date.toISOString().replace("T", " ").slice(0, 23)} UTC`;
}
