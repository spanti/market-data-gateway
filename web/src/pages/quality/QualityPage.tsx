import { PageHeader } from "@/components/page-header";
import { StatusBadge } from "@/components/status-badge";
import { BackfillJobsSection } from "@/pages/quality/backfill-jobs-section";
import { CollectorGapSnapshot } from "@/pages/quality/collector-gap-snapshot";
import { PersistentGapSection } from "@/pages/quality/persistent-gap-section";
import { useRuntimeMode } from "@/runtime/runtime-mode-context";

export function QualityPage() {
  const { capability } = useRuntimeMode();
  const isAdminMode = capability?.mode === "admin-api";

  return (
    <>
      <PageHeader
        title="Quality & Backfill"
        description="区分 Collector 进程内 Gap 快照与 Admin API 持久 Gap；Backfill Job 只在 Admin API 模式可用。"
        status={
          isAdminMode ? (
            <StatusBadge tone="info">Admin API</StatusBadge>
          ) : (
            <StatusBadge tone="unknown">Current Contract · 匿名只读</StatusBadge>
          )
        }
        meta={
          <span className="text-xs text-faint-foreground">
            当前快照来自 Collector /status · 持久 Gap 与 Job 需要 Admin API
          </span>
        }
      />

      <CollectorGapSnapshot />
      <PersistentGapSection />
      <BackfillJobsSection />
    </>
  );
}
