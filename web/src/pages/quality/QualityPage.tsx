import { PageHeader } from "@/components/page-header";
import { StatusBadge } from "@/components/status-badge";
import { BackfillJobsSection } from "@/pages/quality/backfill-jobs-section";
import { CollectorBackfillGapSnapshot } from "@/pages/quality/collector-gap-snapshot";
import { PersistentBackfillGapSection } from "@/pages/quality/persistent-gap-section";
import { useRuntimeMode } from "@/runtime/runtime-mode-context";

function modeBadge(
  mode: ReturnType<typeof useRuntimeMode>["capability"],
) {
  if (!mode) {
    return <StatusBadge tone="unknown">发现能力中</StatusBadge>;
  }
  switch (mode.mode) {
    case "admin-api":
      return <StatusBadge tone="info">Admin API</StatusBadge>;
    case "current-contract":
      return (
        <StatusBadge tone="unknown">Current Contract · 匿名只读</StatusBadge>
      );
    case "capabilities-unknown":
      return <StatusBadge tone="warning">能力未知</StatusBadge>;
  }
}

export function QualityPage() {
  const { capability } = useRuntimeMode();
  const mode = capability?.mode ?? null;

  const meta =
    mode === "current-contract"
      ? "当前快照来自 Collector /status · 持久 Gap 与 Job 需要 Admin API"
      : mode === "admin-api"
        ? "Admin API 不读取 Collector /status · 持久 Gap 与 Job 来自 Admin API 数据"
        : mode === "capabilities-unknown"
          ? "能力未知 · 不发出 Collector 或 Admin 请求"
          : "正在发现运行时能力…";

  return (
    <>
      <PageHeader
        title="Quality & Backfill"
        description="区分 Collector 进程内 Gap 快照与 Admin API 持久 Gap；Backfill Job 只在 Admin API 模式可用。"
        status={modeBadge(capability)}
        meta={<span className="text-xs text-faint-foreground">{meta}</span>}
      />

      <CollectorBackfillGapSnapshot />
      <PersistentBackfillGapSection />
      <BackfillJobsSection />
    </>
  );
}
