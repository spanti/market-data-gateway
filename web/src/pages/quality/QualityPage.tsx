import { HelpTrigger } from "@/components/help-trigger";
import { PageHeader } from "@/components/page-header";
import { StatusBadge } from "@/components/status-badge";
import { BackfillJobsSection } from "@/pages/quality/backfill-jobs-section";
import { CollectorBackfillGapSnapshot } from "@/pages/quality/collector-gap-snapshot";
import { PersistentBackfillGapSection } from "@/pages/quality/persistent-gap-section";
import { capabilityModePresentation } from "@/runtime/mode-presentation";
import { useRuntimeMode } from "@/runtime/runtime-mode-context";

export function QualityPage() {
  const { capability } = useRuntimeMode();
  const mode = capabilityModePresentation(capability, {
    "current-contract": { label: "Current Contract 匿名只读" },
  });

  return (
    <>
      <PageHeader
        title="Quality & Backfill"
        help={
          <HelpTrigger
            label="Quality & Backfill 说明"
            content="Quality & Backfill 同时呈现两类 Gap 来源。Current Contract 只提供 Collector /status 的进程内 Gap 快照，重启后丢失且不累积为客户端历史。持久 Unrecoverable Backfill Gap 与 Backfill Job 需要 Admin API，由服务端持久化。取消 Backfill Job 是协作式的，已写入的 Revision 不会撤回；重试会创建关联的新 Job，不改写旧 Job。高周期补采为补采 1m 加内部 Bar Aggregation，不调用原生高周期接口。"
            side="bottom"
          />
        }
        status={<StatusBadge tone={mode.tone}>{mode.label}</StatusBadge>}
      />

      <section
        aria-label="Unrecoverable Backfill Gap 来源"
        className="mt-5 grid grid-cols-1 gap-4 xl:grid-cols-2"
      >
        <CollectorBackfillGapSnapshot />
        <PersistentBackfillGapSection />
      </section>

      <BackfillJobsSection className="mt-4" />
    </>
  );
}
