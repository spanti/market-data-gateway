import { DisabledAction } from "@/components/disabled-reason";
import { ModulePlaceholder } from "@/components/module-placeholder";
import { PageHeader } from "@/components/page-header";
import { StatusBadge } from "@/components/status-badge";
import { Panel, PanelDescription, PanelTitle } from "@/components/ui/panel";
import { useActionReason } from "@/runtime/use-permission";

export function QualityPage() {
  const backfillReason = useActionReason("backfill.job.create");

  return (
    <>
      <PageHeader
        title="Quality & Backfill"
        description="区分 Unrecoverable Backfill Gap 与 Backfill Job；Current Contract 只显示 Collector 进程内快照。"
        status={<StatusBadge tone="unknown">等待真实质量数据</StatusBadge>}
      />

      <ModulePlaceholder
        title="Unrecoverable Backfill Gap 尚未接入"
        description="Current Contract 下 gap_unrecoverable 是 Collector 当前进程内快照，重启后丢失；前端不会累积轮询结果冒充服务端持久历史。"
      />

      <ModulePlaceholder
        title="Backfill Job 尚未接入"
        description="Job 的 queued / running / succeeded / partially_succeeded / failed / cancelling / cancelled 状态、计数与当前区间将在后续工单读取真实 Admin API。"
      >
        <div className="flex justify-center">
          {backfillReason ? (
            <DisabledAction label="触发 Backfill Job" reason={backfillReason} />
          ) : (
            <p className="text-xs text-faint-foreground">
              权限已具备；该功能将在后续工单接入。
            </p>
          )}
        </div>
      </ModulePlaceholder>

      <Panel className="mt-6 p-5" aria-label="高周期 Backfill 说明">
        <PanelTitle>高周期 Backfill 的真实计划</PanelTitle>
        <PanelDescription>
          除 1m 外，高周期 Backfill 的真实计划是“补采 1m + 内部 Bar
          Aggregation”，不是调用原生高周期接口。
        </PanelDescription>
      </Panel>
    </>
  );
}
