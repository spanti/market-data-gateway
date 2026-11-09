import { ModulePlaceholder } from "@/components/module-placeholder";
import { PageHeader } from "@/components/page-header";
import { StatusBadge } from "@/components/status-badge";
import { FilterRow } from "@/components/toolbar";

export function MonitoringPage() {
  return (
    <>
      <PageHeader
        title="监控分析工作台"
        description="统一窗口、可核对指标、没有隐藏的状态推断。"
        status={<StatusBadge tone="unknown">等待真实监控数据</StatusBadge>}
        meta={
          <span className="text-xs text-faint-foreground">
            数据源：/status · Prometheus（15 秒刷新，后续工单接入）
          </span>
        }
      />

      <FilterRow note="所有图表、KPI 和表格将使用同一时间窗口；数据接入见后续工单。">
        <span className="text-xs font-medium text-muted-foreground">
          统一时间范围
        </span>
        <span className="rounded-md border border-border bg-card px-2.5 py-1 text-xs">
          最近 24 小时
        </span>
        <span className="text-xs text-faint-foreground">
          Service /status 默认 5 秒轮询
        </span>
      </FilterRow>

      <ModulePlaceholder
        title="监控数据尚未接入"
        description="KPI、QPS、HTTP 延迟、Service Instance 与 Feed Freshness 将读取真实 /status 与 Prometheus 数据；本页不包含模拟指标。"
      />
    </>
  );
}
