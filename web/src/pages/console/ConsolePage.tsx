import { DisabledAction } from "@/components/disabled-reason";
import { ModulePlaceholder } from "@/components/module-placeholder";
import { PageHeader } from "@/components/page-header";
import { StatusBadge } from "@/components/status-badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { useActionReason } from "@/runtime/use-permission";

function ActionAvailability({
  action,
  label,
}: {
  action: Parameters<typeof useActionReason>[0];
  label: string;
}) {
  const reason = useActionReason(action);
  if (!reason) {
    return (
      <p className="text-xs text-faint-foreground">
        权限已具备；该功能将在后续工单接入。
      </p>
    );
  }
  return <DisabledAction label={label} reason={reason} />;
}

export function ConsolePage() {
  return (
    <>
      <PageHeader
        title="Gateway Console"
        description="Feed Subscription、Service Instance 与 WebSocket Debug 分区操作；Desired、Observed 与 Drift 分开呈现。"
        status={<StatusBadge tone="unknown">等待真实控制面数据</StatusBadge>}
      />

      <Tabs defaultValue="feed" className="mt-5">
        <TabsList aria-label="Console 分区">
          <TabsTrigger value="feed">Feed Subscription</TabsTrigger>
          <TabsTrigger value="instance">Service Instance</TabsTrigger>
          <TabsTrigger value="websocket">WebSocket Debug</TabsTrigger>
        </TabsList>

        <TabsContent value="feed">
          <ModulePlaceholder
            title="Feed Subscription 尚未接入"
            description="Desired、Observed、Drift、Last Error 与 409 人工合并流程将在后续工单读取真实 Admin API；Current Contract 不伪造订阅表。"
          >
            <div className="flex justify-center">
              <ActionAvailability
                action="feed.subscription.write"
                label="修改 Feed Subscription"
              />
            </div>
          </ModulePlaceholder>
        </TabsContent>

        <TabsContent value="instance">
          <ModulePlaceholder
            title="Service Instance 生命周期尚未接入"
            description="只展示 collector / api / replay 三个真实 Service Role；Start、Graceful Stop、Reload 需要确认、操作原因与目标实例名。"
          >
            <div className="flex justify-center">
              <ActionAvailability
                action="service.lifecycle.stop"
                label="Graceful Stop / Reload"
              />
            </div>
          </ModulePlaceholder>
        </TabsContent>

        <TabsContent value="websocket">
          <ModulePlaceholder
            title="WebSocket 调试尚未接入"
            description="受控 Topic 表单、subscribe / unsubscribe / ping 白名单、lag_notice 与 10,000 条虚拟列表将在后续工单实现；不会有订阅 ACK 的虚假承诺。"
          >
            <div className="flex flex-wrap justify-center gap-3">
              <ActionAvailability
                action="ws.session.subscribe"
                label="受控 Topic 订阅"
              />
              <ActionAvailability
                action="ws.debug.freeform"
                label="自由 JSON 调试"
              />
            </div>
          </ModulePlaceholder>
        </TabsContent>
      </Tabs>
    </>
  );
}
