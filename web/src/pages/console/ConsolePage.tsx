import { PageHeader } from "@/components/page-header";
import { StatusBadge } from "@/components/status-badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { FeedSubscriptionView } from "@/pages/console/FeedSubscriptionView";
import { ServiceInstanceView } from "@/pages/console/ServiceInstanceView";
import { WebSocketDebugView } from "@/pages/console/WebSocketDebugView";

/**
 * Gateway Console: three clearly distinct views.
 *
 * - Service Instance is real in Current Contract (ops /status + /readyz);
 *   lifecycle writes are Admin API contract targets.
 * - Feed Subscription and flow control are Admin API contract targets,
 *   disabled with visible reasons in Current Contract.
 * - WebSocket Debug is fully real against the public `/ws` transport.
 */
export function ConsolePage() {
  return (
    <>
      <PageHeader
        title="Gateway Console"
        description="Feed Subscription、Service Instance 与 WebSocket Debug 分区操作；Desired、Observed 与 Drift 分开呈现。"
        status={
          <StatusBadge tone="unknown">
            Current Contract 使用真实状态与真实 /ws；管理写为 Admin API 契约目标
          </StatusBadge>
        }
      />

      <Tabs defaultValue="feed" className="mt-5">
        <TabsList aria-label="Console 分区">
          <TabsTrigger value="feed">Feed Subscription</TabsTrigger>
          <TabsTrigger value="instance">Service Instance</TabsTrigger>
          <TabsTrigger value="websocket">WebSocket Debug</TabsTrigger>
        </TabsList>

        <TabsContent value="feed">
          <FeedSubscriptionView />
        </TabsContent>

        <TabsContent value="instance">
          <ServiceInstanceView />
        </TabsContent>

        <TabsContent value="websocket">
          <WebSocketDebugView />
        </TabsContent>
      </Tabs>
    </>
  );
}
