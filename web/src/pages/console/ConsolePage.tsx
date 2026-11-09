import { PageHeader } from "@/components/page-header";
import { StatusBadge } from "@/components/status-badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { FeedSubscriptionView } from "@/pages/console/FeedSubscriptionView";
import { ServiceInstanceView } from "@/pages/console/ServiceInstanceView";
import { WebSocketDebugView } from "@/pages/console/WebSocketDebugView";
import { useRuntimeMode } from "@/runtime/runtime-mode-context";

/**
 * Gateway Console: three clearly distinct views.
 *
 * - Service Instance is real in Current Contract (ops /status + /readyz);
 *   lifecycle writes are Admin API contract targets.
 * - Feed Subscription and flow control are Admin API contract targets,
 *   disabled with visible reasons in Current Contract.
 * - WebSocket Debug follows the resolved WS target: public `/ws` in Current
 *   Contract, admin WS only in Admin API mode, and no connection while the
 *   capability state is unknown.
 */
function consoleModeBadge(
  capability: ReturnType<typeof useRuntimeMode>["capability"],
) {
  if (!capability) {
    return <StatusBadge tone="unknown">正在发现运行时能力…</StatusBadge>;
  }
  switch (capability.mode) {
    case "current-contract":
      return (
        <StatusBadge tone="unknown">
          Current Contract：真实状态 + 公开 /ws；管理写为 Admin API 契约目标
        </StatusBadge>
      );
    case "admin-api":
      return (
        <StatusBadge tone="info">
          Admin API：service-instance 接缝 + 仅 Admin WS（/api/v1/admin/ws）
        </StatusBadge>
      );
    case "capabilities-unknown":
      return (
        <StatusBadge tone="warning">
          能力未知：不连接任何 WebSocket，管理写保持禁用
        </StatusBadge>
      );
  }
}

export function ConsolePage() {
  const { capability } = useRuntimeMode();

  return (
    <>
      <PageHeader
        title="Gateway Console"
        description="Feed Subscription、Service Instance 与 WebSocket Debug 分区操作；Desired、Observed 与 Drift 分开呈现。"
        status={consoleModeBadge(capability)}
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
