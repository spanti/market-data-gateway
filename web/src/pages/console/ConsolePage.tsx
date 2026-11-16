import { HelpTrigger } from "@/components/help-trigger";
import { PageHeader } from "@/components/page-header";
import { StatusBadge } from "@/components/status-badge";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { FeedSubscriptionView } from "@/pages/console/FeedSubscriptionView";
import { FlowControlSection } from "@/pages/console/flow-control-section";
import { ServiceInstanceView } from "@/pages/console/ServiceInstanceView";
import { WebSocketDebugView } from "@/pages/console/WebSocketDebugView";
import { capabilityModePresentation } from "@/runtime/mode-presentation";
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
 *
 * Feed Subscription changes collector acquisition. WebSocket Session
 * Subscription only filters the current debug session and never changes the
 * acquisition range.
 */

export function ConsolePage() {
  const { capability } = useRuntimeMode();
  const mode = capabilityModePresentation(capability);

  return (
    <>
      <PageHeader
        title="Gateway Console"
        help={
          <HelpTrigger
            label="Gateway Console 说明"
            content="Feed Subscription 改变采集配置，要求 Admin API 与相应 Access Role。WebSocket Session Subscription 只筛选当前调试会话的 Topic，不改变实际采集范围。管理写始终由服务端授权，界面禁用不替代服务端拒绝。"
            side="bottom"
          />
        }
        status={
          <>
            <StatusBadge tone={mode.tone}>{mode.label}</StatusBadge>
            <span className="max-w-2xl text-[11px] leading-relaxed text-muted-foreground">
              {mode.detail}
            </span>
          </>
        }
      />

      <Tabs defaultValue="feed" className="mt-5">
        <TabsList
          aria-label="Console 分区"
          className="rounded-full border border-card-border bg-card p-1 shadow-card"
        >
          <TabsTrigger
            value="feed"
            className="rounded-full px-3 data-[state=active]:bg-accent-soft data-[state=active]:text-accent-strong"
          >
            Feed Subscription
          </TabsTrigger>
          <TabsTrigger
            value="instance"
            className="rounded-full px-3 data-[state=active]:bg-accent-soft data-[state=active]:text-accent-strong"
          >
            Service Instance
          </TabsTrigger>
          <TabsTrigger
            value="websocket"
            className="rounded-full px-3 data-[state=active]:bg-accent-soft data-[state=active]:text-accent-strong"
          >
            WebSocket Debug
          </TabsTrigger>
        </TabsList>

        <TabsContent value="feed">
          <FeedSubscriptionView />
          <FlowControlSection />
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
