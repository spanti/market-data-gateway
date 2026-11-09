import { QueryClientProvider } from "@tanstack/react-query";
import { render } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { createQueryClient } from "@/api/query-client";
import { WsFactoryProvider } from "@/api/ws/ws-factory-provider";
import type { WebSocketFactory } from "@/api/ws/transport";
import { TooltipProvider } from "@/components/ui/tooltip";
import { AppRoutes } from "@/routes";
import { RuntimeModeProvider } from "@/runtime/runtime-mode";

export interface RenderAppOptions {
  initialEntries?: string[];
  /** Test-only WebSocket factory injection; production uses the browser. */
  wsFactory?: WebSocketFactory | null;
}

export function renderApp({
  initialEntries = ["/monitoring"],
  wsFactory = null,
}: RenderAppOptions = {}) {
  const queryClient = createQueryClient();
  return {
    queryClient,
    ...render(
      <QueryClientProvider client={queryClient}>
        <RuntimeModeProvider>
          <WsFactoryProvider factory={wsFactory}>
            <TooltipProvider>
              <MemoryRouter initialEntries={initialEntries}>
                <AppRoutes />
              </MemoryRouter>
            </TooltipProvider>
          </WsFactoryProvider>
        </RuntimeModeProvider>
      </QueryClientProvider>,
    ),
  };
}
