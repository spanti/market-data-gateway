import { QueryClientProvider } from "@tanstack/react-query";
import { render } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { createQueryClient } from "@/api/query-client";
import { TooltipProvider } from "@/components/ui/tooltip";
import { AppRoutes } from "@/routes";
import { RuntimeModeProvider } from "@/runtime/runtime-mode";

export interface RenderAppOptions {
  initialEntries?: string[];
}

export function renderApp({ initialEntries = ["/monitoring"] }: RenderAppOptions = {}) {
  const queryClient = createQueryClient();
  return {
    queryClient,
    ...render(
      <QueryClientProvider client={queryClient}>
        <RuntimeModeProvider>
          <TooltipProvider>
            <MemoryRouter initialEntries={initialEntries}>
              <AppRoutes />
            </MemoryRouter>
          </TooltipProvider>
        </RuntimeModeProvider>
      </QueryClientProvider>,
    ),
  };
}
