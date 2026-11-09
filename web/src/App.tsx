import { QueryClientProvider } from "@tanstack/react-query";
import { BrowserRouter } from "react-router-dom";
import { queryClient } from "@/api/query-client";
import { TooltipProvider } from "@/components/ui/tooltip";
import { AppRoutes } from "@/routes";
import { RuntimeModeProvider } from "@/runtime/runtime-mode";

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <RuntimeModeProvider>
        <TooltipProvider>
          <BrowserRouter>
            <AppRoutes />
          </BrowserRouter>
        </TooltipProvider>
      </RuntimeModeProvider>
    </QueryClientProvider>
  );
}
