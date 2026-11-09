import { Navigate, Route, Routes } from "react-router-dom";
import { AppShell } from "@/components/app-shell";
import { BarsPage } from "@/pages/bars/BarsPage";
import { ConsolePage } from "@/pages/console/ConsolePage";
import { MonitoringPage } from "@/pages/monitoring/MonitoringPage";
import { NotFoundPage } from "@/pages/not-found/NotFoundPage";
import { QualityPage } from "@/pages/quality/QualityPage";
import { SecurityPage } from "@/pages/security/SecurityPage";

export function AppRoutes() {
  return (
    <Routes>
      <Route element={<AppShell />}>
        <Route index element={<Navigate to="/monitoring" replace />} />
        <Route path="/monitoring" element={<MonitoringPage />} />
        <Route path="/bars" element={<BarsPage />} />
        <Route path="/console" element={<ConsolePage />} />
        <Route path="/quality" element={<QualityPage />} />
        <Route path="/security" element={<SecurityPage />} />
        <Route path="*" element={<NotFoundPage />} />
      </Route>
    </Routes>
  );
}
