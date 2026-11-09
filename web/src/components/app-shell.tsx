import { Outlet } from "react-router-dom";
import { GlobalHttpNotice } from "@/components/global-http-notice";
import { ModeBanner } from "@/components/mode-banner";
import { TopNav } from "@/components/top-nav";

export function AppShell() {
  return (
    <div className="min-h-screen bg-background">
      <a
        href="#main-content"
        className="sr-only focus:not-sr-only focus:absolute focus:left-4 focus:top-4 focus:z-50 focus:rounded-md focus:bg-card focus:px-3 focus:py-2 focus:text-sm"
      >
        跳到主要内容
      </a>
      <TopNav />
      <ModeBanner />
      <GlobalHttpNotice />
      <main
        id="main-content"
        className="mx-auto w-full max-w-[1600px] px-5 py-6 lg:px-8"
      >
        <Outlet />
      </main>
      <footer className="mx-auto w-full max-w-[1600px] px-5 pb-6 text-[11px] text-faint-foreground lg:px-8">
        Current Contract 为无认证只读服务，仅限可信内网部署；Admin API
        模式下身份、权限、CSRF 与 Operator Audit Event 均由后端执行。
      </footer>
    </div>
  );
}
