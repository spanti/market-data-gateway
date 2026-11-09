import {
  ChartCandlestick,
  CircleGauge,
  DatabaseZap,
  ShieldCheck,
  Terminal,
} from "lucide-react";
import { Link, NavLink } from "react-router-dom";
import { ThemeToggle } from "@/components/theme-toggle";
import { cn } from "@/lib/utils";

const NAV_ITEMS = [
  { to: "/monitoring", label: "监控", icon: CircleGauge },
  { to: "/bars", label: "Bar 图与导出", icon: ChartCandlestick },
  { to: "/console", label: "Gateway Console", icon: Terminal },
  { to: "/quality", label: "Quality & Backfill", icon: DatabaseZap },
  { to: "/security", label: "Security & Audit", icon: ShieldCheck },
] as const;

export function TopNav() {
  return (
    <header className="border-b border-border bg-card">
      <div className="mx-auto flex min-h-14 max-w-[1600px] flex-wrap items-center gap-5 px-5 py-2 lg:px-8">
        <Link
          to="/monitoring"
          className="flex items-center gap-2 rounded-md"
          aria-label="Market Data Gateway Operator Console 首页"
        >
          <CircleGauge
            className="size-5 text-accent-strong"
            aria-hidden="true"
          />
          <strong className="text-sm">Market Data Gateway</strong>
          <span className="text-xs text-faint-foreground">/ Operator Console</span>
        </Link>

        <nav
          aria-label="一级模块导航"
          className="flex flex-wrap items-center gap-1"
        >
          {NAV_ITEMS.map((item) => {
            const Icon = item.icon;
            return (
              <NavLink
                key={item.to}
                to={item.to}
                className={({ isActive }) =>
                  cn(
                    "inline-flex h-8 items-center gap-2 rounded-md px-3 text-xs font-medium transition-colors",
                    isActive
                      ? "bg-accent-soft font-semibold text-accent-strong"
                      : "text-faint-foreground hover:bg-muted hover:text-foreground",
                  )
                }
              >
                <Icon className="size-4" aria-hidden="true" />
                {item.label}
              </NavLink>
            );
          })}
        </nav>

        <div className="ml-auto flex items-center gap-2">
          <ThemeToggle />
        </div>
      </div>
    </header>
  );
}
