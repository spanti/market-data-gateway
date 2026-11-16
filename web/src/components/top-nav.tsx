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

/**
 * Variant D top shell: brand, a pill navigation group and the theme control.
 * Layering comes from the canvas and surface tokens, not a full-width rule.
 */
export function TopNav() {
  return (
    <header className="bg-canvas pt-3">
      <div className="mx-auto flex max-w-[1280px] flex-wrap items-center gap-x-4 gap-y-3 px-5 py-1 lg:px-8">
        <Link
          to="/monitoring"
          className="flex min-w-0 items-center gap-2 rounded-md"
          aria-label="Market Data Gateway Operator Console 首页"
        >
          <span className="flex size-9 shrink-0 items-center justify-center rounded-lg bg-accent-soft text-accent-strong">
            <CircleGauge className="size-5" aria-hidden="true" />
          </span>
          <span className="min-w-0 leading-tight">
            <strong className="block truncate text-sm">Market Data Gateway</strong>
            <span className="block text-[11px] text-faint-foreground">
              Operator Console
            </span>
          </span>
        </Link>

        <nav
          aria-label="一级模块导航"
          className="flex flex-wrap items-center gap-0.5 rounded-full border border-card-border bg-card p-1 shadow-card"
        >
          {NAV_ITEMS.map((item) => {
            const Icon = item.icon;
            return (
              <NavLink
                key={item.to}
                to={item.to}
                className={({ isActive }) =>
                  cn(
                    "inline-flex h-7 items-center gap-1.5 rounded-full px-2.5 text-xs font-medium transition-colors",
                    isActive
                      ? "bg-accent-soft font-semibold text-accent-strong"
                      : "text-muted-foreground hover:bg-muted hover:text-foreground",
                  )
                }
              >
                <Icon className="size-3.5 shrink-0" aria-hidden="true" />
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
