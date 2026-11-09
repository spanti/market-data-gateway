import { AlertTriangle, CheckCircle2, CircleHelp, Info } from "lucide-react";
import type * as React from "react";
import { EmptyState } from "@/components/states";
import type { StatusTone } from "@/components/status-badge";
import { cn } from "@/lib/utils";

export interface TimelineEvent {
  id: string;
  /** ISO-8601 timestamp; rendered with `<time dateTime>`. */
  at: string;
  tone: StatusTone;
  /** Spoken severity; always present even when the icon is the visual cue. */
  severityLabel: string;
  title: string;
  detail?: React.ReactNode;
}

const TONE_CLASS: Record<StatusTone, string> = {
  good: "text-status-good",
  warning: "text-status-warning",
  serious: "text-status-serious",
  critical: "text-status-critical",
  unknown: "text-faint-foreground",
  info: "text-faint-foreground",
};

function ToneIcon({ tone }: { tone: StatusTone }) {
  const Icon =
    tone === "good"
      ? CheckCircle2
      : tone === "info"
        ? Info
        : tone === "unknown"
          ? CircleHelp
          : AlertTriangle;
  return <Icon className="size-4 shrink-0" aria-hidden="true" />;
}

function formatTime(value: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return date.toISOString().slice(11, 19);
}

export interface EventTimelineProps {
  events: TimelineEvent[];
  title?: string;
  emptyMessage?: string;
  className?: string;
}

/**
 * Newest-first event timeline. Severity is icon + visually hidden text, never
 * color alone; timestamps are machine-readable `<time>` elements.
 */
export function EventTimeline({
  events,
  title = "事件时间线",
  emptyMessage = "暂无事件",
  className,
}: EventTimelineProps) {
  return (
    <section data-slot="event-timeline" aria-label={title} className={className}>
      {events.length === 0 ? (
        <EmptyState title={emptyMessage} />
      ) : (
        <ol className="mt-2">
          {events.map((event) => (
            <li
              key={event.id}
              className="grid grid-cols-[64px_18px_1fr] gap-2 pb-5 last:pb-0"
            >
              <time
                dateTime={event.at}
                className="text-[11px] tabular-nums text-faint-foreground"
              >
                {formatTime(event.at)}
              </time>
              <span className="relative flex justify-center pt-0.5">
                <span
                  className="absolute bottom-[-4px] top-5 w-px bg-border"
                  aria-hidden="true"
                />
                <span className={cn("relative z-10 bg-card", TONE_CLASS[event.tone])}>
                  <ToneIcon tone={event.tone} />
                  <span className="sr-only">{event.severityLabel}</span>
                </span>
              </span>
              <div>
                <div className="text-xs font-semibold text-foreground">
                  {event.title}
                </div>
                {event.detail ? (
                  <div className="mt-0.5 text-xs leading-relaxed text-muted-foreground">
                    {event.detail}
                  </div>
                ) : null}
              </div>
            </li>
          ))}
        </ol>
      )}
    </section>
  );
}
