import {
  SourceStatus,
  type SourceStatusProps,
} from "@/components/monitoring/source-status";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { cn } from "@/lib/utils";

export interface SourcePanelProps {
  rows: SourceStatusProps[];
  note: string;
  className?: string;
}

/** Card ledger of every source, its window and its last successful update. */
export function SourcePanel({ rows, note, className }: SourcePanelProps) {
  return (
    <Card
      data-slot="source-panel"
      role="region"
      aria-label="数据源与刷新"
      className={cn("min-w-0 p-5", className)}
    >
      <CardHeader className="p-0">
        <CardTitle>数据源与刷新</CardTitle>
      </CardHeader>
      <CardContent className="p-0 pt-2">
        <p className="text-[11px] leading-relaxed text-muted-foreground">
          {note}
        </p>
        <div className="mt-3 space-y-2">
          {rows.map((row) => (
            <SourceStatus key={row.source} {...row} />
          ))}
        </div>
      </CardContent>
    </Card>
  );
}
