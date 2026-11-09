import {
  SourceStatus,
  type SourceStatusProps,
} from "@/components/monitoring/source-status";
import { PanelDescription, PanelTitle } from "@/components/ui/panel";

export interface SourcePanelProps {
  rows: SourceStatusProps[];
  note: string;
}

/** Side-rail ledger of every source, window and last successful update. */
export function SourcePanel({ rows, note }: SourcePanelProps) {
  return (
    <section
      aria-label="数据源与刷新"
      className="border-t border-border pt-4"
    >
      <div className="flex items-start justify-between gap-3 px-1">
        <div>
          <PanelTitle>数据源与刷新</PanelTitle>
          <PanelDescription>{note}</PanelDescription>
        </div>
      </div>
      <div className="mt-2">
        {rows.map((row) => (
          <SourceStatus key={row.source} {...row} />
        ))}
      </div>
    </section>
  );
}
