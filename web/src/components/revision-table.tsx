import { ChevronLeft, ChevronRight } from "lucide-react";
import { useState } from "react";
import { Button } from "@/components/ui/button";
import { Panel, PanelDescription, PanelHeader, PanelTitle } from "@/components/ui/panel";
import {
  Table,
  TableBody,
  TableCaption,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import type { BarRow } from "@/lib/bar-data";
import { cn } from "@/lib/utils";

const REVISION_PAGE_SIZE = 100;

export interface RevisionTableProps {
  rows: readonly BarRow[];
  className?: string;
}

/**
 * Raw Revision table: every server row, no dedupe, exact decimal/timestamp
 * strings, and a raw JSON detail view. The max-revision chart view is derived
 * elsewhere; this component never rewrites a string.
 */
export function RevisionTable({ rows, className }: RevisionTableProps) {
  const [page, setPage] = useState(0);
  const pageCount = Math.max(1, Math.ceil(rows.length / REVISION_PAGE_SIZE));
  const currentPage = Math.min(page, pageCount - 1);
  const start = currentPage * REVISION_PAGE_SIZE;
  const visible = rows.slice(start, start + REVISION_PAGE_SIZE);

  return (
    <Panel
      className={cn("mt-6 p-5", className)}
      aria-label="原始 Revision 表"
    >
      <PanelHeader>
        <div>
          <PanelTitle>原始 Revision 表（未去重）</PanelTitle>
          <PanelDescription>
            表格展示服务端返回的每一条原始 Revision；十进制与时间戳字符串保持原样。
            图表的最大 Revision 视图与 CSV 均不改变这些原始值。
          </PanelDescription>
        </div>
        <span className="text-xs text-faint-foreground">
          {rows.length} 条原始 Revision
        </span>
      </PanelHeader>

      {rows.length === 0 ? (
        <p className="mt-4 text-xs text-muted-foreground">没有可显示的 Revision。</p>
      ) : (
        <>
          <Table className="mt-4">
            <TableCaption>
              第 {currentPage + 1} / {pageCount} 页 · 共 {rows.length} 条 · 未按
              revision 去重 · 展开“原始 JSON”可核对完整信封。
            </TableCaption>
            <TableHeader>
              <TableRow>
                <TableHead>open_ts</TableHead>
                <TableHead className="text-right">revision</TableHead>
                <TableHead className="text-right">open</TableHead>
                <TableHead className="text-right">high</TableHead>
                <TableHead className="text-right">low</TableHead>
                <TableHead className="text-right">close</TableHead>
                <TableHead className="text-right">volume</TableHead>
                <TableHead className="text-right">trade_count</TableHead>
                <TableHead>ingest_ts</TableHead>
                <TableHead>feed</TableHead>
                <TableHead className="text-right">seq</TableHead>
                <TableHead>aggregation_source</TableHead>
                <TableHead>来源</TableHead>
                <TableHead>详情</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {visible.map((row, index) => {
                const bar = row.bar;
                const rowKey = `${bar.instrument_id}-${bar.payload.interval}-${bar.payload.open_ts}-${bar.payload.revision}-${start + index}`;
                return (
                  <TableRow key={rowKey}>
                    <TableCell className="whitespace-nowrap font-mono">
                      {bar.payload.open_ts}
                    </TableCell>
                    <TableCell className="text-right font-mono tabular-nums">
                      {bar.payload.revision}
                    </TableCell>
                    <TableCell className="text-right font-mono tabular-nums">
                      {bar.payload.open}
                    </TableCell>
                    <TableCell className="text-right font-mono tabular-nums">
                      {bar.payload.high}
                    </TableCell>
                    <TableCell className="text-right font-mono tabular-nums">
                      {bar.payload.low}
                    </TableCell>
                    <TableCell className="text-right font-mono tabular-nums">
                      {bar.payload.close}
                    </TableCell>
                    <TableCell className="text-right font-mono tabular-nums">
                      {bar.payload.volume}
                    </TableCell>
                    <TableCell className="text-right font-mono tabular-nums">
                      {bar.payload.trade_count ?? "—"}
                    </TableCell>
                    <TableCell className="whitespace-nowrap font-mono">
                      {bar.ingest_ts}
                    </TableCell>
                    <TableCell className="font-mono">{bar.feed}</TableCell>
                    <TableCell className="text-right font-mono tabular-nums">
                      {bar.seq ?? "—"}
                    </TableCell>
                    <TableCell className="font-mono">
                      {bar.payload.aggregation_source}
                    </TableCell>
                    <TableCell>
                      <span className="text-xs font-medium">
                        {row.source === "ws" ? "WS 实时" : "REST 历史"}
                      </span>
                    </TableCell>
                    <TableCell>
                      <details>
                        <summary className="cursor-pointer text-xs text-muted-foreground">
                          原始 JSON
                        </summary>
                        <pre className="mt-2 max-h-64 min-w-[420px] overflow-auto whitespace-pre-wrap break-all rounded-sm border border-border bg-muted/30 p-2 font-mono text-[11px]">
                          {JSON.stringify(bar, null, 2)}
                        </pre>
                      </details>
                    </TableCell>
                  </TableRow>
                );
              })}
            </TableBody>
          </Table>

          {pageCount > 1 ? (
            <div className="mt-3 flex items-center justify-end gap-2">
              <Button
                size="sm"
                variant="outline"
                onClick={() => setPage(Math.max(0, currentPage - 1))}
                disabled={currentPage === 0}
              >
                <ChevronLeft aria-hidden="true" />
                上一页
              </Button>
              <Button
                size="sm"
                variant="outline"
                onClick={() =>
                  setPage(Math.min(pageCount - 1, currentPage + 1))
                }
                disabled={currentPage >= pageCount - 1}
              >
                下一页
                <ChevronRight aria-hidden="true" />
              </Button>
            </div>
          ) : null}
        </>
      )}
    </Panel>
  );
}
