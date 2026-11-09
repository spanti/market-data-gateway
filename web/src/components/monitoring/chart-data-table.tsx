import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { alignTrendPoints } from "@/components/monitoring/chart-option";
import type { TrendSeries } from "@/api/monitoring/types";
import { formatNumber, formatUtcDateTime } from "@/lib/format";

export interface ChartDataTableProps {
  series: TrendSeries[];
  unit: string;
  caption: string;
}

/**
 * The accessible alternative and audit view for every chart. It is always
 * rendered inside `<details>`; exact values stay visible without hover.
 */
export function ChartDataTable({
  series,
  unit,
  caption,
}: ChartDataTableProps) {
  const aligned = alignTrendPoints(series);

  return (
    <Table>
      <caption className="sr-only">{caption}</caption>
      <TableHeader>
        <TableRow>
          <TableHead>时间（UTC）</TableHead>
          {series.map((item) => (
            <TableHead key={item.name} className="text-right">
              {item.name}
              {unit ? `（${unit}）` : ""}
            </TableHead>
          ))}
        </TableRow>
      </TableHeader>
      <TableBody>
        {aligned.rows.map((row) => (
          <TableRow key={row.ts}>
            <TableCell className="whitespace-nowrap font-mono text-[11px]">
              {formatUtcDateTime(row.ts)}
            </TableCell>
            {row.values.map((value, index) => (
              <TableCell
                key={`${row.ts}:${series[index]?.name ?? index}`}
                className="text-right tabular-nums"
              >
                {formatNumber(value, 3)}
              </TableCell>
            ))}
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}
