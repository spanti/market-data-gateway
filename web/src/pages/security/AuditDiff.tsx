import { ShieldCheck } from "lucide-react";
import { StatusBadge } from "@/components/status-badge";

export interface AuditDiffProps {
  before: unknown;
  after: unknown;
  redactedPaths: readonly string[];
}

function isNullSide(value: unknown): value is null | undefined {
  return value === null || value === undefined;
}

/**
 * Text-only rendering. Values are never injected as HTML and `[REDACTED]` is
 * shown exactly as the server returned it; the UI never reconstructs a hidden
 * value. Long values live in a focusable scroll container.
 */
function formatValue(value: unknown): string {
  if (typeof value === "string") {
    return value;
  }
  try {
    const serialized = JSON.stringify(value, null, 2);
    return serialized === undefined ? String(value) : serialized;
  } catch {
    return String(value);
  }
}

function DiffSide({
  label,
  testId,
  value,
  emptyCopy,
}: {
  label: string;
  testId: string;
  value: unknown;
  emptyCopy: string;
}) {
  return (
    <section aria-label={label} className="min-w-0">
      <h4 className="text-xs font-medium text-muted-foreground">{label}</h4>
      <pre
        data-testid={testId}
        tabIndex={0}
        className="mt-1 max-h-64 overflow-auto whitespace-pre-wrap break-words border border-border bg-surface-sunken p-2 font-mono text-[11px] leading-relaxed text-foreground"
      >
        {isNullSide(value) ? emptyCopy : formatValue(value)}
      </pre>
    </section>
  );
}

export function AuditDiff({ before, after, redactedPaths }: AuditDiffProps) {
  const created = isNullSide(before) && !isNullSide(after);
  const deleted = !isNullSide(before) && isNullSide(after);
  const label = created ? "创建" : deleted ? "删除" : "变更";
  const tone = created ? "good" : deleted ? "warning" : "info";

  return (
    <div data-slot="audit-diff" className="mt-3">
      <div className="flex flex-wrap items-center gap-2">
        <StatusBadge tone={tone}>{label}</StatusBadge>
        {redactedPaths.length > 0 ? (
          <span className="inline-flex flex-wrap items-center gap-1 text-[11px] text-muted-foreground">
            <ShieldCheck className="size-3.5" aria-hidden="true" />
            脱敏字段（值固定为 [REDACTED]，前端不重建）：
            {redactedPaths.map((path) => (
              <code
                key={path}
                className="rounded-sm border border-border bg-card px-1 font-mono"
              >
                {path}
              </code>
            ))}
          </span>
        ) : null}
      </div>

      <div className="mt-2 grid grid-cols-1 gap-3 lg:grid-cols-2">
        <DiffSide
          label="变更前"
          testId="audit-diff-before"
          value={before}
          emptyCopy="创建前不存在"
        />
        <DiffSide
          label="变更后"
          testId="audit-diff-after"
          value={after}
          emptyCopy="删除后不存在"
        />
      </div>
    </div>
  );
}
