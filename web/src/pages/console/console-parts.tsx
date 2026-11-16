import type * as React from "react";
import { DisabledAction } from "@/components/disabled-reason";
import { HelpTrigger } from "@/components/help-trigger";
import { ErrorState } from "@/components/states";
import { CardTitle } from "@/components/ui/card";
import { queryErrorInfo } from "@/lib/errors";
import type { DisabledReason } from "@/runtime/permissions";

/** Query failure with the backend message and Request ID kept inline. */
export function QueryFailure({
  error,
  fallback,
  onRetry,
}: {
  error: unknown;
  fallback: string;
  onRetry: () => void;
}) {
  const info = queryErrorInfo(error, fallback);
  return (
    <ErrorState
      description={
        <span data-raw-evidence="true" className="break-words">
          {info.message}
        </span>
      }
      requestId={info.requestId}
      onRetry={onRetry}
    />
  );
}

export function CardTitleWithHelp({
  title,
  helpLabel,
  help,
}: {
  title: string;
  helpLabel: string;
  help: React.ReactNode;
}) {
  return (
    <div className="flex min-w-0 items-center gap-2">
      <CardTitle>{title}</CardTitle>
      <HelpTrigger label={helpLabel} content={help} side="bottom" />
    </div>
  );
}

export function UnavailableActions({
  label,
  readReason,
  writeReason,
}: {
  label: string;
  readReason: DisabledReason | null;
  writeReason: DisabledReason | null;
}) {
  return (
    <div className="mt-3 flex flex-wrap items-center gap-2">
      <DisabledAction label={`读取 ${label}`} reason={readReason} />
      <DisabledAction label={`修改 ${label}`} reason={writeReason} />
    </div>
  );
}
