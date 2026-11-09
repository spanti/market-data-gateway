import { useId, useState } from "react";
import type * as React from "react";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";

export interface ConfirmDialogInput {
  reason: string;
  confirmInstanceName?: string;
}

export interface ConfirmDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  title: string;
  description?: React.ReactNode;
  confirmLabel?: string;
  submitting?: boolean;
  /** High-risk actions require an operation reason. */
  requireReason?: boolean;
  /** Graceful Stop / Reload require typing the target instance name. */
  requireInstanceName?: string;
  onConfirm: (input: ConfirmDialogInput) => void;
}

/**
 * Confirmation skeleton for high-risk actions. It enforces a non-empty
 * reason and, when required, an exact instance name; submits are disabled
 * while submitting and are never auto-replayed.
 */
export function ConfirmDialog({
  open,
  onOpenChange,
  title,
  description,
  confirmLabel = "确认执行",
  submitting = false,
  requireReason = true,
  requireInstanceName,
  onConfirm,
}: ConfirmDialogProps) {
  const reasonId = useId();
  const instanceId = useId();
  const [reason, setReason] = useState("");
  const [instanceName, setInstanceName] = useState("");

  const instanceMatches =
    requireInstanceName === undefined || instanceName === requireInstanceName;
  const reasonValid = !requireReason || reason.trim().length > 0;
  const canSubmit = !submitting && instanceMatches && reasonValid;

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>{title}</DialogTitle>
          {description ? (
            <DialogDescription>{description}</DialogDescription>
          ) : null}
        </DialogHeader>

        <div className="mt-4 flex flex-col gap-3">
          {requireReason ? (
            <div className="flex flex-col gap-1">
              <Label htmlFor={reasonId}>操作原因</Label>
              <Textarea
                id={reasonId}
                value={reason}
                onChange={(event) => setReason(event.target.value)}
                placeholder="说明本次操作的原因（必填）"
                disabled={submitting}
              />
            </div>
          ) : null}

          {requireInstanceName !== undefined ? (
            <div className="flex flex-col gap-1">
              <Label htmlFor={instanceId}>
                输入目标实例名 <span className="font-mono">{requireInstanceName}</span>
              </Label>
              <Input
                id={instanceId}
                value={instanceName}
                onChange={(event) => setInstanceName(event.target.value)}
                placeholder={requireInstanceName}
                autoComplete="off"
                spellCheck={false}
                disabled={submitting}
                aria-invalid={!instanceMatches}
              />
              {!instanceMatches ? (
                <p className="text-[11px] text-status-critical">
                  实例名不匹配，无法提交。
                </p>
              ) : null}
            </div>
          ) : null}
        </div>

        <DialogFooter>
          <Button
            variant="outline"
            onClick={() => onOpenChange(false)}
            disabled={submitting}
          >
            取消
          </Button>
          <Button
            variant="destructive"
            disabled={!canSubmit}
            onClick={() =>
              onConfirm({
                reason: reason.trim(),
                ...(requireInstanceName !== undefined
                  ? { confirmInstanceName: instanceName }
                  : {}),
              })
            }
          >
            {submitting ? "提交中…" : confirmLabel}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
