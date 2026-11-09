import { zodResolver } from "@hookform/resolvers/zod";
import { useEffect, useId, useMemo } from "react";
import type * as React from "react";
import { useForm } from "react-hook-form";
import { z } from "zod";
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
import { OPERATION_REASON_MAX_LENGTH } from "@/lib/validation";

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

interface ConfirmationFormValues {
  reason: string;
  confirmInstanceName: string;
}

function buildConfirmationSchema(
  requireReason: boolean,
  requireInstanceName: string | undefined,
) {
  return z
    .object({
      reason: z.string(),
      confirmInstanceName: z.string(),
    })
    .superRefine((values, ctx) => {
      const reason = values.reason.trim();
      if (requireReason) {
        if (reason.length === 0) {
          ctx.addIssue({
            code: z.ZodIssueCode.custom,
            path: ["reason"],
            message: "操作原因不能为空",
          });
        } else if (reason.length > OPERATION_REASON_MAX_LENGTH) {
          ctx.addIssue({
            code: z.ZodIssueCode.custom,
            path: ["reason"],
            message: `操作原因不能超过 ${OPERATION_REASON_MAX_LENGTH} 字符`,
          });
        }
      }
      if (
        requireInstanceName !== undefined &&
        values.confirmInstanceName.trim() !== requireInstanceName
      ) {
        ctx.addIssue({
          code: z.ZodIssueCode.custom,
          path: ["confirmInstanceName"],
          message: "实例名不匹配，无法提交。",
        });
      }
    });
}

/**
 * Confirmation dialog for high-risk actions, validated by React Hook Form +
 * Zod. It enforces a non-empty bounded reason and, when required, an exact
 * instance name; submits are disabled while submitting and never auto-replayed.
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
  const reasonErrorId = useId();
  const instanceErrorId = useId();
  const schema = useMemo(
    () => buildConfirmationSchema(requireReason, requireInstanceName),
    [requireInstanceName, requireReason],
  );
  const {
    register,
    handleSubmit,
    reset,
    formState: { errors, isValid },
  } = useForm<ConfirmationFormValues>({
    resolver: zodResolver(schema),
    mode: "onChange",
    defaultValues: { reason: "", confirmInstanceName: "" },
  });

  useEffect(() => {
    if (open) {
      reset({ reason: "", confirmInstanceName: "" });
    }
  }, [open, reset]);

  const hasRules = requireReason || requireInstanceName !== undefined;
  const canSubmit = !submitting && (!hasRules || isValid);

  const submit = handleSubmit((values) => {
    onConfirm({
      reason: values.reason.trim(),
      ...(requireInstanceName !== undefined
        ? { confirmInstanceName: values.confirmInstanceName }
        : {}),
    });
  });

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>{title}</DialogTitle>
          {description ? (
            <DialogDescription>{description}</DialogDescription>
          ) : null}
        </DialogHeader>

        <form onSubmit={submit} noValidate>
          <div className="mt-4 flex flex-col gap-3">
            {requireReason ? (
              <div className="flex flex-col gap-1">
                <Label htmlFor={reasonId}>操作原因</Label>
                <Textarea
                  id={reasonId}
                  placeholder="说明本次操作的原因（必填）"
                  disabled={submitting}
                  aria-invalid={Boolean(errors.reason)}
                  aria-describedby={
                    errors.reason ? reasonErrorId : undefined
                  }
                  {...register("reason")}
                />
                {errors.reason?.message ? (
                  <p
                    id={reasonErrorId}
                    role="alert"
                    className="text-[11px] text-status-critical"
                  >
                    {errors.reason.message}
                  </p>
                ) : null}
              </div>
            ) : null}

            {requireInstanceName !== undefined ? (
              <div className="flex flex-col gap-1">
                <Label htmlFor={instanceId}>
                  输入目标实例名{" "}
                  <span className="font-mono">{requireInstanceName}</span>
                </Label>
                <Input
                  id={instanceId}
                  placeholder={requireInstanceName}
                  autoComplete="off"
                  spellCheck={false}
                  disabled={submitting}
                  aria-invalid={Boolean(errors.confirmInstanceName)}
                  aria-describedby={
                    errors.confirmInstanceName
                      ? instanceErrorId
                      : undefined
                  }
                  {...register("confirmInstanceName")}
                />
                {errors.confirmInstanceName?.message ? (
                  <p
                    id={instanceErrorId}
                    role="alert"
                    className="text-[11px] text-status-critical"
                  >
                    {errors.confirmInstanceName.message}
                  </p>
                ) : null}
              </div>
            ) : null}
          </div>

          <DialogFooter>
            <Button
              type="button"
              variant="outline"
              onClick={() => onOpenChange(false)}
              disabled={submitting}
            >
              取消
            </Button>
            <Button type="submit" variant="destructive" disabled={!canSubmit}>
              {submitting ? "提交中…" : confirmLabel}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
