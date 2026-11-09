import type { BarsResponse } from "@/api/endpoints/bars";
import { parseIsoInstant } from "@/lib/time-range";

/**
 * Cursor guard for the single-instrument JSON Time Cursor
 * (`GET /api/v1/bars?instrument=…&limit=…`).
 *
 * The API contract has a known Revision-heavy defect: `next_start` is the
 * inclusive `open_ts` cursor, so a page boundary inside a revision group can
 * repeat the same page forever. This module never "fixes" that by advancing a
 * timestamp; it stops and reports the gap so the UI can show partial results.
 */

export type BarsPageStopReason =
  | "cursor_non_advance"
  | "revision_group_boundary"
  | "cursor_protocol_error"
  | "page_cap";

export interface BarsPageStop {
  reason: BarsPageStopReason;
  message: string;
}

export type BarsPageStep =
  | { kind: "complete" }
  | { kind: "continue"; nextStart: string }
  | { kind: "stop"; stop: BarsPageStop };

export interface BarsPageDecisionInput {
  /** Inclusive `start` cursor used for the page that was just returned. */
  requestStart: string;
  page: BarsResponse;
  /** Maximum `open_ts` returned across all pages so far, if any. */
  lastReturnedOpenTs: string | null;
  pageCount: number;
  maxPages: number;
}

const CURSOR_GAP_MESSAGE =
  "分页已停止：next_start 未严格前进。这是已知的 Revision-heavy Time Cursor 缺陷，剩余 Revision 无法通过该游标到达；下方为部分结果，请缩小范围。";

export function decideBarsPageStep(
  input: BarsPageDecisionInput,
): BarsPageStep {
  const info = input.page.page;
  if (info === undefined) {
    return {
      kind: "stop",
      stop: {
        reason: "cursor_protocol_error",
        message:
          "分页已停止：响应缺少 page 元数据（instrument + limit 时必须返回）。下方为部分结果。",
      },
    };
  }

  if (!info.has_more) {
    return { kind: "complete" };
  }

  if (input.pageCount >= input.maxPages) {
    return {
      kind: "stop",
      stop: {
        reason: "page_cap",
        message: `已达到 ${input.maxPages} 页安全上限，仅显示部分结果；请缩小时间范围。`,
      },
    };
  }

  if (info.next_start === null) {
    return {
      kind: "stop",
      stop: {
        reason: "cursor_protocol_error",
        message:
          "分页已停止：has_more=true 但 next_start 为空，违反 Time Cursor 契约。下方为部分结果。",
      },
    };
  }

  if (input.page.bars.length === 0) {
    return {
      kind: "stop",
      stop: {
        reason: "cursor_protocol_error",
        message:
          "分页已停止：has_more=true 但本页没有返回任何行，无法继续。下方为部分结果。",
      },
    };
  }

  const next = parseIsoInstant(info.next_start);
  if (next === null) {
    return {
      kind: "stop",
      stop: {
        reason: "cursor_protocol_error",
        message:
          "分页已停止：next_start 不是可解析的 ISO-8601 时间。下方为部分结果。",
      },
    };
  }

  const requestStart = parseIsoInstant(input.requestStart);
  if (requestStart === null) {
    return {
      kind: "stop",
      stop: {
        reason: "cursor_protocol_error",
        message: "分页已停止：请求 start 不是可解析的 ISO-8601 时间。",
      },
    };
  }

  if (next.getTime() <= requestStart.getTime()) {
    return {
      kind: "stop",
      stop: { reason: "cursor_non_advance", message: CURSOR_GAP_MESSAGE },
    };
  }

  if (input.lastReturnedOpenTs !== null) {
    const lastReturned = parseIsoInstant(input.lastReturnedOpenTs);
    if (lastReturned !== null && next.getTime() <= lastReturned.getTime()) {
      return {
        kind: "stop",
        stop: {
          reason: "revision_group_boundary",
          message: CURSOR_GAP_MESSAGE,
        },
      };
    }
  }

  return { kind: "continue", nextStart: info.next_start };
}
