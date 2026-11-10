import type { BarsV2Response } from "@/api/endpoints/bars-v2";

/**
 * Cursor guard for the revision-safe v2 JSON history
 * (`GET /api/v2/bars?instrument=…&limit=…&cursor=…`).
 *
 * The server returns an opaque, signed `next_cursor` and guarantees strict
 * progress in the total order `(open_ts, revision, ingest_ts,
 * row_fingerprint)`. The cursor is opaque, so this module cannot verify that
 * the position actually advanced by parsing it; it can only guarantee that
 * the walk never loops or continues on a broken page. A missing, unchanged
 * or already-seen cursor stops the walk with a user-visible error.
 */

export type BarsPageStopReason =
  | "cursor_non_advance"
  | "cursor_protocol_error"
  | "page_cap";

export interface BarsPageStop {
  reason: BarsPageStopReason;
  message: string;
}

export type BarsPageStep =
  | { kind: "complete" }
  | { kind: "continue"; nextCursor: string }
  | { kind: "stop"; stop: BarsPageStop };

export interface BarsPageDecisionInput {
  /** Cursor used for the page that was just returned; null on the first page. */
  requestCursor: string | null;
  page: BarsV2Response;
  /** Cursors already followed in this walk, including `requestCursor`. */
  seenCursors: readonly string[];
  pageCount: number;
  maxPages: number;
}

const CURSOR_GAP_MESSAGE =
  "分页已停止：next_cursor 未前进或重复。为避免无限循环，仅显示已返回的部分结果；请缩小时间范围后重试。";

const PROTOCOL_MESSAGE =
  "分页已停止：响应缺少有效的 page.next_cursor。下方为部分结果。";

export function decideBarsPageStep(
  input: BarsPageDecisionInput,
): BarsPageStep {
  const info = input.page.page;
  if (
    info === undefined ||
    typeof info.has_more !== "boolean" ||
    (info.next_cursor !== null && typeof info.next_cursor !== "string")
  ) {
    return {
      kind: "stop",
      stop: { reason: "cursor_protocol_error", message: PROTOCOL_MESSAGE },
    };
  }

  if (!info.has_more) {
    if (info.next_cursor !== null) {
      return {
        kind: "stop",
        stop: {
          reason: "cursor_protocol_error",
          message:
            "分页已停止：has_more=false 但 next_cursor 非空，违反 revision-safe cursor 契约。下方为部分结果。",
        },
      };
    }
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

  if (info.next_cursor === null || info.next_cursor.length === 0) {
    return {
      kind: "stop",
      stop: { reason: "cursor_protocol_error", message: PROTOCOL_MESSAGE },
    };
  }

  if (info.next_cursor === input.requestCursor) {
    return {
      kind: "stop",
      stop: { reason: "cursor_non_advance", message: CURSOR_GAP_MESSAGE },
    };
  }

  if (input.seenCursors.includes(info.next_cursor)) {
    return {
      kind: "stop",
      stop: { reason: "cursor_non_advance", message: CURSOR_GAP_MESSAGE },
    };
  }

  return { kind: "continue", nextCursor: info.next_cursor };
}
