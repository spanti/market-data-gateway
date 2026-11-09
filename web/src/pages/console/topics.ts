import {
  BAR_INTERVALS,
  isBarInterval,
  type BarInterval,
} from "@/lib/time-range";

/**
 * WebSocket Debug topic grammar and free-form whitelist.
 *
 * The Streaming Interface treats a subscription as a prefix and delivers a
 * topic when it starts with the subscribed pattern (api-contract.md §5.4).
 * Only live Bar topics are offered by the Operator Console; replay topics are
 * out of scope for the browser console (admin-api-contract.md §10.3).
 */

export const TOPIC_ROOT = "market.bar.";

export interface TopicPrefixHint {
  topic: string;
  label: string;
}

export const TOPIC_PREFIX_HINTS: readonly TopicPrefixHint[] = [
  { topic: "market.bar.", label: "全部 Bar（前缀）" },
  { topic: "market.bar.1m.", label: "1m 全部 Instrument（前缀）" },
  {
    topic: "market.bar.1m.binance:BTC/USDT",
    label: "精确 Topic（InstrumentId 示例）",
  },
];

export interface TopicValidationValid {
  valid: true;
  topic: string;
}

export interface TopicValidationInvalid {
  valid: false;
  error: string;
}

export type TopicValidation = TopicValidationValid | TopicValidationInvalid;

const INSTRUMENT_ID_PATTERN = /^[^\s:]+:\S+$/;

export function isInstrumentId(value: string): boolean {
  return INSTRUMENT_ID_PATTERN.test(value);
}

function invalid(error: string): TopicValidationInvalid {
  return { valid: false, error };
}

/**
 * Validates the only three forms the console may subscribe to:
 *
 *   market.bar.
 *   market.bar.<interval>.
 *   market.bar.<interval>.<instrument_id>
 */
export function validateControlledTopic(raw: string): TopicValidation {
  const topic = raw.trim();
  if (topic.length === 0) {
    return invalid("Topic 不能为空");
  }
  if (!topic.startsWith(TOPIC_ROOT)) {
    return invalid(
      "只允许 market.bar.* Topic；replay.* 和内部 Topic 不可用",
    );
  }

  const rest = topic.slice(TOPIC_ROOT.length);
  if (rest.length === 0) {
    // `market.bar.` — every live Bar.
    return { valid: true, topic };
  }

  if (rest.endsWith(".")) {
    const interval = rest.slice(0, -1);
    if (!isBarInterval(interval)) {
      return invalid(
        `Bar Interval 必须是 ${BAR_INTERVALS.join(" / ")}；当前为“${interval}”`,
      );
    }
    return { valid: true, topic };
  }

  const separator = rest.indexOf(".");
  if (separator <= 0 || separator === rest.length - 1) {
    return invalid(
      "Topic 必须是 market.bar.、market.bar.<interval>. 或 market.bar.<interval>.<instrument_id>",
    );
  }

  const interval = rest.slice(0, separator);
  const instrumentId = rest.slice(separator + 1);
  if (!isBarInterval(interval)) {
    return invalid(
      `Bar Interval 必须是 ${BAR_INTERVALS.join(" / ")}；当前为“${interval}”`,
    );
  }
  if (!isInstrumentId(instrumentId)) {
    return invalid(
      `InstrumentId 必须是 <venue>:<symbol>；当前为“${instrumentId}”`,
    );
  }
  return { valid: true, topic };
}

export type TopicScope = BarInterval | "all";

export function buildControlledTopic(
  scope: TopicScope,
  instrumentId?: string,
): string {
  if (scope === "all") {
    return TOPIC_ROOT;
  }
  const prefix = `${TOPIC_ROOT}${scope}.`;
  const instrument = instrumentId?.trim() ?? "";
  return instrument.length > 0 ? `${prefix}${instrument}` : prefix;
}

export type FreeformFrame =
  | { op: "subscribe"; topics: string[] }
  | { op: "unsubscribe"; topics: string[] }
  | { op: "ping" };

export type FreeformParseResult =
  | { ok: true; frame: FreeformFrame }
  | { ok: false; error: string };

export const FREEFORM_MAX_TOPICS = 20;
export const FREEFORM_MAX_TOPIC_LENGTH = 512;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * Parses the free-form JSON debug editor. Only the protocol whitelist
 * `subscribe / unsubscribe / ping` is accepted; every topic must satisfy the
 * controlled grammar. The server ignores invalid frames, so the client must
 * reject them with a visible error instead of sending silently.
 */
export function parseFreeformFrame(text: string): FreeformParseResult {
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    return { ok: false, error: "不是有效 JSON，请检查引号与括号" };
  }

  if (!isRecord(parsed)) {
    return { ok: false, error: "JSON 必须是对象" };
  }

  const op = parsed.op;
  if (op !== "subscribe" && op !== "unsubscribe" && op !== "ping") {
    return {
      ok: false,
      error: "自由 JSON 只允许 subscribe / unsubscribe / ping",
    };
  }

  if (op === "ping") {
    return { ok: true, frame: { op: "ping" } };
  }

  const topics = parsed.topics;
  if (!Array.isArray(topics)) {
    return {
      ok: false,
      error: "topics 必须是字符串数组",
    };
  }
  if (topics.length === 0) {
    return { ok: false, error: "topics 不能为空" };
  }
  if (topics.length > FREEFORM_MAX_TOPICS) {
    return {
      ok: false,
      error: `一次最多 ${FREEFORM_MAX_TOPICS} 个 Topic`,
    };
  }

  const normalized: string[] = [];
  for (const value of topics) {
    if (typeof value !== "string" || value.trim().length === 0) {
      return { ok: false, error: "topics 每项必须是非空字符串" };
    }
    if (value.length > FREEFORM_MAX_TOPIC_LENGTH) {
      return {
        ok: false,
        error: `Topic 长度不能超过 ${FREEFORM_MAX_TOPIC_LENGTH} 字符`,
      };
    }
    const validation = validateControlledTopic(value);
    if (!validation.valid) {
      return {
        ok: false,
        error: `Topic 无效：${validation.error}`,
      };
    }
    normalized.push(validation.topic);
  }

  return { ok: true, frame: { op, topics: normalized } };
}
