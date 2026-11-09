/**
 * Bounded in-memory message buffer for the WebSocket Debug view.
 *
 * Product rule: retain at most 10,000 messages, drop the oldest locally when
 * the buffer overflows, and report the local drop count separately from the
 * server `lag_notice.dropped`. Messages are never persisted.
 */

export const WS_DEBUG_MESSAGE_LIMIT = 10_000;

export type DebugMessageKind =
  | "event"
  | "pong"
  | "lag_notice"
  | "protocol_issue"
  | "connection";

export interface DebugMessageInput {
  segment: number;
  kind: DebugMessageKind;
  /** Client receive timestamp (ISO-8601), not the server event timestamp. */
  receivedAt: string;
  topic?: string;
  summary: string;
  /** Exact JSON text rendered as text in the detail view. */
  detailText: string;
}

export interface DebugMessage extends DebugMessageInput {
  /** Monotonic local ID; never reused. */
  id: number;
}

export interface MessageBufferState {
  readonly limit: number;
  readonly messages: readonly DebugMessage[];
  /** Messages dropped from this client buffer because it was full. */
  readonly localDropped: number;
  readonly totalReceived: number;
  readonly nextId: number;
}

export function createMessageBuffer(
  limit: number = WS_DEBUG_MESSAGE_LIMIT,
): MessageBufferState {
  if (!Number.isInteger(limit) || limit < 1) {
    throw new Error("Message buffer limit must be a positive integer");
  }
  return {
    limit,
    messages: [],
    localDropped: 0,
    totalReceived: 0,
    nextId: 1,
  };
}

/**
 * Appends one message. When the buffer is full the oldest entry is removed
 * and `localDropped` increments; the server-side lag counter is a separate
 * value owned by the WS session.
 */
export function appendMessage(
  state: MessageBufferState,
  input: DebugMessageInput,
): MessageBufferState {
  const message: DebugMessage = { ...input, id: state.nextId };
  const combined = [...state.messages, message];
  const overflow = Math.max(0, combined.length - state.limit);
  return {
    ...state,
    messages: overflow > 0 ? combined.slice(overflow) : combined,
    localDropped: state.localDropped + overflow,
    totalReceived: state.totalReceived + 1,
    nextId: state.nextId + 1,
  };
}

/** Clears the list; drop accounting is historical and is not rewritten. */
export function clearMessages(state: MessageBufferState): MessageBufferState {
  return { ...state, messages: [] };
}

function searchableText(message: DebugMessage): string {
  return [
    message.kind,
    message.topic ?? "",
    message.summary,
    message.detailText,
  ]
    .join("\n")
    .toLowerCase();
}

export function filterMessages(
  messages: readonly DebugMessage[],
  filter: string,
): readonly DebugMessage[] {
  const needle = filter.trim().toLowerCase();
  if (needle.length === 0) {
    return messages;
  }
  return messages.filter((message) =>
    searchableText(message).includes(needle),
  );
}
