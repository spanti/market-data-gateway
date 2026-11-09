import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useWsFactory } from "@/api/ws/use-ws-factory";
import { GatewayWsTransport, type WsProtocolIssueCode } from "@/api/ws/transport";
import { resolveWsTarget } from "@/api/ws/ws-target";
import { matchesBarQuery, type BarRow } from "@/lib/bar-data";
import { containsInstant, type BarInterval } from "@/lib/time-range";
import { useRuntimeMode } from "@/runtime/runtime-mode-context";
import { useActionReason } from "@/runtime/use-permission";

/**
 * Live Bar append.
 *
 * - The user must explicitly enable it, and the range must include now.
 * - Historical-only ranges never open a WebSocket.
 * - The transport target is resolved by `resolveWsTarget`: public `/ws` only in
 *   Current Contract, Admin `/api/v1/admin/ws` only with `ws_admin_session`,
 *   and no socket while capabilities are pending/unknown.
 * - The Access Role must allow `ws.session.subscribe`.
 * - There is no resume position: every successful open is a NEW receive
 *   segment; a close marks the current segment interrupted. The UI must never
 *   claim seamless recovery, and REST refetch only backfills persisted
 *   history.
 */

export type LiveFeedStatus =
  | "disabled"
  | "blocked"
  | "connecting"
  | "receiving"
  | "interrupted";

export interface LiveSegment {
  segment: number;
  state: "connecting" | "receiving" | "interrupted";
  startedAt: string;
  endedAt: string | null;
  eventCount: number;
}

export interface LiveLagNotice {
  segment: number;
  dropped: number;
  since: string;
  receivedAt: string;
}

export interface LiveProtocolIssue {
  segment: number;
  code: WsProtocolIssueCode;
  message: string;
  receivedAt: string;
}

export interface UseBarLiveFeedOptions {
  instrumentId: string | null;
  interval: BarInterval | null;
  start: Date;
  end: Date;
  /** False when the chart range policy rejects the span (for example >31d). */
  rangeAllowed?: boolean;
  /** Deterministic clock for tests; production uses the wall clock. */
  now?: Date;
}

export interface BarLiveFeedState {
  enabled: boolean;
  rangeIncludesNow: boolean;
  /** False when the runtime mode/role/capability forbids a socket at all. */
  canEnable: boolean;
  /** Why no socket may be opened, even before the user enables the control. */
  unavailableReason: string | null;
  blockedReason: string | null;
  status: LiveFeedStatus;
  topic: string | null;
  segments: LiveSegment[];
  rows: BarRow[];
  lagNotices: LiveLagNotice[];
  protocolIssues: LiveProtocolIssue[];
  enable: () => void;
  disable: () => void;
  reconnect: () => void;
}

function wsTopic(interval: BarInterval, instrumentId: string): string {
  return `market.bar.${interval}.${instrumentId}`;
}

export function useBarLiveFeed({
  instrumentId,
  interval,
  start,
  end,
  rangeAllowed = true,
  now,
}: UseBarLiveFeedOptions): BarLiveFeedState {
  const factory = useWsFactory();
  const { capability } = useRuntimeMode();
  const subscribeReason = useActionReason("ws.session.subscribe");
  const wsTarget = useMemo(() => resolveWsTarget(capability), [capability]);
  const unavailableReason =
    wsTarget.reason?.message ?? subscribeReason?.message ?? null;
  const [enabled, setEnabled] = useState(false);
  const [connectionStatus, setConnectionStatus] = useState<
    "idle" | "connecting" | "receiving" | "interrupted"
  >("idle");
  const [segments, setSegments] = useState<LiveSegment[]>([]);
  const [rows, setRows] = useState<BarRow[]>([]);
  const [lagNotices, setLagNotices] = useState<LiveLagNotice[]>([]);
  const [protocolIssues, setProtocolIssues] = useState<LiveProtocolIssue[]>(
    [],
  );
  const segmentCounter = useRef(0);
  const transportRef = useRef<GatewayWsTransport | null>(null);

  const startMs = start.getTime();
  const endMs = end.getTime();
  const nowMs = (now ?? new Date()).getTime();
  const rangeIncludesNow =
    Number.isFinite(startMs) &&
    Number.isFinite(endMs) &&
    containsInstant(new Date(startMs), new Date(endMs), new Date(nowMs));

  const identityReady = instrumentId !== null && interval !== null;
  const targetReady = wsTarget.url !== null && unavailableReason === null;
  const canEnable = targetReady;
  const canConnect =
    enabled && identityReady && rangeIncludesNow && rangeAllowed && targetReady;

  let blockedReason: string | null = null;
  if (enabled && unavailableReason !== null) {
    blockedReason = unavailableReason;
  } else if (enabled && !identityReady) {
    blockedReason = "请先选择 InstrumentId 与 Bar Interval。";
  } else if (enabled && !rangeAllowed) {
    blockedReason =
      "范围超过 31 个 UTC 天：图表与实时跟随都需要先缩小范围。";
  } else if (enabled && !rangeIncludesNow) {
    blockedReason =
      "历史范围不连接实时追加；请将结束时间调整到包含当前时间后再开启。";
  }

  const status: LiveFeedStatus = !enabled
    ? "disabled"
    : !canConnect
      ? "blocked"
      : connectionStatus === "idle"
        ? "connecting"
        : connectionStatus;

  const targetUrl = wsTarget.url;

  useEffect(() => {
    if (
      !canConnect ||
      targetUrl === null ||
      instrumentId === null ||
      interval === null
    ) {
      setConnectionStatus("idle");
      return;
    }

    const identity = {
      instrumentId,
      interval,
      start: new Date(startMs),
      end: new Date(endMs),
    };
    const topic = wsTopic(interval, instrumentId);
    let active = true;
    let segment = 0;

    const nextSegmentNumber = () => {
      segmentCounter.current += 1;
      return segmentCounter.current;
    };

    const markReceivingInterrupted = (endedAt: string) => {
      setSegments((previous) =>
        previous.map((item) =>
          item.state === "receiving"
            ? { ...item, state: "interrupted" as const, endedAt }
            : item,
        ),
      );
    };

    const transport = new GatewayWsTransport({
      url: targetUrl,
      ...(factory ? { createSocket: factory } : {}),
      onOpen: () => {
        if (!active) {
          return;
        }
        segment = nextSegmentNumber();
        const startedAt = new Date().toISOString();
        setSegments((previous) => [
          ...previous.map((item) =>
            item.state === "receiving"
              ? { ...item, state: "interrupted" as const, endedAt: startedAt }
              : item,
          ),
          {
            segment,
            state: "receiving",
            startedAt,
            endedAt: null,
            eventCount: 0,
          },
        ]);
        setConnectionStatus("receiving");
        // There is no subscribe ACK; pong only proves op ordering.
        transport.subscribe([topic]);
        transport.ping();
      },
      onClose: () => {
        if (!active) {
          return;
        }
        setConnectionStatus("interrupted");
        markReceivingInterrupted(new Date().toISOString());
      },
      onEvent: ({ data }) => {
        if (!active || !matchesBarQuery(data, identity)) {
          return;
        }
        setRows((previous) => [...previous, { bar: data, source: "ws" }]);
        setSegments((previous) =>
          previous.map((item) =>
            item.segment === segment
              ? { ...item, eventCount: item.eventCount + 1 }
              : item,
          ),
        );
      },
      onLagNotice: ({ dropped, since }) => {
        if (!active) {
          return;
        }
        setLagNotices((previous) => [
          ...previous,
          {
            segment,
            dropped,
            since,
            receivedAt: new Date().toISOString(),
          },
        ]);
      },
      onProtocolIssue: (issue) => {
        if (!active) {
          return;
        }
        setProtocolIssues((previous) => [
          ...previous,
          {
            segment: issue.segment,
            code: issue.code,
            message: issue.message,
            receivedAt: new Date().toISOString(),
          },
        ]);
      },
    });

    transportRef.current = transport;
    setConnectionStatus("connecting");
    transport.open();

    return () => {
      active = false;
      transport.close();
      if (transportRef.current === transport) {
        transportRef.current = null;
      }
      markReceivingInterrupted(new Date().toISOString());
      setConnectionStatus("idle");
    };
  }, [canConnect, targetUrl, instrumentId, interval, startMs, endMs, factory]);

  const enable = useCallback(() => setEnabled(true), []);
  const disable = useCallback(() => setEnabled(false), []);
  const reconnect = useCallback(() => {
    transportRef.current?.open();
  }, []);

  return {
    enabled,
    rangeIncludesNow,
    canEnable,
    unavailableReason,
    blockedReason,
    status,
    topic:
      instrumentId !== null && interval !== null
        ? wsTopic(interval, instrumentId)
        : null,
    segments,
    rows,
    lagNotices,
    protocolIssues,
    enable,
    disable,
    reconnect,
  };
}
