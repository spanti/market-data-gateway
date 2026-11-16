import { act, fireEvent, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { WebSocketLike } from "@/api/ws/transport";
import type { CapabilityState } from "@/api/capabilities";
import type { PermissionSubject } from "@/runtime/permissions";
import {
  RuntimeModeContext,
  type RuntimeModeContextValue,
} from "@/runtime/runtime-mode-context";
import type { AdminSession } from "@/runtime/session";
import { TooltipProvider } from "@/components/ui/tooltip";
import { WebSocketDebugView } from "@/pages/console/WebSocketDebugView";
import { WsDebugSession } from "@/pages/console/ws-debug-session";

const envelope = {
  schema_version: 1,
  data_type: "bar",
  instrument_id: "binance:BTC/USDT",
  event_ts: "2024-01-01T00:01:00.000000000Z",
  ingest_ts: "2026-11-09T04:24:42.946489000Z",
  feed: "fake",
  seq: null,
  payload: {
    interval: "1m",
    open_ts: "2024-01-01T00:00:00.000000000Z",
    close_ts: "2024-01-01T00:01:00.000000000Z",
    aggregation_source: "external",
    open: "65000.100000000000000001",
    high: "65010.0",
    low: "64990.0",
    close: "65005.5",
    volume: "12.34",
    trade_count: 128,
    revision: 0,
  },
};

class FakeWebSocket implements WebSocketLike {
  readyState = 0;
  onopen: WebSocketLike["onopen"] = null;
  onmessage: WebSocketLike["onmessage"] = null;
  onclose: WebSocketLike["onclose"] = null;
  onerror: WebSocketLike["onerror"] = null;
  readonly sent: string[] = [];

  send(data: string): void {
    this.sent.push(data);
  }

  close(code = 1000, reason = ""): void {
    this.readyState = 3;
    this.onclose?.({ code, reason, wasClean: true } as CloseEvent);
  }

  fireOpen(): void {
    this.readyState = 1;
    this.onopen?.(new Event("open"));
  }

  fireMessage(payload: unknown): void {
    this.onmessage?.({
      data: typeof payload === "string" ? payload : JSON.stringify(payload),
    } as MessageEvent);
  }

  fireServerClose(): void {
    this.readyState = 3;
    this.onclose?.({ code: 1006, reason: "gone", wasClean: false } as CloseEvent);
  }
}

const currentContract: CapabilityState = {
  mode: "current-contract",
  lastSuccessAt: null,
};

function adminCapability(
  wsState: "enabled" | "unsupported" = "enabled",
): CapabilityState {
  return {
    mode: "admin-api",
    compatible: true,
    lastSuccessAt: "2026-11-09T12:00:00.000000000Z",
    document: {
      server_time: "2026-11-09T12:00:00.000000000Z",
      version: { admin_api: "1.0.0", contract: 1, supported_contracts: [1] },
      auth: {
        required: true,
        roles: ["SuperAdmin", "Operator", "Auditor"],
      },
      adapters: [],
      capabilities: {
        ws_admin_session: { state: wsState },
      },
    },
  };
}

function adminSession(role: "SuperAdmin" | "Operator" | "Auditor"): AdminSession {
  return {
    authenticated: true,
    access_role: role,
    user: { subject: "oidc-subject-1", displayName: "Ada", email: null },
    server_time: null,
    session: null,
    csrf: { header_name: "X-CSRF-Token", token: "csrf" },
    csrfToken: "csrf",
    received_at: 0,
  };
}

function renderView(options: {
  subject: PermissionSubject;
  capability: CapabilityState | null;
  session?: AdminSession | null;
  limit?: number;
}) {
  const sockets: FakeWebSocket[] = [];
  const sessionFactory = () =>
    new WsDebugSession({
      url: "ws://test.invalid/ws",
      createSocket: () => {
        const socket = new FakeWebSocket();
        sockets.push(socket);
        return socket;
      },
      ...(options.limit !== undefined ? { limit: options.limit } : {}),
      now: () => new Date("2026-11-09T12:00:00.000000000Z"),
    });

  const value: RuntimeModeContextValue = {
    capability: options.capability,
    isLoading: false,
    session: options.session ?? null,
    sessionLoading: false,
    role:
      options.subject.kind === "role" ? options.subject.role : null,
    subject: options.subject,
    refreshCapabilities: () => undefined,
    sessionIssue: null,
    sessionExpiry: {
      state: "none",
      bound: null,
      remainingMs: null,
      remainingSeconds: null,
    },
    writesStopped: false,
    authPending: false,
    logout: async () => undefined,
    refreshSession: async () => undefined,
  };

  const view = render(
    <TooltipProvider>
      <RuntimeModeContext.Provider value={value}>
        <WebSocketDebugView sessionFactory={sessionFactory} />
      </RuntimeModeContext.Provider>
    </TooltipProvider>,
  );
  return { ...view, sockets };
}

async function connect(
  user: ReturnType<typeof userEvent.setup>,
  sockets: FakeWebSocket[],
) {
  await user.click(screen.getByRole("button", { name: "连接" }));
  act(() => {
    sockets[0]?.fireOpen();
  });
}

/**
 * UI-generated copy must not contain the banned separators. Raw evidence
 * regions render exact text and are explicitly exempt.
 */
function bannedUiCopy(container: HTMLElement): string[] {
  const walker = document.createTreeWalker(container, NodeFilter.SHOW_TEXT);
  const hits: string[] = [];
  while (walker.nextNode()) {
    const node = walker.currentNode;
    const element = node.parentElement;
    if (!element) {
      continue;
    }
    if (element.closest('[data-raw-evidence="true"]')) {
      continue;
    }
    if (element.closest(".sr-only")) {
      continue;
    }
    const text = node.textContent ?? "";
    if (text.includes("·") || text.includes("：")) {
      hits.push(text.trim());
    }
  }
  return hits;
}

beforeEach(() => {
  // The virtualizer measures clientWidth/clientHeight. jsdom reports 0, so
  // give it a stable viewport to exercise the real TanStack Virtual list.
  vi.spyOn(HTMLElement.prototype, "clientHeight", "get").mockReturnValue(420);
  vi.spyOn(HTMLElement.prototype, "clientWidth", "get").mockReturnValue(800);
});

describe("WebSocket Debug role matrix", () => {
  it("Current Contract anonymous gets the controlled form but free-form JSON is disabled", () => {
    renderView({
      subject: { kind: "anonymous" },
      capability: currentContract,
    });

    expect(
      screen.getByLabelText("InstrumentId（可选）"),
    ).toBeEnabled();
    expect(screen.getByLabelText("自由 JSON")).toBeDisabled();
    expect(screen.getAllByText("需要 Admin API").length).toBeGreaterThan(0);
  });

  it("Auditor gets the controlled form only, with a visible role reason", () => {
    renderView({
      subject: { kind: "role", role: "Auditor" },
      capability: adminCapability(),
      session: adminSession("Auditor"),
    });

    expect(screen.getByLabelText("InstrumentId（可选）")).toBeEnabled();
    expect(screen.getByLabelText("自由 JSON")).toBeDisabled();
    expect(
      screen.getAllByText("需要 SuperAdmin 或 Operator 角色").length,
    ).toBeGreaterThan(0);
  });

  it.each(["Operator", "SuperAdmin"] as const)(
    "%s can use the whitelisted free-form JSON editor",
    (role) => {
      renderView({
        subject: { kind: "role", role },
        capability: adminCapability(),
        session: adminSession(role),
      });

      expect(screen.getByLabelText("自由 JSON")).toBeEnabled();
      expect(
        screen.queryByText("需要 SuperAdmin 或 Operator 角色"),
      ).not.toBeInTheDocument();
    },
  );

  it("shows a capability gap instead of pretending role control when admin WS is unsupported", () => {
    renderView({
      subject: { kind: "role", role: "SuperAdmin" },
      capability: adminCapability("unsupported"),
      session: adminSession("SuperAdmin"),
    });

    expect(screen.getByLabelText("自由 JSON")).toBeDisabled();
    expect(screen.getAllByText("后端未实现").length).toBeGreaterThan(0);
    expect(screen.getByRole("button", { name: "连接" })).toBeDisabled();
  });

  it("does not allow connect while capability discovery is unknown", async () => {
    const user = userEvent.setup();
    const { sockets } = renderView({
      subject: { kind: "unknown" },
      capability: {
        mode: "capabilities-unknown",
        lastSuccessAt: null,
        errorCode: "timeout",
        message: "timed out",
      },
    });

    const connect = screen.getByRole("button", { name: "连接" });
    expect(connect).toBeDisabled();
    expect(
      screen.getAllByText("能力未知，暂不可用").length,
    ).toBeGreaterThan(0);

    await user.click(connect);
    expect(sockets).toHaveLength(0);
  });
});

describe("WebSocket Debug session subscription scope and states", () => {
  it("distinguishes the session subscription from Feed Subscription and exposes help", () => {
    renderView({
      subject: { kind: "anonymous" },
      capability: currentContract,
    });

    expect(
      screen.getByRole("button", { name: "WebSocket Debug 说明" }),
    ).toBeInTheDocument();
    expect(
      screen.getByRole("button", {
        name: "WebSocket Session Subscription 说明",
      }),
    ).toBeInTheDocument();
    expect(
      screen.getAllByText(/只筛选当前调试会话/).length,
    ).toBeGreaterThan(0);
    expect(
      screen.getAllByText(/不改变 Feed Subscription 的采集范围/).length,
    ).toBeGreaterThan(0);
    expect(screen.getByText("尚未连接")).toBeInTheDocument();
  });

  it("shows loading while capability discovery is still in flight", () => {
    renderView({
      subject: { kind: "anonymous" },
      capability: null,
    });

    expect(screen.getByText("正在发现运行时能力…")).toBeInTheDocument();
  });

  it("shows an explicit unknown target when capability discovery failed", () => {
    renderView({
      subject: { kind: "unknown" },
      capability: {
        mode: "capabilities-unknown",
        lastSuccessAt: null,
        errorCode: "timeout",
        message: "timed out",
      },
    });

    expect(
      screen.getByText("运行时能力发现失败，无法确认 WebSocket 目标。"),
    ).toBeInTheDocument();
    expect(screen.getByTestId("ws-segment-state")).toHaveTextContent(
      "接收段 0",
    );
  });

  it("shows a real connection error without inventing a receive segment", async () => {
    const user = userEvent.setup();
    const { sockets } = renderView({
      subject: { kind: "anonymous" },
      capability: currentContract,
    });

    await user.click(screen.getByRole("button", { name: "连接" }));
    act(() => {
      sockets[0]?.fireServerClose();
    });

    const alert = await screen.findByTestId("ws-connection-error");
    expect(alert).toHaveTextContent("连接失败");
    expect(alert.querySelector('[data-raw-evidence="true"]')).toHaveTextContent(
      "code 1006 gone",
    );
    expect(screen.getByTestId("ws-segment-state")).toHaveTextContent(
      "接收段 0",
    );
  });

  it("labels a connected session with no retained messages as empty, not failed", async () => {
    const user = userEvent.setup();
    const { sockets } = renderView({
      subject: { kind: "anonymous" },
      capability: currentContract,
    });

    await connect(user, sockets);
    await user.click(screen.getByRole("button", { name: "清空" }));
    expect(await screen.findByText("暂无报文")).toBeInTheDocument();
  });
});

describe("WebSocket Debug controlled Topic form", () => {
  it("shows prefix hints and validates the InstrumentId before sending", async () => {
    const user = userEvent.setup();
    const { sockets } = renderView({
      subject: { kind: "anonymous" },
      capability: currentContract,
    });

    expect(
      screen.getByText("market.bar.1m.binance:BTC/USDT"),
    ).toBeInTheDocument();
    expect(screen.getByTestId("ws-topic-preview")).toHaveTextContent(
      "market.bar.1m.",
    );

    await connect(user, sockets);

    const instrument = screen.getByLabelText("InstrumentId（可选）");
    await user.type(instrument, "BTCUSDT");
    expect(screen.getByTestId("ws-topic-preview")).toHaveTextContent(
      "market.bar.1m.BTCUSDT",
    );
    expect(
      screen.getByText(/InstrumentId 必须是 <venue>:<symbol>/),
    ).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "订阅" })).toBeDisabled();

    await user.clear(instrument);
    await user.type(instrument, "binance:BTC/USDT");
    expect(screen.getByRole("button", { name: "订阅" })).toBeEnabled();
    await user.click(screen.getByRole("button", { name: "订阅" }));

    expect(sockets[0]?.sent).toContain(
      '{"op":"subscribe","topics":["market.bar.1m.binance:BTC/USDT"]}',
    );
    expect(screen.queryByText(/订阅已确认/)).not.toBeInTheDocument();
  });

  it("switches to the all-bars prefix and disables the instrument input", async () => {
    const user = userEvent.setup();
    renderView({
      subject: { kind: "anonymous" },
      capability: currentContract,
    });

    await user.selectOptions(screen.getByLabelText("Topic 范围"), "all");
    expect(screen.getByTestId("ws-topic-preview")).toHaveTextContent(
      "market.bar.",
    );
    expect(
      screen.queryByLabelText("InstrumentId（可选）"),
    ).not.toBeInTheDocument();
  });
});

describe("WebSocket Debug ping/pong barrier", () => {
  it("shows 已同步 after pong and never calls it a subscription confirmation", async () => {
    const user = userEvent.setup();
    const { sockets } = renderView({
      subject: { kind: "anonymous" },
      capability: currentContract,
    });

    await connect(user, sockets);
    expect(screen.getByTestId("ws-connection-state")).toHaveTextContent(
      "已连接",
    );
    expect(screen.getByTestId("ws-segment-state")).toHaveTextContent(
      "接收段 1",
    );

    await user.click(screen.getByRole("button", { name: "Ping" }));
    expect(screen.getByText("等待 pong…")).toBeInTheDocument();

    act(() => {
      sockets[0]?.fireMessage({ op: "pong" });
    });

    expect(await screen.findByText("已同步")).toBeInTheDocument();
    expect(screen.queryByText(/订阅已确认/)).not.toBeInTheDocument();
    expect(
      screen.getByText(/pong 仅证明操作顺序，不是订阅 ACK/),
    ).toBeInTheDocument();
  });
});

describe("WebSocket Debug bounded list and counters", () => {
  it("keeps server lag and local UI drops separate", async () => {
    const user = userEvent.setup();
    const { sockets } = renderView({
      subject: { kind: "anonymous" },
      capability: currentContract,
      limit: 2,
    });

    await connect(user, sockets);
    act(() => {
      sockets[0]?.fireMessage({
        op: "lag_notice",
        dropped: 12,
        since: "2026-11-09T12:00:00.000000000Z",
      });
      sockets[0]?.fireMessage({
        op: "event",
        topic: "market.bar.1m.binance:BTC/USDT",
        data: envelope,
      });
      sockets[0]?.fireMessage({
        op: "event",
        topic: "market.bar.1m.binance:ETH/USDT",
        data: { ...envelope, instrument_id: "binance:ETH/USDT" },
      });
    });

    expect(
      await screen.findByText(/服务端丢弃（lag_notice） 12/),
    ).toBeInTheDocument();
    expect(screen.getByText(/界面丢弃数 2/)).toBeInTheDocument();
    expect(screen.getByText(/最多保留 10,000 条/)).toBeInTheDocument();
  });

  it("filters, clears and pauses/resumes auto-scroll", async () => {
    const user = userEvent.setup();
    const { sockets } = renderView({
      subject: { kind: "anonymous" },
      capability: currentContract,
    });

    await connect(user, sockets);
    act(() => {
      sockets[0]?.fireMessage({ op: "pong" });
      sockets[0]?.fireMessage({
        op: "event",
        topic: "market.bar.1m.binance:BTC/USDT",
        data: envelope,
      });
    });

    await screen.findAllByTestId("ws-message");
    await user.type(screen.getByLabelText("筛选消息"), "pong");
    expect(screen.getAllByTestId("ws-message")).toHaveLength(1);
    expect(screen.getByText(/筛选 1 \/ 总计 3/)).toBeInTheDocument();

    await user.clear(screen.getByLabelText("筛选消息"));
    await user.click(screen.getByRole("button", { name: "清空" }));
    expect(screen.queryAllByTestId("ws-message")).toHaveLength(0);
    expect(screen.getByText(/界面丢弃数 0/)).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "暂停自动滚动" }));
    expect(screen.getByTestId("ws-autoscroll-state")).toHaveTextContent(
      "自动滚动已暂停",
    );
    await user.click(screen.getByRole("button", { name: "恢复自动滚动" }));
    expect(screen.getByTestId("ws-autoscroll-state")).toHaveTextContent(
      "自动滚动开启",
    );
  });
});

describe("WebSocket Debug receive segments", () => {
  it("marks a disconnected segment as interrupted and starts a new segment on reconnect", async () => {
    const user = userEvent.setup();
    const { sockets } = renderView({
      subject: { kind: "anonymous" },
      capability: currentContract,
    });

    await connect(user, sockets);
    act(() => {
      sockets[0]?.fireMessage({ op: "pong" });
      sockets[0]?.fireServerClose();
    });

    expect(
      (await screen.findAllByText(/接收段 1 中断/)).length,
    ).toBeGreaterThan(0);
    expect(
      screen.getByText(
        "接收段 1 已中断。没有恢复位点，重连不会补齐缺失报文。",
      ),
    ).toBeInTheDocument();

    await user.click(screen.getByRole("button", { name: "连接" }));
    act(() => {
      sockets[1]?.fireOpen();
    });

    expect(
      await screen.findByTestId("ws-connection-state"),
    ).toHaveTextContent("已连接");
    expect(screen.getByTestId("ws-segment-state")).toHaveTextContent(
      "接收段 2",
    );
    expect(screen.getByText(/接收段 2 已连接/)).toBeInTheDocument();
  });
});

describe("WebSocket Debug raw detail", () => {
  it("renders exact JSON as text without interpreting HTML", async () => {
    const user = userEvent.setup();
    const { sockets } = renderView({
      subject: { kind: "anonymous" },
      capability: currentContract,
    });

    await connect(user, sockets);
    act(() => {
      sockets[0]?.fireMessage({
        op: "event",
        topic: "market.bar.1m.binance:BTC/USDT",
        data: {
          ...envelope,
          instrument_id: "binance:<img src=x onerror=alert(1)>",
        },
      });
    });

    const matches = await screen.findAllByText(
      /binance:<img src=x onerror=alert\(1\)>/,
    );
    const eventRow = matches[0]?.closest(
      '[data-testid="ws-message"]',
    ) as HTMLElement | null;
    expect(eventRow).not.toBeNull();
    if (!eventRow) {
      throw new Error("event row not found");
    }

    await user.click(within(eventRow).getByText("原始 JSON"));
    const detail = within(eventRow).getByTestId("ws-message-detail");
    expect(detail).toHaveAttribute("data-raw-evidence", "true");
    expect(detail.textContent).toContain("65000.100000000000000001");
    expect(detail.textContent).toContain(
      "<img src=x onerror=alert(1)>",
    );
    expect(eventRow.querySelector("img")).toBeNull();
  });

  it("renders raw JSON with banned separators verbatim while UI copy stays clean", async () => {
    const user = userEvent.setup();
    const { sockets, container } = renderView({
      subject: { kind: "anonymous" },
      capability: currentContract,
    });

    await connect(user, sockets);
    act(() => {
      sockets[0]?.fireMessage({
        op: "event",
        topic: "market.bar.1m.binance:BTC/USDT",
        data: { ...envelope, note: "a·b：c" },
      });
    });

    const matches = await screen.findAllByText(/a·b：c/);
    const eventRow = matches[0]?.closest(
      '[data-testid="ws-message"]',
    ) as HTMLElement | null;
    expect(eventRow).not.toBeNull();
    if (!eventRow) {
      throw new Error("event row not found");
    }

    await user.click(within(eventRow).getByText("原始 JSON"));
    const detail = within(eventRow).getByTestId("ws-message-detail");
    expect(detail).toHaveAttribute("data-raw-evidence", "true");
    expect(detail.textContent).toContain('"note": "a·b：c"');

    expect(bannedUiCopy(container)).toEqual([]);
  });

  it("never persists messages in web storage", async () => {
    const user = userEvent.setup();
    const { sockets } = renderView({
      subject: { kind: "anonymous" },
      capability: currentContract,
    });

    await connect(user, sockets);
    act(() => {
      sockets[0]?.fireMessage({
        op: "event",
        topic: "market.bar.1m.binance:BTC/USDT",
        data: envelope,
      });
    });
    await screen.findAllByTestId("ws-message");

    expect(window.localStorage.length).toBe(0);
    expect(window.sessionStorage.length).toBe(0);
  });
});

describe("WebSocket Debug free-form whitelist", () => {
  it("rejects forbidden ops with a visible error and sends only whitelisted frames", async () => {
    const user = userEvent.setup();
    const { sockets } = renderView({
      subject: { kind: "role", role: "Operator" },
      capability: adminCapability(),
      session: adminSession("Operator"),
    });

    await connect(user, sockets);

    const editor = screen.getByLabelText("自由 JSON");
    fireEvent.change(editor, { target: { value: '{"op":"event"}' } });
    await user.click(screen.getByRole("button", { name: "校验并发送" }));

    expect(
      screen.getByText(/只允许 subscribe \/ unsubscribe \/ ping/),
    ).toBeInTheDocument();
    expect(sockets[0]?.sent).toEqual([]);

    fireEvent.change(editor, { target: { value: '{"op":"ping"}' } });
    await user.click(screen.getByRole("button", { name: "校验并发送" }));
    expect(sockets[0]?.sent).toEqual(['{"op":"ping"}']);
  });
});
