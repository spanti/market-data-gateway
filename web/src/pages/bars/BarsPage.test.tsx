import { fireEvent, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { http, HttpResponse } from "msw";
import { afterEach, describe, expect, it, vi } from "vitest";
import type { BarMessageEnvelope } from "@/api/endpoints/bars";
import { parseDateTimeInput, parseIsoInstant } from "@/lib/time-range";
import { renderApp } from "@/test/render-app";
import { server } from "@/test/server";

vi.mock("echarts/core", () => ({
  use: vi.fn(),
  init: vi.fn(() => ({
    setOption: vi.fn(),
    dispatchAction: vi.fn(),
    resize: vi.fn(),
    dispose: vi.fn(),
  })),
}));

const INSTRUMENT = "binance:BTC/USDT";

function envelope(
  overrides: {
    instrument?: string;
    interval?: "1m" | "5m" | "1h";
    openTs?: string;
    revision?: number;
    close?: string;
    ingestTs?: string;
  } = {},
): BarMessageEnvelope {
  const openTs =
    overrides.openTs ?? "2024-01-01T00:00:00.000000000Z";
  return {
    schema_version: 1,
    data_type: "bar",
    instrument_id: overrides.instrument ?? INSTRUMENT,
    event_ts: openTs,
    ingest_ts: overrides.ingestTs ?? "2026-11-09T04:24:42.946489000Z",
    feed: "fake",
    seq: null,
    payload: {
      interval: overrides.interval ?? "1m",
      open_ts: openTs,
      close_ts: openTs,
      aggregation_source: "external",
      open: "65000.1",
      high: "65010.0",
      low: "64990.0",
      close: overrides.close ?? "65005.5",
      volume: "12.34",
      trade_count: 128,
      revision: overrides.revision ?? 0,
    },
  };
}

function pageResponse(
  bars: BarMessageEnvelope[],
  options: { hasMore?: boolean; nextStart?: string | null } = {},
) {
  return HttpResponse.json({
    bars,
    page: {
      has_more: options.hasMore ?? false,
      next_start: options.nextStart ?? null,
    },
  });
}

function barsUrl(params: Record<string, string>): string {
  return `/bars?${new URLSearchParams(params).toString()}`;
}

const HISTORICAL = {
  instrument_id: INSTRUMENT,
  interval: "1m",
  start: "2024-01-01T00:00:00.000Z",
  end: "2024-01-01T00:02:00.000Z",
};

afterEach(() => {
  vi.restoreAllMocks();
  // jsdom does not implement these; tests that need them define them locally.
  delete (URL as unknown as { createObjectURL?: unknown }).createObjectURL;
  delete (URL as unknown as { revokeObjectURL?: unknown }).revokeObjectURL;
});

describe("Bars page filter and URL state", () => {
  it("defaults to the latest 24 hours and never invents a query", async () => {
    let barsRequests = 0;
    server.use(
      http.get("*/api/v1/bars", () => {
        barsRequests += 1;
        return pageResponse([]);
      }),
    );
    renderApp({ initialEntries: ["/bars"] });

    const intervalSelect = screen.getByLabelText(
      "Bar Interval",
    ) as HTMLSelectElement;
    expect(
      Array.from(intervalSelect.options).map((option) => option.value),
    ).toEqual(["", "1m", "5m", "15m", "1h", "1d"]);

    const startInput = screen.getByLabelText("开始（闭区间）");
    const endInput = screen.getByLabelText("结束（开区间）");
    expect(startInput).toHaveValue();
    expect(endInput).toHaveValue();

    const start = parseDateTimeInput(
      (startInput as HTMLInputElement).value,
      "utc",
    );
    const end = parseDateTimeInput(
      (endInput as HTMLInputElement).value,
      "utc",
    );
    expect(start).not.toBeNull();
    expect(end).not.toBeNull();
    expect(end!.getTime() - start!.getTime()).toBe(24 * 60 * 60 * 1000);

    expect(screen.getByText("没有数据")).toBeInTheDocument();
    expect(
      screen.getByText(/不会自动扩大时间范围/),
    ).toBeInTheDocument();
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(barsRequests).toBe(0);
  });

  it("round-trips applied values through the URL and preserves the instant across zone display", async () => {
    const user = userEvent.setup();
    const requested: URL[] = [];
    server.use(
      http.get("*/api/v1/bars", ({ request }) => {
        requested.push(new URL(request.url));
        return pageResponse([envelope()]);
      }),
    );
    renderApp({ initialEntries: [barsUrl(HISTORICAL)] });

    expect(await screen.findByTestId("applied-instrument")).toHaveTextContent(
      INSTRUMENT,
    );
    expect(screen.getByTestId("applied-interval")).toHaveTextContent("1m");
    expect(screen.getByTestId("applied-start")).toHaveTextContent(
      "2024-01-01T00:00:00.000Z",
    );
    expect(screen.getByTestId("applied-end")).toHaveTextContent(
      "2024-01-01T00:02:00.000Z",
    );
    expect(screen.getByLabelText("开始（闭区间）")).toHaveValue(
      "2024-01-01T00:00",
    );

    await waitFor(() => expect(requested.length).toBeGreaterThan(0));
    expect(requested[0]?.searchParams.get("instrument")).toBe(INSTRUMENT);
    expect(requested[0]?.searchParams.get("interval")).toBe("1m");
    expect(requested[0]?.searchParams.get("start")).toBe(HISTORICAL.start);
    expect(requested[0]?.searchParams.get("end")).toBe(HISTORICAL.end);
    expect(requested[0]?.searchParams.get("limit")).toBe("5000");

    await user.selectOptions(screen.getByLabelText("显示时区"), "local");

    expect(
      parseIsoInstant(screen.getByTestId("applied-start").textContent ?? "")
        ?.toISOString(),
    ).toBe(HISTORICAL.start);
    expect(
      parseIsoInstant(screen.getByTestId("applied-end").textContent ?? "")
        ?.toISOString(),
    ).toBe(HISTORICAL.end);
  });

  it("applies edited controls to the URL-backed query", async () => {
    const user = userEvent.setup();
    const requested: URL[] = [];
    server.use(
      http.get("*/api/v1/bars", ({ request }) => {
        requested.push(new URL(request.url));
        return pageResponse([envelope({ openTs: "2024-02-01T10:00:00.000000000Z" })]);
      }),
    );
    renderApp({ initialEntries: [barsUrl(HISTORICAL)] });

    const start = screen.getByLabelText("开始（闭区间）");
    const end = screen.getByLabelText("结束（开区间）");
    fireEvent.change(start, { target: { value: "2024-02-01T10:00" } });
    fireEvent.change(end, { target: { value: "2024-02-01T12:00" } });
    await user.click(screen.getByRole("button", { name: "应用筛选" }));

    expect(await screen.findByTestId("applied-start")).toHaveTextContent(
      "2024-02-01T10:00:00.000Z",
    );
    expect(screen.getByTestId("applied-end")).toHaveTextContent(
      "2024-02-01T12:00:00.000Z",
    );
    await waitFor(() =>
      expect(
        requested.some(
          (url) => url.searchParams.get("start") === "2024-02-01T10:00:00.000Z",
        ),
      ).toBe(true),
    );
  });

  it("rejects a chart range over 31 UTC days without querying JSON and points to Export Job", async () => {
    let barsRequests = 0;
    server.use(
      http.get("*/api/v1/bars", () => {
        barsRequests += 1;
        return pageResponse([]);
      }),
    );
    const start = new Date("2024-01-01T00:00:00.000Z");
    const end = new Date(start.getTime() + 32 * 24 * 60 * 60 * 1000);
    renderApp({
      initialEntries: [
        barsUrl({
          instrument_id: INSTRUMENT,
          interval: "1m",
          start: start.toISOString(),
          end: end.toISOString(),
        }),
      ],
    });

    expect(
      (await screen.findAllByText(/图表与原始 Revision 仅支持 ≤31 个 UTC 天/))
        .length,
    ).toBeGreaterThan(0);
    expect(screen.getByText(/当前范围：32 个 UTC 天/)).toBeInTheDocument();
    expect(barsRequests).toBe(0);
    expect(
      screen.getByRole("button", { name: "直接下载 CSV（≤31 天）" }),
    ).toBeDisabled();
    expect(
      screen.getByRole("button", { name: "创建 Export Job" }),
    ).toBeDisabled();
    await waitFor(() =>
      expect(screen.getByTestId("runtime-mode-banner")).toHaveAttribute(
        "data-mode",
        "current-contract",
      ),
    );
    expect(screen.getAllByText("需要 Admin API").length).toBeGreaterThan(0);
  });
});

describe("Bars page data and revision views", () => {
  it("shows every raw Revision while the chart keeps the max Revision and exact strings", async () => {
    const rows = [
      envelope({ revision: 0, close: "65005.5" }),
      envelope({ revision: 2, close: "0.000000000000000001" }),
    ];
    server.use(http.get("*/api/v1/bars", () => pageResponse(rows)));

    renderApp({ initialEntries: [barsUrl(HISTORICAL)] });

    expect(await screen.findByTestId("history-summary")).toHaveTextContent(
      "共 2 条原始 Revision（未去重）",
    );
    expect(screen.getByTestId("history-summary")).toHaveTextContent(
      "1 个最大 Revision 点",
    );

    const revisionTable = screen.getByRole("region", {
      name: "原始 Revision 表",
    });
    const table = within(revisionTable).getByRole("table");
    const bodyRows = within(table).getAllByRole("row");
    // Header + two raw revisions.
    expect(bodyRows).toHaveLength(3);
    expect(within(revisionTable).getByText("0.000000000000000001")).toBeInTheDocument();
    expect(within(revisionTable).getByText("65005.5")).toBeInTheDocument();

    const chart = screen.getByRole("region", { name: "最大 Revision Bar 图" });
    expect(
      within(chart).getAllByText("0.000000000000000001").length,
    ).toBeGreaterThan(0);

    const details = within(revisionTable).getAllByText("原始 JSON")[1]!;
    await userEvent.setup().click(details);
    expect(
      within(revisionTable).getByText(/"close": "0.000000000000000001"/),
    ).toBeInTheDocument();
  });

  it("stops on a non-advancing cursor and reports partial results honestly", async () => {
    let requests = 0;
    server.use(
      http.get("*/api/v1/bars", () => {
        requests += 1;
        return pageResponse([envelope()], {
          hasMore: true,
          nextStart: "2024-01-01T00:00:00.000Z",
        });
      }),
    );

    renderApp({ initialEntries: [barsUrl(HISTORICAL)] });

    expect(
      await screen.findByText(/分页已停止：next_start 未严格前进/),
    ).toBeInTheDocument();
    expect(screen.getByTestId("history-summary")).toHaveTextContent(
      "分页已停止（部分结果）",
    );
    expect(requests).toBe(1);
  });

  it("maps the 100,000-row JSON ceiling to a shrink-range message with Request ID", async () => {
    server.use(
      http.get("*/api/v1/bars", () =>
        HttpResponse.json(
          {
            error: {
              code: "range_too_large",
              message:
                "JSON output is limited to 100000 Bars; use format=arrow or format=csv for larger ranges",
              details: { row_count: 100001, limit: 100000 },
              request_id: "ceiling-request-id",
            },
          },
          { status: 400 },
        ),
      ),
    );

    renderApp({ initialEntries: [barsUrl(HISTORICAL)] });

    expect(
      await screen.findByText(/原始行超过 100,000 行/),
    ).toBeInTheDocument();
    expect(screen.getByText(/row_count=100,001/)).toBeInTheDocument();
    expect(screen.getByText(/ceiling-request-id/)).toBeInTheDocument();
  });
});

describe("Bars page CSV direct path", () => {
  const CSV_TEXT = [
    "schema_version,data_type,instrument_id,interval,open_ts,close_ts,event_ts,ingest_ts,feed,seq,aggregation_source,open,high,low,close,volume,trade_count,revision",
    "1,bar,binance:BTC/USDT,1m,2024-01-01T00:00:00.000000000Z,2024-01-01T00:01:00.000000000Z,2024-01-01T00:01:00.000000000Z,2026-11-09T04:24:42.946489000Z,fake,,external,65000.1,65010.0,64990.0,65005.5,12.34,128,0",
    "",
  ].join("\n");

  function readBlobText(blob: Blob): Promise<string> {
    if (typeof blob.text === "function") {
      return blob.text();
    }
    return new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = () => resolve(String(reader.result ?? ""));
      reader.onerror = () => reject(reader.error);
      reader.readAsText(blob);
    });
  }

  it("downloads the exact server Blob with the Content-Disposition filename", async () => {
    server.use(
      http.get("*/api/v1/bars", ({ request }) => {
        const url = new URL(request.url);
        if (url.searchParams.get("format") === "csv") {
          return HttpResponse.text(CSV_TEXT, {
            headers: {
              "Content-Type": "text/csv; charset=utf-8",
              "Content-Disposition": 'inline; filename="1m-bars.csv"',
            },
          });
        }
        return pageResponse([envelope()]);
      }),
    );

    const blobs: Blob[] = [];
    Object.defineProperty(URL, "createObjectURL", {
      configurable: true,
      writable: true,
      value: (blob: Blob) => {
        blobs.push(blob);
        return "blob:csv-test";
      },
    });
    Object.defineProperty(URL, "revokeObjectURL", {
      configurable: true,
      writable: true,
      value: () => {},
    });
    let downloadedName: string | null = null;
    vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(
      function (this: HTMLAnchorElement) {
        downloadedName = this.download;
      },
    );

    const user = userEvent.setup();
    renderApp({ initialEntries: [barsUrl(HISTORICAL)] });
    await screen.findByTestId("history-summary");

    await user.click(
      screen.getByRole("button", { name: "直接下载 CSV（≤31 天）" }),
    );

    expect(
      await screen.findByText(/已触发浏览器下载/),
    ).toBeInTheDocument();
    expect(screen.getByText("1m-bars.csv")).toBeInTheDocument();
    expect(downloadedName).toBe("1m-bars.csv");
    expect(blobs).toHaveLength(1);
    expect(await readBlobText(blobs[0]!)).toBe(CSV_TEXT);
  });

  it("surfaces a Blob error envelope with the Request ID", async () => {
    server.use(
      http.get("*/api/v1/bars", ({ request }) => {
        const url = new URL(request.url);
        if (url.searchParams.get("format") === "csv") {
          return new HttpResponse(
            JSON.stringify({
              error: {
                code: "range_too_large",
                message: "too many rows",
                details: { row_count: 100001 },
                request_id: "csv-error-request-id",
              },
            }),
            {
              status: 400,
              headers: { "Content-Type": "application/json" },
            },
          );
        }
        return pageResponse([envelope()]);
      }),
    );

    const user = userEvent.setup();
    renderApp({ initialEntries: [barsUrl(HISTORICAL)] });
    await screen.findByTestId("history-summary");

    await user.click(
      screen.getByRole("button", { name: "直接下载 CSV（≤31 天）" }),
    );

    expect(
      await screen.findByText("CSV 下载失败"),
    ).toBeInTheDocument();
    expect(screen.getByText(/range_too_large/)).toBeInTheDocument();
    expect(screen.getByText(/csv-error-request-id/)).toBeInTheDocument();
  });

  it("supports aborting an in-flight download", async () => {
    server.use(
      http.get("*/api/v1/bars", async ({ request }) => {
        const url = new URL(request.url);
        if (url.searchParams.get("format") === "csv") {
          await new Promise((resolve) => setTimeout(resolve, 1000));
          return HttpResponse.text(CSV_TEXT);
        }
        return pageResponse([envelope()]);
      }),
    );

    const user = userEvent.setup();
    renderApp({ initialEntries: [barsUrl(HISTORICAL)] });
    await screen.findByTestId("history-summary");

    await user.click(
      screen.getByRole("button", { name: "直接下载 CSV（≤31 天）" }),
    );
    await user.click(screen.getByRole("button", { name: "取消下载" }));

    expect(
      await screen.findByText(/下载已取消（AbortController）/),
    ).toBeInTheDocument();
  });
});

describe("Bars page export boundary", () => {
  const basis = new Date("2024-01-01T00:00:00.000Z");

  function rangeUrl(days: number): string {
    return barsUrl({
      instrument_id: INSTRUMENT,
      interval: "1m",
      start: basis.toISOString(),
      end: new Date(
        basis.getTime() + days * 24 * 60 * 60 * 1000,
      ).toISOString(),
    });
  }

  it("enables direct CSV at exactly 31 days and does not offer an Export Job", async () => {
    server.use(http.get("*/api/v1/bars", () => pageResponse([envelope()])));
    renderApp({ initialEntries: [rangeUrl(31)] });

    expect(
      await screen.findByRole("button", { name: "直接下载 CSV（≤31 天）" }),
    ).toBeEnabled();
    expect(screen.getByText(/不需要 Export Job/)).toBeInTheDocument();
  });

  it("routes 31–366 days away from Blob downloads", async () => {
    server.use(http.get("*/api/v1/bars", () => pageResponse([])));
    renderApp({ initialEntries: [rangeUrl(45)] });

    expect(
      await screen.findByRole("button", { name: "直接下载 CSV（≤31 天）" }),
    ).toBeDisabled();
    expect(
      screen.getByText(/只创建 Admin Export Job，不自动退回超大 Blob/),
    ).toBeInTheDocument();
  });

  it("shows split guidance above 366 days", async () => {
    renderApp({ initialEntries: [rangeUrl(800)] });

    const exportPanel = await screen.findByRole("region", {
      name: "Export Job",
    });
    expect(
      within(exportPanel).getByText(/需要拆分为 3 个 Export Job/),
    ).toBeInTheDocument();
    expect(
      within(exportPanel).getAllByText(/不支持多 Instrument ZIP/).length,
    ).toBeGreaterThan(0);
    expect(
      screen.getByRole("button", { name: "创建 Export Job" }),
    ).toBeDisabled();
  });
});

describe("Bars page live follow", () => {
  class FakeSocket {
    readyState = 0;
    onopen: ((event: Event) => void) | null = null;
    onmessage: ((event: MessageEvent) => void) | null = null;
    onclose: ((event: CloseEvent) => void) | null = null;
    onerror: ((event: Event) => void) | null = null;
    readonly sent: string[] = [];

    send(data: string): void {
      this.sent.push(data);
    }
    close(): void {
      this.readyState = 3;
    }
    fireOpen(): void {
      this.readyState = 1;
      this.onopen?.(new Event("open"));
    }
    fireEvent(data: unknown): void {
      this.onmessage?.({ data: JSON.stringify(data) } as MessageEvent);
    }
    fireServerClose(): void {
      this.readyState = 3;
      this.onclose?.({
        code: 1006,
        reason: "server gone",
        wasClean: false,
      } as CloseEvent);
    }
  }

  function setupLivePage() {
    const sockets: FakeSocket[] = [];
    const factory = vi.fn(() => {
      const socket = new FakeSocket();
      sockets.push(socket);
      return socket;
    });
    const now = new Date();
    const start = new Date(now.getTime() - 60 * 60 * 1000);
    const end = new Date(now.getTime() + 60 * 60 * 1000);
    const liveOpenTs = new Date(
      now.getTime() - 10 * 60 * 1000,
    ).toISOString();
    server.use(
      http.get("*/api/v1/bars", () =>
        pageResponse([
          envelope({
            openTs: new Date(now.getTime() - 30 * 60 * 1000).toISOString(),
          }),
        ]),
      ),
    );
    const rendered = renderApp({
      initialEntries: [
        barsUrl({
          instrument_id: INSTRUMENT,
          interval: "1m",
          start: start.toISOString(),
          end: end.toISOString(),
        }),
      ],
      wsFactory: factory,
    });
    return { ...rendered, sockets, factory, liveOpenTs };
  }

  it("does not connect for a historical-only range", async () => {
    const factory = vi.fn();
    server.use(http.get("*/api/v1/bars", () => pageResponse([envelope()])));
    renderApp({
      initialEntries: [barsUrl(HISTORICAL)],
      wsFactory: factory,
    });

    expect(
      await screen.findByRole("checkbox", { name: "实时跟随" }),
    ).toBeDisabled();
    expect(
      screen.getByText(/当前范围不包含现在，不能开启实时跟随/),
    ).toBeInTheDocument();
    expect(factory).not.toHaveBeenCalled();
  });

  it("connects only after explicit enable, appends exact strings, and marks segment discontinuity", async () => {
    const { sockets, factory, liveOpenTs } = setupLivePage();
    const user = userEvent.setup();

    await screen.findByTestId("history-summary");
    expect(factory).not.toHaveBeenCalled();

    await user.click(screen.getByRole("checkbox", { name: "实时跟随" }));
    await waitFor(() => expect(sockets).toHaveLength(1));
    sockets[0]!.fireOpen();

    expect(
      await screen.findByText("接收中（新接收段）"),
    ).toBeInTheDocument();
    expect(sockets[0]!.sent).toContain(
      JSON.stringify({
        op: "subscribe",
        topics: [`market.bar.1m.${INSTRUMENT}`],
      }),
    );

    sockets[0]!.fireEvent({
      op: "event",
      topic: `market.bar.1m.${INSTRUMENT}`,
      data: envelope({
        openTs: liveOpenTs,
        revision: 5,
        close: "123.456789012345678901",
      }),
    });

    expect(
      (await screen.findAllByText("123.456789012345678901")).length,
    ).toBeGreaterThan(0);
    expect(screen.getAllByText("WS 实时").length).toBeGreaterThan(0);

    sockets[0]!.fireServerClose();
    expect(
      await screen.findByText("接收段已中断"),
    ).toBeInTheDocument();

    await user.click(
      screen.getByRole("button", { name: /重新连接（新建接收段）/ }),
    );
    await waitFor(() => expect(sockets).toHaveLength(2));
    sockets[1]!.fireOpen();

    expect(
      await screen.findByText(/存在多个接收段：断线后的新连接与上一段不连续/),
    ).toBeInTheDocument();
    expect(screen.getByText("接收段 #1")).toBeInTheDocument();
    expect(screen.getByText("接收段 #2")).toBeInTheDocument();
  });
});
