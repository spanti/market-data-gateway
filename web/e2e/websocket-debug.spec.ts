import { expect, test, type Page } from "@playwright/test";
import { emitBars, expectNoBannedUiSymbols } from "./support";

/**
 * Real WebSocket Debug acceptance against the local gateway.
 *
 * Events travel collector -> ZMQ -> FastAPI WebSocketHub -> browser. The
 * control endpoint only enqueues closed Bars; it does not inject WebSocket
 * frames.
 */

async function openWebSocketDebug(page: Page) {
  await page.goto("/console");
  await page.getByRole("tab", { name: "WebSocket Debug" }).click();
  await expect(page.getByRole("heading", { name: "WebSocket Debug" })).toBeVisible();
}

function retainedCountFrom(text: string): number {
  return Number(/界面保留 (\d+)/.exec(text.replace(/\s+/g, " "))?.[1] ?? "0");
}

test("bounded virtual list, ping barrier and receive segments use the real WS", async ({
  page,
  request,
}) => {
  await openWebSocketDebug(page);

  await page.getByRole("button", { name: "连接", exact: true }).click();
  await expect(page.getByTestId("ws-connection-state")).toHaveText("已连接");
  await expect(page.getByTestId("ws-segment-state")).toHaveText("接收段 1");

  // WebSocket Session Subscription filters this debug session only. It must
  // stay visibly distinct from Feed Subscription's acquisition scope.
  await expect(
    page.getByRole("group", { name: "WebSocket Session Subscription" }),
  ).toContainText("不改变 Feed Subscription 的采集范围");
  await expect(page.getByTestId("ws-topic-preview")).toHaveText(
    "market.bar.1m.",
  );
  await page.getByRole("button", { name: "订阅", exact: true }).click();
  await expect(page.getByText(/没有订阅 ACK/).first()).toBeVisible();
  await expect(page.getByText(/订阅已确认/)).toHaveCount(0);

  // pong is an ordering barrier. It shows 已同步 and never an ACK claim.
  await page.getByRole("button", { name: "Ping", exact: true }).click();
  await expect(page.getByTestId("ws-sync-state")).toHaveText("已同步");
  await expect(page.getByText(/订阅已确认/)).toHaveCount(0);

  // The real collector ingests these Bars and the API fans them out.
  await emitBars(request, 80);

  const retained = page.getByTestId("ws-retained");
  await expect(retained).toBeVisible();
  await expect
    .poll(
      async () => retainedCountFrom(await retained.innerText()),
      { timeout: 30_000 },
    )
    .toBeGreaterThanOrEqual(20);

  const retainedCount = retainedCountFrom(await retained.innerText());
  expect(retainedCount).toBeGreaterThanOrEqual(20);

  // Virtualization: only a window of rows is mounted in the DOM.
  const domMessages = await page.getByTestId("ws-message").count();
  expect(domMessages).toBeGreaterThan(0);
  expect(domMessages).toBeLessThan(retainedCount);

  // The message list scrolls inside its own card, not the page.
  const listMetrics = await page.getByTestId("ws-message-list").evaluate((element) => ({
    overflowY: window.getComputedStyle(element).overflowY,
    scrollHeight: element.scrollHeight,
    clientHeight: element.clientHeight,
  }));
  expect(["auto", "scroll"]).toContain(listMetrics.overflowY);
  expect(listMetrics.scrollHeight).toBeGreaterThan(listMetrics.clientHeight);

  // Raw JSON is exact text evidence, marked as such, and scrolls locally.
  const firstEvent = page
    .getByTestId("ws-message")
    .filter({ hasText: "event" })
    .first();
  await firstEvent.getByText("原始 JSON").click();
  const detail = firstEvent.getByTestId("ws-message-detail");
  await expect(detail).toHaveAttribute("data-raw-evidence", "true");
  await expect(detail).toContainText('"data_type": "bar"');
  const detailOverflow = await detail.evaluate(
    (element) => window.getComputedStyle(element).overflowY,
  );
  expect(["auto", "scroll"]).toContain(detailOverflow);

  // Local UI drops and server lag_notice drops stay separate counters.
  await expect(page.getByTestId("ws-local-dropped")).toContainText(
    "界面丢弃数 0",
  );
  await expect(page.getByTestId("ws-server-lag")).toContainText(
    "服务端丢弃（lag_notice） 0",
  );

  // Every UI-generated string on the WS tab avoids the banned separators;
  // the raw JSON evidence is exempt.
  await expectNoBannedUiSymbols(page, "#main-content");

  // Disconnect ends segment 1 and marks the interruption; no seamless resume.
  await page.getByRole("button", { name: "断开", exact: true }).click();
  await expect(page.getByTestId("ws-connection-state")).toContainText(
    "接收段 1 中断",
  );
  await expect(page.getByTestId("ws-interruption")).toHaveText(
    "接收段 1 已中断。没有恢复位点，重连不会补齐缺失报文。",
  );

  // Reconnect starts a new segment, not a recovery position.
  await page.getByRole("button", { name: "连接", exact: true }).click();
  await expect(page.getByTestId("ws-connection-state")).toHaveText("已连接");
  await expect(page.getByTestId("ws-segment-state")).toHaveText("接收段 2");
});
