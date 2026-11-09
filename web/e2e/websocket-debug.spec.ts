import { expect, test, type Page } from "@playwright/test";
import { emitBars } from "./support";

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

test("bounded virtual list, ping barrier and receive segments use the real WS", async ({
  page,
  request,
}) => {
  await openWebSocketDebug(page);

  await page.getByRole("button", { name: "连接", exact: true }).click();
  await expect(page.getByText("已连接 · 接收段 1")).toBeVisible();

  // Controlled topic form: 1m prefix for all instruments.
  await expect(page.getByTestId("ws-topic-preview")).toHaveText("market.bar.1m.");
  await page.getByRole("button", { name: "订阅", exact: true }).click();
  await expect(page.getByText(/没有订阅 ACK/).first()).toBeVisible();
  await expect(page.getByText(/订阅已确认/)).toHaveCount(0);

  await page.getByRole("button", { name: "Ping" }).click();
  await expect(page.getByTestId("ws-sync-state")).toHaveText("已同步");

  // The real collector ingests these Bars and the API fans them out.
  await emitBars(request, 80);

  const retained = page.getByText(/界面保留 \d+ \/ 10,000/);
  await expect(retained).toBeVisible();
  await expect
    .poll(
      async () => {
        const text = (await retained.innerText()).replace(/\s+/g, " ");
        return Number(/界面保留 (\d+)/.exec(text)?.[1] ?? "0");
      },
      { timeout: 30_000 },
    )
    .toBeGreaterThanOrEqual(20);

  const retainedText = (await retained.innerText()).replace(/\s+/g, " ");
  const retainedCount = Number(/界面保留 (\d+)/.exec(retainedText)?.[1] ?? "0");
  expect(retainedCount).toBeGreaterThanOrEqual(20);

  // Virtualization: only a window of rows is mounted in the DOM.
  const domMessages = await page.getByTestId("ws-message").count();
  expect(domMessages).toBeGreaterThan(0);
  expect(domMessages).toBeLessThan(retainedCount);

  // Local and server drop counters are separate and both visible.
  await expect(page.getByTestId("ws-local-dropped")).toContainText("界面丢弃数 0");
  await expect(page.getByTestId("ws-server-lag")).toContainText(
    "服务端丢弃（lag_notice） 0",
  );

  // Disconnect ends segment 1 and marks the interruption; no seamless resume.
  await page.getByRole("button", { name: "断开", exact: true }).click();
  await expect(page.getByText("接收段 1 中断").first()).toBeVisible();
  await expect(
    page.getByText(/接收段 1 已中断；没有恢复位点，重连不会补齐缺失报文。/),
  ).toBeVisible();

  // Reconnect starts a new segment, not a recovery position.
  await page.getByRole("button", { name: "连接", exact: true }).click();
  await expect(page.getByText("已连接 · 接收段 2")).toBeVisible();
  await expect(page.getByText(/接收段 2/).first()).toBeVisible();
});
