import { afterEach, describe, expect, it } from "vitest";
import { bannedUiSymbolHits } from "@/test/ui-copy";

let root: HTMLElement | null = null;

afterEach(() => {
  root?.remove();
  root = null;
});

function mount(html: string): HTMLElement {
  root = document.createElement("div");
  root.innerHTML = html;
  document.body.append(root);
  return root;
}

describe("banned UI symbol scan", () => {
  it("finds separators in rendered text", () => {
    const scope = mount("<p>数据源 · Collector</p>");

    expect(bannedUiSymbolHits(scope)).toEqual([
      { symbol: "·", text: "数据源 · Collector" },
    ]);
  });

  it("finds separators in accessible and visible attribute copy", () => {
    const scope = mount(`
      <input placeholder="输入 · 值" />
      <div aria-label="说明：字段"></div>
      <span title="提示 · 文本"></span>
      <img alt="图：示例" src="" />
    `);

    expect(bannedUiSymbolHits(scope).map((hit) => hit.text)).toEqual([
      "[placeholder] 输入 · 值",
      "[aria-label] 说明：字段",
      "[title] 提示 · 文本",
      "[alt] 图：示例",
    ]);
  });

  it("scans assistive-only text instead of skipping it", () => {
    const scope = mount(
      '<span class="sr-only">表格说明 · 隐藏文本</span>',
    );

    expect(bannedUiSymbolHits(scope)).toEqual([
      { symbol: "·", text: "表格说明 · 隐藏文本" },
    ]);
  });

  it("exempts raw evidence text and attributes", () => {
    const scope = mount(`
      <div data-raw-evidence="true" aria-label="raw · aria">
        原始 · 错误：timeout
      </div>
      <p>界面文案干净</p>
    `);

    expect(bannedUiSymbolHits(scope)).toEqual([]);
  });

  it("reports each banned symbol independently", () => {
    const scope = mount('<p aria-label="a · b：c"></p>');

    expect(bannedUiSymbolHits(scope).map((hit) => hit.symbol)).toEqual([
      "·",
      "：",
    ]);
  });
});
