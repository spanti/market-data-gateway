import { expect } from "vitest";

/**
 * Separators UI-generated copy must never contain. Raw evidence (audit Diff,
 * backend error text, WebSocket JSON) is exempt and must render verbatim.
 */
export const BANNED_UI_SYMBOLS = ["·", "："] as const;

export interface BannedUiSymbolHit {
  symbol: string;
  text: string;
}

/**
 * Collects banned separators from rendered text nodes, excluding nodes inside
 * `data-raw-evidence="true"` containers and visually hidden assistive text.
 * Mirrors the Playwright helper so unit and browser scans share one boundary.
 */
export function bannedUiSymbolHits(
  root: ParentNode = document.body,
): BannedUiSymbolHit[] {
  const hits: BannedUiSymbolHit[] = [];
  const walker = document.createTreeWalker(root as Node, NodeFilter.SHOW_TEXT);
  const skipped = new Set(["SCRIPT", "STYLE", "NOSCRIPT", "TEMPLATE"]);
  while (walker.nextNode()) {
    const node = walker.currentNode;
    const element = node.parentElement;
    if (!element || skipped.has(element.tagName)) {
      continue;
    }
    if (element.closest('[data-raw-evidence="true"]')) {
      continue;
    }
    if (element.closest(".sr-only")) {
      continue;
    }
    const text = node.textContent ?? "";
    for (const symbol of BANNED_UI_SYMBOLS) {
      if (text.includes(symbol)) {
        hits.push({ symbol, text: text.trim().slice(0, 160) });
      }
    }
  }
  return hits;
}

export function expectNoBannedUiSymbols(root?: ParentNode): void {
  expect(bannedUiSymbolHits(root)).toEqual([]);
}
