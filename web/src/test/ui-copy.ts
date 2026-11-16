import { expect } from "vitest";

/**
 * Separators UI-generated copy must never contain. Raw evidence (audit Diff,
 * backend error text, WebSocket JSON) is exempt and must render verbatim.
 */
export const BANNED_UI_SYMBOLS = ["·", "："] as const;

/**
 * Attribute copy that reaches users visually or through the accessibility
 * tree. `aria-valuetext` and `aria-roledescription` are included because
 * screen readers announce them as interface text.
 */
const COPY_ATTRIBUTES = [
  "placeholder",
  "aria-label",
  "aria-description",
  "aria-placeholder",
  "aria-roledescription",
  "aria-valuetext",
  "title",
  "alt",
] as const;

const SKIPPED_TAGS = new Set(["SCRIPT", "STYLE", "NOSCRIPT", "TEMPLATE"]);

export interface BannedUiSymbolHit {
  symbol: string;
  text: string;
}

function isRawEvidence(element: Element): boolean {
  return element.closest('[data-raw-evidence="true"]') !== null;
}

function collect(text: string, hits: BannedUiSymbolHit[]): void {
  const trimmed = text.trim().slice(0, 160);
  if (!trimmed) {
    return;
  }
  for (const symbol of BANNED_UI_SYMBOLS) {
    if (trimmed.includes(symbol)) {
      hits.push({ symbol, text: trimmed });
    }
  }
}

function elementsIn(root: ParentNode): Element[] {
  const elements = Array.from(root.querySelectorAll("*"));
  return root instanceof Element ? [root, ...elements] : elements;
}

/**
 * Collects banned separators from rendered text nodes (including visually
 * hidden assistive text) and from accessibility/visible attribute copy.
 * Nodes inside `data-raw-evidence="true"` containers are exempt because they
 * render backend evidence verbatim.
 */
export function bannedUiSymbolHits(
  root: ParentNode = document.body,
): BannedUiSymbolHit[] {
  const hits: BannedUiSymbolHit[] = [];
  const walker = document.createTreeWalker(root as Node, NodeFilter.SHOW_TEXT);
  while (walker.nextNode()) {
    const node = walker.currentNode;
    const element = node.parentElement;
    if (
      !element ||
      SKIPPED_TAGS.has(element.tagName) ||
      isRawEvidence(element)
    ) {
      continue;
    }
    collect(node.textContent ?? "", hits);
  }

  for (const element of elementsIn(root)) {
    if (SKIPPED_TAGS.has(element.tagName) || isRawEvidence(element)) {
      continue;
    }
    for (const attribute of COPY_ATTRIBUTES) {
      const value = element.getAttribute(attribute);
      if (value) {
        collect(`[${attribute}] ${value}`, hits);
      }
    }
  }

  return hits;
}

export function expectNoBannedUiSymbols(root?: ParentNode): void {
  expect(bannedUiSymbolHits(root)).toEqual([]);
}
