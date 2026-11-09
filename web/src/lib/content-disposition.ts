/**
 * Content-Disposition filename handling for CSV/export downloads.
 *
 * The current backend only emits the simple quoted `filename` parameter, but
 * Admin Export Job responses may use RFC 5987 `filename*`. Both are parsed,
 * `filename*` wins, and the result is reduced to a safe basename. A download
 * is triggered as `attachment` even when the server says `inline`.
 */

const FALLBACK_FILENAME = "bars.csv";
const MAX_FILENAME_LENGTH = 150;

function splitHeaderParams(header: string): string[] {
  const parts: string[] = [];
  let current = "";
  let inQuotes = false;

  for (let index = 0; index < header.length; index += 1) {
    const char = header[index];
    if (char === undefined) {
      break;
    }
    if (char === "\\" && inQuotes) {
      current += char;
      const next = header[index + 1];
      if (next !== undefined) {
        current += next;
        index += 1;
      }
      continue;
    }
    if (char === '"') {
      inQuotes = !inQuotes;
      current += char;
      continue;
    }
    if (char === ";" && !inQuotes) {
      parts.push(current);
      current = "";
      continue;
    }
    current += char;
  }
  parts.push(current);
  return parts;
}

function unquote(value: string): string {
  const trimmed = value.trim();
  if (trimmed.length >= 2 && trimmed.startsWith('"') && trimmed.endsWith('"')) {
    return trimmed
      .slice(1, -1)
      .replace(/\\(["\\])/g, "$1");
  }
  return trimmed;
}

function parseParams(header: string): Map<string, string> {
  const params = new Map<string, string>();
  const [, ...rest] = splitHeaderParams(header);
  for (const part of rest) {
    const equals = part.indexOf("=");
    if (equals < 0) {
      continue;
    }
    const key = part.slice(0, equals).trim().toLowerCase();
    if (!key) {
      continue;
    }
    params.set(key, unquote(part.slice(equals + 1)));
  }
  return params;
}

function decodeLatin1(value: string): string {
  return value.replace(/%([0-9A-Fa-f]{2})/g, (_match, hex: string) =>
    String.fromCharCode(Number.parseInt(hex, 16)),
  );
}

function decodeExtendedValue(value: string): string | null {
  const match = /^([^']*)'([^']*)'(.*)$/.exec(value);
  const charset = (match?.[1] ?? "utf-8").toLowerCase();
  const encoded = match ? (match[3] ?? "") : value;

  if (charset !== "utf-8" && charset !== "iso-8859-1" && charset !== "") {
    return null;
  }
  try {
    if (charset === "iso-8859-1") {
      const decoded = decodeLatin1(encoded);
      return decoded.length > 0 ? decoded : null;
    }
    const decoded = decodeURIComponent(encoded);
    return decoded.length > 0 ? decoded : null;
  } catch {
    return null;
  }
}

/**
 * Reduces a server-provided name to a safe basename. Returns null when nothing
 * safe remains, so the caller must use its fallback.
 */
function stripControlCharacters(input: string): string {
  let result = "";
  for (const char of input) {
    const code = char.charCodeAt(0);
    if (code <= 0x1f || code === 0x7f) {
      continue;
    }
    result += char;
  }
  return result;
}

export function sanitizeFilename(input: string): string | null {
  if (!input) {
    return null;
  }
  const withoutControls = stripControlCharacters(input);
  const normalized = withoutControls.replace(/\\/g, "/");
  const segments = normalized.split("/");
  const basename = segments[segments.length - 1] ?? "";

  const cleaned = basename
    .replace(/[<>:"|?*]/g, "")
    .replace(/^[.\s]+/, "")
    .replace(/[.\s]+$/, "");

  if (!cleaned || cleaned === "." || cleaned === "..") {
    return null;
  }
  return cleaned.slice(0, MAX_FILENAME_LENGTH);
}

export function parseContentDispositionFilename(
  header: string | null | undefined,
  fallback: string = FALLBACK_FILENAME,
): string {
  if (!header) {
    return fallback;
  }

  const params = parseParams(header);
  const extended = params.get("filename*");
  const simple = params.get("filename");

  let candidate: string | null = extended
    ? decodeExtendedValue(extended)
    : null;
  if (!candidate && simple) {
    candidate = simple;
  }
  if (!candidate) {
    return fallback;
  }

  return sanitizeFilename(candidate) ?? fallback;
}

/**
 * Triggers a native browser download for an in-memory Blob. Used by the direct
 * small-CSV path, including `inline` responses.
 */
export function triggerBlobDownload(blob: Blob, filename: string): void {
  const safeName = sanitizeFilename(filename) ?? FALLBACK_FILENAME;
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = safeName;
  anchor.rel = "noopener";
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  setTimeout(() => URL.revokeObjectURL(url), 0);
}
