import { describe, expect, it } from "vitest";
import {
  parseContentDispositionFilename,
  sanitizeFilename,
} from "@/lib/content-disposition";

describe("Content-Disposition filename parsing", () => {
  it("parses a quoted filename", () => {
    expect(
      parseContentDispositionFilename(
        'attachment; filename="1m-bars.csv"',
        "fallback.csv",
      ),
    ).toBe("1m-bars.csv");
  });

  it("parses filenames even when the disposition is inline", () => {
    expect(
      parseContentDispositionFilename(
        'inline; filename="1m-bars.csv"',
        "fallback.csv",
      ),
    ).toBe("1m-bars.csv");
  });

  it("prefers RFC 5987 filename* and decodes UTF-8", () => {
    expect(
      parseContentDispositionFilename(
        "attachment; filename=\"fallback.csv\"; filename*=UTF-8''%E6%9F%B1%E5%9B%BE.csv",
        "fallback.csv",
      ),
    ).toBe("柱图.csv");
  });

  it("accepts a language tag in filename*", () => {
    expect(
      parseContentDispositionFilename(
        "attachment; filename*=UTF-8'zh-CN'%E6%9F%B1%E5%9B%BE.csv",
      ),
    ).toBe("柱图.csv");
  });

  it("falls back to the simple filename when filename* is malformed", () => {
    expect(
      parseContentDispositionFilename(
        'attachment; filename="safe.csv"; filename*=UTF-8\'\'%E0%A4%A',
      ),
    ).toBe("safe.csv");
  });

  it("cleans directory traversal and path components", () => {
    expect(
      parseContentDispositionFilename('attachment; filename="../../etc/passwd.csv"'),
    ).toBe("passwd.csv");
    expect(
      parseContentDispositionFilename(
        'attachment; filename="C:\\\\temp\\\\bars.csv"',
      ),
    ).toBe("bars.csv");
  });

  it("removes dangerous characters and control characters", () => {
    expect(sanitizeFilename('a<>:"|?*b\u0000.csv')).toBe("ab.csv");
  });

  it("uses the fallback when the header is missing or has no filename", () => {
    expect(parseContentDispositionFilename(null, "bars.csv")).toBe("bars.csv");
    expect(parseContentDispositionFilename(undefined, "bars.csv")).toBe(
      "bars.csv",
    );
    expect(
      parseContentDispositionFilename("attachment", "bars.csv"),
    ).toBe("bars.csv");
    expect(
      parseContentDispositionFilename('attachment; filename=""', "bars.csv"),
    ).toBe("bars.csv");
  });

  it("does not allow a sanitized name to collapse to a dot segment", () => {
    expect(sanitizeFilename("..")).toBeNull();
    expect(sanitizeFilename("path/..")).toBeNull();
    expect(sanitizeFilename("<>|")).toBeNull();
  });
});
