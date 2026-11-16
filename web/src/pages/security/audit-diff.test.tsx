import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { AuditDiff } from "@/pages/security/AuditDiff";
import { expectNoBannedUiSymbols } from "@/test/ui-copy";

describe("Operator Audit Event redacted diff", () => {
  it("labels a creation where before is null", () => {
    render(
      <AuditDiff
        before={null}
        after={{ enabled: true, version: 1 }}
        redactedPaths={[]}
      />,
    );

    expect(screen.getByText("创建")).toBeInTheDocument();
    expect(screen.getByText("创建前不存在")).toBeInTheDocument();
    expect(screen.getByTestId("audit-diff-after")).toHaveTextContent(
      '"enabled": true',
    );
  });

  it("labels a deletion where after is null", () => {
    render(
      <AuditDiff
        before={{ enabled: false, version: 4 }}
        after={null}
        redactedPaths={[]}
      />,
    );

    expect(screen.getByText("删除")).toBeInTheDocument();
    expect(screen.getByText("删除后不存在")).toBeInTheDocument();
    expect(screen.getByTestId("audit-diff-before")).toHaveTextContent(
      '"version": 4',
    );
  });

  it("shows both sides for a modification and keeps the stable [REDACTED] literal", () => {
    render(
      <AuditDiff
        before={{ enabled: false, client_secret: "[REDACTED]" }}
        after={{ enabled: true, client_secret: "[REDACTED]" }}
        redactedPaths={["/client_secret"]}
      />,
    );

    expect(screen.getByText("变更前")).toBeInTheDocument();
    expect(screen.getByText("变更后")).toBeInTheDocument();
    expect(screen.getAllByText(/\[REDACTED\]/).length).toBeGreaterThanOrEqual(2);
    expect(screen.getByText("/client_secret")).toBeInTheDocument();
    expect(screen.getByText(/脱敏字段/)).toBeInTheDocument();
  });

  it("renders long values in a scrollable, keyboard-reachable text block", () => {
    const longValue = `${"a".repeat(5000)}-END-OF-VALUE`;
    render(
      <AuditDiff before={null} after={{ payload: longValue }} redactedPaths={[]} />,
    );

    const after = screen.getByTestId("audit-diff-after");
    expect(after).toHaveTextContent("-END-OF-VALUE");
    expect(after.className).toContain("overflow-auto");
    expect(after.tagName).toBe("PRE");
    expect(after).toHaveAttribute("tabindex", "0");
  });

  it("renders values as text, never as executable HTML", () => {
    const { container } = render(
      <AuditDiff
        before={null}
        after={{ note: '<img src=x onerror="alert(1)">' }}
        redactedPaths={[]}
      />,
    );

    expect(screen.getByTestId("audit-diff-after")).toHaveTextContent(
      /<img src=x onerror=.*alert\(1\)/,
    );
    expect(container.querySelector("img")).toBeNull();
  });

  it("offers no edit, delete or export controls", () => {
    render(
      <AuditDiff
        before={{ enabled: false }}
        after={{ enabled: true }}
        redactedPaths={[]}
      />,
    );

    expect(screen.queryByRole("button")).not.toBeInTheDocument();
    expect(screen.queryByRole("link")).not.toBeInTheDocument();
  });

  it("preserves raw diff bytes containing banned separators and marks them as evidence", () => {
    const before = "left · right：value";
    const after = { note: "a·b：c" };
    render(
      <AuditDiff
        before={before}
        after={after}
        redactedPaths={["/client·secret：x"]}
      />,
    );

    const beforePre = screen.getByTestId("audit-diff-before");
    expect(beforePre).toHaveAttribute("data-raw-evidence", "true");
    expect(beforePre.textContent).toBe("left · right：value");

    const afterPre = screen.getByTestId("audit-diff-after");
    expect(afterPre).toHaveAttribute("data-raw-evidence", "true");
    expect(afterPre.textContent).toBe(JSON.stringify(after, null, 2));

    const redactedPath = screen.getByText("/client·secret：x");
    expect(redactedPath).toHaveAttribute("data-raw-evidence", "true");
    expect(redactedPath.textContent).toBe("/client·secret：x");
  });

  it("keeps the surrounding diff copy free of banned separators", () => {
    const { container } = render(
      <AuditDiff
        before={{ note: "a·b：c" }}
        after={null}
        redactedPaths={["/secret·path：x"]}
      />,
    );

    expectNoBannedUiSymbols(container);
  });
});
