import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it } from "vitest";
import { HelpTrigger } from "@/components/help-trigger";
import { TooltipProvider } from "@/components/ui/tooltip";
import { installResizeObserverStub } from "@/test/tooltip-env";

installResizeObserverStub();

function renderHelp() {
  return render(
    <TooltipProvider>
      <HelpTrigger
        label="Realtime QPS 说明"
        content="1 分钟滑动窗口的请求速率，只取最近一个成功样本。"
      />
    </TooltipProvider>,
  );
}

async function expectClosed() {
  await waitFor(() => {
    expect(screen.queryByRole("tooltip")).not.toBeInTheDocument();
  });
}

describe("HelpTrigger", () => {
  it("exposes an accessible name and readable help on hover", async () => {
    const user = userEvent.setup();
    renderHelp();

    const trigger = screen.getByRole("button", {
      name: "Realtime QPS 说明",
    });
    expect(trigger).toHaveAttribute("data-slot", "help-trigger");

    await user.hover(trigger);
    const tip = await screen.findByRole("tooltip");
    expect(tip).toHaveTextContent(
      "1 分钟滑动窗口的请求速率，只取最近一个成功样本。",
    );
  });

  it("opens on keyboard focus and closes with Escape", async () => {
    const user = userEvent.setup();
    renderHelp();

    await user.tab();
    const trigger = screen.getByRole("button", {
      name: "Realtime QPS 说明",
    });
    expect(trigger).toHaveFocus();
    expect(await screen.findByRole("tooltip")).toHaveTextContent(
      "1 分钟滑动窗口的请求速率",
    );

    await user.keyboard("{Escape}");
    await expectClosed();
  });

  it("toggles on touch tap and closes on the second tap", async () => {
    renderHelp();
    const trigger = screen.getByRole("button", {
      name: "Realtime QPS 说明",
    });

    fireEvent.pointerDown(trigger, { pointerType: "touch", button: 0 });
    fireEvent.pointerUp(trigger, { pointerType: "touch", button: 0 });
    fireEvent.click(trigger);
    expect(await screen.findByRole("tooltip")).toBeInTheDocument();

    fireEvent.pointerDown(trigger, { pointerType: "touch", button: 0 });
    fireEvent.pointerUp(trigger, { pointerType: "touch", button: 0 });
    fireEvent.click(trigger);
    await expectClosed();
  });

  it("keeps keyboard activation open after focus already showed help", async () => {
    const user = userEvent.setup();
    renderHelp();

    await user.tab();
    expect(await screen.findByRole("tooltip")).toBeInTheDocument();
    await user.keyboard("{Enter}");
    expect(screen.getByRole("tooltip")).toBeInTheDocument();
  });
});
