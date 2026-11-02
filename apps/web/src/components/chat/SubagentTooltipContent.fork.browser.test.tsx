import { expect, it } from "vite-plus/test";
import { ProviderInstanceId } from "@t3tools/contracts";
import { render } from "../../../test/render";
import { SubagentTooltipContent } from "./SubagentTooltipContent";

// fork: telemetry coverage moved from the removed lineage unit suite.
it("updates delegated usage without treating unknown totals as zero", async () => {
  const props = {
    title: "Worker",
    model: null,
    providerInstanceId: ProviderInstanceId.make("codex"),
    origin: "app_owned" as const,
    status: "running" as const,
  };
  await using screen = await render(
    <SubagentTooltipContent {...props} usage={{ totalTokens: 271_000, toolUses: 101 }} />,
  );
  await expect.element(screen.getByText("271k tok", { exact: false })).toBeVisible();
  await expect.element(screen.getByText("101 tools", { exact: false })).toBeVisible();
  await screen.rerender(<SubagentTooltipContent {...props} usage={{ toolUses: 4 }} />);
  await expect.element(screen.getByText("— tok", { exact: false })).toBeVisible();
  await expect.element(screen.getByText("4 tools", { exact: false })).toBeVisible();
  await screen.rerender(<SubagentTooltipContent {...props} usage={null} />);
  await expect.element(screen.getByText("— tok", { exact: false })).toBeVisible();
  await expect.element(screen.getByText("tools", { exact: false })).not.toBeInTheDocument();
  await screen.rerender(
    <SubagentTooltipContent {...props} origin="provider_native" usage={null} />,
  );
  await expect.element(screen.getByText("tok", { exact: false })).not.toBeInTheDocument();
  await screen.rerender(<SubagentTooltipContent {...props} />);
  await expect.element(screen.getByText("tok", { exact: false })).not.toBeInTheDocument();
});
