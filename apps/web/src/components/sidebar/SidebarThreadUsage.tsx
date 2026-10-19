import type { OrchestrationV2ThreadShell } from "@t3tools/contracts";
import { formatSubagentTokenCount } from "@t3tools/client-runtime/state/subagentRuntime";
import { GaugeIcon } from "lucide-react";
import { formatContextWindowTokens } from "../../lib/contextWindow";

export function SidebarThreadUsage({ usage }: { usage: OrchestrationV2ThreadShell["usage"] }) {
  if (usage === undefined) return null;
  const context =
    usage.contextUsedTokens === undefined
      ? "—"
      : formatContextWindowTokens(usage.contextUsedTokens);
  const limit =
    usage.contextMaxTokens === undefined ? "?" : formatContextWindowTokens(usage.contextMaxTokens);
  const total = usage.totalTokens === undefined ? "—" : formatSubagentTokenCount(usage.totalTokens);
  return (
    <div className="flex min-w-0 items-center gap-2">
      <GaugeIcon className="size-3 shrink-0 stroke-muted-foreground" />
      <div className="min-w-0 wrap-break-word text-foreground/75">
        {`${context}/${limit} ctx · ${total} tok · ${usage.toolUses} tools`}
      </div>
    </div>
  );
}
