import { useGT } from "gt-react";
import type { OrchestrationV2ThreadShell } from "@t3tools/contracts";
import { formatSubagentTokenCount } from "@t3tools/client-runtime/state/subagentRuntime";
import { GaugeIcon } from "lucide-react";
import { formatContextWindowTokens } from "../../lib/contextWindow";

export function SidebarThreadUsage({ usage }: { usage: OrchestrationV2ThreadShell["usage"] }) {
  const gt = useGT();
  if (usage === undefined) return null;
  const context =
    usage.contextUsedTokens === undefined
      ? "\u2014"
      : formatContextWindowTokens(usage.contextUsedTokens);
  const limit =
    usage.contextMaxTokens === undefined ? "?" : formatContextWindowTokens(usage.contextMaxTokens);
  const total =
    usage.totalTokens === undefined ? "\u2014" : formatSubagentTokenCount(usage.totalTokens);
  return (
    <div className="flex min-w-0 items-center gap-2">
      <GaugeIcon className="size-3 shrink-0 stroke-muted-foreground" />
      <div className="min-w-0 wrap-break-word text-foreground/75">
        {gt("{context}/{limit} ctx · {total} tok · {tools} tools", {
          context,
          limit,
          total,
          tools: usage.toolUses,
        })}
      </div>
    </div>
  );
}
