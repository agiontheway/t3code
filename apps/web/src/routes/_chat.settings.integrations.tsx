import { createFileRoute } from "@tanstack/react-router";
import { IntegrationsSettingsPanel } from "../components/settings/IntegrationsSettings";

// fork: agent and Signal integrations keep their settings destination.
export const Route = createFileRoute("/_chat/settings/integrations")({
  component: IntegrationsSettingsPanel,
});
