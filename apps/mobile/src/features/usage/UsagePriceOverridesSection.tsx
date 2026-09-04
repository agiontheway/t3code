import { useAtomValue } from "@effect/atom-react";
import { squashAtomCommandFailure } from "@t3tools/client-runtime/state/runtime";
import { AuthOrchestrationOperateScope, type UsageModelPriceOverride } from "@t3tools/contracts";
import {
  parseUsagePriceForm,
  USAGE_PRICE_FIELDS,
  usagePriceForm,
  type UsagePriceForm,
} from "@t3tools/shared/usagePriceOverrides";
import * as Option from "effect/Option";
import { AsyncResult } from "effect/unstable/reactivity";
import { useState } from "react";
import { Pressable, View } from "react-native";

import { AppText as Text, AppTextInput as TextInput } from "../../components/AppText";
import { ErrorBanner } from "../../components/ErrorBanner";
import { useEnvironments, type EnvironmentPresentation } from "../../state/environments";
import { serverEnvironment } from "../../state/server";
import { environmentSession } from "../../state/session";
import { useAtomCommand } from "../../state/use-atom-command";
import { SettingsSection } from "../settings/components/SettingsSection";

export function UsagePriceOverridesSection() {
  const { environments } = useEnvironments();
  const [expanded, setExpanded] = useState(false);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const selected =
    environments.find((entry) => entry.environmentId === selectedId) ?? environments[0];

  if (!selected) return null;

  return (
    <SettingsSection title="Model prices" card>
      <Pressable
        accessibilityRole="button"
        accessibilityState={{ expanded }}
        onPress={() => setExpanded(!expanded)}
        className="flex-row items-center justify-between gap-3 p-4 active:opacity-70"
      >
        <View className="flex-1 gap-1">
          <Text className="text-base font-t3-medium text-foreground">Price overrides</Text>
          <Text className="text-sm text-foreground-muted">
            Set custom rates for any model, including past usage.
          </Text>
        </View>
        <Text className="text-sm font-t3-medium text-foreground">
          {expanded ? "Hide" : "Manage"}
        </Text>
      </Pressable>
      {expanded ? (
        <View className="gap-4 border-t border-border-subtle p-4">
          {environments.length > 1 ? (
            <View className="flex-row flex-wrap gap-2">
              {environments.map((environment) => (
                <Pressable
                  key={environment.environmentId}
                  accessibilityRole="button"
                  accessibilityState={{
                    selected: selected.environmentId === environment.environmentId,
                  }}
                  onPress={() => setSelectedId(environment.environmentId)}
                  className={
                    selected.environmentId === environment.environmentId
                      ? "rounded-full bg-subtle-strong px-3 py-2"
                      : "rounded-full bg-subtle px-3 py-2"
                  }
                >
                  <Text className="text-sm text-foreground">{environment.label}</Text>
                </Pressable>
              ))}
            </View>
          ) : null}
          <EnvironmentPriceOverrides key={selected.environmentId} environment={selected} />
        </View>
      ) : null}
    </SettingsSection>
  );
}

function EnvironmentPriceOverrides({
  environment,
}: {
  readonly environment: EnvironmentPresentation;
}) {
  const sessionResult = useAtomValue(
    environmentSession.sessionStateAtom(environment.environmentId),
  );
  const session = Option.getOrNull(AsyncResult.value(sessionResult));
  const updateSettings = useAtomCommand(serverEnvironment.updateSettings, { reportFailure: false });
  const [form, setForm] = useState<UsagePriceForm | null>(null);
  const [editingModel, setEditingModel] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const settings = environment.serverConfig?.settings;
  const overrides = settings?.usagePriceOverrides ?? {};
  const supported = environment.serverConfig?.environment.capabilities.usagePriceOverrides === true;
  const canEdit =
    environment.connection.phase === "connected" &&
    settings !== undefined &&
    supported &&
    session?.authenticated === true &&
    session.scopes?.includes(AuthOrchestrationOperateScope) === true;

  const writeOverride = async (model: string, price: UsageModelPriceOverride | null) => {
    if (!canEdit || busy) return;
    setBusy(true);
    setError(null);
    const result = await updateSettings({
      environmentId: environment.environmentId,
      input: { patch: { usagePriceOverrides: { [model]: price } } },
    });
    setBusy(false);
    if (result._tag === "Failure") {
      const cause = squashAtomCommandFailure(result);
      setError(cause instanceof Error ? cause.message : "Could not save model prices. Try again.");
      return;
    }
    setForm(null);
    setEditingModel(null);
  };

  const save = () => {
    if (!form) return;
    const parsed = parseUsagePriceForm(form);
    if (!parsed) {
      setError("Enter a model ID, input and output rates, and use zero or more for every price.");
      return;
    }
    if (editingModel === null && Object.hasOwn(overrides, parsed.model)) {
      setError("This model already has an override. Edit its existing prices.");
      return;
    }
    void writeOverride(parsed.model, parsed.price);
  };

  return (
    <View className="gap-4">
      <Text className="text-sm text-foreground-muted">
        Rates are USD per million tokens and apply to {environment.label}. Model IDs must match
        exactly. Removing an override restores automatic pricing.
      </Text>
      {error ? <ErrorBanner message={error} /> : null}
      {Object.entries(overrides).map(([model, price]) => (
        <View key={model} className="gap-2 border-t border-border-subtle pt-3">
          <Text selectable className="text-base font-t3-medium text-foreground">
            {model}
          </Text>
          <Text className="text-sm text-foreground-muted">
            Input ${price.inputCostPerMillionTokens} · Output ${price.outputCostPerMillionTokens}
            {"\n"}Cache read $
            {price.cacheReadCostPerMillionTokens ?? price.inputCostPerMillionTokens}
            {" · "}Cache write $
            {price.cacheWriteCostPerMillionTokens ?? price.inputCostPerMillionTokens}
          </Text>
          {canEdit && form === null ? (
            <View className="flex-row gap-2">
              <Pressable
                accessibilityRole="button"
                accessibilityLabel={`Edit prices for ${model}`}
                disabled={busy}
                onPress={() => {
                  setError(null);
                  setEditingModel(model);
                  setForm(usagePriceForm(model, price));
                }}
                className="min-h-11 justify-center rounded-full bg-subtle px-4 active:opacity-70"
              >
                <Text className="text-sm text-foreground">Edit</Text>
              </Pressable>
              <Pressable
                accessibilityRole="button"
                accessibilityLabel={`Remove price override for ${model}`}
                disabled={busy}
                onPress={() => void writeOverride(model, null)}
                className="min-h-11 justify-center rounded-full px-4 active:opacity-70"
              >
                <Text className="text-sm text-destructive">Remove</Text>
              </Pressable>
            </View>
          ) : null}
        </View>
      ))}
      {!canEdit ? (
        <Text className="text-sm text-foreground-muted">
          {environment.connection.phase !== "connected"
            ? "Connect this environment to edit model prices."
            : settings === undefined
              ? "Loading model prices…"
              : !supported
                ? "Update this environment's server to use price overrides."
                : session === null
                  ? sessionResult._tag === "Failure"
                    ? "Could not check permissions. Reconnect this environment to edit model prices."
                    : "Checking access to model prices…"
                  : "This connection does not have permission to edit model prices."}
        </Text>
      ) : form ? (
        <View className="gap-3 border-t border-border-subtle pt-4">
          <Text className="text-base font-t3-medium text-foreground">
            {editingModel === null ? "Add price override" : "Edit price override"}
          </Text>
          <View className="gap-1.5">
            <Text className="text-sm text-foreground-muted">Model ID</Text>
            <TextInput
              accessibilityLabel="Model ID"
              placeholder="Exact model ID"
              autoCapitalize="none"
              autoCorrect={false}
              editable={!busy && editingModel === null}
              value={form.model}
              onChangeText={(model) => setForm({ ...form, model })}
            />
          </View>
          {USAGE_PRICE_FIELDS.map(({ key, label, optional }) => (
            <View key={key} className="gap-1.5">
              <Text className="text-sm text-foreground-muted">
                {label}
                {optional ? " · optional" : ""}
              </Text>
              <TextInput
                accessibilityLabel={`${label}, USD per million tokens`}
                keyboardType="decimal-pad"
                placeholder={optional ? "Use input rate" : "0.00"}
                editable={!busy}
                value={form[key]}
                onChangeText={(value) => setForm({ ...form, [key]: value })}
              />
            </View>
          ))}
          <Text className="text-xs text-foreground-muted">
            Leave cache rates blank to use the input rate.
          </Text>
          <View className="flex-row gap-3">
            <Pressable
              accessibilityRole="button"
              disabled={busy}
              onPress={save}
              className="min-h-11 justify-center rounded-full bg-foreground px-5 active:opacity-70"
            >
              <Text className="text-sm font-t3-medium text-sheet">{busy ? "Saving…" : "Save"}</Text>
            </Pressable>
            <Pressable
              accessibilityRole="button"
              disabled={busy}
              onPress={() => {
                setForm(null);
                setEditingModel(null);
                setError(null);
              }}
              className="min-h-11 justify-center rounded-full bg-subtle px-4 active:opacity-70"
            >
              <Text className="text-sm text-foreground">Cancel</Text>
            </Pressable>
          </View>
        </View>
      ) : (
        <Pressable
          accessibilityRole="button"
          disabled={busy}
          onPress={() => {
            setError(null);
            setEditingModel(null);
            setForm(usagePriceForm());
          }}
          className="min-h-11 items-center justify-center rounded-full bg-subtle px-4 active:opacity-70"
        >
          <Text className="text-sm font-t3-medium text-foreground">
            {busy ? "Saving…" : "Add price override"}
          </Text>
        </Pressable>
      )}
    </View>
  );
}
