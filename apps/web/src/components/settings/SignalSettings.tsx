import { T, Var, useGT, msg } from "gt-react";
import { m as resolveNow } from "../../i18n/messages";
import { useEffect, useState, useRef } from "react";
import {
  ThreadId,
  AuthAccessWriteScope,
  AuthAccessReadScope,
  type EnvironmentId,
  type SignalBinding,
  type SignalCommandRoute,
} from "@t3tools/contracts";
import { createRuntimeCommand } from "@t3tools/client-runtime/state/runtime";
import { signalRequest } from "@t3tools/client-runtime/state/signal";
import { connectionAtomRuntime } from "../../connection/runtime";
import * as Option from "effect/Option";
import { usePreparedConnection, useEnvironmentSessionState } from "../../state/session";
import { useThreadShells } from "../../state/entities";
import { useAtomCommand } from "../../state/use-atom-command";
import { useSettingsScope } from "./SettingsScopeContext";
import { SettingsSection } from "./settingsLayout";
import { Input } from "../ui/input";
import { Button } from "../ui/button";
import {
  Combobox,
  ComboboxTrigger,
  ComboboxPopup,
  ComboboxSearchInput,
  ComboboxList,
  ComboboxItem,
  ComboboxEmpty,
} from "../ui/combobox";
import { signalThreadOptions } from "./SignalSettings.logic";
import {
  SelectButton,
  Select,
  SelectTrigger,
  SelectValue,
  SelectPopup,
  SelectItem,
} from "../ui/select";

const request = createRuntimeCommand(connectionAtomRuntime, {
  label: resolveNow(msg("Signal integration")),
  execute: signalRequest,
});
const blankRoute = (): SignalCommandRoute => ({
  command: "/reviews",
  threadId: ThreadId.make("unselected"),
  skill: "",
  skillArguments: "",
  description: "",
});

function CommandEditor({
  environmentId,
  route,
  onChange,
  onRemove,
}: {
  environmentId: EnvironmentId;
  route: SignalCommandRoute;
  onChange: (route: SignalCommandRoute) => void;
  onRemove: () => void;
}) {
  const gt = useGT();
  const prepared = Option.getOrNull(usePreparedConnection(environmentId));
  const allThreads = useThreadShells();
  const [query, setQuery] = useState("");
  const [open, setOpen] = useState(false);
  const threads = signalThreadOptions(allThreads, environmentId, query);
  const selectedThread = allThreads.find(
    (thread) => thread.environmentId === environmentId && thread.id === route.threadId,
  );
  const [skills, setSkills] = useState<readonly string[]>([]);
  const [error, setError] = useState("");
  const run = useAtomCommand(request, { reportFailure: false });
  useEffect(() => {
    setSkills([]);
    setError("");
    if (!prepared || route.threadId === "unselected") return;
    let cancelled = false;
    void run({ prepared, action: "choices", threadId: route.threadId }).then((result) => {
      if (cancelled) return;
      if (result._tag === "Success") setSkills(result.value.skills ?? []);
      else
        setError(
          gt(
            "Cannot discover skills for this thread. Select a thread with supported skill discovery.",
          ),
        );
    });
    return () => {
      cancelled = true;
    };
  }, [prepared, route.threadId, run, gt]);
  return (
    <div className="space-y-2 rounded border p-3">
      <Input
        aria-label={gt("Signal command")}
        placeholder="/command"
        value={route.command}
        maxLength={33}
        onChange={(event) => onChange({ ...route, command: event.target.value })}
      />
      <Input
        aria-label={gt("Signal command description")}
        placeholder={gt("Description shown by /help")}
        value={route.description ?? ""}
        maxLength={200}
        onChange={(event) => onChange({ ...route, description: event.target.value })}
      />
      <Combobox
        items={threads.map((thread) => thread.id)}
        filteredItems={threads.map((thread) => thread.id)}
        value={route.threadId === "unselected" ? null : route.threadId}
        open={open}
        onOpenChange={(next) => {
          setOpen(next);
          if (!next) setQuery("");
        }}
        onValueChange={(value) => {
          if (typeof value !== "string") return;
          onChange({ ...route, threadId: ThreadId.make(value), skill: "" });
          setOpen(false);
          setQuery("");
        }}
        autoHighlight
      >
        <ComboboxTrigger aria-label={gt("Signal target thread")} render={<SelectButton />}>
          {selectedThread?.title ?? gt("Select an existing thread")}
        </ComboboxTrigger>
        <ComboboxPopup>
          <ComboboxSearchInput
            aria-label={gt("Search Signal threads")}
            placeholder={gt("Search threads…")}
            value={query}
            onChange={(event) => setQuery(event.target.value)}
          />
          <ComboboxEmpty>
            <T>No matching threads.</T>
          </ComboboxEmpty>
          <ComboboxList>
            {threads.map((thread, index) => (
              <ComboboxItem key={thread.id} value={thread.id} index={index}>
                <span className="min-w-0 truncate">{thread.title}</span>
                <span className="ml-2 text-xs text-muted-foreground">
                  {thread.settledAt === null ? gt("Active") : gt("Settled")}
                </span>
              </ComboboxItem>
            ))}
          </ComboboxList>
        </ComboboxPopup>
      </Combobox>
      <Select
        value={route.skill}
        onValueChange={(value) => onChange({ ...route, skill: value ?? "" })}
      >
        <SelectTrigger aria-label={gt("Signal skill")}>
          <SelectValue placeholder={gt("Select a discovered skill")} />
        </SelectTrigger>
        <SelectPopup>
          {skills.map((name) => (
            <SelectItem key={name} value={name}>
              {name}
            </SelectItem>
          ))}
        </SelectPopup>
      </Select>
      <Input
        aria-label={gt("Signal skill arguments")}
        placeholder={gt("Fixed skill arguments (e.g. owner/repo)")}
        value={route.skillArguments ?? ""}
        maxLength={2000}
        onChange={(event) => onChange({ ...route, skillArguments: event.target.value })}
      />
      {error && <p role="alert">{error}</p>}
      {!error && route.threadId !== "unselected" && skills.length === 0 && (
        <p>
          <T>No supported skills found for this thread.</T>
        </p>
      )}
      <Button variant="outline" onClick={onRemove}>
        <T>Remove command</T>
      </Button>
    </div>
  );
}

export function SignalSettings() {
  const gt = useGT();
  const { environments } = useSettingsScope();
  const environment = environments.length === 1 ? environments[0] : null;
  return environment ? (
    <SignalSettingsForm
      key={environment.environmentId}
      environmentId={environment.environmentId}
      label={environment.label}
    />
  ) : (
    <SettingsSection id="signal" title={gt("Signal")}>
      <p>
        <T>Select exactly one machine in the settings header to connect Signal.</T>
      </p>
    </SettingsSection>
  );
}

function SignalSettingsForm({
  environmentId,
  label,
}: {
  environmentId: EnvironmentId;
  label: string;
}) {
  const gt = useGT();
  const session = useEnvironmentSessionState(environmentId);
  const canRead = session.data?.scopes?.includes(AuthAccessReadScope);
  const canWrite = session.data?.scopes?.includes(AuthAccessWriteScope) === true;
  const prepared = Option.getOrNull(usePreparedConnection(environmentId));
  const [commands, setCommands] = useState<readonly SignalCommandRoute[]>([blankRoute()]);
  const [binding, setBinding] = useState<SignalBinding | null>(null);
  const [token, setToken] = useState<string | null>(null);
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const [loaded, setLoaded] = useState(false);
  const [loading, setLoading] = useState(true);
  const [retry, setRetry] = useState(0);
  const generation = useRef(0);

  const run = useAtomCommand(request, { reportFailure: false });
  useEffect(() => {
    if (loaded || !prepared) return;
    const requestGeneration = ++generation.current;
    setLoading(true);
    setError("");
    let cancelled = false;
    void run({ prepared, action: "get" }).then((result) => {
      if (cancelled || requestGeneration !== generation.current) return;
      setLoading(false);
      if (result._tag === "Success") {
        setLoaded(true);
        const saved = result.value.binding ?? null;
        setBinding(saved);
        setCommands(saved?.commands ?? [blankRoute()]);
      } else
        setError(
          canRead === false
            ? gt(
                "This browser does not have administrator access (access:read) on this machine. Reconnect with an administrator pairing link and retry. Existing bot settings have not been changed.",
              )
            : gt(
                "Cannot load Signal settings. Check this environment connection and administrator access, then retry. Existing bot settings have not been changed.",
              ),
        );
    });
    return () => {
      cancelled = true;
    };
  }, [prepared, run, loaded, retry, canRead, gt]);
  const save = async (revoke = false) => {
    if (!prepared || !loaded || !canWrite) return;
    const requestGeneration = ++generation.current;
    setBusy(true);
    setError("");
    setToken(null);
    const result = await run({
      prepared,
      action: revoke ? "revoke" : "configure",
      configure: { commands },
    });
    if (requestGeneration !== generation.current) return;
    setBusy(false);
    if (result._tag === "Success") {
      setBinding(result.value.binding ?? null);
      setToken(result.value.token ?? null);
    } else
      setError(
        gt(
          "Signal settings were not saved. Check command names (unique, not /help, /commands, /ping, /status or /pair), threads, skills and access permissions.",
        ),
      );
  };
  return (
    <SettingsSection id="signal" title={gt("Signal")}>
      <div className="space-y-3 px-4 py-3 text-sm">
        <>
          <p>
            <T>
              Machine: <Var name="machine">{label}</Var>
            </T>{" "}
            <span className="text-muted-foreground">({environmentId})</span>
          </p>
          <p>
            <T>
              Any member of the linked Signal group can use the commands configured here. Each
              command invokes its selected skill in its selected thread on this machine. Busy
              threads wait until idle.
            </T>
          </p>
          {!loaded && (
            <div role="status">
              {loading ? (
                <p>
                  <T>Loading Signal settings…</T>
                </p>
              ) : (
                <Button variant="outline" onClick={() => setRetry((value) => value + 1)}>
                  <T>Retry loading</T>
                </Button>
              )}
            </div>
          )}
          {loaded && (
            <>
              {!canWrite && (
                <p>
                  <T>
                    Administrator write access (access:write) is required to change these settings.
                  </T>
                </p>
              )}
              <fieldset disabled={busy || !prepared || !canWrite} className="max-w-lg space-y-3">
                {commands.map((route, index) => (
                  <CommandEditor
                    key={`${environmentId}:${index}`}
                    environmentId={environmentId}
                    route={route}
                    onChange={(next) =>
                      setCommands((items) => items.map((item, i) => (i === index ? next : item)))
                    }
                    onRemove={() => setCommands((items) => items.filter((_, i) => i !== index))}
                  />
                ))}
                <Button
                  variant="outline"
                  disabled={commands.length >= 20}
                  onClick={() =>
                    setCommands((items) => [...items, { ...blankRoute(), command: "" }])
                  }
                >
                  <T>Add command</T>
                </Button>
              </fieldset>
              <p>
                <T>
                  Pair the bot with one Signal group on the broker. After every save, copy the new
                  connection and command list to its local worker and broker route settings. /help
                  and /commands list available commands.
                </T>
              </p>
              <div className="flex gap-2">
                <Button
                  disabled={
                    busy ||
                    !prepared ||
                    !canWrite ||
                    commands.length === 0 ||
                    commands.some((route) => !route.skill || route.threadId === "unselected")
                  }
                  onClick={() => void save()}
                >
                  <T>Save / rotate credential</T>
                </Button>
                <Button
                  variant="outline"
                  disabled={busy || !prepared || !canWrite || !binding?.enabled}
                  onClick={() => void save(true)}
                >
                  <T>Disable and revoke</T>
                </Button>
              </div>
              {binding && (
                <p>
                  {binding.enabled ? gt("Enabled") : gt("Disabled")}: {binding.connectionId};{" "}
                  <T>
                    <Var name="count">{binding.commands.length}</Var> commands. Broker
                    synchronization is configured separately.
                  </T>
                </p>
              )}
              {token && binding && (
                <div className="space-y-2">
                  <p>
                    <T>Copy this configuration. The credential is shown only once.</T>
                  </p>
                  <pre className="overflow-auto whitespace-pre-wrap break-all rounded border p-3 text-xs">
                    {JSON.stringify(
                      {
                        connection_id: binding.connectionId,
                        environment_id: binding.environmentId,
                        commands: binding.commands,
                        token,
                      },
                      null,
                      2,
                    )}
                  </pre>
                </div>
              )}
            </>
          )}
        </>
        {error && <p role="alert">{error}</p>}
      </div>
    </SettingsSection>
  );
}
