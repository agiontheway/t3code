# Cross-provider agents

A Claude or Codex thread can hand work to a child thread on one of your _other_ configured
providers: a Claude orchestrator can delegate to your Codex subscription, and a Codex thread can
delegate to Claude. Each child is an ordinary thread in the same project, worktree, and branch, and
appears under **Agents → Direct Spawns** on the parent while it runs. Delegating inside one provider
keeps using that provider's own subagent tools; this feature only covers crossing providers.

## Turning it on

Open **Settings → Integrations → Agents** and enable **Cross-provider agent access**. New and resumed
Claude sessions and new Codex sessions started after that get the tools; running sessions pick them
up on their next start.

Children use the provider instance you configured, with that instance's own sign-in and billing, so
a delegated task may consume the other provider's paid quota.

## Choosing who can be a target

By default every enabled Claude and Codex instance is a target with all of its models, including
API-key-backed instances; only an instance that is explicitly signed out is left out.
Expand **Routes** to switch instances off or limit the models an agent may pick. **Restore
defaults** discards your edits and goes back to tracking your provider list.

**Max orchestration depth** decides how far delegation can nest: `1` lets the parent spawn children
that cannot delegate further; `2` (the default) lets a child that was explicitly granted the tools
fan out one more level.

**Child output cap** limits how much of a child's final answer is returned to the parent in one
piece. Longer answers come back as a head and a tail, clearly marked, and the parent can request
the rest.

## Things to know

- A Codex thread only receives the tools when its session starts. If T3 has to resume that Codex
  session later (for example after the server restarts), the thread shows a notice and the tools
  are unavailable until you start a new thread. Claude sessions get the tools again on resume.
- Interrupting a child, following it up, or reading its result works from the parent thread as well
  as from the child's own view.
