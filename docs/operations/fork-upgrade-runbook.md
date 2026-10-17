# Fork upgrade runbook: 0.0.45 → 0.1.0

How to move an installed copy of this fork from the 0.0.45 desktop app ("T3 Code (Alpha)") to the
0.1.0 desktop app ("T3 Code (Beta)"), what changes on disk, and how to roll back. Upgrade one Linux
machine first and run the checks below before upgrading the others.

## What changes

|                                  | 0.0.45                 | 0.1.0                 |
| -------------------------------- | ---------------------- | --------------------- |
| App name                         | T3 Code (Alpha)        | T3 Code (Beta)        |
| Linux install directory          | `/opt/T3 Code (Alpha)` | `/opt/T3 Code (Beta)` |
| macOS app                        | `T3 Code (Alpha).app`  | `T3 Code (Beta).app`  |
| Electron profile                 | `T3 Code (Alpha)`      | `t3code-v2`           |
| Database in `<T3 home>/userdata` | `state.sqlite`         | `statev2.sqlite`      |

The Electron profile lives in the platform's app-data directory (`~/.config` on Linux,
`~/Library/Application Support` on macOS). The T3 home (`~/.t3` by default), settings, secrets,
attachments and Signal connection state stay where they are and are shared by both versions.

On first start, 0.1.0 takes a read-only snapshot of `state.sqlite` and saves it as
`statev2.sqlite`, then migrates only the copy. `state.sqlite` is never modified. Threads, scheduled
wake-ups and cross-provider children from 0.0.45 are carried into the copy; children keep their
place under their parent's **Lineage** instead of appearing in the sidebar.

## Before you start

1. Let running agents finish, or interrupt them.
2. Quit T3 Code, including any background server it started.
3. Optional: back up `<T3 home>/userdata`. The upgrade does not write to `state.sqlite`, but a
   backup makes rollback independent of that guarantee.

## Linux (`.deb`)

1. Install the 0.1.0 package over the old one, for example `sudo apt install ./T3-Code-*.deb`.
   dpkg replaces the package, so `/opt/T3 Code (Alpha)` is removed and the app is now in
   `/opt/T3 Code (Beta)`.
2. Update anything that points at the old path: custom launchers, autostart entries, shell aliases,
   scripts or service units.
3. If you used the AppImage instead, replace the old AppImage file with the new one and update
   launchers the same way.

## macOS

1. Install `T3 Code (Beta).app` from the 0.1.0 dmg into Applications.
2. Quit `T3 Code (Alpha)` if it is still running, then delete `T3 Code (Alpha).app`, so only one
   version starts against the shared T3 home.

## First start checks

1. Start T3 Code (Beta). The first start is slower while `statev2.sqlite` is created and migrated.
2. **Connect sign-in.** The new `t3code-v2` Electron profile starts empty apart from drafts, layout
   and theme, which are copied from the old profile. Open **Settings → Connections** and sign in to
   T3 Connect again if it shows you signed out.
3. **Threads.** Your projects and threads are listed as before. Cross-provider children do not
   appear in the sidebar; they are under **Thread details → Lineage** on their parent.
4. **Scheduled wake-ups.** Existing schedules stay dormant until you send a message in their
   thread, as after any restart. Minute wake-ups that were pending at upgrade time are gone.
5. **Cross-provider agents.** Settings under **Settings → Integrations → Agents** are kept. The
   agent tools are now named `mcp__t3-code__agent_*` for both Claude and Codex. Update any prompt,
   skill or permission rule that used the old `mcp__t3__agent_*` names.
6. **Signal.** The connector worker needs no changes. Send `/help` in the linked group and confirm a
   command reaches its thread.

## Rollback

1. Quit T3 Code (Beta).
2. Reinstall the 0.0.45 package (Linux: install the 0.0.45 `.deb`, which moves the app back to
   `/opt/T3 Code (Alpha)`; macOS: reinstall `T3 Code (Alpha).app` and delete the Beta app).
3. 0.0.45 reads the untouched `state.sqlite` and its own Electron profile. You get your data as it
   was at the moment of the upgrade; anything done in 0.1.0 stays only in `statev2.sqlite`.

## Re-upgrading after a rollback

0.1.0 creates `statev2.sqlite` only when it does not exist. If you upgrade again, it reopens the
existing copy and does not pick up work done in 0.0.45 since the first upgrade.

To import again from `state.sqlite`, quit T3 Code and delete `statev2.sqlite` together with its
`statev2.sqlite-wal` and `statev2.sqlite-shm` files, then start 0.1.0. This discards everything
that was done in 0.1.0.
