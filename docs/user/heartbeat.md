# Scheduled wake-ups

An agent can schedule one of its own later turns. Ask it in the thread, for example: "wake up in
10 minutes and check the test run", "schedule a check every morning at 9:00" or "list my scheduled
prompts". The agent receives `t3_heartbeat_create`, `t3_heartbeat_list`, `t3_heartbeat_delete` and
`t3_heartbeat_wakeup` tools for its own thread and can combine them with everything else it is
allowed to do.

Scheduling is available to main threads and to orchestration-enabled subagents, not leaf agents.
The scheduled prompt arrives later in the same thread as a normal user message, word for word, and
only while the thread is idle. Ending the turn is what lets the wake-up fire; a busy turn waits and
gets the prompt after it finishes. The agent continues its existing conversation, so it keeps
working with the thread's history and can delegate further.

Things to know:

- The T3 host must be running. Closing a browser or app window does not stop schedules; stopping
  the host does. A powered-off machine cannot fire a wake-up. After the host sleeps and wakes,
  missed schedules are delivered once, coalesced, without replaying everything in between.
- Schedules survive a server restart and stay dormant until you send a new message in that thread,
  including future one-shots. Wake-ups set for a number of minutes ("wake me in 10 minutes") are
  lost on restart.
- Interrupting a turn cancels that turn's pending minute wake-up. Stopping, archiving or deleting
  the thread removes all of its schedules.
- Recurring schedules expire seven days after they are created; the agent is told the expiry time
  when creating one.

An administrator can turn scheduling off entirely with the `enableHeartbeatAccess` server setting.
