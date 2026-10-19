# Signal commands

With a separately hosted Signal connector, members of a linked Signal group can invoke skills in existing T3 threads.

Open **Settings → Integrations → Signal** and select exactly one machine. Add a command, choose its existing thread and discovered skill, and supply any fixed arguments the skill needs. You can route different commands to different threads on that machine. The initial supported providers are Codex, Claude and Cursor.

Save the configuration and copy the one-time credential to the local connector worker. Synchronize the connection IDs and public command descriptions with the Signal broker, then pair its Signal account and enroll the intended group. Group members can send `/help` or `/commands` to see the configured commands. These setup steps are separate: saving T3 settings does not pair a phone or update the broker automatically.

A command uses the selected thread's permission and interaction settings. Busy threads wait until idle; queued requests expire after 15 minutes. The Signal delivery acknowledgment means T3 accepted the message. Skill results and approval requests appear in the thread.

Saving again rotates the credential and invalidates requests from the old connection. Update the worker and broker together after changing routes. **Disable and revoke** stops new deliveries through that connection. Other Signal groups cannot use it.

This integration targets an environment and thread; it does not restrict which machines an agent's existing tools can access. The Signal transport may miss messages during receive-listener outages. Treat the bot's queued acknowledgment as acceptance, rather than Signal's delivery tick.
