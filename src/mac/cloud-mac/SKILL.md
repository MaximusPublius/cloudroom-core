---
name: cloud-mac
description: 'Run commands and copy files on the user’s paired Mac from a Cloudroom cloud thread. Use when a task needs the user’s own computer: its files, logs, configs, repositories, or logins.'
---

# The user's Mac

- `cloudroom mac run 'COMMAND'` runs it on the user's Mac as the user, in a login shell in their home folder. It prints the output and exits with the command's code. Add `--cwd DIR` to change folder, or `--stdin` to forward input.
- `cloudroom mac pull MAC_PATH [VM_FOLDER]` copies a Mac file or folder here. `cloudroom mac push VM_PATH [MAC_FOLDER]` copies to the Mac (default: its home folder). Limit: 16 MiB compressed.
- `Mac unavailable` means it is offline, asleep, or access is off. Continue the cloud work; tell the user only if the Mac is essential.
- If the Mac disconnects mid-command, the command keeps running there. Check it with `cloudroom mac result JOB`. Never re-run it automatically.
- This is the user's real computer. Read before changing. Never delete or overwrite their work unless asked.
