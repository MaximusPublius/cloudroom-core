#!/bin/bash
# Start the core on a machine without systemd, such as a sandbox. Run as root on every boot or wake.
# It recreates what the systemd unit provides: a delegated cgroup, runtime and cache directories,
# the service account's capabilities, and restart after a crash. Safe to run again while the core runs.
# Configuration: /etc/cloudroom/image.env (fixed per image) and /etc/cloudroom/core.env (per machine).
# It stays in the foreground as the supervisor, so start it detached.
set -euo pipefail
[ "$(id -u)" = 0 ] || { echo 'Run as root' >&2; exit 1; }
root=/usr/local/lib/cloudroom config=/etc/cloudroom log=/var/log/cloudroom
cgroup=/sys/fs/cgroup/cloudroom pidfile=/run/cloudroom-supervisor.pid
if [ -s "$pidfile" ] && kill -0 "$(cat "$pidfile")" 2>/dev/null; then echo 'Core already running'; exit 0; fi
[ -x "$root/cloudroom" ] && [ -f "$config/core.env" ] && [ -f "$config/storage.json" ] || { echo 'Core is not configured' >&2; exit 1; }

# Only CLOUDROOM_ settings, never shell code: the files are parsed, not sourced.
settings=()
for file in "$config/image.env" "$config/core.env"; do
  [ -f "$file" ] || continue
  while IFS= read -r line || [ -n "$line" ]; do
    [[ "$line" =~ ^(CLOUDROOM_[A-Z0-9_]+)=(.*)$ ]] || continue
    value=${BASH_REMATCH[2]}; value=${value#\"}; value=${value%\"}
    settings+=("${BASH_REMATCH[1]}=$value")
  done < "$file"
done
settings+=("CLOUDROOM_STORAGE_POLICY=$config/storage.json")

# The core and its agent groups live in one delegated cgroup, as under systemd's Delegate=yes.
mkdir -p "$cgroup/agents"
install -d -m 0711 -o cloudroom -g cloudroom /run/cloudroom
install -d -m 0700 -o cloudroom-agent -g cloudroom-agent /var/cache/cloudroom-agent
install -d -m 0750 -o root -g cloudroom "$log"

supervise() {
  echo $$ > "$cgroup/cgroup.procs"
  chown -R cloudroom:cloudroom "$cgroup"
  ulimit -n 524288
  umask 0077
  local caps=-all,+setuid,+setgid,+dac_read_search,+kill delay=1
  while :; do
    # Keep the log bounded without a log daemon.
    [ "$(stat -c %s "$log/core.log" 2>/dev/null || echo 0)" -lt 52428800 ] || mv -f "$log/core.log" "$log/core.previous.log"
    started=$SECONDS
    status=0
    setpriv --reuid=cloudroom --regid=cloudroom --init-groups --inh-caps="$caps" --ambient-caps="$caps" \
      --bounding-set="$caps" env -i PATH=/usr/local/bin:/usr/bin:/bin "${settings[@]}" "$root/cloudroom" \
      >> "$log/core.log" 2>&1 || status=$?
    [ "$status" = 0 ] && break
    # Restart after a crash, backing off when it keeps failing.
    [ $((SECONDS - started)) -gt 60 ] && delay=1 || delay=$((delay < 30 ? delay * 2 : 30))
    echo "Core exited with $status; restarting in ${delay}s" >> "$log/core.log"
    sleep "$delay"
  done
  rm -f "$pidfile"
}
echo $$ > "$pidfile"
echo 'Core starting'
supervise < /dev/null
