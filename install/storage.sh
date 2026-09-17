#!/bin/bash
# Run explicitly as root on a prepared, single-customer ext4 VM. Never formats a disk.
# Usage: storage.sh AGENT_USER SERVICE_USER /root/protected-storage-policy.json [install|restore]
set -euo pipefail
export LC_ALL=C
[ "$(id -u)" = 0 ] || { echo 'Run as provisioning administrator' >&2; exit 1; }
agent=${1:?agent account}; service=${2:?protected service account}; policy=${3:?policy output path}
mode=${4:-install}
[[ "$mode" = install || "$mode" = restore ]] || { echo 'Use install or restore' >&2; exit 1; }
uid=$(id -u "$agent"); gid=$(id -g "$agent")
[ "$uid" != 0 ] || { echo 'Agent account must not be root' >&2; exit 1; }
[ "$agent" != "$service" ] || { echo 'Agent and service accounts must be different' >&2; exit 1; }
[ "$(findmnt -n -o FSTYPE /)" = ext4 ] || { echo 'Only verified ext4 root quotas are supported' >&2; exit 1; }
# Rootful Docker, sudo, and supplementary access must not defeat user quotas.
[ "$(id -G "$agent")" = "$gid" ] || { echo 'Agent must have only its own primary group' >&2; exit 1; }
cache=/var/cache/cloudroom-agent
# Restore this path even if quota tooling fails; the core can then report blocked protection.
install -d -m 0755 -o root -g root /var/cache
install -d -m 0700 -o "$agent" -g "$agent" "$cache"
if [ "$mode" = restore ]; then
    hard=$(python3 - "$policy" "$uid" "$gid" "$cache" <<'PY'
import json,sys
from pathlib import Path
path,uid,gid,cache=sys.argv[1:]; p=Path(path)
if not p.is_absolute(): raise SystemExit('Use an absolute protected policy path')
for part in (p,*p.parents):
    s=part.lstat()
    if part.is_symlink() or s.st_uid==int(uid) or s.st_mode & 0o022:
        raise SystemExit('Policy must be protected from agent writes')
d=json.loads(p.read_text()); limit=d['quota_limit_bytes']
if (d['agent_uid']!=int(uid) or d['agent_gid']!=int(gid) or d['quota_mount']!='/'
        or d['cache_dir']!=cache or type(limit) is not int or limit<=0 or limit%1024):
    raise SystemExit('Saved quota policy does not match this installation')
print(limit//1024)
PY
)
else
    [ ! -e "$policy" ] && [ ! -L "$policy" ] || { echo 'Policy already exists; use restore to keep its limits' >&2; exit 1; }
fi
command -v quota >/dev/null && command -v setquota >/dev/null || { echo 'Install the Linux quota package first' >&2; exit 1; }
quota_state=$(quotaon -p -u / 2>/dev/null || true)
if ! grep -q '^user quota .* is on$' <<<"$quota_state"; then
    mount -o remount,usrquota /
    quotacheck -cum /
    quotaon -u /
fi
if [ "$mode" = restore ]; then
    setquota -u "$agent" 0 "$hard" 0 1000000 /
    echo 'Saved quota restored; the core verifies enforcement before admitting agents.'
    exit 0
fi
# Persist the mount option only during installation.
python3 - <<'PY'
from pathlib import Path
p=Path('/etc/fstab'); rows=p.read_text().splitlines(); found=False
for i,line in enumerate(rows):
    parts=line.split()
    if not line.lstrip().startswith('#') and len(parts)>=4 and parts[1]=='/':
        opts=parts[3].split(',')
        if 'usrquota' not in opts: opts.append('usrquota')
        parts[3]=','.join(opts); rows[i]='\t'.join(parts); found=True
if not found: raise SystemExit('No root mount in fstab; configure quota persistence explicitly')
p.write_text('\n'.join(rows)+'\n')
PY
# Quotas are per user across the filesystem, including /tmp and /var/tmp.
used=$(repquota -u / | awk -v u="$agent" '$1==u {print $3}')
available=$(df -Pk / | awk 'NR==2 {print $4}')
reserve_kib=$(( (10000000000 + 1023) / 1024 ))
hard=$(( ${used:-0} + available - reserve_kib ))
[ "$hard" -gt "${used:-0}" ] || { echo 'Free at least 10 GB before enabling disk protection' >&2; exit 1; }
setquota -u "$agent" 0 "$hard" 0 1000000 /
umask 077
python3 - "$policy" "$uid" "$gid" "$hard" "$cache" <<'PY'
import json,sys
from pathlib import Path
path,uid,gid,hard,cache=sys.argv[1:]
p=Path(path)
if not p.is_absolute() or p.is_symlink(): raise SystemExit('Use an absolute protected policy path')
p.write_text(json.dumps(dict(agent_uid=int(uid),agent_gid=int(gid),quota_mount='/',quota_limit_bytes=int(hard)*1024,cache_dir=cache,cgroup_root='/sys/fs/cgroup/system.slice/cloudroom.service/agents',reserve_bytes=10000000000),indent=2)+'\n')
PY
chown "$service:$service" "$policy"
chmod 0600 "$policy"
echo 'Quota set; install the protected service unit and verify the API storage status before admitting agents.'
