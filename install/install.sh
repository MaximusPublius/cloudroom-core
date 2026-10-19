#!/bin/bash
# Install Cloudroom core on a fresh Ubuntu 24.04 VM from a CI-tested release (docs/setup.md).
#   curl -fsSL https://raw.githubusercontent.com/davidondrej/cloudroom-core/main/install/install.sh -o install.sh
#   sudo bash install.sh [--version X.Y.Z]
# The PostgreSQL URL comes from CLOUDROOM_DATABASE_URL or a hidden prompt, never from arguments.
set -euo pipefail

REPO=davidondrej/cloudroom-core
LIB=/usr/local/lib/cloudroom
ETC=/etc/cloudroom
version=''

fail() { echo "Cloudroom install failed: $*" >&2; exit 1; }

while [ $# -gt 0 ]; do
  case $1 in
    --version) version=${2:-}; [[ $version =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || fail '--version needs X.Y.Z'; shift 2 ;;
    *) fail "unknown option $1" ;;
  esac
done

[ "$(id -u)" = 0 ] || fail 'run as root, for example with sudo'
command -v systemctl >/dev/null || fail 'systemd is required'
[ -f /sys/fs/cgroup/cgroup.controllers ] || fail 'cgroup v2 is required'
[ ! -e $ETC/core.env ] || fail "$ETC/core.env already exists. This script only does fresh installs; see docs/setup.md to upgrade."
. /etc/os-release
[ "${ID:-}" = ubuntu ] && [ "${VERSION_ID:-}" = 24.04 ] || echo "Warning: only Ubuntu 24.04 is tested; this is ${PRETTY_NAME:-unknown}." >&2

db=${CLOUDROOM_DATABASE_URL:-}
if [ -z "$db" ]; then
  [ -r /dev/tty ] || fail 'set CLOUDROOM_DATABASE_URL or run this in a terminal'
  echo 'PostgreSQL URL for a dedicated database outside this VM, such as postgresql://USER:PASSWORD@HOST/DB' >&2
  read -rsp 'URL (input hidden): ' db </dev/tty
  echo >&2
fi
case $db in postgres://* | postgresql://*) ;; *) fail 'expected a postgresql:// URL' ;; esac
[[ $db != *[[:space:]\"\\\$\`]* ]] || fail 'the URL must not contain spaces, quotes, $ or backslashes; URL-encode them'
if [[ $db != *sslrootcert=* ]]; then
  # The core always verifies TLS. Default to the system trust store for publicly trusted certificates.
  [[ $db == *\?* ]] && db="$db&sslrootcert=/etc/ssl/certs/ca-certificates.crt" || db="$db?sslrootcert=/etc/ssl/certs/ca-certificates.crt"
fi

echo 'Installing packages...'
apt-get update -qq
DEBIAN_FRONTEND=noninteractive apt-get install -y -qq curl git ca-certificates python3 postgresql-client openssl >/dev/null
[ -x /usr/local/bin/node ] || [ -x /usr/bin/node ] ||
  echo 'Warning: Node.js is not installed system-wide. Codex and Pi need Node.js 24 in /usr/local/bin or /usr/bin.' >&2

echo 'Downloading the release...'
stage=$(mktemp -d)
trap 'rm -rf "$stage"' EXIT
url=$(python3 - "$REPO" "$(uname -m)" "$version" <<'PY'
import json, sys, urllib.request
repo, arch, version = sys.argv[1:]
with urllib.request.urlopen(f'https://api.github.com/repos/{repo}/releases?per_page=50', timeout=30) as response:
    releases = json.load(response)
for release in releases:
    for asset in release['assets']:
        name = asset['name']
        if name.endswith(f'-linux-{arch}.tar.gz') and (not version or name == f'cloudroom-core-{version}-linux-{arch}.tar.gz'):
            sys.exit(print(asset['browser_download_url']))
sys.exit(f'No cloudroom-core {version or "release"} build for {arch}. Follow the manual steps in docs/setup.md to build from source.')
PY
)
curl --fail --silent --show-error --location --proto '=https' --tlsv1.2 --retry 3 -o "$stage/release.tar.gz" "$url"
tar -xzf "$stage/release.tar.gz" -C "$stage" --no-same-owner
sum=$(python3 -c 'import json, sys; print(json.load(open(sys.argv[1]))["sha256"])' "$stage/update-manifest.json")
echo "$sum  $stage/cloudroom" | sha256sum --check --status || fail 'the downloaded binary does not match its manifest'

echo 'Creating accounts and folders...'
id cloudroom >/dev/null 2>&1 ||
  useradd --system --user-group --create-home --home-dir /var/lib/cloudroom --shell /usr/sbin/nologin cloudroom
id cloudroom-agent >/dev/null 2>&1 || useradd --create-home --user-group --shell /bin/bash cloudroom-agent
agent_home=$(getent passwd cloudroom-agent | cut -d: -f6)
chmod 700 /var/lib/cloudroom "$agent_home"
install -d -m 700 -o cloudroom -g cloudroom /var/lib/cloudroom/history
install -d -m 750 -o root -g cloudroom $ETC
install -d -m 755 $LIB $LIB/sql
install -d -m 700 -o cloudroom-agent -g cloudroom-agent /code
install -m 755 "$stage/cloudroom" $LIB/cloudroom
install -m 755 "$stage/install/configure.py" $LIB/configure.py
install -m 644 "$stage"/sql/*.sql $LIB/sql/
install -m 644 "$stage/install/cloudroom.service" /etc/systemd/system/cloudroom.service

echo 'Writing configuration...'
token=$(openssl rand -hex 32)
(
  umask 077
  set -C
  cat > $ETC/core.env <<EOF
CLOUDROOM_TOKEN=$token
CLOUDROOM_LISTEN=127.0.0.1:9840
CLOUDROOM_STORE=self-hosted
CLOUDROOM_DATABASE_URL="$db"
CLOUDROOM_STATE_DIR=/var/lib/cloudroom/history
CLOUDROOM_ACCOUNT_HOME=$agent_home
EOF
)
chown cloudroom:cloudroom $ETC/core.env
python3 $LIB/configure.py --storage-only $ETC

echo 'Starting the service...'
systemctl daemon-reload
systemctl enable --now cloudroom.service
for _ in $(seq 30); do
  printf 'header = "Authorization: Bearer %s"\n' "$token" |
    curl --config - --silent --fail http://127.0.0.1:9840/v1/health >/dev/null && break
  sleep 1
done || true
systemctl is-active --quiet cloudroom.service || fail 'the service did not start; check: journalctl -u cloudroom.service -n 50'

cat <<EOF

Cloudroom core $("$LIB/cloudroom" --version | cut -d' ' -f2) is running on 127.0.0.1:9840.
The API token is in $ETC/core.env (readable by root only).

Two steps remain:
1. Apply the database schema once, as the database owner:
   psql 'YOUR_DATABASE_URL' -v ON_ERROR_STOP=1 -f $LIB/sql/0001-session-records.sql -f $LIB/sql/0002-diagnostics.sql
   If your provider uses its own CA certificate, copy it to $ETC/database-ca.crt and set
   sslrootcert=$ETC/database-ca.crt in the URL in $ETC/core.env.
2. Install a harness and sign in as the cloudroom-agent account (docs/setup.md, docs/harnesses.md).
   For Codex or Pi, add its settings to $ETC/core.env. Then run: sudo systemctl restart cloudroom

Run your first task: docs/setup.md#5-run-your-first-task
EOF
