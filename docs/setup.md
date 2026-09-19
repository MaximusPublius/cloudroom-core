# Run Cloudroom on Ubuntu 24.04

This developer alpha runs Codex or Pi through an HTTP API. No Cloudroom account or GUI is required. Start with Codex below; [Pi configuration](harnesses.md#configure) uses the same service and API.

Use a **fresh VM for one trusted user**, with sudo access, systemd, cgroup v2, an ext4 root filesystem, and at least 15 GB free after installing tools. The installer enables filesystem quotas and updates `/etc/fstab`. Containers and other Linux distributions are outside this guide.

Agents run without approval prompts and share their account's files. Do not give that account sudo or access to a Docker socket.

## 1. Install tools and build

Run these commands in an SSH terminal on the VM, as your administrator account:

```sh
sudo apt-get update
sudo apt-get install -y build-essential curl git ca-certificates pkg-config quota python3 postgresql-client openssl
```

Install [current stable Rust](https://rustup.rs/) and [Node.js 24](https://nodejs.org/en/download). Node and npm must be installed system-wide, under `/usr/local/bin` or `/usr/bin`, not only in your administrator's nvm directory.

```sh
git clone https://github.com/davidondrej/cloudroom-core.git
cd cloudroom-core
cargo build --locked --release --jobs 2
sudo npm install --global --prefix /usr/local @openai/codex@0.154.0
```

Run the remaining commands from the repository root. Codex 0.154.0 is the previously tested version; newer harness versions may change their protocols.

## 2. Create the service and agent accounts

The service keeps credentials and history separate from agent work. These commands are for a new installation:

```sh
sudo useradd --system --user-group --create-home --home-dir /var/lib/cloudroom --shell /usr/sbin/nologin cloudroom
sudo useradd --create-home --user-group --shell /bin/bash cloudroom-agent
sudo chmod 700 /var/lib/cloudroom /home/cloudroom-agent
sudo install -d -m 700 -o cloudroom -g cloudroom /var/lib/cloudroom/history
sudo install -d -m 750 -o root -g cloudroom /etc/cloudroom
sudo install -d -m 755 /usr/local/lib/cloudroom
sudo install -d -m 700 -o cloudroom-agent -g cloudroom-agent /code
sudo install -m 755 target/release/cloudroom /usr/local/lib/cloudroom/cloudroom
sudo install -m 755 install/storage.sh /usr/local/lib/cloudroom/storage.sh
sudo install -m 644 install/cloudroom.service /etc/systemd/system/cloudroom.service
sudo install -d -m 700 -o cloudroom-agent -g cloudroom-agent /code/example
sudo -u cloudroom-agent -H git -C /code/example init
sudo -u cloudroom-agent -H /usr/local/bin/codex login --device-auth
```

Complete the login in your browser. If device login is unavailable for your account, use [Codex's supported login methods](https://developers.openai.com/codex/auth/) under the same `cloudroom-agent` account. Do not put inference credentials in the service environment.

## 3. Prepare PostgreSQL

Supply a **dedicated PostgreSQL database outside this VM**, with an owner login and TLS. Any compatible PostgreSQL provider works; Supabase is optional. Never share this database login across unrelated users.

Replace `/path/to/database-ca.crt` in both commands with your provider's CA certificate. For a database using a publicly trusted certificate, use `/etc/ssl/certs/ca-certificates.crt`. The service gets its own copy in the protected configuration directory.

```sh
sudo install -m 644 /path/to/database-ca.crt /etc/cloudroom/database-ca.crt
psql 'postgresql://OWNER@DB_HOST/DATABASE?sslmode=verify-full&sslrootcert=/path/to/database-ca.crt' \
  -W -v ON_ERROR_STOP=1 \
  -f docs/database/0001-session-records.sql \
  -f docs/database/0002-diagnostics.sql
```

Replace `OWNER`, `DB_HOST`, and `DATABASE`; enter the password when prompted. Apply these migrations once to your empty database. The service never applies SQL itself.

## 4. Configure the service

Generate a token directly into a protected file, then edit it:

```sh
sudo sh -c 'umask 077; set -C; printf "CLOUDROOM_TOKEN=%s\n" "$(openssl rand -hex 32)" > /etc/cloudroom/core.env'
sudoedit /etc/cloudroom/core.env
```

Keep the generated `CLOUDROOM_TOKEN` line. Add the following, replacing the database URL and model ID. URL-encode special characters in the database username/password.

```ini
CLOUDROOM_LISTEN=127.0.0.1:9840
CLOUDROOM_STORE=self-hosted
CLOUDROOM_DATABASE_URL="postgresql://OWNER:PASSWORD@DB_HOST/DATABASE?sslrootcert=/etc/cloudroom/database-ca.crt"
CLOUDROOM_STATE_DIR=/var/lib/cloudroom/history
CLOUDROOM_ACCOUNT_HOME=/home/cloudroom-agent
CLOUDROOM_REPOSITORY=/code/example
CLOUDROOM_CODEX_BINARY=/usr/local/bin/codex
CLOUDROOM_CODEX_HOME=/home/cloudroom-agent/.codex
CLOUDROOM_CODEX_MODEL=REPLACE_WITH_A_MODEL_YOUR_ACCOUNT_SUPPORTS
```

The service unit already sets `CLOUDROOM_STORAGE_POLICY=/etc/cloudroom/storage.json`. Keep the token and database password out of Git, prompts, and agent-owned files.

```sh
sudo chown cloudroom:cloudroom /etc/cloudroom/core.env
sudo chmod 600 /etc/cloudroom/core.env
sudo bash install/storage.sh cloudroom-agent cloudroom /etc/cloudroom/storage.json
sudo systemctl daemon-reload
sudo systemctl enable --now cloudroom.service
```

Do not bypass failed quota setup with `CLOUDROOM_UNPROTECTED_TEST_MODE`. See [disk setup](storage.md#provisioning) if your VM does not support the required protection.

## 5. Run your first task

Keep using the administrator's SSH terminal. This helper reads the token without placing it in curl's command-line arguments:

```sh
api() {
  sudo awk -F= '/^CLOUDROOM_TOKEN=/{print "header = \"Authorization: Bearer " $2 "\""}' /etc/cloudroom/core.env |
    curl --config - --silent --show-error --fail-with-body "$@"
}
api http://127.0.0.1:9840/v1/ready
api --json '{"request_id":"quickstart","harness":"codex"}' http://127.0.0.1:9840/v1/sessions
api http://127.0.0.1:9840/v1/sessions/cr_quickstart
```

Readiness should return `{"ready":true}`. Session creation is asynchronous: repeat the last command until `session.state` is `idle`, then send a task:

```sh
api --json '{"request_id":"hello","text":"Create hello.txt containing Hello from Cloudroom."}' \
  http://127.0.0.1:9840/v1/sessions/cr_quickstart/prompts
api --no-buffer http://127.0.0.1:9840/v1/sessions/cr_quickstart/stream
```

HTTP 202 means the request was accepted, not completed. Watch for the `hello` receipt to become `completed`. Ctrl+C or disconnecting SSH only closes your view; the service keeps working.

Reconnect, define `api` again, and read the saved events or send another message:

```sh
api http://127.0.0.1:9840/v1/sessions/cr_quickstart/events
sudo -u cloudroom-agent cat /code/example/hello.txt
api --json '{"request_id":"followup","text":"Read hello.txt and tell me what it contains."}' \
  http://127.0.0.1:9840/v1/sessions/cr_quickstart/prompts
```

Follow-ups sent while the agent is busy queue on the VM. Reuse a request ID only to retry the same command; use a new ID for new work. Event pages contain at most 256 records; request `?after=LAST_SEQUENCE` for the next page. The stream also accepts that cursor.

When finished, release the session's agent slot without deleting its history:

```sh
api --json '{"request_id":"close-quickstart"}' http://127.0.0.1:9840/v1/sessions/cr_quickstart/close
```

## Use your repository and connect an app

- To change the default repository, clone it as `cloudroom-agent`, update `CLOUDROOM_REPOSITORY` to its absolute Git root, and restart the service **after closing existing sessions**. Add other projects through [workspace imports](#import-additional-projects) without changing that default.
- The API listens on the VM's loopback address. For remote apps, put it behind an HTTPS reverse proxy on the VM that supports SSE without buffering. Forward the `Authorization` header. Keep port 9840 private; do not expose plain HTTP to the internet.
- Restarting the core interrupts running work. [Session lifecycle](session-lifecycle.md) explains recovery and uncertain outcomes. History saved externally survives VM loss; files in your workspace and pending history uploads do not have that guarantee.

## If something fails

```sh
sudo journalctl -u cloudroom.service -n 50 --no-pager
api http://127.0.0.1:9840/v1/health
sudo -u cloudroom-agent -H /usr/local/bin/codex login status
```

A failed readiness check usually means the database, TLS certificate, migrations, or disk protection needs attention. A failed agent start may mean login or model configuration. The default limit is 2 open harness sessions; close finished sessions before starting more. Keep logs private when asking for help.

## Import additional projects

Use `python3 src/workspace/transfer.py pack LOCAL_FOLDER SNAPSHOT.tar.gz` on the source machine. Upload the archive with authenticated `POST /v1/workspaces/WORKSPACE_ID?name=FOLDER_NAME` (`Content-Type: application/gzip`). A 201 response contains the stable workspace ID and `/code/` path. GET the same route to check readiness. Send `"workspace":"WORKSPACE_ID"` with a session start; starts and recovery retain that folder. Requests without it retain the legacy configured repository behavior.

Imports require `/code` to belong to the unprivileged agent on its quota-protected filesystem. The managed installer prepares this directory. Existing installations need this directory permission update before using imports; do not move running sessions. No new environment variable or SQL migration is required.

Snapshots preserve Git remote URLs and project files, including any embedded credentials. Prefer Git URLs without tokens or passwords, and keep snapshot archives private; credentials are not stripped.

The first copy includes unpublished Git state, working files, and project configuration. Dependency/cache directories are excluded; external symlinks and special files are rejected. Limits are 4 GiB compressed/unpacked and 200,000 archive entries. Retries reuse the same workspace and preserve cloud edits. This is first-copy preparation; later edits are not continuously synchronized.
