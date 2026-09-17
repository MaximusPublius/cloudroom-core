#!/usr/bin/env python3
"""Configure a preinstalled core from private JSON on stdin. Never log that JSON."""
import json
import os
from pathlib import Path
import pwd
import subprocess
import sys
import tempfile
from urllib.parse import urlsplit

CODE_ROOT = Path('/code')

def configure(data, directory=Path('/etc/cloudroom')):
    if os.geteuid() != 0:
        raise ValueError('Run as the provisioning administrator')
    if not isinstance(data, dict) or set(data) != {'userId', 'coreToken', 'databaseUrl', 'release'}:
        raise ValueError('Unexpected setup fields')
    owner, token, database, release = (data[k] for k in ('userId', 'coreToken', 'databaseUrl', 'release'))
    if not all(isinstance(v, str) for v in (owner, token, database, release)):
        raise ValueError('Setup fields must be strings')
    if any(any(ord(c) < 32 or ord(c) == 127 for c in value) for value in (owner, token, database, release)):
        raise ValueError('Invalid configuration value')
    if not owner.strip() or len(token) < 32 or not all(33 <= ord(c) < 127 for c in token):
        raise ValueError('A store name and a strong core token are required')
    connection = urlsplit(database)
    if (connection.scheme not in ('postgres', 'postgresql') or connection.fragment
            or not (connection.hostname or connection.path)):
        raise ValueError('A PostgreSQL connection URL is required')
    _ = connection.port  # Reject malformed ports before writing protected configuration.
    if (Path('/usr/local/lib/cloudroom/version').read_text().strip() != release):
        raise ValueError('The requested release does not match this template')
    service = pwd.getpwnam('cloudroom')
    agent = pwd.getpwnam('cloudroom-agent')
    if agent.pw_uid == 0 or service.pw_uid == 0 or agent.pw_uid == service.pw_uid:
        raise ValueError('Separate service and agent accounts are required')
    path = directory / 'core.env'
    if not directory.is_absolute() or directory.is_symlink() or path.is_symlink():
        raise ValueError('Configuration paths must not be symlinks')
    directory.mkdir(mode=0o750, exist_ok=True)
    os.chown(directory, 0, service.pw_gid)
    directory.chmod(0o750)
    # Reruns never replace settings, rotate credentials, or restart existing workloads.
    if path.exists():
        previous = dict(line.split('=', 1) for line in path.read_text().splitlines() if '=' in line)
        if any(previous.get(k) != json.dumps(v, ensure_ascii=False) for k, v in {
            'CLOUDROOM_STORE': owner, 'CLOUDROOM_TOKEN': token, 'CLOUDROOM_DATABASE_URL': database
        }.items()):
            raise ValueError('Existing configuration differs; no changes made')
    else:
        settings = {
            'CLOUDROOM_STORE': owner, 'CLOUDROOM_TOKEN': token, 'CLOUDROOM_DATABASE_URL': database,
            'CLOUDROOM_LISTEN': '0.0.0.0:9840', 'CLOUDROOM_ACCOUNT_HOME': agent.pw_dir,
            'CLOUDROOM_STATE_DIR': str(Path(service.pw_dir) / 'history'),
            'CLOUDROOM_STORAGE_POLICY': str(directory / 'storage.json'),
        }
        if any(any(ord(c) < 32 or ord(c) == 127 for c in v) for v in settings.values()):
            raise ValueError('Invalid configuration value')
        fd, temp = tempfile.mkstemp(dir=directory, prefix='.core-')
        try:
            with os.fdopen(fd, 'w') as f:
                f.write(''.join(k + '=' + json.dumps(v, ensure_ascii=False) + '\n' for k, v in settings.items()))
                f.flush(); os.fsync(f.fileno())
            os.chown(temp, service.pw_uid, service.pw_gid)
            os.replace(temp, path)
        finally:
            Path(temp).unlink(missing_ok=True)
    workspaces = CODE_ROOT
    if workspaces.is_symlink():
        raise ValueError('/code must be a real directory')
    workspaces.mkdir(mode=0o700, exist_ok=True)
    os.chown(workspaces, agent.pw_uid, agent.pw_gid)
    workspaces.chmod(0o700)
    policy = directory / 'storage.json'
    if not policy.exists():
        subprocess.run(['bash', '/usr/local/lib/cloudroom/storage.sh', 'cloudroom-agent', 'cloudroom', str(policy)], check=True)
    subprocess.run(['systemctl', 'enable', '--now', 'cloudroom.service'], check=True)
    print('Core installed. Agent setup is still required.')


if __name__ == '__main__':
    try:
        raw = sys.stdin.buffer.read(16385)
        if len(raw) > 16384:
            raise ValueError('Setup input is too large')
        if len(sys.argv) > 2:
            raise ValueError('Use configure.py [protected-configuration-directory]')
        configure(json.loads(raw), Path(sys.argv[1]) if len(sys.argv) == 2 else Path('/etc/cloudroom'))
    except (ValueError, OSError, KeyError, subprocess.SubprocessError):
        # Native exceptions may include the database password; keep provider logs generic.
        print('Core configuration failed. Inspect protected configuration and service status.', file=sys.stderr)
        sys.exit(1)
