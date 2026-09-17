#!/usr/bin/env python3
"""Installer contract checks using a temporary directory, never host configuration."""
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('installer', Path(__file__).resolve().parents[1] / 'install/configure.py')
installer = importlib.util.module_from_spec(spec)
spec.loader.exec_module(installer)


class InstallerTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='cloudroom-installer-')
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.directory = self.root / 'config'
        self.data = dict(userId='self-hosted-é', coreToken='safe-token-' * 4,
                         databaseUrl='postgresql://ordinary_user:ordinary%40password@localhost/history', release='test')
        self.calls = []
        workspaces = patch.object(installer, 'CODE_ROOT', self.root / 'code')
        workspaces.start(); self.addCleanup(workspaces.stop)
        read_text = Path.read_text

        def read(path, *args, **kwargs):
            return 'test' if str(path) == '/usr/local/lib/cloudroom/version' else read_text(path, *args, **kwargs)

        def run(args, **kwargs):
            self.calls.append(args)
            if args[0] == 'bash':
                Path(args[-1]).write_text('{}')

        for target, value in [
            ('os.geteuid', lambda: 0), ('os.chown', lambda *args: None),
            ('pwd.getpwnam', lambda name: SimpleNamespace(pw_uid=1001 if name == 'cloudroom-agent' else 1000,
                                                        pw_gid=1000, pw_dir=str(self.root / name))),
            ('subprocess.run', run), ('pathlib.Path.read_text', read),
        ]:
            p = patch(target, value); p.start(); self.addCleanup(p.stop)

    def test_self_hosted_credentials_and_stable_retries(self):
        installer.configure(self.data, self.directory)
        path = self.directory / 'core.env'
        original = path.read_bytes()
        values = dict(line.split('=', 1) for line in path.read_text().splitlines())
        self.assertEqual(json.loads(values['CLOUDROOM_STORE']), self.data['userId'])
        self.assertEqual(json.loads(values['CLOUDROOM_DATABASE_URL']), self.data['databaseUrl'])
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        self.assertEqual(self.directory.stat().st_mode & 0o777, 0o750)
        self.assertEqual((self.root / 'code').stat().st_mode & 0o777, 0o700)
        installer.configure(self.data, self.directory)
        self.assertEqual(path.read_bytes(), original)
        self.assertEqual(sum(call[0] == 'bash' for call in self.calls), 1)
        self.assertTrue(all(call == ['systemctl', 'enable', '--now', 'cloudroom.service'] for call in self.calls if call[0] == 'systemctl'))
        with self.assertRaisesRegex(ValueError, 'differs'):
            installer.configure({**self.data, 'databaseUrl': self.data['databaseUrl'] + '2'}, self.directory)
        self.assertEqual(path.read_bytes(), original)

    def test_invalid_input_never_writes_configuration(self):
        for field, value in [('userId', ''), ('coreToken', 'weak'), ('coreToken', 'x' * 32 + ' '),
                             ('databaseUrl', 'https://localhost/history'), ('databaseUrl', 'postgres://localhost:bad/db'),
                             ('databaseUrl', 'postgres://localhost/db#fragment'), ('release', 'other'),
                             ('userId', 'bad\nCLOUDROOM_TOKEN=bad')]:
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                installer.configure({**self.data, field: value}, self.directory)
            self.assertFalse(self.directory.exists())
        self.assertEqual(self.calls, [])

    def test_configuration_symlink_is_rejected(self):
        target = self.root / 'target'; target.mkdir()
        self.directory.symlink_to(target)
        with self.assertRaisesRegex(ValueError, 'symlinks'):
            installer.configure(self.data, self.directory)
        self.assertEqual(list(target.iterdir()), [])


class RestoreTests(unittest.TestCase):
    def test_restore_reuses_policy_without_measuring_free_space(self):
        with tempfile.TemporaryDirectory(prefix='cloudroom-restore-') as temporary:
            root = Path(temporary).resolve(); root.chmod(0o700)
            policy = root / 'policy.json'; calls = root / 'quota.json'
            uid = os.getuid() + 1000
            policy.write_text(json.dumps(dict(agent_uid=uid, agent_gid=uid, quota_mount='/',
                                             quota_limit_bytes=123456 * 1024, cache_dir='/var/cache/cloudroom-agent')))
            policy.chmod(0o600); original = policy.read_bytes()
            # Only fake kernel commands run. Python may read this policy, never /etc/fstab.
            shim = root / 'shim'
            shim.write_text(f'''#!{sys.executable}
import json,sys
from pathlib import Path
name=Path(sys.argv[0]).name
if name=='id': print(0 if len(sys.argv)==2 else {uid})
elif name=='findmnt': print('ext4')
elif name=='quotaon': print('user quota on / is on')
elif name=='setquota': Path({str(calls)!r}).write_text(json.dumps(sys.argv[1:]))
elif name=='python3':
    assert sys.argv[1:]==['-', {str(policy)!r}, '{uid}', '{uid}', '/var/cache/cloudroom-agent']
    sys.argv=sys.argv[1:]
    exec(compile(sys.stdin.read(), '<restore-policy>', 'exec'))
elif name not in ['install','quota']: raise SystemExit('Unexpected command: '+name)
''')
            shim.chmod(0o755)
            for name in ['id', 'findmnt', 'quotaon', 'setquota', 'python3', 'install', 'quota', 'df', 'repquota', 'mount', 'quotacheck', 'chown', 'chmod']:
                (root / name).symlink_to(shim)
            command = ['bash', str(Path(__file__).resolve().parents[1] / 'install/storage.sh'), 'agent', 'service', str(policy)]
            env = {**os.environ, 'PATH': str(root) + os.pathsep + os.environ['PATH']}
            result = subprocess.run([*command, 'restore'], env=env, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads(calls.read_text()), ['-u', 'agent', '0', '123456', '0', '1000000', '/'])
            self.assertEqual(policy.read_bytes(), original)
            self.assertNotEqual(subprocess.run(command, env=env, capture_output=True).returncode, 0)
            self.assertEqual(policy.read_bytes(), original)


if __name__ == '__main__':
    unittest.main()
