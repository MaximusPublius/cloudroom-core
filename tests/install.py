#!/usr/bin/env python3
"""Installer contract checks using a temporary directory, never host configuration."""
import importlib.util
import json
import os
from pathlib import Path
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
        environment = patch.dict(os.environ)
        environment.start(); self.addCleanup(environment.stop)
        os.environ.pop('CLOUDROOM_LISTEN', None)
        os.environ.pop('CLOUDROOM_ALLOW_NON_LOOPBACK_HTTP', None)
        for name, path in [('CODE_ROOT', self.root / 'code'), ('CACHE_ROOT', self.root / 'cache')]:
            setting = patch.object(installer, name, path)
            setting.start(); self.addCleanup(setting.stop)
        read_text = Path.read_text

        def read(path, *args, **kwargs):
            return 'test' if str(path) == '/usr/local/lib/cloudroom/version' else read_text(path, *args, **kwargs)

        for target, value in [
            ('os.geteuid', lambda: 0), ('os.chown', lambda *args: None), ('os.fchown', lambda *args: None),
            ('os.getgrouplist', lambda name, gid: [gid]),
            ('pwd.getpwnam', lambda name: SimpleNamespace(pw_uid=1001 if name == 'cloudroom-agent' else 1000,
                                                        pw_gid=1001 if name == 'cloudroom-agent' else 1000,
                                                        pw_dir=str(self.root / name))),
            ('subprocess.run', lambda args, **kwargs: self.calls.append(args)), ('pathlib.Path.read_text', read),
        ]:
            p = patch(target, value); p.start(); self.addCleanup(p.stop)

    def test_self_hosted_credentials_and_stable_retries(self):
        installer.configure(self.data, self.directory)
        path = self.directory / 'core.env'
        original = path.read_bytes()
        values = dict(line.split('=', 1) for line in path.read_text().splitlines())
        self.assertEqual(json.loads(values['CLOUDROOM_STORE']), self.data['userId'])
        self.assertEqual(json.loads(values['CLOUDROOM_DATABASE_URL']), self.data['databaseUrl'])
        self.assertEqual(json.loads(values['CLOUDROOM_LISTEN']), '127.0.0.1:9840')
        self.assertNotIn('CLOUDROOM_ALLOW_NON_LOOPBACK_HTTP', values)
        self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        self.assertEqual(self.directory.stat().st_mode & 0o777, 0o750)
        self.assertEqual((self.root / 'code').stat().st_mode & 0o777, 0o700)
        policy = self.directory / 'storage.json'
        saved = policy.read_bytes()
        installer.configure(self.data, self.directory)
        self.assertEqual(path.read_bytes(), original)
        self.assertEqual(policy.read_bytes(), saved)
        self.assertEqual(self.calls, [['systemctl', 'enable', '--now', 'cloudroom.service']] * 2)
        with self.assertRaisesRegex(ValueError, 'differs'):
            installer.configure({**self.data, 'databaseUrl': self.data['databaseUrl'] + '2'}, self.directory)
        self.assertEqual(path.read_bytes(), original)

    def test_explicit_listener_and_existing_bindings_survive_retries(self):
        with patch.dict(os.environ, CLOUDROOM_LISTEN='0.0.0.0:9840'):
            installer.configure(self.data, self.directory)
        path = self.directory / 'core.env'
        original = path.read_bytes()
        values = dict(line.split('=', 1) for line in path.read_text().splitlines())
        self.assertEqual(json.loads(values['CLOUDROOM_LISTEN']), '0.0.0.0:9840')
        self.assertNotIn('CLOUDROOM_ALLOW_NON_LOOPBACK_HTTP', values)
        for listen in [None, '127.0.0.1:9840']:
            with patch.dict(os.environ, CLOUDROOM_ALLOW_NON_LOOPBACK_HTTP='1'):
                if listen is not None:
                    os.environ['CLOUDROOM_LISTEN'] = listen
                installer.configure(self.data, self.directory)
            self.assertEqual(path.read_bytes(), original)
        self.assertEqual(self.calls, [['systemctl', 'enable', '--now', 'cloudroom.service']] * 3)

    def test_http_permission_is_saved_only_when_explicit_and_survives_retries(self):
        for index, permission in enumerate(['', '0', 'true', '01', ' 1 ', '1']):
            directory = self.root / f'permission-{index}'
            with self.subTest(permission=permission):
                with patch.dict(os.environ, CLOUDROOM_LISTEN='0.0.0.0:9840',
                                CLOUDROOM_ALLOW_NON_LOOPBACK_HTTP=permission):
                    installer.configure(self.data, directory)
                path = directory / 'core.env'
                original = path.read_bytes()
                values = dict(line.split('=', 1) for line in path.read_text().splitlines())
                self.assertEqual(values.get('CLOUDROOM_ALLOW_NON_LOOPBACK_HTTP'),
                                 json.dumps('1') if permission == '1' else None)
                installer.configure(self.data, directory)
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

    def test_unsafe_accounts_are_rejected_before_writing(self):
        lookup = installer.pwd.getpwnam
        for name, field, value in [('cloudroom-agent', 'pw_uid', 0), ('cloudroom', 'pw_uid', 0),
                                   ('cloudroom-agent', 'pw_uid', 1000), ('cloudroom-agent', 'pw_gid', 0),
                                   ('cloudroom-agent', 'pw_gid', 1000)]:
            def account(requested):
                result = lookup(requested)
                if requested == name:
                    setattr(result, field, value)
                return result
            with self.subTest(name=name, field=field, value=value), patch('pwd.getpwnam', account):
                with self.assertRaisesRegex(ValueError, 'Separate unprivileged'):
                    installer.configure(self.data, self.directory)
            self.assertFalse(self.directory.exists())
        with patch('os.geteuid', return_value=1001), self.assertRaisesRegex(ValueError, 'administrator'):
            installer.configure(self.data, self.directory)
        with patch('os.getgrouplist', return_value=[1001, 0]), self.assertRaisesRegex(ValueError, 'primary group'):
            installer.configure(self.data, self.directory)
        self.assertFalse(self.directory.exists())
        self.assertEqual(self.calls, [])

    def test_disk_policy_without_quotas_and_retries_preserve_settings(self):
        service, agent = installer.accounts()
        installer.setup_storage(self.directory, service, agent)
        policy = self.directory / 'storage.json'
        self.assertEqual(json.loads(policy.read_text()), {
            'agent_uid': 1001, 'agent_gid': 1001, 'cache_dir': str(self.root / 'cache'),
            'cgroup_root': '/sys/fs/cgroup/system.slice/cloudroom.service/agents',
        })
        self.assertEqual(policy.stat().st_mode & 0o777, 0o600)
        self.assertEqual((self.root / 'cache').stat().st_mode & 0o777, 0o700)
        saved = {**json.loads(policy.read_text()), 'warning_bytes': 6_000_000_000}
        policy.write_text(json.dumps(saved))
        before = policy.read_bytes()
        installer.setup_storage(self.directory, service, agent)
        self.assertEqual(policy.read_bytes(), before)
        self.assertEqual(self.calls, [])
        outside = self.root / 'outside'; outside.write_text('keep')
        policy.unlink(); policy.symlink_to(outside)
        with self.assertRaisesRegex(ValueError, 'symlinks'):
            installer.setup_storage(self.directory, service, agent)
        self.assertEqual(outside.read_text(), 'keep')


if __name__ == '__main__':
    unittest.main()
