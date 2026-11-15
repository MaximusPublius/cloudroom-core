"""Upgrade safety on disposable files; systemd, account setup and network are mocked."""
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('updater', Path(__file__).resolve().parents[1] / 'install/update.py')
update = importlib.util.module_from_spec(spec)
spec.loader.exec_module(update)


class UpdateTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix='cloudroom-update-test-')
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        self.stage = root / 'stage'
        (self.stage / 'install').mkdir(parents=True)
        (self.stage / 'cloudroom').write_bytes(b'new-core')
        self.manifest = {'commit': 'synthetic-target', 'version': '0.1.111',
                         'sha256': update.sha(self.stage / 'cloudroom')}
        for name in update.FILES:
            (self.stage / 'install' / name).write_bytes(b'new-' + name.encode())
        for name in ('ROOT', 'STATE', 'BACKUPS'):
            folder = root / name.lower()
            folder.mkdir()
            self.enter(patch.object(update, name, folder))
        self.enter(patch.object(update, 'BINARY', update.ROOT / 'cloudroom'))
        update.BINARY.write_bytes(b'old-core')
        for name in update.FILES:
            (update.ROOT / name).write_bytes(b'old-' + name.encode())
        self.enter(patch.object(update.os, 'fchown'))
        self.enter(patch.object(update, 'service_env', return_value={}))
        self.enter(patch.object(update, 'api', return_value={'runtime': {'version': '0.1.24'}}))
        self.busy = self.enter(patch.object(update, 'busy', return_value=None))
        self.enter(patch.object(update, 'sessions', return_value=({}, {})))
        self.enter(patch.object(update, 'ensure_claude'))
        self.enter(patch.object(update, 'guard_guide'))
        self.enter(patch.object(update, 'previews'))
        self.enter(patch.object(update, 'prune'))
        self.enter(patch.object(update.time, 'sleep'))
        self.enter(patch.object(update.shutil, 'disk_usage', return_value=type('Disk', (), {'free': 10**12})()))
        self.gate = self.enter(patch.object(update, 'gate'))
        self.run = self.enter(patch.object(update, 'run'))
        self.start = self.enter(patch.object(update, 'start', return_value=0))
        self.verify = self.enter(patch.object(update, 'verify'))
        self.enter(contextlib.redirect_stdout(io.StringIO()))

    def enter(self, manager):
        value = manager.__enter__()
        self.addCleanup(manager.__exit__, None, None, None)
        return value

    def result(self):
        return json.loads((update.STATE / 'state.json').read_text())['result']

    def test_copy_rejects_checksum_mismatch_before_replacing_files(self):
        with self.assertRaisesRegex(RuntimeError, 'manifest'):
            update.copy_release(self.stage, {**self.manifest, 'sha256': '0' * 64})
        self.assertEqual(update.BINARY.read_bytes(), b'old-core')
        self.assertEqual((update.ROOT / update.FILES[0]).read_bytes(), b'old-' + update.FILES[0].encode())

    def test_upgrade_preserves_old_release_and_verifies_new_binary(self):
        update.upgrade(self.stage, self.manifest)
        self.assertEqual(self.result(), 'updated')
        self.assertEqual(update.BINARY.read_bytes(), b'new-core')
        backup, = update.BACKUPS.iterdir()
        self.assertEqual((backup / 'cloudroom').read_bytes(), b'old-core')
        self.assertEqual(self.verify.call_args.args[2], self.manifest['sha256'])
        self.assertEqual(self.gate.call_args_list[-1].args, ({}, False))

    def test_failed_readiness_restores_every_old_file_and_reopens_gate(self):
        self.verify.side_effect = [RuntimeError('synthetic readiness failure'), None]
        with self.assertRaises(SystemExit) as exit:
            update.upgrade(self.stage, self.manifest)
        self.assertEqual(exit.exception.code, 1)
        self.assertEqual(self.result(), 'rolled_back')
        self.assertEqual(update.BINARY.read_bytes(), b'old-core')
        for name in update.FILES:
            self.assertEqual((update.ROOT / name).read_bytes(), b'old-' + name.encode())
        self.assertEqual(self.verify.call_count, 2)
        self.assertEqual(self.gate.call_args_list[-1].args, ({}, False))

    def test_newer_installed_version_is_never_downgraded(self):
        with patch.object(update, 'api', return_value={'runtime': {'version': '0.1.112'}}):
            update.upgrade(self.stage, self.manifest)
        self.assertEqual(self.result(), 'ahead')
        self.assertEqual(update.BINARY.read_bytes(), b'old-core')
        self.gate.assert_not_called()
        self.run.assert_not_called()

    def test_busy_agents_defer_upgrade_before_stopping_service(self):
        self.busy.return_value = 'agents are working'
        update.upgrade(self.stage, self.manifest)
        self.assertEqual(self.result(), 'waiting')
        self.run.assert_not_called()
        self.gate.assert_not_called()

    def test_current_release_does_not_restart_service(self):
        update.BINARY.write_bytes(b'new-core')
        update.upgrade(self.stage, self.manifest)
        self.assertEqual(self.result(), 'current')
        self.start.assert_not_called()
        self.gate.assert_not_called()


if __name__ == '__main__':
    unittest.main()
