"""Real HTTP imports, Git fidelity, and per-workspace harness recovery."""
import concurrent.futures
import gzip
import http.client
import io
import json
from pathlib import Path
import subprocess
import sys
import tarfile
import unittest

import core_fixture
from core_e2e import until

SCRIPT = Path(__file__).resolve().parents[1] / 'src/workspace/transfer.py'


class WorkspaceTests(unittest.TestCase):
    setUp, tearDown, start = core_fixture.ReplayTests.setUp, core_fixture.ReplayTests.tearDown, core_fixture.ReplayTests.start

    def snapshot(self, source):
        archive = self.root / 'project.tar.gz'
        subprocess.run([sys.executable, str(SCRIPT), 'pack', str(source), str(archive)], check=True, capture_output=True)
        return archive.read_bytes()

    def upload(self, key, body, name='project', expected=201, token=True):
        connection = http.client.HTTPConnection(self.service.address, timeout=20)
        headers = {'Content-Type': 'application/gzip'}
        if token:
            headers['Authorization'] = 'Bearer ' + self.env['CLOUDROOM_TOKEN']
        connection.request('POST', f'/v1/workspaces/{key}?name={name}', body, headers)
        response = connection.getresponse()
        data = response.read()
        connection.close()
        self.assertEqual(response.status, expected, data)
        return json.loads(data)

    def git(self, folder, *args):
        return subprocess.check_output(['git', '-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid', '-C', str(folder), *args], stderr=subprocess.DEVNULL)

    def test_projects_added_later_collisions_retries_and_existing_cloud_edits(self):
        (self.repo / 'file').write_text('local')
        (self.repo / '.env').write_text('SYNTHETIC=1')
        (self.repo / 'run').write_text('#!/bin/sh\n'); (self.repo / 'run').chmod(0o755)
        (self.repo / 'link').symlink_to('file')
        (self.repo / 'node_modules').mkdir(); (self.repo / 'node_modules/skip').touch()
        archive = self.snapshot(self.repo)
        self.start()
        self.upload('first', archive, expected=401, token=False)
        with concurrent.futures.ThreadPoolExecutor(2) as pool:
            results = list(pool.map(lambda _: self.upload('first', archive), range(2)))
        self.assertEqual(results[0], results[1])
        first = Path(results[0]['path'])
        self.assertEqual((first / '.env').read_text(), 'SYNTHETIC=1')
        self.assertTrue((first / 'run').stat().st_mode & 0o111)
        self.assertTrue((first / 'link').is_symlink())
        self.assertFalse((first / 'node_modules').exists())
        (first / 'file').write_text('cloud edit')
        self.assertEqual(self.upload('first', archive), results[0])
        self.assertEqual((first / 'file').read_text(), 'cloud edit')
        second = self.upload('second', archive)
        self.assertNotEqual(second['path'], str(first))
        with concurrent.futures.ThreadPoolExecutor(2) as pool:
            collisions = list(pool.map(lambda key: self.upload(key, archive, name='collision'), ['a', 'b']))
        self.assertNotEqual(collisions[0]['path'], collisions[1]['path'])
        empty = self.root / 'empty'; empty.mkdir()
        third = self.upload('third', self.snapshot(empty), name='new-project')
        self.assertEqual(list(Path(third['path']).iterdir()), [])
        self.service.stop()
        (self.state / 'workspaces/first.json').rename(self.state / 'workspaces/first.pending')
        (first / '.cloudroom-imported').write_text('first')
        self.start()
        self.assertEqual(self.service.request('GET', '/v1/workspaces/first'), results[0])
        self.assertFalse((self.state / 'workspaces/first.pending').exists())
        self.assertEqual(self.service.request('GET', '/v1/workspaces/third'), third)

    def test_dirty_git_linked_worktrees_and_submodules_are_portable(self):
        (self.repo / 'tracked').write_text('base')
        (self.repo / 'deleted').write_text('delete me')
        self.git(self.repo, 'add', '.'); self.git(self.repo, 'commit', '-qm', 'unpublished')
        linked = self.root / 'linked'
        self.git(self.repo, 'worktree', 'add', '-qb', 'feature', str(linked))
        (linked / 'tracked').write_text('staged'); self.git(linked, 'add', 'tracked')
        (linked / 'tracked').write_text('unstaged')
        (linked / 'deleted').unlink(); (linked / 'untracked').write_text('new')
        sub = self.root / 'sub'; sub.mkdir(); self.git(sub, 'init', '-q')
        (sub / 'nested').write_text('nested'); self.git(sub, 'add', '.'); self.git(sub, 'commit', '-qm', 'sub')
        self.git(linked, '-c', 'protocol.file.allow=always', 'submodule', 'add', '-q', str(sub), 'module')
        (linked / 'module/nested').write_text('submodule edit')
        expected_status = self.git(linked, 'status', '--porcelain')
        archive = self.snapshot(linked)
        self.start()
        copied = Path(self.upload('git', archive)['path'])
        self.assertEqual(self.git(copied, 'status', '--porcelain'), expected_status)
        self.assertEqual(self.git(copied, 'branch', '--show-current'), b'feature\n')
        self.assertEqual(self.git(copied, 'show', ':tracked'), b'staged')
        self.assertEqual((copied / 'tracked').read_text(), 'unstaged')
        self.assertEqual(self.git(copied / 'module', 'show', 'HEAD:nested'), b'nested')
        self.assertEqual((copied / 'module/nested').read_text(), 'submodule edit')
        self.assertTrue((copied / '.git').is_dir())

    def test_invalid_and_interrupted_archives_never_publish_a_workspace(self):
        self.start()
        for index, (name, kind, link) in enumerate([
            ('../escape', tarfile.REGTYPE, ''), ('files/link', tarfile.SYMTYPE, '/tmp/escape'),
            ('files/hard', tarfile.LNKTYPE, '../escape'), ('files/.git/config', tarfile.REGTYPE, ''),
        ]):
            output = io.BytesIO()
            with tarfile.open(fileobj=output, mode='w:gz') as archive:
                entry = tarfile.TarInfo(name); entry.type = kind; entry.linkname = link
                archive.addfile(entry)
            self.upload(f'bad{index}', output.getvalue(), expected=409)
            self.service.request('GET', f'/v1/workspaces/bad{index}', expected=404)
        entry = tarfile.TarInfo('files/huge'); entry.size = 4 * 1024**3 + 1
        self.upload('huge', gzip.compress(entry.tobuf()), expected=409)
        self.upload('retry', self.snapshot(self.repo)[:20], expected=409)
        self.upload('retry', self.snapshot(self.repo))
        outside = self.root / 'outside'; outside.write_text('private')
        (self.repo / 'external').symlink_to(outside)
        result = subprocess.run([sys.executable, str(SCRIPT), 'pack', str(self.repo), str(self.root / 'bad.tar.gz')], capture_output=True)
        self.assertNotEqual(result.returncode, 0)

    def test_sessions_resume_in_their_recorded_folders(self):
        self.start()
        archive = self.snapshot(self.repo)
        workspaces = [self.upload(key, archive, name=key) for key in ['one', 'two']]
        self.service.stop()
        native_root = Path(self.env['CLOUDROOM_CODEX_HOME']) / 'sessions'; native_root.mkdir()
        sequence = 0
        for workspace in workspaces:
            key = workspace['id']
            path = native_root / f'{key}.jsonl'; path.write_text('{"fixture":"seed"}\n')
            for kind, data in [('receipt', {'request_id': key, 'command': 'start', 'input': {'workspace': key}, 'workspace': workspace, 'state': 'completed'}),
                               ('native_identity', {'id': key, 'path': str(path)}), ('state', {'state': 'idle'})]:
                sequence += 1
                (self.state / f'{sequence:020}.record').write_text(json.dumps({'sequence': sequence, 'session_id': 'cr_' + key, 'kind': kind, 'data': data}))
        for attempt in range(2):
            self.start()
            for workspace in workspaces:
                key = workspace['id']; sid = 'cr_' + key
                until(lambda: self.service.session(sid)['state'] == 'idle', 'workspace resume', 10)
                self.service.request('POST', '/v1/sessions', {'request_id': key, 'workspace': key}, 202)
                self.service.request('POST', '/v1/sessions', {'request_id': key, 'workspace': 'different'}, 409)
                request = f'check-{attempt}'
                self.service.request('POST', f'/v1/sessions/{sid}/prompts', {'request_id': request, 'text': 'hello'}, 202)
                until(lambda: self.service.session(sid)['receipts'][request]['state'] == 'completed', 'workspace prompt', 10)
                self.assertIn(request, (Path(workspace['path']) / (key + '.requests')).read_text())
            self.service.stop()


if __name__ == '__main__':
    unittest.main()
