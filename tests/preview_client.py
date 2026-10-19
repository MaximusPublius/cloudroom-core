"""Browser-facing preview regressions. Real HTTP/Unix sockets; no SSH or production access."""
import concurrent.futures
import contextlib
from http.server import BaseHTTPRequestHandler
import http.client
import importlib.util
import json
from pathlib import Path
import socketserver
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location('preview', Path(__file__).parents[1] / 'src/preview/client.py')
preview = importlib.util.module_from_spec(spec)
spec.loader.exec_module(preview)


class App(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def log_message(self, *_):
        pass

    def do_HEAD(self):
        if self.path == '/slow':
            time.sleep(3)
        self.send_response(405)
        self.send_header('Content-Length', '0')
        self.end_headers()

    def do_GET(self):
        if self.path == '/events':
            self.send_response(200)
            self.send_header('Content-Type', 'text/event-stream')
            self.send_header('Connection', 'close')
            self.end_headers()
            for n in range(3):
                self.wfile.write(f'data: {n}\n\n'.encode()); self.wfile.flush(); time.sleep(.03)
        else:
            self.send_response(200)
            self.send_header('Content-Length', '2')
            self.end_headers(); self.wfile.write(b'ok')

    def do_POST(self):
        data = self.rfile.read(int(self.headers['Content-Length']))
        self.send_response(200)
        self.send_header('Content-Length', str(len(data)))
        self.end_headers(); self.wfile.write(data)


class UnixServer(socketserver.ThreadingUnixStreamServer):
    daemon_threads = True


class PreviewTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='cr-preview-', dir='/tmp')
        self.addCleanup(self.directory.cleanup)
        sock = Path(self.directory.name) / 'upstream.sock'
        self.upstream = UnixServer(str(sock), App)
        self.tunnel = SimpleNamespace(socket_path=sock, generation='test-generation', closed=False,
            process=SimpleNamespace(poll=lambda: None), connections=set(), connections_lock=threading.Lock())
        self.server = preview.Server(('127.0.0.1', 0), preview.Proxy)
        self.server.authority = f'p3000-test.localhost:{self.server.server_port}'
        self.server.tunnel = self.tunnel
        self.tunnel.server = self.server
        for server in [self.upstream, self.server]:
            threading.Thread(target=server.serve_forever, kwargs={'poll_interval': .01}, daemon=True).start()
            self.addCleanup(server.server_close)
            self.addCleanup(server.shutdown)

    def request(self, method='GET', path='/', headers=None, body=None):
        with contextlib.closing(http.client.HTTPConnection('127.0.0.1', self.server.server_port, timeout=5)) as connection:
            connection.request(method, path, body=body, headers={'Host': self.server.authority, **(headers or {})})
            response = connection.getresponse()
            return response.status, response.getheaders(), response.read()

    def test_browser_path_streaming_upload_and_readiness(self):
        self.assertTrue(preview.Tunnel.healthy(self.tunnel))  # HEAD 405 still proves transport works.
        self.assertEqual(self.request()[2], b'ok')
        self.assertEqual(self.request('GET', '/events')[2], b'data: 0\n\ndata: 1\n\ndata: 2\n\n')
        body = b'x' * 131072
        self.assertEqual(self.request('POST', '/', body=body)[2], body)
        self.assertIn(('Content-Security-Policy', "frame-ancestors 'self'"), self.request()[1])

    def test_host_origin_and_websocket_requests_are_private(self):
        for headers in [
            {'Host': 'attacker.example'}, {'Origin': 'https://attacker.example'},
            {'Upgrade': 'websocket'},
            {'Sec-Fetch-Site': 'cross-site', 'Sec-Fetch-Mode': 'navigate', 'Sec-Fetch-Dest': 'iframe'},
        ]:
            self.assertEqual(self.request(headers=headers)[0], 403)
        self.assertEqual(self.request(headers={'Sec-Fetch-Site': 'cross-site', 'Sec-Fetch-Mode': 'navigate', 'Sec-Fetch-Dest': 'document'})[0], 200)
        self.assertEqual(self.request('POST', '/', headers={'Transfer-Encoding': 'chunked'})[0], 411)

    def test_healthy_upstream_does_not_hide_broken_local_proxy(self):
        with contextlib.closing(preview.Upstream(self.tunnel.socket_path)) as connection:
            connection.request('HEAD', '/')
            self.assertEqual(connection.getresponse().status, 405)
        self.server.RequestHandlerClass = App
        self.assertFalse(preview.Tunnel.healthy(self.tunnel))

    def test_slow_http_does_not_block_other_browser_requests(self):
        with concurrent.futures.ThreadPoolExecutor() as pool:
            slow = pool.submit(self.request, 'HEAD', '/slow')
            started = time.monotonic()
            self.assertEqual(self.request()[2], b'ok')
            self.assertLess(time.monotonic() - started, 2)
            self.assertEqual(slow.result()[0], 405)

    def test_mac_thread_approval_is_scoped_to_authenticated_session(self):
        jobs = preview.MacJobs(Path(self.directory.name))
        core = SimpleNamespace(key='core')
        answers = [b'Allow for this thread\n', b'Deny\n']

        def process(*_args, **_kwargs):
            answer = answers.pop(0).decode()
            return SimpleNamespace(communicate=lambda: (answer, ''))

        with mock.patch.object(preview.subprocess, 'Popen', side_effect=process) as popen:
            first = {'id': 'one', 'session': 'thread-a', 'command': 'touch one'}
            same = {'id': 'two', 'session': 'thread-a', 'command': 'touch two'}
            other = {'id': 'three', 'session': 'thread-b', 'command': 'touch three'}
            self.assertTrue(jobs.approve(core, first))
            self.assertTrue(jobs.approve(core, same))
            self.assertFalse(jobs.approve(core, other))
            self.assertEqual(popen.call_count, 2)

    def test_mac_approval_without_session_is_not_cached(self):
        jobs = preview.MacJobs(Path(self.directory.name))
        core = SimpleNamespace(key='core')
        answers = [b'Allow for this thread\n', b'Deny\n']

        def process(*_args, **_kwargs):
            answer = answers.pop(0).decode()
            return SimpleNamespace(communicate=lambda: (answer, ''))

        with mock.patch.object(preview.subprocess, 'Popen', side_effect=process) as popen:
            job = {'id': 'legacy', 'command': 'touch legacy'}
            self.assertTrue(jobs.approve(core, job))
            self.assertFalse(jobs.approve(core, job))
            self.assertEqual(jobs.trusted, set())
            self.assertEqual(popen.call_count, 2)

    def test_mac_job_without_session_posts_terminal_result(self):
        folder = Path(self.directory.name)
        (folder / 'config.json').write_text(json.dumps({'macAccess': 'ask'}))
        (folder / 'config.json').chmod(0o600)
        jobs = preview.MacJobs(folder)
        posted = []
        core = SimpleNamespace(
            key='core', url='https://core.example', headers={},
            fetch=lambda _url, body, *_args: posted.append(body),
        )
        dialog = SimpleNamespace(communicate=lambda: ('Deny\n', ''))

        with mock.patch.object(preview.sys, 'platform', 'darwin'), \
             mock.patch.object(preview.subprocess, 'Popen', return_value=dialog):
            jobs.execute(core, 'device', {'id': 'legacy', 'command': 'touch legacy', 'write': True})

        self.assertEqual([result['state'] for result in posted], ['running', 'done'])
        self.assertEqual(posted[-1]['code'], 126)


if __name__ == '__main__':
    unittest.main()
