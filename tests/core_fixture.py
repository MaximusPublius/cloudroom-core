#!/usr/bin/env python3
"""Deterministic Rust HTTP/Codex-protocol checks; no inference or database required."""
import json
import os
from pathlib import Path
import signal
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import unittest
import uuid

from core_e2e import Service, run, until, next_event


def codex():
    native = str(uuid.uuid4())
    path = Path(os.environ["CODEX_HOME"]) / "sessions" / (native + ".jsonl")
    path.parent.mkdir(exist_ok=True)
    path.write_text('{"fixture":"start"}\n')
    children, turn, slow_exit = {}, "", False

    def send(message):
        print(json.dumps(message), flush=True)

    def event(method, **params):
        send({"method": method, "params": {"threadId": native, **params}})

    try:
        for line in sys.stdin:
            message = json.loads(line)
            if "id" not in message:
                continue
            method, params, result = message["method"], message.get("params", {}), {}
            if method == "thread/start":
                slow_exit = params["model"] == "slow-exit"
                result = {"thread": {"id": native, "path": str(path)}, "model": params["model"]}
            elif method == "thread/resume":
                if Path("hold-resume").exists():
                    Path("resume-ready").touch()
                    while not Path("release-resume").exists():
                        time.sleep(.01)
                if Path("reject-resume").exists():
                    send({"id": message["id"], "error": {"code": -32602, "message": "resume rejected"}})
                    continue
                # Reattach to the caller's existing rollout and append to the same file.
                native = params["threadId"]
                path = Path(os.environ["CODEX_HOME"]) / "sessions" / (native + ".jsonl")
                result = {"thread": {"id": native, "path": str(path)}, "model": params["model"]}
            elif method == "turn/start":
                with Path(native + ".requests").open("a") as audit:
                    audit.write(params["clientUserMessageId"] + "\n")
                text = params["input"][0]["text"]
                if text == "crash":
                    Path("crash-ready").touch()
                    while not Path("release-crash").exists():
                        time.sleep(.01)
                    os._exit(1)
                if text in ("reject", "late-reject"):
                    if text == "late-reject":
                        Path("reject-ready").touch()
                        while not Path("release-reject").exists():
                            time.sleep(.01)
                    send({"id": message["id"], "error": {"code": -32602, "message": "fixture rejection"}})
                    if text == "late-reject":
                        while not Path("finish-reject").exists():
                            time.sleep(.01)
                    continue
                if text == "no-reply":
                    Path("no-reply").touch()
                    continue
                if text == "delay":
                    while not Path("release").exists():
                        time.sleep(.02)
                turn = str(uuid.uuid4())
                reply = {"id": message["id"], "result": {"turn": {"id": turn}}}
                if text == "reply-first":
                    send(reply)
                    time.sleep(.05)
                event("turn/started", turn={"id": turn, "status": "inProgress"})
                if text == "finish-first":
                    event("turn/completed", turn={"id": turn, "status": "completed"})
                    time.sleep(.05)
                    send(reply)
                    continue
                if text != "reply-first":
                    send(reply)
                if text == "hold":
                    job = f"from pathlib import Path; import time\nfor n in range(300):\n Path('{native}.ticks').write_text(str(n)); time.sleep(.1)\n"
                    child = subprocess.Popen([sys.executable, "-c", job], start_new_session=True,
                                             stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    children[str(child.pid)] = child
                    Path(native + ".pid").write_text(str(child.pid))
                    event("item/started", turnId=turn, item={"id": "tool", "type": "commandExecution", "processId": str(child.pid)})
                elif text == "hang":
                    time.sleep(30)
                else:
                    event("item/agentMessage/delta", turnId=turn, itemId="text", delta="hello")
                    event("turn/completed", turn={"id": turn, "status": "completed"})
                continue
            elif method == "turn/interrupt":
                event("turn/completed", turn={"id": turn, "status": "interrupted"})
            elif method == "thread/backgroundTerminals/terminate":
                child = children.pop(params["processId"], None)
                if child:
                    child.kill(); child.wait()
                result = {"terminated": child is not None}
            elif method == "thread/backgroundTerminals/clean":
                for child in children.values():
                    child.kill(); child.wait()
                children.clear()
            send({"id": message["id"], "result": result})
    finally:
        if slow_exit:
            with open(os.devnull, "w") as sink:
                os.dup2(sink.fileno(), 1)
            time.sleep(.6)
        for child in children.values():
            child.kill(); child.wait()
        with path.open("a") as file:
            file.write('{"fixture":"shutdown"}\n')


class ReplayTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="cloudroom-http-fixture-")
        self.root = Path(self.directory.name)
        self.repo = self.root / "repo"
        self.repo.mkdir()
        run("git", "init", "--quiet", str(self.repo))
        home = self.root / "home"
        (home / ".codex").mkdir(parents=True)
        self.state = self.root / "state"
        self.state.mkdir()
        self.env = {
            "PATH": "/usr/local/bin:/usr/bin:/bin", "CLOUDROOM_UNPROTECTED_TEST_MODE": "1", "CLOUDROOM_LISTEN": "127.0.0.1:0",
            "CLOUDROOM_TOKEN": "fixture-token-" + "x" * 32, "CLOUDROOM_STATE_DIR": str(self.state),
            "CLOUDROOM_REPOSITORY": str(self.repo), "CLOUDROOM_DATABASE_URL": "postgres://127.0.0.1:1/fixture",
            "CLOUDROOM_STORE": "fixture", "CLOUDROOM_ALLOW_INSECURE_DATABASE": "1",
            "CLOUDROOM_CODEX_BINARY": str(Path(__file__).resolve()), "CLOUDROOM_ACCOUNT_HOME": str(home),
            "CLOUDROOM_CODEX_HOME": str(home / ".codex"), "CLOUDROOM_MODEL": "fixture",
        }
        self.sequence, self.service = 0, None

    def tearDown(self):
        if self.service:
            self.service.stop()
        self.directory.cleanup()

    def append(self, session, data, native=None):
        self.sequence += 1
        record = {"sequence": self.sequence, "session_id": session, "kind": "text_delta", "data": data}
        if native is not None:
            record["native"] = native
        file = self.state / f"{self.sequence:020}.record"
        file.write_text(json.dumps(record))
        return file

    def start(self):
        self.service = Service(self.env, self.root / "service.log").start()

    def test_core_boots_before_agent_setup_and_does_not_fake_database_readiness(self):
        for key in ['CLOUDROOM_CODEX_BINARY', 'CLOUDROOM_CODEX_HOME', 'CLOUDROOM_MODEL', 'CLOUDROOM_REPOSITORY']:
            self.env.pop(key)
        self.start()
        data = self.service.request('GET', '/v1/dashboard')
        self.assertFalse(data['runtime']['configured'])
        self.assertEqual(data['sessions'], [])
        self.service.request('GET', '/v1/ready', expected=401, token=None)
        self.assertFalse(self.service.request('GET', '/v1/ready', expected=503)['ready'])
        error = self.service.request('POST', '/v1/sessions', {'request_id': 'not-configured'}, 409)
        self.assertIn('setup is incomplete', error['error'])

    def test_dashboard_is_authenticated_bounded_and_contains_only_summaries(self):
        for n in range(1001):
            record = {"sequence": n + 1, "session_id": f"session-{n}", "kind": "state",
                      "data": {"state": "closed", "secret": "SECRET-CANARY"}, "native": "PRIVATE-TRANSCRIPT"}
            (self.state / f"{n + 1:020}.record").write_text(json.dumps(record))
        self.start()
        self.service.request("GET", "/v1/dashboard", expected=401, token=None)
        self.service.request("GET", "/v1/dashboard", expected=401, token="wrong")
        data = self.service.request("GET", "/v1/dashboard")
        self.assertEqual(data["sessionCount"], 1001)
        self.assertEqual(len(data["sessions"]), 1000)
        self.assertEqual(data["sessions"][0]["id"], "session-1000")
        self.assertEqual(data["sessions"][0]["state"], "stopped")
        self.assertIsNone(data["sessions"][0]["lastActivity"])
        self.assertIsNone(data["sessions"][0]["model"])
        self.assertEqual(data["capabilities"], {"settings": False, "updates": False})
        self.assertEqual(data["appConnectivity"], "unknown")
        self.assertEqual(data["onboarding"], {"localConnected": None, "offlineTaskVerified": None})
        for secret in ["SECRET-CANARY", "PRIVATE-TRANSCRIPT", str(self.root), self.env["CLOUDROOM_TOKEN"], "receipts", "native_path"]:
            self.assertNotIn(secret, json.dumps(data))
        until(lambda: self.service.request("GET", "/v1/dashboard")["sampledAt"] is not None, "resource sampling", 15)
        sampled = self.service.request("GET", "/v1/dashboard")
        self.assertLessEqual(sampled["sampledAt"], int(time.time() * 1000))
        self.assertTrue(all(v is None or 0 <= v <= 100 for v in sampled["resources"].values()))

    def test_replay_does_not_read_other_sessions(self):
        self.append("a", {"delta": "first"})
        other = self.append("b", {"delta": "unrelated"})
        self.append("a", {"delta": "last"})
        self.start()
        # A damaged unrelated file must not be opened to answer session A's read.
        other.write_text("unreadable unrelated history")
        records = self.service.records("a")
        self.assertEqual([r["data"]["delta"] for r in records if r["kind"] == "text_delta"], ["first", "last"])

    def test_compact_storage_preserves_legacy_wire_fields(self):
        params = {"threadId": "native", "itemId": "text", "delta": "hello"}
        raw = json.dumps({"method": "item/agentMessage/delta", "params": params}) + "\n"
        self.append("a", {"method": "item/agentMessage/delta", "delta": "hello"}, raw)
        self.start()
        record = self.service.records("a")[0]
        self.assertEqual(record["data"].get("value"), params)
        self.assertEqual(record["native"], raw)

    def test_slow_stream_cannot_hold_shutdown(self):
        for _ in range(64):
            self.append("a", {"delta": "x" * (128 * 1024)})
        self.start()
        host, port = self.service.address.rsplit(":", 1)
        client = socket.create_connection((host, int(port)))
        client.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024)
        client.sendall(("GET /v1/sessions/a/stream HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer "
                        + self.env["CLOUDROOM_TOKEN"] + "\r\n\r\n").encode())
        try:
            self.assertIn(b"200", client.recv(256))
            time.sleep(.2)
            self.service.process.send_signal(signal.SIGTERM)
            self.service.process.wait(timeout=7)
            self.assertEqual(self.service.process.returncode, 0)
        finally:
            client.close()


class RecoveryTests(unittest.TestCase):
    """Actual Rust HTTP service; only Codex and the crash-boundary journal are fixtures."""
    setUp, tearDown, start = ReplayTests.setUp, ReplayTests.tearDown, ReplayTests.start
    def seed(self, extra=()):
        native = "recovery-native"
        path = Path(self.env["CLOUDROOM_CODEX_HOME"]) / "sessions" / (native + ".jsonl")
        path.parent.mkdir(); path.write_text('{"fixture":"seed"}\n')
        records = [("receipt", {"request_id": "seed", "command": "start", "input": {}, "state": "completed"}),
                   ("native_identity", {"id": native, "path": str(path)}), ("state", {"state": "idle"}), *extra]
        for n, (kind, data) in enumerate(records, 1):
            (self.state / f"{n:020}.record").write_text(json.dumps({"sequence": n, "session_id": "cr_seed", "kind": kind, "data": data}))
        self.native = native

    def prompt(self, key, text):
        return self.service.request("POST", "/v1/sessions/cr_seed/prompts", {"request_id": key, "text": text}, 202)

    def status(self):
        return self.service.session("cr_seed")

    def wait(self, check, label):
        return until(check, label, 6)

    def ready(self):
        self.wait(lambda: self.status()["state"] == "idle", "resumed idle session")
        self.assertEqual(self.status()["native_id"], self.native)

    def test_partial_acceptance_and_legacy_enqueue_run_once(self):
        receipt = {"request_id": "pending", "command": "prompt", "input": {"text": "hello"}, "state": "accepted"}
        self.seed([("receipt", receipt)])  # Crash after receipt fsync, before old enqueue fsync.
        self.start()
        self.wait(lambda: self.status()["receipts"]["pending"]["state"] == "completed", "recovered partial acceptance")
        self.assertEqual(self.prompt("pending", "hello")["receipt"]["state"], "completed")
        self.service.request("POST", "/v1/sessions/cr_seed/prompts", {"request_id": "pending", "text": "different"}, 409)
        self.service.stop()
        # Historical logs with a separate enqueue record must not double-queue.
        for p in self.state.glob("*.record"):
            p.unlink()
        (self.state / "saved").unlink(missing_ok=True)
        path = Path(self.env["CLOUDROOM_CODEX_HOME"]) / "sessions"
        shutil.rmtree(path)
        self.seed([("receipt", receipt), ("enqueue", {"request_id": "pending"})])
        self.start()
        self.wait(lambda: self.status()["receipts"]["pending"]["state"] == "completed", "legacy queue")
        self.ready()
        self.assertEqual((self.repo / (self.native + ".requests")).read_text().splitlines(), ["pending", "pending"])

    def test_graceful_restart_preserves_queue_and_close_settles_it(self):
        self.seed(); self.start(); self.ready()
        self.prompt("active", "no-reply")
        self.wait(lambda: (self.repo / "no-reply").exists(), "active native command")
        self.prompt("pending", "hello")
        self.service.stop(); self.start()
        self.wait(lambda: self.status()["receipts"]["pending"]["state"] == "completed", "queue after graceful restart")
        self.ready()
        self.assertIn(self.status()["receipts"]["active"]["state"], ["unknown", "unknown_after_restart"])
        self.assertEqual((self.repo / (self.native + ".requests")).read_text().splitlines(), ["active", "pending"])
        self.prompt("active2", "no-reply"); self.prompt("cancelled", "hello")
        self.service.request("POST", "/v1/sessions/cr_seed/close", {"request_id": "close"}, 202)
        self.wait(lambda: self.status()["state"] == "closed", "deliberate close")
        self.assertEqual(self.status()["receipts"]["cancelled"]["state"], "failed")
        self.service.stop(); self.start()
        self.assertEqual(self.status()["state"], "closed")
        self.assertEqual(self.status()["queue"], [])

    def test_manual_stop_keeps_queue_paused_across_restart_until_resume(self):
        self.seed(); self.start(); self.ready()
        self.prompt("running", "hold")
        self.wait(lambda: self.status()["state"] == "running", "running turn")
        self.prompt("queued", "hello")
        self.service.request("POST", "/v1/sessions/cr_seed/stop", {"request_id": "stop"}, 202)
        self.wait(lambda: self.status()["receipts"]["stop"]["state"] == "completed", "stop acknowledgement")
        self.ready()
        self.assertTrue(self.status()["queue_paused"])
        self.assertEqual(self.status()["queue"], ["queued"])
        self.service.stop(); self.start(); self.ready()
        self.assertTrue(self.status()["queue_paused"])
        self.assertEqual(self.status()["queue"], ["queued"])
        self.assertEqual((self.repo / (self.native + ".requests")).read_text().splitlines(), ["running"])
        self.service.request("POST", "/v1/sessions/cr_seed/resume", {"request_id": "resume"}, 202)
        self.wait(lambda: self.status()["receipts"]["queued"]["state"] == "completed", "resumed queue")
        self.assertFalse(self.status()["queue_paused"])
        self.service.request("POST", "/v1/sessions/cr_seed/resume", {"request_id": "resume"}, 202)
        self.assertEqual((self.repo / (self.native + ".requests")).read_text().splitlines(), ["running", "queued"])

    def test_agent_crash_recovers_once_and_failed_resume_settles_queue(self):
        self.seed(); self.start(); self.ready()
        self.prompt("crash", "crash")
        self.wait(lambda: (self.repo / "crash-ready").exists(), "crash gate")
        self.prompt("pending", "hello")
        (self.repo / "release-crash").touch()
        self.wait(lambda: self.status()["receipts"]["pending"]["state"] == "completed", "agent crash recovery")
        self.ready()
        self.assertEqual(self.status()["receipts"]["crash"]["state"], "unknown_after_restart")
        (self.repo / "release-crash").unlink(); (self.repo / "crash-ready").unlink()
        self.prompt("crash2", "crash")
        self.wait(lambda: (self.repo / "crash-ready").exists(), "second crash gate")
        self.prompt("unrun", "hello")
        (self.repo / "reject-resume").touch(); (self.repo / "release-crash").touch()
        self.wait(lambda: self.status()["state"] == "process_lost", "failed resume")
        self.wait(lambda: self.status()["receipts"]["unrun"]["state"] == "failed", "failed pending receipt")
        launches = len([r for r in self.service.records("cr_seed") if r["kind"] == "harness" and r["data"]["pid"] is not None])
        time.sleep(.5)
        self.assertEqual(len([r for r in self.service.records("cr_seed") if r["kind"] == "harness" and r["data"]["pid"] is not None]), launches)
        self.assertEqual((self.repo / (self.native + ".requests")).read_text().splitlines(), ["crash", "pending", "crash2"])

    def test_repeated_crash_does_not_loop(self):
        self.seed(); self.start(); self.ready()
        self.prompt("first", "crash")
        self.wait(lambda: (self.repo / "crash-ready").exists(), "first crash")
        self.prompt("second", "crash"); self.prompt("unrun", "hello")
        (self.repo / "release-crash").touch()
        self.wait(lambda: self.status()["receipts"]["unrun"]["state"] == "failed", "bounded recovery")
        self.assertEqual(self.status()["state"], "process_lost")
        time.sleep(.3)
        self.assertEqual((self.repo / (self.native + ".requests")).read_text().splitlines(), ["first", "second"])
        self.assertEqual(len([r for r in self.service.records("cr_seed") if r["kind"] == "harness" and r["data"]["pid"] is not None]), 2)

    def test_close_during_resume_never_dispatches_queued_input(self):
        self.seed(); (self.repo / "hold-resume").touch(); self.start()
        self.wait(lambda: (self.repo / "resume-ready").exists(), "native resume gate")
        try:
            self.prompt("unrun", "hello")
            self.assertEqual(self.status()["queue"], ["unrun"])
            self.service.request("POST", "/v1/sessions/cr_seed/close", {"request_id": "close"}, 202)
        finally:
            (self.repo / "release-resume").touch()
        self.wait(lambda: self.status()["state"] == "closed", "close while resuming")
        self.assertEqual(self.status()["receipts"]["unrun"]["state"], "failed")
        self.assertFalse((self.repo / (self.native + ".requests")).exists())
        self.service.stop(); self.start()
        self.assertEqual(self.status()["state"], "closed")

    def test_reused_pid_never_kills_unrelated_process(self):
        sleeper = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)", self.env["CLOUDROOM_CODEX_BINARY"]],
                                   stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            self.seed([("harness", {"pid": sleeper.pid})])
            self.start()
            self.wait(lambda: self.status()["state"] in ["idle", "process_lost"], "reconciliation")
            self.assertIsNone(sleeper.poll(), "unrelated process was killed by a command-text match")
            self.assertEqual(self.status()["state"], "process_lost", "unknown ownership must not launch a replacement")
        finally:
            if sleeper.poll() is None:
                sleeper.kill()
            sleeper.wait()


def progress_checks(service, root):
    sid = service.request("POST", "/v1/sessions", {"request_id": "ordering"}, 202)["session_id"]
    until(lambda: service.session(sid)["state"] == "idle", "ordering session")
    for text in ["reply-first", "finish-first", "events-first", "reject"]:
        expected = "failed" if text == "reject" else "completed"
        service.request("POST", f"/v1/sessions/{sid}/prompts", {"request_id": text, "text": text}, 202)
        until(lambda: service.session(sid)["state"] == "idle" and service.session(sid)["receipts"][text]["state"] == expected, text)
        time.sleep(.1)  # Let a trailing RPC acknowledgement arrive after completion.
        assert service.session(sid)["receipts"][text]["state"] == expected
        dashboard = service.request("GET", "/v1/dashboard")
        summary = next(s for s in dashboard["sessions"] if s["id"] == sid)
        assert summary["state"] == ("failed" if expected == "failed" else "waiting")
        assert isinstance(summary["lastActivity"], int)
        assert summary["model"] == service.env["CLOUDROOM_MODEL"]
        running = [r for r in service.records(sid) if r["kind"] == "state" and r["data"].get("state") == "running" and r["data"].get("request_id") == text]
        assert len(running) == (0 if text == "reject" else 1), "turn progress was recorded more than once"
    service.request("POST", f"/v1/sessions/{sid}/prompts", {"request_id": "late", "text": "late-reject"}, 202)
    until(lambda: (root / "repo/reject-ready").exists(), "pending rejection")
    try:
        service.request("POST", f"/v1/sessions/{sid}/close", {"request_id": "close"}, 202)
    finally:
        (root / "repo/release-reject").touch()
    try:
        until(lambda: service.session(sid)["receipts"]["late"]["state"] == "failed", "late rejection delivered", 3)
        assert service.session(sid)["state"] == "closing"
    finally:
        (root / "repo/finish-reject").touch()
    until(lambda: service.session(sid)["state"] == "closed", "close despite late rejection")
    states = [r["data"]["state"] for r in service.records(sid) if r["kind"] == "state"]
    assert "idle" not in states[states.index("closing"):], "late reply reopened a closing session"


def session_checks(service, root):
    """Additional API checks run by core_e2e.py --fixture with disposable PostgreSQL."""
    progress_checks(service, root)
    ids = []
    for key in ["fixture-a", "fixture-b"]:
        sid = service.request("POST", "/v1/sessions", {"request_id": key}, 202)["session_id"]
        until(lambda: service.session(sid)["state"] == "idle", "fixture start")
        ids.append(sid)
    a, b = ids
    native_a = service.session(a)["native_id"]
    repo = root / "repo"
    until(lambda: service.session(a)["native_offset"] > 0, "initial native tail")
    connection, response = service.stream(a, service.session(a)["last_sequence"])
    try:
        service.request("POST", f"/v1/sessions/{a}/prompts", {"request_id": "delay", "text": "delay"}, 202)
        # The harness emits nothing until released: only the durable acceptance can wake SSE.
        event = next_event(response)
        assert event["kind"] == "receipt" and event["data"]["request_id"] == "delay"
        assert event["data"]["state"] == "accepted"
    finally:
        (repo / "release").write_text("release")
        response.close(); connection.close()
    until(lambda: service.session(a)["state"] == "idle", "delayed fixture completion")
    service.request("POST", "/v1/sessions", {"request_id": "full"}, 409)
    service.request("POST", f"/v1/sessions/{a}/prompts", {"request_id": "hold", "text": "hold"}, 202)
    tick_a = repo / (native_a + ".ticks")
    until(tick_a.exists, "active peer tool")
    close = {"request_id": "close-b"}
    service.request("POST", f"/v1/sessions/{b}/close", close, 202)
    until(lambda: service.session(b)["state"] == "closed" and service.session(b)["receipts"]["close-b"]["state"] == "completed", "idle session close")
    service.request("POST", f"/v1/sessions/{b}/close", close, 202)
    service.request("POST", f"/v1/sessions/{b}/prompts", {"request_id": "after-close", "text": "no"}, 409)
    c = service.request("POST", "/v1/sessions", {"request_id": "freed"}, 202)["session_id"]
    until(lambda: service.session(c)["state"] == "idle", "capacity released")
    native_c = service.session(c)["native_id"]
    service.request("POST", f"/v1/sessions/{c}/prompts", {"request_id": "hold", "text": "hold"}, 202)
    tick_c = repo / (native_c + ".ticks")
    until(tick_c.exists, "close target tool")
    pid_c = int((repo / (native_c + ".pid")).read_text())
    service.request("POST", f"/v1/sessions/{c}/close", {"request_id": "close-active"}, 202)
    until(lambda: service.session(c)["state"] == "closed" and service.session(c)["receipts"]["close-active"]["state"] == "completed", "active close")
    before_a, before_c = tick_a.read_text(), tick_c.read_text()
    time.sleep(.4)
    assert tick_c.read_text() == before_c and tick_a.read_text() != before_a
    assert not Path(f"/proc/{pid_c}").exists(), "close left the target tool alive"
    assert service.session(a)["native_id"] == native_a
    service.stop()
    before_a = tick_a.read_text()
    time.sleep(.3)
    assert tick_a.read_text() == before_a, "tool kept writing after core shutdown"
    service.start()
    assert service.session(c)["state"] == "closed"
    assert service.request("POST", f"/v1/sessions/{c}/close", {"request_id": "close-active"}, 202)["receipt"]["state"] == "completed"
    until(lambda: service.session(a)["state"] == "idle", "peer resumes after graceful shutdown")
    assert service.session(a)["native_id"] == native_a
    service.request("POST", f"/v1/sessions/{a}/close", {"request_id": "close-resumed"}, 202)
    until(lambda: service.session(a)["state"] == "closed", "release resumed peer capacity")
    queue_and_recovery_checks(service, root)


def queue_and_recovery_checks(service, root):
    """Ordered queued delivery, safe retries, and crash-resume of a running session."""
    repo = root / "repo"
    release = repo / "release"
    if release.exists():
        release.unlink()
    sid = service.request("POST", "/v1/sessions", {"request_id": "queueing"}, 202)["session_id"]
    until(lambda: service.session(sid)["state"] == "idle", "queue session start")
    # Hold the first turn open, then queue two more prompts while busy.
    service.request("POST", f"/v1/sessions/{sid}/prompts", {"request_id": "q1", "text": "delay"}, 202)
    until(lambda: service.session(sid)["state"] == "starting_turn", "first turn in flight")
    for key in ["q2", "q3"]:
        r = service.request("POST", f"/v1/sessions/{sid}/prompts", {"request_id": key, "text": key}, 202)
        assert r["receipt"]["state"] == "accepted", r
    assert service.session(sid)["queue"] == ["q2", "q3"], service.session(sid)["queue"]
    # Retrying a queued id is idempotent; conflicting content is rejected.
    service.request("POST", f"/v1/sessions/{sid}/prompts", {"request_id": "q2", "text": "q2"}, 202)
    service.request("POST", f"/v1/sessions/{sid}/prompts", {"request_id": "q2", "text": "different"}, 409)
    release.write_text("go")
    for key in ["q1", "q2", "q3"]:
        until(lambda k=key: service.session(sid)["receipts"][k]["state"] == "completed", f"{key} completed")
    until(lambda: service.session(sid)["state"] == "idle", "queue drained")
    order = [r["data"]["request_id"] for r in service.records(sid)
             if r["kind"] == "state" and r["data"].get("state") == "starting_turn"]
    assert order == ["q1", "q2", "q3"], order

    # A deliberately closed session must never be revived by recovery.
    closed = service.request("POST", "/v1/sessions", {"request_id": "closed-keep"}, 202)["session_id"]
    until(lambda: service.session(closed)["state"] == "idle", "closable session")
    service.request("POST", f"/v1/sessions/{closed}/close", {"request_id": "shut"}, 202)
    until(lambda: service.session(closed)["state"] == "closed", "closed before crash")

    # Crash with an in-flight turn and a never-dispatched queued prompt.
    native = service.session(sid)["native_id"]
    selected_model = service.session(sid)["receipts"]["queueing"]["model"]
    assert selected_model and selected_model != "resumed-fixture"
    release.unlink()
    service.request("POST", f"/v1/sessions/{sid}/prompts", {"request_id": "held", "text": "delay"}, 202)
    until(lambda: service.session(sid)["state"] == "starting_turn", "held turn in flight")
    service.request("POST", f"/v1/sessions/{sid}/prompts", {"request_id": "pending", "text": "pending"}, 202)
    assert service.session(sid)["queue"] == ["pending"]
    before = service.records(sid)
    service.stop(crash=True)
    release.write_text("go")  # reconcile must have killed the orphan before this frees it
    service.env["CLOUDROOM_MODEL"] = "resumed-fixture"
    service.start()
    # The Cloudroom session and native identity survive; history is append-only.
    until(lambda: service.session(sid)["state"] == "idle", "resumed after crash", 30)
    assert service.session(sid)["native_id"] == native
    summary = next(s for s in service.request("GET", "/v1/dashboard")["sessions"] if s["id"] == sid)
    # Saved selections survive deployment-default changes; the dashboard must agree.
    assert summary["model"] == selected_model
    assert service.records(sid)[:len(before)] == before, "history changed across restart"
    # The uncertain in-flight turn is not resent; the queued prompt is delivered.
    assert service.session(sid)["receipts"]["held"]["state"] == "unknown_after_restart"
    until(lambda: service.session(sid)["receipts"]["pending"]["state"] == "completed", "queued work survives restart")
    assert service.session(closed)["state"] == "closed", "recovery revived a closed session"


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "app-server":
        codex()
    else:
        unittest.main()
