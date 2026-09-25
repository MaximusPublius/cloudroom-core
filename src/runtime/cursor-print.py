"""Speak the ACP subset Core's Cursor adapter uses, running one `cursor-agent -p` per prompt.

Cursor's ACP mode only runs each model at its default reasoning; print mode honors every variant.
"""
import hashlib
import json
import os
import re
import signal
import subprocess
import sys
import threading
import uuid
from pathlib import Path

BASE = [sys.argv[1], '--disable-auto-update']
MODEL_LINE = re.compile(r'^(\S+) - (.+)$')
lock = threading.Lock()
state = {'session': None, 'cwd': None, 'model': None, 'turn': None}


def send(value):
    with lock:
        sys.stdout.write(json.dumps(value, separators=(',', ':')) + '\n')
        sys.stdout.flush()


def reply(request, result=None, error=None):
    send({'jsonrpc': '2.0', 'id': request, **({'error': error} if error else {'result': result})})


def update(session_update, **fields):
    send({'jsonrpc': '2.0', 'method': 'session/update',
          'params': {'sessionId': state['session'], 'update': {'sessionUpdate': session_update, **fields}}})


def cli(args, cwd):
    result = subprocess.run(BASE + args, cwd=cwd, capture_output=True, text=True, timeout=60, stdin=subprocess.DEVNULL)
    if result.returncode:
        raise RuntimeError((result.stderr or result.stdout).strip()[-300:] or 'Cursor CLI failed')
    return result.stdout


def open_session(request, session, cwd):
    if str(uuid.UUID(session)) != session:
        raise ValueError('invalid Cursor chat ID')
    models = [{'modelId': m.group(1), 'name': m.group(2)}
              for m in map(MODEL_LINE.match, cli(['--list-models'], cwd).splitlines()) if m]
    state.update(session=session, cwd=cwd)
    # Cursor stores print-mode chats under chats/<md5 of the working folder>/<chat ID>.
    store = Path.home() / '.cursor' / 'chats' / hashlib.md5(cwd.encode()).hexdigest() / session / 'meta.json'
    reply(request, {'sessionId': session, 'path': str(store), 'models': {'availableModels': models}})


def descendants(pid):
    table = subprocess.run(['ps', '-A', '-o', 'pid=', '-o', 'ppid='], capture_output=True, text=True).stdout
    children = {}
    for row in table.splitlines():
        child, parent = map(int, row.split())
        children.setdefault(parent, []).append(child)
    found, stack = [], [pid]
    while stack:
        for child in children.get(stack.pop(), []):
            found.append(child)
            stack.append(child)
    return found


def kill(pids, sig):
    # Cursor starts shell tools in their own process groups, so stop each group.
    for pid in pids:
        try:
            os.killpg(os.getpgid(pid), sig)
        except ProcessLookupError:
            pass


class Turn:
    def __init__(self, request, text):
        state['turn'] = self
        self.request = request
        self.cancelled = False
        command = BASE + ['-p', '--trust', '--force', '--output-format', 'stream-json', '--stream-partial-output',
                          '--resume', state['session']] + (['--model', state['model']] if state['model'] else [])
        self.child = subprocess.Popen(command, cwd=state['cwd'], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                      stderr=subprocess.PIPE, text=True, start_new_session=True)
        self.child.stdin.write(text)
        self.child.stdin.close()
        self.stderr = []
        threading.Thread(target=lambda: self.stderr.extend(self.child.stderr), daemon=True).start()
        threading.Thread(target=self.stream, daemon=True).start()

    def cancel(self):
        self.cancelled = True
        pids = [self.child.pid] + descendants(self.child.pid)
        kill(pids, signal.SIGTERM)
        threading.Timer(5, kill, (pids, signal.SIGKILL)).start()

    def stream(self):
        segment, outcome = '', None
        for line in self.child.stdout:
            try:
                event = json.loads(line)
                if event.get('type') == 'result':
                    outcome = event
                segment = self.translate(event, segment)
            except (ValueError, TypeError, AttributeError, KeyError):
                continue  # One unreadable event must not end the turn.
        self.child.wait()
        state['turn'] = None
        if self.cancelled:
            reply(self.request, {'stopReason': 'cancelled'})
        elif outcome and not outcome.get('is_error'):
            reply(self.request, {'stopReason': 'end_turn'})
        else:
            detail = (outcome or {}).get('result') or ''.join(self.stderr).strip()[-500:] or f'exit code {self.child.returncode}'
            reply(self.request, error={'code': -32603, 'message': f'Cursor turn failed: {detail}'})

    def translate(self, event, segment):
        kind = event.get('type')
        if kind == 'assistant':
            content = (event.get('message') or {}).get('content') or []
            chunk = ''.join(c.get('text', '') for c in content if c.get('type') == 'text')
            # Partial output repeats each finished segment in full; skip that copy.
            if chunk and chunk != segment:
                update('agent_message_chunk', content={'type': 'text', 'text': chunk})
                return segment + chunk
            return segment
        if kind == 'thinking' and event.get('text'):
            update('agent_thought_chunk', content={'type': 'text', 'text': event['text']})
        elif kind == 'tool_call':
            self.tool(event)
        return ''

    def tool(self, event):
        call = event.get('tool_call') or {}
        name, body = next(((k, v) for k, v in call.items() if k.endswith('ToolCall')), ('toolCall', {}))
        args = body.get('args') or {}
        tool_id = str(event.get('call_id') or call.get('toolCallId')).replace('\n', '/')
        if event.get('subtype') == 'started':
            title = (args.get('toolName') or args.get('name')) if name == 'mcpToolCall' else None
            update('tool_call', toolCallId=tool_id, title=title or name[:-8].capitalize(), kind=name[:-8],
                   status='in_progress', rawInput=args)
        elif event.get('subtype') == 'completed':
            result = body.get('result') or {}
            update('tool_call_update', toolCallId=tool_id, status='completed' if 'success' in result else 'failed',
                   rawOutput=result)


def handle(message):
    method, params, request = message.get('method'), message.get('params') or {}, message.get('id')
    if method == 'session/cancel':
        if state['turn']:
            state['turn'].cancel()
        return
    if request is None:
        return
    try:
        if method == 'initialize':
            reply(request, {'protocolVersion': 1, 'agentCapabilities': {'loadSession': True}})
        elif method == 'session/new':
            open_session(request, cli(['create-chat'], params['cwd']).strip().splitlines()[-1], params['cwd'])
        elif method == 'session/load':
            open_session(request, params['sessionId'], params['cwd'])
        elif method == 'session/set_config_option' and params.get('configId') == 'model':
            state['model'] = params['value']
            reply(request, {'configOptions': [{'id': 'model', 'currentValue': params['value']}]})
        elif method == 'session/prompt':
            if state['turn'] or params.get('sessionId') != state['session']:
                raise ValueError('Cursor turn already running or unknown session')
            text = ''.join(block.get('text', '') for block in params.get('prompt', []) if block.get('type') == 'text')
            Turn(request, text)
        else:
            reply(request, error={'code': -32601, 'message': f'Unsupported operation: {method}'})
    except (OSError, ValueError, KeyError, RuntimeError, subprocess.SubprocessError) as error:
        reply(request, error={'code': -32602, 'message': str(error)})


for raw in sys.stdin:
    handle(json.loads(raw))
if state['turn']:
    state['turn'].cancel()
    state['turn'].child.wait(timeout=6)
