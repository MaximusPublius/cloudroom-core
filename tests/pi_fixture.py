#!/usr/bin/env python3
"""Pi RPC fixture with real owned tools; no model or external credentials."""
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
import uuid

args = sys.argv[1:]
if args == ['--version']:
    print('0.85.1')
    raise SystemExit(0)
path = Path(args[args.index('--session') + 1])
model = args[args.index('--model') + 1]
provider = args[args.index('--provider') + 1]
helper = Path(args[args.index('-e') + 1])
if not path.read_text():
    path.write_text(json.dumps({'type':'session','version':3,'id':str(uuid.uuid4()),'cwd':str(Path.cwd())})+'\n')
native = json.loads(path.read_text().splitlines()[0])['id']
lock = threading.Lock()
running = False
compacting = False
native_queue = False
children = []
held = []
active = None
thinking = 'medium'

def send(value):
    with lock:
        print(json.dumps(value, ensure_ascii=False), flush=True)

def entry(message):
    value = {'type':'message','id':uuid.uuid4().hex[:8],'parentId':None,'message':message}
    with lock:
        with path.open('a') as f:
            f.write(json.dumps(value)+'\n')
    send({'type':'message_end','message':message})

def finish(reason='stop'):
    global running
    running = False
    entry({'role':'assistant','content':[{'type':'text','text':'done'}],'stopReason':reason})
    send({'type':'turn_end'})
    send({'type':'agent_end','messages':[],'willRetry':False})
    send({'type':'agent_settled'})

try:
    for line in sys.stdin:
        v = json.loads(line); kind = v['type']; ident = v.get('id'); data = None
        if kind == 'get_state':
            data = {'sessionId':native,'sessionFile':str(path),'model':{'id':model,'provider':provider},'thinkingLevel':thinking,'isStreaming':running,'isCompacting':compacting,'pendingMessageCount':int(native_queue)}
        elif kind == 'get_available_thinking_levels':
            data = {'levels':['off','medium','high']}
        elif kind == 'set_thinking_level':
            assert v['level'] in ['off','medium','high']
            thinking = v['level']
        elif kind == 'get_commands':
            data = {'commands':[{'name':'cloudroom_context','description':'Cloudroom context-only notice (v1)','source':'extension','sourceInfo':{'path':str(helper)}}]}
        elif kind == 'prompt':
            text = v['message']
            if text.startswith('/cloudroom_context '):
                notice = json.loads(text.split(' ',1)[1])['text']
                if Path('hold-cache').exists():
                    held.append(open(Path('hold-cache').read_text(), 'rb'))
                with Path('warning-'+native).open('a') as warning:
                    warning.write(notice+'\n')
                entry({'role':'custom','customType':'cloudroom_notice','content':notice})
            elif text == 'reject':
                send({'id':ident,'type':'response','command':kind,'success':False,'error':'rejected before acceptance'})
                continue
            elif text == 'handled':
                pass
            elif text == 'dialog':
                active = ident
                send({'type':'extension_ui_request','id':'dialog','method':'confirm','title':'Approval?'})
                continue
            else:
                if running:
                    raise RuntimeError('Cloudroom dispatched concurrent Pi prompts')
                running = True
                send({'type':'response','id':ident,'command':'prompt','success':True})
                send({'type':'agent_start'})
                entry({'role':'user','content':text})
                send({'type':'turn_start'})
                if text in ('hold', 'hold-native-queue'):
                    native_queue = text == 'hold-native-queue'
                    code = "from pathlib import Path;import time,os\np=Path("+repr(native+'.ticks')+");Path("+repr(native+'.pid')+").write_text(str(os.getpid()))\nfor n in range(10000):\n p.write_text(str(n));time.sleep(.05)"
                    child = subprocess.Popen([sys.executable,'-c',code],start_new_session=True,stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
                    children.append(child)
                    send({'type':'tool_execution_start','toolCallId':'tool-1','toolName':'bash','args':{'command':'fixture'}})
                elif text == 'retry':
                    def retry():
                        global compacting
                        send({'type':'turn_end'})
                        send({'type':'agent_end','willRetry':True})
                        compacting = True
                        send({'type':'compaction_start','reason':'overflow'})
                        send({'type':'agent_settled'})  # A premature signal must not finish busy work.
                        time.sleep(.3)
                        compacting = False
                        send({'type':'compaction_end','willRetry':True})
                        send({'type':'agent_start'})
                        send({'type':'tool_execution_update','toolCallId':'tool','partialResult':{'content':[{'type':'text','text':'one'}]}})
                        send({'type':'tool_execution_update','toolCallId':'tool','partialResult':{'content':[{'type':'text','text':'one two'}]}})
                        finish()
                    threading.Thread(target=retry,daemon=True).start()
                else:
                    send({'type':'message_update','assistantMessageEvent':{'type':'text_delta','delta':'unicode \u2028 \u2029 héllo'}})
                    finish('error' if text == 'failure' else 'stop')
                continue
        elif kind == 'extension_ui_response':
            assert v.get('cancelled') is True, 'fixture must never be approved'
            send({'type':'response','id':active,'command':'prompt','success':True})
            continue
        elif kind == 'abort':
            if native_queue:
                Path('unwanted-native-continuation').touch()
                native_queue = False
            for child in children:
                if child.poll() is None:
                    child.kill();child.wait()
            if running:
                # Real Pi may abort inside a tool without another assistant message.
                running = False
                entry({'role':'toolResult','toolCallId':'tool-1','isError':True,'content':'Command aborted'})
                send({'type':'agent_end','willRetry':False,'messages':[]})
                send({'type':'agent_settled'})
        elif kind in ('abort_bash','clear_queue'):
            if kind == 'clear_queue': native_queue = False
            data = {'steering':[],'followUp':[]}
        else:
            raise RuntimeError('unexpected RPC command '+kind)
        send({'type':'response','id':ident,'command':kind,'success':True,**({'data':data} if data is not None else {})})
finally:
    for child in children:
        if child.poll() is None:
            child.kill();child.wait()
