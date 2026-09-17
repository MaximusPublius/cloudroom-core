#!/usr/bin/env python3
"""Disk safety on an explicitly disposable Linux VM. Run as root, never a customer host.
Uses real ext4 user quotas, cgroup freezing, the Rust API and a protocol fixture.
Model/DB fixtures are not a claim of BB or real-inference delivery.
"""
import errno
import hashlib
import http.client
import json
import os
from pathlib import Path
import pwd
import secrets
import shutil
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS = ROOT / 'docs/database'
if not (MIGRATIONS / '0001-session-records.sql').is_file():
    MIGRATIONS = ROOT.parent / 'docs/database'


def fixture():
    native = 'disk-' + str(os.getpid())
    home = Path(os.environ['CODEX_HOME']) / 'sessions'
    home.mkdir(exist_ok=True)
    rollout = home / (native + '.jsonl')
    rollout.write_text('{"start":true}\n')
    child = None
    held = []
    def send(v): print(json.dumps(v), flush=True)
    for line in sys.stdin:
        v = json.loads(line); method = v.get('method'); p = v.get('params', {})
        if 'id' not in v: continue
        result = {}
        if method == 'thread/inject_items':
            if (Path.cwd() / 'hold-cache').exists():
                held.append(open((Path.cwd() / 'hold-cache').read_text(), 'rb'))
        if method in ('thread/start', 'thread/resume'):
            result = {'thread': {'id': native, 'path': str(rollout)}, 'model': p['model']}
        elif method == 'turn/start':
            turn = p['clientUserMessageId']
            send({'method': 'turn/started', 'params': {'threadId': native, 'turn': {'id': turn, 'status': 'inProgress'}}})
            result = {'turn': {'id': turn}}
            # Detached child proves freeze covers descendants even after setsid().
            child = subprocess.Popen([sys.executable, '-c',
                "from pathlib import Path; import time,os\np=Path('ticks-" + native + "');Path('pid-" + native + "').write_text(str(os.getpid()))\nfor n in range(10000):\n p.write_text(str(n));time.sleep(.05)"],
                start_new_session=True, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        elif method == 'thread/inject_items':
            with (Path.cwd() / ('warning-' + native)).open('a') as f: f.write(json.dumps(p['items']) + '\n')
            with rollout.open('a') as f: f.write(json.dumps({'injected': p['items']}) + '\n')
        send({'id': v['id'], 'result': result})
    if child: child.kill(); child.wait()


def run(*args, **kw):
    p = subprocess.run(args, capture_output=True, text=True, timeout=kw.pop('timeout', 30), **kw)
    if p.returncode: raise AssertionError((args[:3], p.returncode, p.stdout[-2000:], p.stderr[-2000:]))
    return p.stdout.strip()


def wait(check, label, timeout=30):
    until = time.monotonic() + timeout
    while time.monotonic() < until:
        result = check()
        if result: return result
        time.sleep(.1)
    raise AssertionError(label)


def main():
    assert os.geteuid() == 0 and sys.argv[1:] in (['--disposable'], ['--disposable', '--mixed']), 'Explicit disposable VM only'
    mixed = '--mixed' in sys.argv
    agent = pwd.getpwnam('cr-disk-test'); service = pwd.getpwnam('cr-service-test')
    run('setquota', '-u', agent.pw_name, '0', '0', '0', '0', '/')
    unit = 'cloudroom-storage-e2e-' + secrets.token_hex(4)
    base = Path('/var/lib') / unit; base.mkdir(mode=0o755)
    work = base / 'work'; home = base / 'home'; cache = base / 'cache'; state = base / 'state'
    for p in (work, home, cache):
        p.mkdir(mode=0o700); os.chown(p, agent.pw_uid, agent.pw_gid)
    state.mkdir(mode=0o700); os.chown(state, service.pw_uid, service.pw_gid)
    (home / '.codex').mkdir(); os.chown(home / '.codex', agent.pw_uid, agent.pw_gid)
    (home / '.codex').chmod(0o700)
    if mixed:
        (home / '.pi/agent').mkdir(parents=True)
        for p in (home / '.pi', home / '.pi/agent'):
            os.chown(p, agent.pw_uid, agent.pw_gid); p.chmod(0o700)
    run('runuser', '-u', agent.pw_name, '--', 'git', 'init', '-q', str(work))
    used = int(next(l for l in run('repquota', '-u', '/').splitlines() if l.startswith(agent.pw_name+' ')).split()[2])
    hard = used + 256 * 1024
    run('setquota', '-u', agent.pw_name, '0', str(hard), '0', '1000000', '/')
    # An owner-local disposable PostgreSQL; no production data/credentials.
    container = unit
    token = secrets.token_hex(24)
    policy = dict(agent_uid=agent.pw_uid, agent_gid=agent.pw_gid, quota_mount='/', quota_limit_bytes=hard*1024,
                  cache_dir=str(cache), cgroup_root='/sys/fs/cgroup/system.slice/'+unit+'.service/agents', reserve_bytes=10_000_000_000,
                  warning_bytes=64*1024*1024, pause_bytes=32*1024*1024, resume_bytes=80*1024*1024)
    policy_file = base / 'policy.json'; policy_file.write_text(json.dumps(policy)); policy_file.chmod(0o644)
    env = base / 'env'
    env.write_text('\n'.join([f'CLOUDROOM_TOKEN={token}',f'CLOUDROOM_STORAGE_POLICY={policy_file}',f'CLOUDROOM_STATE_DIR={state}',
        f'CLOUDROOM_REPOSITORY={work}',f'CLOUDROOM_ACCOUNT_HOME={home}',f'CLOUDROOM_CODEX_HOME={home}/.codex',
        f'CLOUDROOM_CODEX_BINARY={__file__}', 'CLOUDROOM_MODEL=fixture','CLOUDROOM_MAX_HARNESSES=2','CLOUDROOM_LISTEN=127.0.0.1:19842',
        'CLOUDROOM_ALLOW_INSECURE_DATABASE=1','CLOUDROOM_STORE=disk-fixture','CLOUDROOM_DATABASE_URL=postgres://postgres:fixture@127.0.0.1:19843/disk_fixture'])+'\n')
    if mixed:
        with env.open('a') as f:
            f.write(f'CLOUDROOM_PI_BINARY={ROOT}/tests/pi_fixture.py\nCLOUDROOM_PI_HOME={home}/.pi/agent\nCLOUDROOM_PI_MODEL=fixture\nCLOUDROOM_PI_PROVIDER=fixture\n')
    env.chmod(0o600)
    def ticks():
        return list(work.glob('ticks-*')) + list(work.glob('*.ticks'))
    def request(method, path, body=None, status=200):
        c=http.client.HTTPConnection('127.0.0.1',19842,timeout=5)
        c.request(method,path,json.dumps(body) if body is not None else None,{'Authorization':'Bearer '+token,'Content-Type':'application/json'})
        r=c.getresponse();text=r.read();c.close(); assert r.status==status,(r.status,text)
        return json.loads(text)
    def ready():
        try:return request('GET','/v1/health')['storage']['level']=='normal'
        except (OSError,AssertionError):return False
    def session(sid):return request('GET','/v1/sessions/'+sid)['session']
    def events(sid):return request('GET','/v1/sessions/'+sid+'/events')['events']
    def write_as_agent(path, size):
        run('runuser','-u',agent.pw_name,'--','python3','-c',
            "from pathlib import Path\np=Path("+repr(str(path))+ ");p.parent.mkdir(parents=True,exist_ok=True)\nwith p.open('wb') as f:\n for _ in range("+str(size)+"):f.write(b'x'*1048576)")
    checks=[]
    def passed(text):checks.append(text);print('PASS:',text,flush=True)
    try:
        probe = """import errno,os,shutil\nfrom pathlib import Path\na=Path('quota-home'); b=Path('/tmp/"""+unit+"""-quota')\ntry:\n a.write_bytes(b'x'*1048576*16)\n with b.open('wb') as f:\n  for _ in range(300):f.write(b'x'*1048576);f.flush()\n raise AssertionError('quota did not stop writes')\nexcept OSError as e:\n assert e.errno==errno.EDQUOT,e\n assert shutil.disk_usage('/').free>10000000000\nfinally:\n a.unlink(missing_ok=True);b.unlink(missing_ok=True)\n"""
        run('runuser','-u',agent.pw_name,'--','python3','-c',probe,cwd=work)
        run('runuser','-u',service.pw_name,'--','sh','-c','echo protected > '+str(state/'probe'))
        passed('kernel quota stops writes across workspace and /tmp; protected service retains 10 GB and can write')
        run('docker','run','-d','--name',container,'--memory','256m','--cpus','.5','--pids-limit','64','-e','POSTGRES_PASSWORD=fixture','-e','POSTGRES_DB=disk_fixture','-p','127.0.0.1:19843:5432','postgres:16-alpine',timeout=120)
        wait(lambda: subprocess.run(['docker','exec',container,'pg_isready','-h','127.0.0.1','-U','postgres'],capture_output=True).returncode==0,'database')
        for name in ('0001-session-records.sql', '0002-diagnostics.sql'):
            run('docker','exec','-i',container,'psql','-U','postgres','-d','disk_fixture','-v','ON_ERROR_STOP=1',input=(MIGRATIONS/name).read_text())
        run('systemd-run','--unit='+unit,'--property=User='+service.pw_name,'--property=Group='+service.pw_name,
            '--property=Delegate=yes','--property=AmbientCapabilities=CAP_SETUID CAP_SETGID CAP_DAC_READ_SEARCH CAP_KILL',
            '--property=CapabilityBoundingSet=CAP_SETUID CAP_SETGID CAP_DAC_READ_SEARCH CAP_KILL','--property=NoNewPrivileges=yes',
            '--property=KillMode=control-group','--property=EnvironmentFile='+str(env),str(ROOT/'target/debug/cloudroom'))
        wait(ready,'protected core ready')
        passed('protected service starts with verified root quota and delegated workload groups')
        ids=[]
        for name in ('a','b'):
            body = {'request_id':name}
            if mixed and name == 'b': body['harness'] = 'pi'
            sid=request('POST','/v1/sessions',body,202)['session_id']; ids.append(sid)
            wait(lambda:session(sid)['state']=='idle','session initialization')
            request('POST','/v1/sessions/'+sid+'/prompts',{'request_id':'job-'+name,'text':'hold'},202)
        wait(lambda:len(ticks())==2,'two live tools')
        names=[session(s)['native_id'] for s in ids]
        for p in [*work.glob('pid-*'), *work.glob('*.pid')]:
            pid=p.read_text(); status=Path('/proc/'+pid+'/status').read_text()
            assert 'NoNewPrivs:\t1' in status and 'CapEff:\t0000000000000000' in status
        passed('two real detached tool processes run unprivileged without service capabilities')
        # Narrow content-addressed cache plus sensitive sentinel and deliberately wrong digest.
        data=b'x'*1048576*96; digest=hashlib.sha512(data).hexdigest()
        blob=cache/'npm/_cacache/content-v2/sha512'/digest[:2]/digest[2:4]/digest[4:]
        write_as_agent(blob,96)
        sensitive=work/'.env'; sensitive.write_text('synthetic-secret'); os.chown(sensitive,agent.pw_uid,agent.pw_gid)
        fake=blob.with_name('f'*124); fake.write_text('do not delete'); os.chown(fake,agent.pw_uid,agent.pw_gid)
        # A valid cache blob held open by a running harness must survive cleanup.
        active_data=b'active cache data'; active_hash=hashlib.sha512(active_data).hexdigest()
        active=cache/'npm/_cacache/content-v2/sha512'/active_hash[:2]/active_hash[2:4]/active_hash[4:]
        active.parent.mkdir(parents=True,exist_ok=True); active.write_bytes(active_data)
        for p in (active.parent.parent,active.parent,active):os.chown(p,agent.pw_uid,agent.pw_gid)
        held=work/'hold-cache';held.write_text(str(active));os.chown(held,agent.pw_uid,agent.pw_gid)
        # Even a valid hash with another hardlink is not owned disposable storage.
        linked_data=b'customer kept this'; linked_hash=hashlib.sha512(linked_data).hexdigest()
        linked=cache/'npm/_cacache/content-v2/sha512'/linked_hash[:2]/linked_hash[2:4]/linked_hash[4:]
        linked.parent.mkdir(parents=True,exist_ok=True);linked.write_bytes(linked_data)
        for p in (linked.parent.parent,linked.parent,linked):os.chown(p,agent.pw_uid,agent.pw_gid)
        os.link(linked,work/'kept-output')
        symlink=blob.parent/('e'*124);symlink.symlink_to(sensitive)
        filler=work/'large-output'; write_as_agent(filler,105)
        wait(lambda:request('GET','/v1/health')['storage']['level']=='low_space','warning threshold')
        wait(lambda:all((work/('warning-'+n)).exists() for n in names),'native warning delivered to every busy agent')
        for sid in ids:
            records=events(sid); warnings=[r for r in records if r['kind']=='storage_warning'];assert len(warnings)==1
            # Reconnect reads and live SSE expose the same saved system warning.
            c=http.client.HTTPConnection('127.0.0.1',19842,timeout=5)
            c.request('GET','/v1/sessions/'+sid+'/stream?after='+str(warnings[0]['sequence']-1),headers={'Authorization':'Bearer '+token})
            response=c.getresponse();assert response.status==200
            while True:
                line=response.readline().decode()
                if line.startswith('data:'):
                    assert json.loads(line[5:])['kind']=='storage_warning';break
            response.close();c.close()
        request('POST','/v1/sessions',{'request_id':'blocked'},409)
        request('POST','/v1/sessions/'+ids[0]+'/prompts',{'request_id':'blocked-input','text':'never run'},409)
        passed('warning reaches both busy harnesses and replay history; new sessions and execution blocked')
        time.sleep(3)
        for n in names:assert len((work/('warning-'+n)).read_text().splitlines())==1
        passed('warnings are deduplicated during one low-space episode')
        extra=work/'more-output'; write_as_agent(extra,30)
        wait(lambda:not blob.exists(),'safe cache cleanup',40)
        wait(ready,'automatic recovery after cache cleanup',40)
        assert fake.read_text()=='do not delete' and sensitive.read_text()=='synthetic-secret'
        assert active.read_bytes()==active_data and linked.read_bytes()==linked_data and symlink.is_symlink()
        passed('active cache files, hardlinked outputs, symlinks and wrong-hash files survive cleanup')
        assert filler.stat().st_size==105*1048576 and extra.stat().st_size==30*1048576
        for sid,n in zip(ids,names):
            records=events(sid)
            assert any(r['kind']=='storage_pause' for r in records)
            assert any(r['kind']=='storage_recovered' for r in records)
            assert session(sid)['native_id']==n
        passed('cache cleanup frees space; protected files/outputs survive; same workloads resume')
        before={p:p.read_text() for p in ticks()};time.sleep(.3)
        assert all(p.read_text()!=v for p,v in before.items())
        passed('descendants resume in place without duplicate prompts')
        # No disposable cache remains: remain paused until the operator frees generated output.
        write_as_agent(work/'final-output',105)
        wait(lambda:all(session(s)['storage_paused'] for s in ids),'emergency pause',30)
        before={p:p.read_text() for p in ticks()};time.sleep(.3)
        assert all(p.read_text()==v for p,v in before.items())
        assert request('GET','/v1/health')['status']=='ready'
        assert shutil.disk_usage('/').free>10_000_000_000
        passed('failed cleanup leaves both workloads frozen while core and 10 GB reserve remain available')
        (work/'final-output').unlink();filler.unlink();extra.unlink()
        wait(ready,'operator recovery',30)
        wait(lambda:all(not session(s)['storage_paused'] for s in ids),'confirmed thaw')
        passed('operator frees output; only storage-paused workloads are thawed')
        run('docker','stop','-t','2',container)
        write_as_agent(work/'db-outage-output',230)
        wait(lambda:all(session(s)['storage_paused'] for s in ids),'disk emergency during DB outage')
        assert request('GET','/v1/health')['saving']['pending_records']>0
        (work/'db-outage-output').unlink(); wait(ready,'disk recovery during DB outage')
        passed('database outage preserves pending history while local disk emergency pauses work')
        # The storage guard never authorizes work when kernel quota enforcement disappears.
        run('setquota','-u',agent.pw_name,'0','0','0','0','/')
        wait(lambda:request('GET','/v1/health')['storage']['reason']=='measurement_unavailable','missing quota fails closed')
        request('POST','/v1/sessions',{'request_id':'no-quota'},409)
        passed('missing or mismatched kernel quota blocks execution rather than claiming protection')
    finally:
        subprocess.run(['systemctl','stop',unit],capture_output=True,timeout=20)
        subprocess.run(['docker','rm','-f','-v',container],capture_output=True)
        run('setquota','-u',agent.pw_name,'0','0','0','0','/')
        evidence=ROOT/'private/storage-e2e.json';evidence.parent.mkdir(exist_ok=True)
        evidence.write_text(json.dumps({'checks':checks,'fixture':True,'harnesses':['codex','pi'] if mixed else ['codex'],'root':str(base)},indent=2))
        os.chown(evidence.parent,ROOT.stat().st_uid,ROOT.stat().st_gid)
        os.chown(evidence,ROOT.stat().st_uid,ROOT.stat().st_gid)
        # Keep logs/history for investigation; discard only this test's large filler files.
        for p in work.glob('*output'):p.unlink(missing_ok=True)
    print('Storage checks:',len(checks))

if __name__=='__main__':
    if len(sys.argv)>1 and sys.argv[1]=='app-server':fixture()
    else:main()
