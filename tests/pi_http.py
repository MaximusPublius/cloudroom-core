"""Shared Rust HTTP lifecycle with mixed Codex/Pi protocol fixtures and no database."""
import json
from pathlib import Path
import time
import unittest

import core_fixture
from core_e2e import until, next_event

class MixedHarnessTests(unittest.TestCase):
    tearDown, start = core_fixture.ReplayTests.tearDown, core_fixture.ReplayTests.start

    def setUp(self):
        core_fixture.ReplayTests.setUp(self)
        home = self.root / 'pi-home'
        home.mkdir()
        self.env.update(CLOUDROOM_PI_HOME=str(home), CLOUDROOM_PI_BINARY=str(Path(__file__).with_name('pi_fixture.py').resolve()), CLOUDROOM_PI_MODEL='fixture', CLOUDROOM_PI_PROVIDER='fixture')
        sequence = 0
        for kind in ['codex','pi']:
            native = kind+'-native'
            directory = Path(self.env['CLOUDROOM_'+kind.upper()+'_HOME'])/'sessions'
            directory.mkdir()
            path = directory/(native+'.jsonl')
            path.write_text(json.dumps({'type':'session','version':3,'id':native,'cwd':str(self.repo.resolve())})+'\n' if kind=='pi' else '{"fixture":"seed"}\n')
            records = [('receipt',{'request_id':kind,'command':'start','input':{'harness':'pi'} if kind=='pi' else {},'state':'completed'}),('native_identity',{'id':native,'path':str(path)}),('state',{'state':'idle'})]
            for name,data in records:
                sequence += 1
                (self.state/f'{sequence:020}.record').write_text(json.dumps({'sequence':sequence,'session_id':'cr_'+kind,'kind':name,'data':data}))
        self.start()
        for kind in ['codex','pi']:
            until(lambda:self.service.session('cr_'+kind)['state']=='idle','ready '+kind,10)

    def send(self,kind,key,text):
        return self.service.request('POST',f'/v1/sessions/cr_{kind}/prompts',{'request_id':key,'text':text},202)

    def settled(self,kind,key,state='completed'):
        until(lambda:self.service.session('cr_'+kind)['receipts'][key]['state']==state,'settled '+key,10)

    def test_same_api_queue_interrupt_replay_and_restart(self):
        for kind in ['codex','pi']:
            self.assertEqual(self.service.request('POST','/v1/sessions',{'request_id':kind,'harness':kind},202)['session_id'],'cr_'+kind)
            self.service.request('POST','/v1/sessions',{'request_id':kind,'harness':'pi' if kind=='codex' else 'codex'},409)
            self.send(kind,'active','hold')
        until(lambda:(self.repo/'pi-native.ticks').exists() and (self.repo/'codex-native.ticks').exists(),'both tools active',8)
        for kind in ['codex','pi']:
            for key in ['queued-a','queued-b']:
                self.send(kind,key,'hello')
            self.send(kind,'queued-a','hello')
            self.service.request('POST',f'/v1/sessions/cr_{kind}/prompts',{'request_id':'queued-a','text':'different'},409)
            self.assertEqual(self.service.session('cr_'+kind)['queue'],['queued-a','queued-b'])
        before=(self.repo/'codex-native.ticks').read_text()
        self.service.request('POST','/v1/sessions/cr_pi/interrupt',{'request_id':'stop','target_request_id':'active'},202)
        self.settled('pi','active','interrupted');self.settled('pi','queued-b')
        time.sleep(.2)
        self.assertNotEqual((self.repo/'codex-native.ticks').read_text(),before)
        self.service.request('POST','/v1/sessions/cr_codex/interrupt',{'request_id':'stop','target_request_id':'active'},202)
        self.settled('codex','active','interrupted');self.settled('codex','queued-b')
        for kind in ['codex','pi']:
            records=self.service.records('cr_'+kind)
            order=[r['data']['request_id'] for r in records if r['kind']=='state' and r['data'].get('state')=='starting_turn']
            self.assertEqual(order,['active','queued-a','queued-b'])
            connection,response=self.service.stream('cr_'+kind,records[-2]['sequence'])
            try:self.assertEqual(next_event(response),records[-1])
            finally:response.close();connection.close()
        self.service.stop();self.start()
        for kind in ['codex','pi']:
            until(lambda:self.service.session('cr_'+kind)['state']=='idle','resume '+kind,10)
            self.assertEqual(self.service.session('cr_'+kind)['native_id'],kind+'-native')
            self.send(kind,'after-restart','hello');self.settled(kind,'after-restart')
        dashboard=self.service.request('GET','/v1/dashboard')
        self.assertEqual({s['harness'] for s in dashboard['sessions']},{'codex','pi'})

    def test_pi_thinking_selection_survives_restart_and_preserves_request_identity(self):
        self.service.stop()
        path = self.state / '00000000000000000004.record'
        record = json.loads(path.read_text())
        self.assertEqual(record['data']['request_id'], 'pi')
        record['data']['input']['reasoning'] = 'high'
        path.write_text(json.dumps(record))
        self.start()
        until(lambda:self.service.session('cr_pi')['state']=='idle','Pi with selected thinking',10)
        self.assertEqual(self.service.session('cr_pi')['reasoning'], 'high')
        self.service.request('POST','/v1/sessions',{'request_id':'pi','harness':'pi','reasoning':'high'},202)
        self.service.request('POST','/v1/sessions',{'request_id':'pi','harness':'pi','reasoning':'medium'},409)
        frames=[json.loads(r['native']) for r in self.service.records('cr_pi') if r.get('native') and r['kind']!='native_record']
        self.assertTrue(any(f.get('command')=='get_state' and f.get('data',{}).get('thinkingLevel')=='high' for f in frames))
        self.send('pi','thinking-check','hello');self.settled('pi','thinking-check')
        self.service.stop();self.start()
        until(lambda:self.service.session('cr_pi')['state']=='idle','Pi thinking after restart',10)
        self.assertEqual(self.service.session('cr_pi')['reasoning'], 'high')

    def test_interrupt_clears_pi_internal_continuations_not_cloudroom_queue(self):
        self.send('pi','active','hold-native-queue')
        until(lambda:(self.repo/'pi-native.ticks').exists(),'native tool with internal queue',5)
        self.send('pi','next-user-request','hello')
        self.service.request('POST','/v1/sessions/cr_pi/interrupt',{'request_id':'stop','target_request_id':'active'},202)
        self.settled('pi','active','interrupted')
        self.settled('pi','next-user-request')
        self.assertFalse((self.repo/'unwanted-native-continuation').exists())

    def test_missing_pi_file_keeps_history_and_other_harness_available(self):
        native_path = Path(self.service.session('cr_pi')['native_path'])
        self.service.stop()
        native_path.unlink()
        self.start()
        self.assertEqual(self.service.session('cr_pi')['state'], 'process_lost')
        self.assertTrue(self.service.records('cr_pi'))
        self.assertFalse(native_path.exists())
        until(lambda:self.service.session('cr_codex')['state']=='idle','Codex survives unrelated native loss',10)
        self.send('codex','unaffected','hello');self.settled('codex','unaffected')

    def test_pi_no_run_input_retry_compaction_and_close(self):
        for text in ['handled','dialog','retry','failure','reject']:
            self.send('pi',text,text)
            self.settled('pi',text,'failed' if text in ['failure','reject'] else 'completed')
        self.send('pi','hold','hold')
        until(lambda:(self.repo/'pi-native.ticks').exists(),'tool before close',5)
        self.send('pi','must-not-run','hello')
        self.service.request('POST','/v1/sessions/cr_pi/close',{'request_id':'close'},202)
        until(lambda:self.service.session('cr_pi')['state']=='closed','closed',7)
        self.settled('pi','must-not-run','failed')
        before=(self.repo/'pi-native.ticks').read_text();time.sleep(.2)
        self.assertEqual((self.repo/'pi-native.ticks').read_text(),before)
        self.service.stop();self.start()
        self.assertEqual(self.service.session('cr_pi')['state'],'closed')

if __name__=='__main__': unittest.main()
