import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from main import Monitor, TRACK_TOPIC, format_result, parse_sockjs
from storage import SQLiteStore


def catalog(race_status='OPEN'):
    return [
        {'id':5227,'country':'URU','raceCards':[{'id':58205,'name':'Maronas URU','races':[{'id':2644019,'raceNumber':14,'status':'OPEN'}]}]},
        {'id':690,'country':'PE','raceCards':[{'id':58215,'name':'Monterrico','races':[{'id':2644114,'raceNumber':1,'status':race_status}]}]}
    ]


def result():
    return {'raceId':2644114,'raceNumber':1,'complete':True,'runnersResults':[
        {'position':1,'programNumber':'2','horse':{'name':'DR. BRUNO'},'payouts':{'win':'4.20','place':'3.60'}},
        {'position':2,'programNumber':'7','horse':{'name':'LUCIANA MARIA'},'payouts':{'place':'5.10'}},
        {'position':3,'programNumber':'1','horse':{'name':'COMANDO SVR'},'payouts':{}},
        {'position':4,'programNumber':'6','horse':{'name':'TED LASSO'},'payouts':{}}
    ],'exoticBetsResults':[{'betType':{'name':'Exacta'},'winningRunners':'2/7','amount':'2.00','payout':'9.60'}]}


class TestMonitor(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.dbpath = Path(self.temp.name)/'test.sqlite3'
        self.m = Monitor(SQLiteStore(self.dbpath))
        self.sent_stomp=[]
        self.m.set_sender(self.sent_stomp.append)
        self.rid=2644114

    def tearDown(self):
        self.temp.cleanup()

    def test_formatting(self):
        text=format_result(58215,1,result())
        self.assertIn('DR. BRUNO',text)
        self.assertIn('S/ 4.20',text)
        self.assertIn('Exacta',text)
        self.assertNotIn('5. –',text)

    def test_filters_maronas_and_ignores_historical(self):
        self.m.accept('MESSAGE',TRACK_TOPIC,catalog('OFFICIALLY_COMPLETED'))
        self.assertEqual(self.m.monitored,{})
        self.assertEqual(self.m.db.existing_digest(self.rid),None)
        self.assertNotIn(2644019,self.m.monitored)

    def test_queues_once_and_resume_after_reconnect(self):
        self.m.accept('MESSAGE',TRACK_TOPIC,catalog())
        self.assertNotIn(2644019,self.m.monitored)
        self.assertIn(self.rid,self.m.monitored)
        dest_result=f'/broker/v2.race.{self.rid}.es-PR.result'
        dest_status=f'/broker/v3.race.{self.rid}.es-PR'
        self.m.accept('MESSAGE',dest_result,result())
        self.assertIsNone(self.m.db.existing_digest(self.rid))
        self.m.accept('MESSAGE',dest_status,{'raceCardId':58215,'status':'OFFICIALLY_COMPLETED'})
        digest=self.m.db.existing_digest(self.rid)
        self.assertIsNotNone(digest)
        self.m.accept('MESSAGE',dest_result,result())
        self.assertEqual(digest,self.m.db.existing_digest(self.rid))
        second=Monitor(SQLiteStore(self.dbpath))
        second.set_sender(lambda _:None)
        second.accept('MESSAGE',TRACK_TOPIC,catalog('OFFICIALLY_COMPLETED'))
        second.accept('MESSAGE',dest_result,result())
        self.assertEqual(digest,second.db.existing_digest(self.rid))

    def test_sockjs_stomp_result_payload(self):
        import json
        frame='MESSAGE\ndestination:/broker/v2.race.2644114.es-PR.result\ncontent-type:application/json\n\n'+json.dumps(result())+'\x00'
        parsed=parse_sockjs('a'+json.dumps([frame]))
        self.assertEqual(parsed[0][0],'MESSAGE')
        self.assertEqual(parsed[0][2]['raceId'],self.rid)

if __name__=='__main__': unittest.main()
