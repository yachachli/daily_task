import copy
from datetime import datetime, timezone
import json
import sys
from pathlib import Path
import unittest

from daily_bets.publication_contract import receipt, acknowledge, timing, valid_receipt, quote_snapshot, sha

START = '2030-01-02T20:00:00Z'
TABLE = 'public.v2_nfl_daily_bets'


def sample():
    payload = dict(input=dict(player_id=7, stat='receptions', line=4.5), over_under='over',
                   grade=80, projected_value=6, model_version='model:revision',
                   forecast_generated_at='2030-01-02T12:00:00Z',
                   model_provenance=dict(deployment_revision='revision', source_manifest_sha256='a'*64))
    return receipt(json.dumps(payload), event_id='event', event_start=START,
                   publisher={'publisher_repository':'example/publisher'},
                   now=datetime(2030,1,2,12,1,tzinfo=timezone.utc))


def publication(analysis):
    event = acknowledge(analysis, source_table=TABLE, event_start=START,
                        now=datetime(2030,1,2,12,2,tzinfo=timezone.utc))
    return dict(source_table=TABLE, source_row='1', receipt_id=event['receipt_id'],
                recorded_at='2030-01-02T12:02:01Z', payload=event)


class ContractTests(unittest.TestCase):
    def test_generation_response_and_publication_are_distinct_utc_times(self):
        a = sample();p = publication(a)
        published,e = timing(a, datetime(2030,1,2,12,2), START, TABLE, p)
        self.assertEqual(published,'2030-01-02T12:02:01+00:00')
        self.assertEqual(e['forecast_generated_at'],'2030-01-02T12:00:00+00:00')
        self.assertEqual(e['source_created_at_raw'],'2030-01-02 12:02:00')
        self.assertIsNone(e['source_created_at_utc'])
        self.assertFalse(e['app_exposure_verified'])

    def test_naive_history_and_response_receipt_never_become_publication(self):
        for a in ({}, {'_publication_receipt':{'schema':'publisher-receipt-v1','response_received_at':'2030-01-02T12:00:00Z'}}, sample()):
            self.assertIsNone(timing(a,'2030-01-02T12:00:00',START,TABLE)[0])
        self.assertIsNone(timing(sample(),'2030-01-02T12:00:00Z',START,TABLE)[0])
        self.assertEqual(timing({},'2030-01-02T12:00:00-05:00',START,TABLE)[0], '2030-01-02T17:00:00+00:00')

    def test_exact_forecast_receipt_and_ack_binding_rejects_changes(self):
        a = sample();p = publication(a)
        for key,value in [('grade',81),('over_under','under'),('projected_value',7),('model_version','other'),
                          ('forecast_generated_at','2030-01-02T13:00:00Z')]:
            changed=dict(a,**{key:value})
            self.assertFalse(valid_receipt(changed,START))
            self.assertIsNone(timing(changed,None,START,TABLE,p)[0])
        for key,value in [('receipt_id','b'*64),('source_table','public.v2_cfb_daily_bets')]:
            changed=copy.deepcopy(p);changed[key]=value
            self.assertIsNone(timing(a,None,START,TABLE,changed)[0])
        changed=copy.deepcopy(p);changed['payload']['acknowledged_at']='2030-01-02T11:00:00Z'
        changed['payload']['ack_id']=sha({k:v for k,v in changed['payload'].items() if k!='ack_id'})
        self.assertIsNone(timing(a,None,START,TABLE,changed)[0])

    def test_missing_artifact_or_generation_metadata_stays_unknown(self):
        for key in ['forecast_generated_at','model_provenance','model_version']:
            original=json.loads(sample()['_publication_receipt']['response_text']);original.pop(key)
            a=receipt(json.dumps(original), event_id='event',event_start=START,publisher={},
                      now=datetime(2030,1,2,12,1,tzinfo=timezone.utc))
            self.assertIsNone(timing(a,None,START,TABLE,publication(a))[0])

    def test_wrong_event_and_naive_times_cannot_acknowledge(self):
        with self.assertRaises(ValueError):
            acknowledge(sample(),source_table=TABLE,event_start='2030-01-02T21:00:00Z')
        with self.assertRaises(ValueError):
            acknowledge(sample(),source_table=TABLE,event_start=START,now=datetime(2030,1,2))
        self.assertFalse(valid_receipt({'_publication_receipt':[]},START))

    def test_quote_preserves_provider_and_observation_times_and_identity(self):
        q=quote_snapshot(provider='provider',event_id='event',bookmaker='book',market='receptions',
                         side='Over',line=4.5,price=1.9,provider_updated_at='2030-01-02T06:59:00-05:00',
                         observed_at='2030-01-02T12:00:00Z')
        self.assertEqual(q['provider_updated_at'],'2030-01-02T11:59:00+00:00')
        self.assertEqual(q['observed_at'],'2030-01-02T12:00:00+00:00')
        self.assertEqual(q['quote_fingerprint'],sha({k:v for k,v in q.items() if k!='quote_fingerprint'}))
        missing=quote_snapshot(provider='p',event_id='e',bookmaker='b',market='m',side='over',line=1,price=2,
                              provider_updated_at='2030-01-02T12:00:00',observed_at='2030-01-02T12:00:00Z')
        self.assertIsNone(missing['provider_updated_at'])


if __name__ == '__main__':
    unittest.main()
