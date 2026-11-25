from datetime import datetime, timezone
import json
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, patch

from daily_bets.publication_contract import acknowledge
from daily_bets.publication_receipt import record_publication, recover_publications, record_quotes, QuoteBatch
from test_publication_contract import sample, TABLE, START


class PublicationWriteTests(unittest.IsolatedAsyncioTestCase):
    async def test_confirmation_is_a_separate_append_after_source_commit(self):
        conn=SimpleNamespace(is_in_transaction=lambda:False,execute=AsyncMock(return_value='INSERT 0 1'))
        def at(a,**kw):return acknowledge(a,**kw,now=datetime(2030,1,2,12,2,tzinfo=timezone.utc))
        with patch('daily_bets.publication_receipt.acknowledge',side_effect=at):
            await record_publication(conn,analysis_text=json.dumps(sample()),source_table=TABLE,event_start=START)
        sql,payload,receipt_id=conn.execute.await_args.args
        self.assertIn('INSERT INTO propgpt_evidence.publication_receipts',sql)
        self.assertIn('ON CONFLICT',sql)
        self.assertNotIn('UPDATE',sql)
        event=json.loads(payload)
        self.assertEqual(event['receipt_id'],receipt_id)
        self.assertEqual(event['acknowledged_at'],'2030-01-02T12:02:00+00:00')
        self.assertFalse(event['app_exposure_verified'])

    async def test_enclosing_transaction_cannot_claim_a_commit(self):
        conn=SimpleNamespace(is_in_transaction=lambda:True,execute=AsyncMock())
        with self.assertRaisesRegex(ValueError,'must commit'):
            await record_publication(conn,analysis_text=json.dumps(sample()),source_table=TABLE,event_start=START)
        conn.execute.assert_not_awaited()

    async def test_failed_evidence_append_does_not_report_success(self):
        conn=SimpleNamespace(is_in_transaction=lambda:False,execute=AsyncMock(side_effect=RuntimeError('append failed')))
        def at(a,**kw):return acknowledge(a,**kw,now=datetime(2030,1,2,12,2,tzinfo=timezone.utc))
        with patch('daily_bets.publication_receipt.acknowledge',side_effect=at),self.assertRaisesRegex(RuntimeError,'append failed'):
            await record_publication(conn,analysis_text=json.dumps(sample()),source_table=TABLE,event_start=START)

    async def test_recovery_records_a_later_bound_without_reinserting_forecasts(self):
        conn=SimpleNamespace(is_in_transaction=lambda:False,execute=AsyncMock(return_value='INSERT 0 1'),
             fetch=AsyncMock(return_value=[dict(analysis=sample(),game_time=START)]))
        def at(a,**kw):return acknowledge(a,**kw,now=datetime(2030,1,2,12,5,tzinfo=timezone.utc))
        with patch('daily_bets.publication_receipt.acknowledge',side_effect=at):
            self.assertEqual(await recover_publications(conn,TABLE),1)
        sql,payload,_=conn.execute.await_args.args
        self.assertIn('publication_receipts',sql)
        self.assertEqual(json.loads(payload)['publication_basis'],'committed_source_row_reobserved_by_publisher')

    async def test_quote_capture_retains_both_players_and_books_before_deduplication(self):
        from datetime import date
        from neverraise import Ok
        from daily_bets.analysis import nfl
        from daily_bets.odds_api import SportEvent,Game,Bookmaker,Market,Outcome
        event=SportEvent('event','americanfootball_nfl','NFL',START,'Home','Away')
        outcomes=[Outcome('Over',name,1.9,5.5) for name in ['Player A','Player B']]
        books=[Bookmaker(book,book,[Market('player_receptions','2020-01-01T00:00:00Z',outcomes)]) for book in ['book-a','book-b']]
        game=Game(event.id,event.sport_key,event.sport_title,event.commence_time,event.home_team,event.away_team,books)
        with patch.object(nfl,'fetch_tomorrow_events',AsyncMock(return_value=Ok([event]))),patch.object(nfl,'fetch_game',AsyncMock(return_value=Ok(game))),patch.object(nfl.asyncio,'sleep',AsyncMock()):
            batch=await nfl.get_analysis_params(object(),date(2030,1,2),date(2030,1,2))
        self.assertEqual(len(batch),2)
        self.assertEqual(len(batch.quote_observations),4)
        self.assertEqual(len({q['quote_fingerprint'] for q in batch.quote_observations}),4)
        self.assertEqual({q['subject_name'] for q in batch.quote_observations},{'Player A','Player B'})
        conn=SimpleNamespace(executemany=AsyncMock())
        acquire=AsyncMock();acquire.__aenter__.return_value=conn
        self.assertEqual(await record_quotes(SimpleNamespace(acquire=lambda:acquire),batch),4)
        sql,rows=conn.executemany.await_args.args
        self.assertIn('market_quotes',sql)
        self.assertTrue(all(r[1].tzinfo is not None for r in rows))
