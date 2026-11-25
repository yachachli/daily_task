"""Freeze exact responses/quotes, then acknowledge committed source publications."""
import json
import os
import re
from datetime import datetime
from .publication_contract import receipt, acknowledge, ack_insert_sql, quote_snapshot, missing_publications_sql


def attach_receipt(response_text, *, request_json, endpoint_alias, event_id,
                   event_start, publisher_job, environ=None, now=None, quote_evidence=None, game_tag=None):
    if publisher_job not in {'nfl', 'nba', 'nba_alt'}:
        from .legacy_publication_receipt import attach_receipt as legacy
        return legacy(response_text, request_json=request_json, endpoint_alias=endpoint_alias,
                      event_id=event_id, event_start=event_start, publisher_job=publisher_job,
                      environ=environ, now=now)
    env = os.environ if environ is None else environ
    commit = env.get('GITHUB_SHA', '')
    publisher = {
        'publisher_repository':env.get('GITHUB_REPOSITORY'),
        'publisher_commit':commit if re.fullmatch('[0-9a-f]{40}', commit) else None,
        'publisher_run_id':env.get('GITHUB_RUN_ID'),
        'publisher_run_attempt':env.get('GITHUB_RUN_ATTEMPT'),
        'publisher_job':publisher_job, 'endpoint_alias':endpoint_alias,
        'event_provider':'the-odds-api',
    }
    payload = receipt(response_text, request=json.loads(request_json), publisher=publisher,
                      event_id=event_id, event_start=event_start, now=now,
                      quote_evidence=quote_evidence,game_tag=game_tag)
    return json.dumps(payload, separators=(',', ':'), allow_nan=False)


async def record_publication(conn, *, analysis_text, source_table, event_start,
                             basis='source_insert_commit_acknowledged_by'):
    # The upsert's implicit transaction must finish before this separate append.
    if conn.is_in_transaction():
        raise ValueError('Source INSERT must commit before acknowledgement')
    event = acknowledge(json.loads(analysis_text), source_table=source_table,
                        event_start=event_start, basis=basis)
    return await conn.execute(ack_insert_sql(source_table, asyncpg=True),
                              json.dumps(event), event['receipt_id'])


async def recover_publications(conn, source_table):
    if conn.is_in_transaction():
        raise ValueError('Recovery requires an idle connection')
    rows = await conn.fetch(missing_publications_sql(source_table))
    for row in rows:
        analysis = row['analysis']
        await record_publication(conn, analysis_text=analysis if isinstance(analysis,str) else json.dumps(analysis),
            source_table=source_table, event_start=row['game_time'],
            basis='committed_source_row_reobserved_by_publisher')
    return len(rows)


def freeze_quote(game, bookmaker, market, outcome, observed_at):
    return quote_snapshot(provider='the-odds-api', event_id=game.id,
                          subject_name=outcome.description,
                          bookmaker=bookmaker.key, market=market.key,
                          side=outcome.name, line=outcome.point, price=outcome.price,
                          provider_updated_at=market.last_update, observed_at=observed_at)


class QuoteBatch(list):
    """Original recommendation candidates plus every fetched quote observation."""
    def __init__(self, params, quotes):
        super().__init__(params)
        self.quote_observations = quotes


async def record_quotes(pool, params):
    quotes = getattr(params, 'quote_observations', [])
    if not quotes:
        return 0
    async with pool.acquire() as conn:
        await conn.executemany('''INSERT INTO propgpt_evidence.market_quotes
            (quote_fingerprint,observed_at,payload) VALUES ($1,$2,$3::jsonb)
            ON CONFLICT (quote_fingerprint) DO NOTHING''',
            [(q['quote_fingerprint'], datetime.fromisoformat(q['observed_at']),
              json.dumps(q)) for q in quotes])
    return len(quotes)
