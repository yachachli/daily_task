"""Prospective publisher evidence. Kept identical in producer/collector exports.

An acknowledgement bounds publication after the source INSERT committed. It is
not an exact commit instant, independent observation, or proof of user exposure.
"""
from datetime import datetime, timezone
import hashlib
import json
import math

SCHEMA = 'publisher-receipt-v2'
TABLES = {'public.v2_nfl_daily_bets', 'public.v2_nba_daily_bets',
          'public.v2_nba_alt_daily_bets', 'public.v2_cfb_daily_bets'}


def is_sha(value):
    return isinstance(value,str) and len(value)==64 and all(c in '0123456789abcdef' for c in value)


def utc(value):
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace('Z', '+00:00'))
        except ValueError:
            return None
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        return None
    return value.astimezone(timezone.utc).isoformat()


def sha(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
                                     allow_nan=False).encode()).hexdigest()


def aware_now(value=None):
    result = utc(value if value is not None else datetime.now(timezone.utc))
    if result is None:
        raise ValueError('Explicit timezone required')
    return result


def receipt(response_text, *, event_id, event_start, publisher, request=None,
            quote_evidence=None, now=None):
    payload = json.loads(response_text)
    if not isinstance(payload, dict) or '_publication_receipt' in payload:
        raise ValueError('Expected a response without reserved publication metadata')
    start = utc(event_start)
    if not start:
        raise ValueError('Explicit event timezone required')
    evidence = dict(schema=SCHEMA, response_received_at=aware_now(now),
                    event_id=str(event_id), scheduled_start=start, publisher=publisher,
                    request=request, quote_evidence=quote_evidence,
                    forecast_generated_at=utc(payload.get('forecast_generated_at')),
                    model_version=payload.get('model_version'),
                    model_provenance=payload.get('model_provenance'),
                    response_text=response_text,
                    response_text_sha256=hashlib.sha256(response_text.encode()).hexdigest(),
                    forecast_payload_sha256=sha(payload), app_exposure_verified=False,
                    backend_model_version=payload.get('model_version'),
                    backend_artifact_linkage='serving_revision_and_source_metadata' if payload.get('model_provenance') else 'unverified',
                    probability_semantics='unvalidated; grade is not a probability')
    for key in ('publisher_repository', 'publisher_commit', 'publisher_run_id',
                'publisher_run_attempt', 'publisher_job', 'endpoint_alias', 'event_provider'):
        evidence[key] = publisher.get(key)
    evidence['receipt_id'] = sha(evidence)
    return dict(payload, _publication_receipt=evidence)


def valid_receipt(analysis, event_start):
    if not isinstance(analysis, dict):
        return False
    r = analysis.get('_publication_receipt') or {}
    if not isinstance(r, dict) or r.get('schema') != SCHEMA:
        return False
    try:
        original = json.loads(r['response_text'])
        return (r['receipt_id'] == sha({k:v for k,v in r.items() if k != 'receipt_id'})
                and hashlib.sha256(r['response_text'].encode()).hexdigest() == r['response_text_sha256']
                and sha(original) == r['forecast_payload_sha256']
                and original == {k:v for k,v in analysis.items() if k != '_publication_receipt'}
                and r['scheduled_start'] == utc(event_start)
                and r['forecast_generated_at'] == utc(original.get('forecast_generated_at'))
                and r['model_version'] == original.get('model_version')
                and r['model_provenance'] == original.get('model_provenance'))
    except (KeyError, TypeError, ValueError):
        return False


def acknowledge(analysis, *, source_table, event_start, now=None,
                basis='source_insert_commit_acknowledged_by'):
    if basis not in {'source_insert_commit_acknowledged_by', 'committed_source_row_reobserved_by_publisher'}:
        raise ValueError('Unsupported publication confirmation basis')
    if source_table not in TABLES or not valid_receipt(analysis, event_start):
        raise ValueError('Publication acknowledgement requires an exact supported receipt')
    r = analysis['_publication_receipt']
    at = aware_now(now)
    if r.get('forecast_generated_at') and r['forecast_generated_at'] > at:
        raise ValueError('Generation occurs after acknowledgement')
    if r['response_received_at'] > at:
        raise ValueError('Response occurs after acknowledgement')
    event = dict(schema='publication-ack-v1', receipt_id=r['receipt_id'],
                 source_table=source_table, forecast_payload_sha256=r['forecast_payload_sha256'],
                 acknowledged_at=at, scheduled_start=r['scheduled_start'],
                 publication_basis=basis,
                 app_exposure_verified=False)
    event['ack_id'] = sha(event)
    return event


def ack_insert_sql(source_table, *, asyncpg=False):
    if source_table not in TABLES:
        raise ValueError('Unsupported publication source')
    placeholders = ('$1', '$2') if asyncpg else ('%(event)s', '%(receipt_id)s')
    # The caller MUST have completed the source-row transaction before this
    # separate append. A retry cannot update the original immutable event.
    return f'''INSERT INTO propgpt_evidence.publication_receipts
        (source_table, source_row, receipt_id, acknowledged_at, payload)
        SELECT '{source_table}', id::text, {placeholders[0]}::jsonb->>'receipt_id',
               ({placeholders[0]}::jsonb->>'acknowledged_at')::timestamptz, {placeholders[0]}::jsonb
        FROM {source_table}
        WHERE analysis->'_publication_receipt'->>'receipt_id'={placeholders[1]}
        ON CONFLICT (source_table, source_row, receipt_id) DO NOTHING'''


def missing_publications_sql(source_table):
    if source_table not in TABLES:
        raise ValueError('Unsupported publication source')
    return f'''SELECT analysis,game_time FROM {source_table} s
        WHERE analysis->'_publication_receipt'->>'schema'='{SCHEMA}'
        AND game_time>clock_timestamp() AND NOT EXISTS (
            SELECT 1 FROM propgpt_evidence.publication_receipts p
            WHERE p.source_table='{source_table}' AND p.source_row=s.id::text
            AND p.receipt_id=s.analysis->'_publication_receipt'->>'receipt_id')
        ORDER BY id LIMIT 1000'''


def timing(analysis, source_created_at, event_start, source_table, publication=None):
    """Keep naive historical times unknown; never turn response arrival into publication."""
    raw = str(source_created_at) if source_created_at is not None else None
    r = analysis.get('_publication_receipt') or {}
    r = r if isinstance(r, dict) else {}
    evidence = dict(schema='forecast-timing-v1', source_created_at_raw=raw,
                    source_created_at_utc=utc(source_created_at),
                    source_timezone_status='aware' if utc(source_created_at) else 'missing' if raw is None else 'naive_or_invalid',
                    generation_basis=None, publication_basis='legacy_source_timestamp_proxy',
                    forecast_generated_at=None, publication_confirmed_by=None,
                    publication_ack_id=None, app_exposure_verified=False)
    if r.get('schema') != SCHEMA:
        return utc(source_created_at), evidence
    # Invalid new metadata must not inherit a legacy proxy's apparent certainty.
    evidence['publication_basis'] = 'unverified_receipt_or_acknowledgement'
    if not valid_receipt(analysis, event_start):
        return None, evidence
    generated = utc(r.get('forecast_generated_at'))
    received = utc(r.get('response_received_at'))
    provenance = r.get('model_provenance') or {}
    if not isinstance(provenance, dict):
        return None, evidence
    if not (generated and received and generated <= received and provenance.get('deployment_revision')
            and is_sha(provenance.get('source_manifest_sha256')) and r.get('model_version')):
        return None, evidence
    if source_table == 'public.v2_cfb_daily_bets':
        artifact = provenance.get('artifact_identity') or {}
        if not isinstance(artifact,dict) or not is_sha(artifact.get('release_sha256')):
            return None, evidence
    evidence.update(forecast_generated_at=generated, generation_basis='scoring_response_completed')
    p = publication if isinstance(publication, dict) else {}
    ack = p.get('payload') or {}
    if isinstance(ack, str):
        try:
            ack = json.loads(ack)
        except ValueError:
            return None, evidence
    if not isinstance(ack, dict):
        return None, evidence
    confirmed = utc(ack.get('acknowledged_at'))
    recorded = utc(p.get('recorded_at'))
    if not (ack.get('schema') == 'publication-ack-v1' and confirmed and recorded
            and ack.get('ack_id') == sha({k:v for k,v in ack.items() if k != 'ack_id'})
            and ack.get('receipt_id') == p.get('receipt_id') == r['receipt_id']
            and ack.get('source_table') == p.get('source_table') == source_table
            and ack.get('forecast_payload_sha256') == r['forecast_payload_sha256']
            and ack.get('publication_basis') in {'source_insert_commit_acknowledged_by',
                                                'committed_source_row_reobserved_by_publisher'}
            and ack.get('scheduled_start') == utc(event_start) and received <= confirmed):
        return None, evidence
    evidence.update(publication_confirmed_by=max(confirmed, recorded),
                    publication_basis=ack.get('publication_basis'),
                    publication_ack_id=ack['ack_id'])
    return evidence['publication_confirmed_by'], evidence


def quote_snapshot(*, provider, event_id, bookmaker, market, side, line, price,
                   provider_updated_at, observed_at, quote_id=None, price_format='decimal',
                   subject_id=None, subject_name=None):
    """Freeze decision-time identity and distinguish provider time from local receipt."""
    for value in (line, price):
        if isinstance(value, bool) or value is None or not math.isfinite(float(value)):
            raise ValueError('Finite quote line and price required')
    if utc(observed_at) is None:
        raise ValueError('Quote observation requires an explicit timezone-aware after-read time')
    q = dict(schema='decision-quote-v1', provider=provider, event_id=str(event_id),
             bookmaker=bookmaker, market=market, side=side.lower(), line=float(line),
             price=float(price), price_format=price_format, source_quote_id=quote_id,
             subject_id=str(subject_id) if subject_id is not None else None, subject_name=subject_name,
             provider_updated_at_raw=str(provider_updated_at) if provider_updated_at is not None else None,
             provider_updated_at=utc(provider_updated_at), observed_at=aware_now(observed_at))
    q['quote_fingerprint'] = sha(q)
    return q
