"""Attach publisher evidence without guessing the scoring service's revision."""
from datetime import datetime, timezone
import hashlib
import json
import os
import re


def attach_receipt(response_text, *, request_json, endpoint_alias, event_id,
                   event_start, publisher_job, environ=None, now=None):
    analysis = json.loads(response_text)
    if not isinstance(analysis, dict) or '_publication_receipt' in analysis:
        raise ValueError('Expected an analysis object without reserved publisher metadata')
    request = json.loads(request_json)
    start = datetime.fromisoformat(event_start.replace('Z', '+00:00'))
    if start.tzinfo is None:
        raise ValueError('Event start must include a timezone')
    observed = now or datetime.now(timezone.utc)
    if observed.tzinfo is None:
        raise ValueError('Response receipt time must include a timezone')
    env = os.environ if environ is None else environ
    sha = env.get('GITHUB_SHA', '')
    commit = sha if re.fullmatch('[0-9a-f]{40}', sha) else None
    receipt = {
        'schema': 'publisher-receipt-v1',
        'response_received_at': observed.astimezone(timezone.utc).isoformat(),
        'publisher_repository': env.get('GITHUB_REPOSITORY'),
        'publisher_commit': commit,
        'publisher_run_id': env.get('GITHUB_RUN_ID'),
        'publisher_run_attempt': env.get('GITHUB_RUN_ATTEMPT'),
        'publisher_job': publisher_job,
        'endpoint_alias': endpoint_alias,
        'event_provider': 'the-odds-api',
        'event_id': event_id,
        'scheduled_start': start.astimezone(timezone.utc).isoformat(),
        'request': request,
        # Retain exact decoded response text, so its digest remains reproducible
        # after PostgreSQL JSONB normalizes the enclosing object.
        'response_text': response_text,
        'response_text_sha256': hashlib.sha256(response_text.encode('utf-8')).hexdigest(),
        'backend_model_version': analysis.get('model_version'),
        'backend_artifact_linkage': 'unverified',
        'probability_semantics': 'unvalidated; grade is not a probability',
    }
    receipt['receipt_id'] = hashlib.sha256(json.dumps(receipt, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()
    analysis['_publication_receipt'] = receipt
    return json.dumps(analysis, separators=(',', ':'), allow_nan=False)
