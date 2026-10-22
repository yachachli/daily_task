import hashlib
import json
import unittest
from datetime import datetime, timezone
from daily_bets.publication_receipt import attach_receipt


class ReceiptTests(unittest.TestCase):
    def receipt(self, text='{"grade": 90, "over_under": "over"}', **overrides):
        args = dict(request_json=b'{"player_id":1,"line":2.5}', endpoint_alias='NBA_ANALYSIS_API_URL', event_id='provider-id', event_start='2026-10-23T18:00:00-07:00', publisher_job='nba', environ={'GITHUB_SHA':'a'*40,'GITHUB_RUN_ID':'123'}, now=datetime(2026,10,23,tzinfo=timezone.utc))
        args.update(overrides)
        return json.loads(attach_receipt(text, **args))

    def test_preserves_response_without_inventing_model(self):
        result = self.receipt()
        self.assertEqual(result['grade'], 90)
        self.assertNotIn('model_version', result)
        r = result['_publication_receipt']
        self.assertIsNone(r['backend_model_version'])
        self.assertEqual(r['publisher_commit'], 'a'*40)
        self.assertEqual(r['backend_artifact_linkage'], 'unverified')
        self.assertEqual(r['scheduled_start'], '2026-10-24T01:00:00+00:00')
        self.assertEqual(hashlib.sha256(r['response_text'].encode()).hexdigest(), r['response_text_sha256'])
        original = json.loads(r['response_text'])
        self.assertEqual(original, {k:v for k,v in result.items() if k!='_publication_receipt'})

    def test_version_preserved_and_response_changes_identity(self):
        result = self.receipt('{"model_version":"v7","grade":80}')
        self.assertEqual(result['model_version'], 'v7')
        self.assertEqual(result['_publication_receipt']['backend_model_version'], 'v7')
        self.assertNotEqual(result['_publication_receipt']['receipt_id'], self.receipt()['_publication_receipt']['receipt_id'])

    def test_reserved_metadata_and_naive_dates_rejected(self):
        for text in ('[]', '{"_publication_receipt":{}}'):
            with self.assertRaises(ValueError): self.receipt(text)
        with self.assertRaises(ValueError): self.receipt(event_start='2026-10-24T01:00:00')
        self.assertIsNone(self.receipt(environ={})['_publication_receipt']['publisher_commit'])


if __name__ == '__main__': unittest.main()
