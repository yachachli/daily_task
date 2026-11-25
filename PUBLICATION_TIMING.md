# Prospective NFL/NBA publication evidence

NFL and both NBA publisher paths retain version 2 receipts for exact scoring
responses, with generation time and artifact metadata supplied by the backend.
Quote fingerprints retain provider event, book, player, market, side, threshold,
price, provider update time and after-response observation time.

Fetched quote observations are appended before recommendation deduplication,
without another provider request. Original candidate keys, scoring, selection
and prices remain in force. A forecast INSERT must finish its transaction before
the separate acknowledgement append. The acknowledgement bounds publication;
it is not first publication, independent observation or user exposure.

Retries preserve the first acknowledgement. An upcoming committed row missing
its receipt can receive an explicitly later recovery confirmation without being
inserted again. Apply the additive migration in
`BestBetV1/bestbet_backend/migrations/20261124_publication_receipts.sql` and confirm
writer access before activation. Missing schema/access fails before new forecasts
are written. Deploy backend generation metadata before enabling these publishers.

The dashboard independently observes exact receipts and quotes. Later closing
benchmarks never replace decision-time inputs. MLB/WNBA retain the legacy receipt
implementation. This change introduces no schedule, selection-cap or historical
timestamp change. Revert publisher code for recovery; retain collected evidence.
