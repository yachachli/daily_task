# Publisher receipts

Each new NBA, NBA alternate, WNBA, MLB batter and NFL prop response receives an additive `_publication_receipt` in its existing `analysis` JSON. No table migration or historical rewrite is needed. Existing backups and the PropGPT raw-history collector retain the receipt along with the forecast.

The receipt retains the exact decoded scoring response and SHA-256, request inputs, provider event ID and timezone-aware scheduled start, UTC response-receipt time, publisher repository/commit/run/attempt, endpoint alias and backend-declared model version if present. It contains no endpoint URL, API key or authorization header.

The publisher commit identifies only the writer. It does **not** identify the remotely deployed scoring model. Backend artifact linkage remains explicitly unverified; unknown model versions remain unknown. Grades are not converted to probabilities. Independent collector observation is still required to establish pregame evidence; the publisher timestamp alone is insufficient.

Existing routing (preserved pending intentional product/model mapping review): `analysis/nba.py` calls `NBA_ANALYSIS_API_URL` and writes `v2_nba_alt_daily_bets`; `analysis/nba_alt.py` calls `NBA_ALT_ANALYSIS_API_URL` and writes `v2_nba_daily_bets`. Receipts record the actual job and endpoint alias, avoiding inference from table names.

Validation: `python -m unittest discover -s tests -v`. Receipts begin with newly generated responses after deployment. Deduplication may retain existing rows without receipts; do not delete or relabel old forecasts to manufacture provenance. The remote scoring service must still emit its exact model/code/artifact identifiers before this closes the model-provenance gate.
