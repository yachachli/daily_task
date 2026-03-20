## Quickstart

Install [`uv`](https://docs.astral.sh/uv/getting-started/installation/)

```bash
uv sync
source .venv/bin/activate

python3 -m daily_bets
```

## Developoment

Install [`sqlc`](https://docs.sqlc.dev/en/stable/overview/install.html)

Install `pg_dump`

```bash
# MacOS
brew install postgresql

# Ubuntu
sudo apt install postgresql-client
```

Run the `dump_schema.sh` script.

```bash
./dump_schema.sh
```

Run

## Adding a New Sport

- https://the-odds-api.com/sports-odds-data/betting-markets.html

## Daily Game Prediction Sync

NBA game predictions are synced with:

```bash
python3 daily_bets/scripts/sync_daily_nba_games.py
```

College basketball game predictions are synced with:

```bash
python3 daily_bets/scripts/sync_daily_cbb_games.py
```

Required env vars for the CBB sync:

- `CBB_GAME_PREDICTOR_URL`
- `CBB_GAME_PREDICTOR_BEARER_TOKEN` if the endpoint is protected
- database env vars, or `NEON_DATABASE_URL`
