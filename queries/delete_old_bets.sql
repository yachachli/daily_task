-- name: DeleteOldNbaBets :execrows
DELETE FROM v2_nba_daily_bets WHERE created_at < NOW() - INTERVAL '1 week';

-- name: DeleteOldNflBets :execrows
DELETE FROM v2_nfl_daily_bets WHERE created_at < NOW() - INTERVAL '1 day';

-- name: DeleteOldMlbBets :execrows
DELETE FROM v2_mlb_daily_bets WHERE created_at < NOW() - INTERVAL '1 day';

-- name: DeleteOldWnbaBets :execrows
DELETE FROM v2_wnba_daily_bets WHERE created_at < NOW() - INTERVAL '1 day';
