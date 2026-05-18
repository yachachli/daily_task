from __future__ import annotations

from datetime import datetime
from decimal import Decimal


ExistingBetKey = tuple[datetime, str, int, str, Decimal]


def make_existing_bet_key(
    game_time: datetime,
    game_tag: str,
    player_id: int,
    stat: str,
    line: float | int | Decimal,
) -> ExistingBetKey:
    return (game_time, game_tag, player_id, stat, Decimal(str(line)))
