import json
import unittest

from daily_bets.odds_api import _decode_game


def _payload():
    return {
        "id": "e1", "sport_key": "americanfootball_nfl", "sport_title": "NFL",
        "commence_time": "2026-11-10T01:15:00Z",
        "home_team": "Minnesota Vikings", "away_team": "Buffalo Bills",
        "bookmakers": [
            {"key": "prizepicks", "title": "PrizePicks", "markets": [
                {"key": "player_receptions", "last_update": "x", "outcomes": [
                    {"name": "Over", "description": "Justin Jefferson", "price": 1.8, "point": 6.5},
                ]},
            ]},
            {"key": "dabble_us_dfs", "title": "Dabble", "markets": [
                {"key": "player_anytime_td", "last_update": "x", "outcomes": [
                    {"name": "Yes", "description": "Josh Allen", "price": 2.1},
                ]},
            ]},
        ],
    }


class OddsDecodeTests(unittest.TestCase):
    def test_outcomes_without_line_are_dropped_not_fatal(self):
        game = _decode_game(json.dumps(_payload()))
        self.assertEqual(len(game.bookmakers), 2)
        self.assertEqual(game.bookmakers[0].markets[0].outcomes[0].point, 6.5)
        self.assertEqual(game.bookmakers[1].markets[0].outcomes, [])


if __name__ == "__main__":
    unittest.main()
