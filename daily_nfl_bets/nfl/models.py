import typing as t
from dataclasses import dataclass

# If your code uses this for JSON dumping
from daily_bets.utils import json_dumps_dataclass  

#
# Basic event/player/team classes
#
@dataclass
class SportEvent:
    id: str
    sport_key: str
    sport_title: str
    commence_time: str
    home_team: str
    away_team: str

@dataclass
class NflPlayer:
    id: str  # DB primary key or something
    team_id: str
    name: str

@dataclass
class NflTeam:
    id: str
    name: str
    code: str

#
# The new dataclasses for NFL fields
#
@dataclass
class PlayerTeamInfo:
    team_id: int
    team_name: str
    team_code: str

@dataclass
class DefenseData:
    """
    Matches the "defense_data" object from your JSON.
    For example, "id": 894, "name": "Kansas City Chiefs", etc.
    Add or remove fields as needed.
    """
    id: int
    name: str
    team_code: str
    wins: int
    losses: int
    ties: int
    points_for: int
    points_against: int
    total_tackles: int
    fumbles_lost: int
    defensive_touchdowns: int
    fumbles_recovered: int
    solo_tackles: int
    defensive_interceptions: int
    qb_hits: int
    tackles_for_loss: int
    pass_deflections: int
    sacks: int
    fumbles: int
    passing_td_allowed: int
    passing_yards_allowed: int
    rushing_yards_allowed: int
    rushing_td_allowed: int
    avg_rush_yds_allowed: float
    avg_pass_yds_allowed: float
    avg_rush_td_allowed: float
    avg_pass_td_allowed: float
    avg_tackles_for_loss: float
    avg_fumbles_lost: float
    avg_def_touchdowns: float
    avg_fumbles_recovered: float
    avg_solo_tackles: float
    avg_def_interceptions: float
    avg_qb_hits: float
    avg_sacks: float
    avg_fumbles: float
    avg_pass_deflections: float
    avg_points_for: float
    avg_points_against: float
    avg_total_tackles: float

@dataclass
class PlayerStats:
    """
    Matches the "player_stats" object from your JSON.
    e.g. "avg_pass_int": 0.0, "avg_rec_tds": 0.1, etc.
    """
    avg_pass_int: float
    avg_pass_tds: float
    avg_pass_yards: float
    avg_pass_attempts: float
    avg_pass_completions: float
    avg_rec_tds: float
    avg_rec_yards: float
    avg_rec_targets: float
    avg_rec_receptions: float
    avg_rush_tds: float
    avg_rush_carries: float
    avg_rush_yards: float
    avg_field_goals: float
    avg_xtra_pts: float
    avg_kicking_poing: float
    avg_rushrec_tds: float
    avg_rushrec_yds: float
    avg_passrecrush_tds: float
    avg_passrush_yds: float

@dataclass
class PreviousGameStats:
    """
    Each element in "previous_game_stats" array,
    e.g. {"date": "2025-01-19", "home_team": "BUF", "away_team": "BAL", "pass_yds": 0.0, ...}
    """
    date: str
    home_team: str
    away_team: str
    pass_yds: float
    rush_yds: float
    rec_yds: float
    pass_td: float
    rush_td: float
    rec_td: float
    opponent_pass_yards_allowed: float
    opponent_rush_yards_allowed: float
    opponent_pass_td_allowed: float
    opponent_rush_td_allowed: float
    opponent_interceptions: float
    after_injury: bool

#
# Graph structures
#
@dataclass
class GraphData:
    label: str
    value: float

@dataclass
class Graph:
    version: int
    data: list[GraphData]
    title: str
    threshold: float

#
# The main BetAnalysis class
#
@dataclass
class BetAnalysis:
    player_name: str
    player_position: str
    player_team_info: PlayerTeamInfo
    stat_type: str
    graph_key: str
    over_under: str
    bet_grade: str
    hit_rate: str
    original_bet_query: str
    threshold: float
    short_answer: str
    long_answer: str
    injuries: t.Optional[str]
    need_injury_report: bool
    injury_designation: t.Optional[str]
    insights: list[str]

    player_stats: PlayerStats
    defense_data: DefenseData
    previous_game_stats: list[PreviousGameStats]

    graphs: list[Graph]

#
# Possibly a function to convert BetAnalysis -> JSON or something else
#

# def json_dumps_bet_analysis(bet: BetAnalysis) -> str:
#     """
#     Example: Convert entire BetAnalysis to JSON. 
#     Alternatively, use your existing `json_dumps_dataclass`.
#     """
#     return json_dumps_dataclass(bet)


def bet_analysis_from_json(data: dict[str, t.Any]) -> BetAnalysis:
    """
    Convert the NFL backend's JSON into a BetAnalysis object,
    parsing all fields: defense_data, player_stats, previous_game_stats, graphs, etc.
    """

    # 1) player_team_info
    player_team_raw = data["player_team_info"]
    player_team_info = PlayerTeamInfo(
        team_id=player_team_raw["team_id"],
        team_name=player_team_raw["team_name"],
        team_code=player_team_raw["team_code"],
    )

    # 2) defense_data
    defense_raw = data["defense_data"]  # e.g. { "id": 894, "name": "Kansas City Chiefs", ... }
    defense_data = DefenseData(**defense_raw)

    # 3) player_stats
    stats_raw = data["player_stats"]  # e.g. { "avg_pass_int": 0.0, "avg_rec_tds": 0.1, ... }
    player_stats = PlayerStats(**stats_raw)

    # 4) previous_game_stats (array of objects)
    prev_games_raw = data.get("previous_game_stats", [])
    previous_game_stats_list = [PreviousGameStats(**g) for g in prev_games_raw]

    # 5) graphs
    raw_graphs = data.get("graphs", [])
    graphs_parsed: list[Graph] = []
    for g in raw_graphs:
        gd_list = [GraphData(**gd) for gd in g["data"]]
        new_graph = Graph(
            version=g["version"],
            data=gd_list,
            title=g["title"],
            threshold=g["threshold"],
        )
        graphs_parsed.append(new_graph)

    # Now exclude the nested keys we just handled from data
    skip_keys = {
        "player_team_info",
        "defense_data",
        "player_stats",
        "previous_game_stats",
        "graphs",
    }
    filtered_data = {k: v for k, v in data.items() if k not in skip_keys}

    # 6) Build the BetAnalysis
    return BetAnalysis(
        **filtered_data,
        player_team_info=player_team_info,
        defense_data=defense_data,
        player_stats=player_stats,
        previous_game_stats=previous_game_stats_list,
        graphs=graphs_parsed,
    )
