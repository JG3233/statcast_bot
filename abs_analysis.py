"""
abs_analysis.py
---------------
Data fetching and analysis utilities for MLB's ABS (Automated Ball-Strike)
Challenge System, introduced in the 2026 regular season.

Data sources:
  - Baseball Savant ABS Leaderboard  (aggregated player/team stats)
  - Baseball Savant Statcast Search  (pitch-level data via pybaseball)

Usage:
    from abs_analysis import (
        fetch_abs_leaderboard,
        fetch_statcast_abs_pitches,
        compute_team_challenge_stats,
        find_underchallengers,
    )
"""

import io
import warnings
import requests
import pandas as pd
from pybaseball import statcast

warnings.filterwarnings("ignore")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LEADERBOARD_URL = "https://baseballsavant.mlb.com/leaderboard/abs-challenges"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    )
}

# Statcast `description` values that indicate an ABS challenge was triggered.
# Naming follows Baseball Savant's documented pitch-level event codes.
ABS_CHALLENGE_DESCRIPTIONS = {
    # Batter challenged a called strike
    "called_strike_challenge",
    # Pitcher/catcher challenged a called ball
    "ball_challenge",
    # Challenge succeeded → strike overturned to ball
    "called_strike_overturned",
    # Challenge succeeded → ball overturned to strike
    "ball_overturned",
}

# Descriptions where the challenge succeeded (call was changed)
OVERTURN_DESCRIPTIONS = {"called_strike_overturned", "ball_overturned"}


# ---------------------------------------------------------------------------
# Leaderboard fetching
# ---------------------------------------------------------------------------

def fetch_abs_leaderboard(
    year: int = 2026,
    game_type: str = "regular",
    challenge_type: str = "batter",
    level: str = "mlb",
    min_challenges: int = 0,
    min_opp_challenges: int = 0,
) -> pd.DataFrame:
    """
    Download the Baseball Savant ABS challenge leaderboard as a DataFrame.

    Parameters
    ----------
    year : int
        Season year (ABS launched in MLB for 2026).
    game_type : str
        "regular", "spring", or "postseason".
    challenge_type : str
        "batter"  → batter-initiated challenges
        "catcher" → pitcher/catcher-initiated challenges (defensive side)
    level : str
        "mlb" (default) or "aaa", etc.
    min_challenges : int
        Minimum challenges initiated (filters out very small samples).
    min_opp_challenges : int
        Minimum opponent challenges against the player.

    Returns
    -------
    pd.DataFrame
        Leaderboard with one row per player. Typical columns include:
        player_id, player_name, team_id, team_abbrev, year,
        challenges, overturns, overturn_rate, challenge_rate,
        expected_challenges, opp_challenges, opp_overturns,
        delta_win_exp, etc.
    """
    params = {
        "year": year,
        "gameType": game_type,
        "challengeType": challenge_type,
        "level": level,
        "minChal": min_challenges,
        "minOppChal": min_opp_challenges,
        "csv": "true",
    }

    resp = requests.get(LEADERBOARD_URL, params=params, headers=HEADERS, timeout=30)
    resp.raise_for_status()

    df = pd.read_csv(io.StringIO(resp.text))

    # Normalize common column name variants
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Ensure numeric types
    numeric_cols = [
        "challenges", "overturns", "overturn_rate", "challenge_rate",
        "expected_challenges", "opp_challenges", "opp_overturns",
        "delta_win_exp",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["challenge_type"] = challenge_type
    df["year"] = year
    df["game_type"] = game_type
    return df


def fetch_abs_leaderboards_combined(
    year: int = 2026,
    game_type: str = "regular",
    level: str = "mlb",
    min_challenges: int = 0,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Fetch both batter and catcher/pitcher ABS leaderboards.

    Returns
    -------
    (batter_df, pitcher_df) : tuple of DataFrames
    """
    batter_df = fetch_abs_leaderboard(
        year=year, game_type=game_type, challenge_type="batter",
        level=level, min_challenges=min_challenges,
    )
    pitcher_df = fetch_abs_leaderboard(
        year=year, game_type=game_type, challenge_type="catcher",
        level=level, min_challenges=min_challenges,
    )
    return batter_df, pitcher_df


# ---------------------------------------------------------------------------
# Pitch-level Statcast data
# ---------------------------------------------------------------------------

def fetch_statcast_abs_pitches(
    start_date: str,
    end_date: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Pull Statcast pitch-by-pitch data and isolate ABS challenge events.

    Parameters
    ----------
    start_date, end_date : str  ("YYYY-MM-DD")

    Returns
    -------
    (all_pitches, challenge_pitches) : tuple of DataFrames
        all_pitches       – full Statcast pull for the date range
        challenge_pitches – rows where an ABS challenge was triggered
    """
    print(f"Fetching Statcast data {start_date} → {end_date} …")
    all_pitches = statcast(start_dt=start_date, end_dt=end_date)
    all_pitches.columns = [c.strip().lower() for c in all_pitches.columns]

    if "description" in all_pitches.columns:
        challenge_pitches = all_pitches[
            all_pitches["description"].isin(ABS_CHALLENGE_DESCRIPTIONS)
        ].copy()
    else:
        print("  Warning: 'description' column not found; returning empty challenge df.")
        challenge_pitches = pd.DataFrame(columns=all_pitches.columns)

    # Add convenience flags
    if not challenge_pitches.empty:
        challenge_pitches["is_overturn"] = challenge_pitches["description"].isin(
            OVERTURN_DESCRIPTIONS
        )
        challenge_pitches["challenger"] = challenge_pitches["description"].apply(
            lambda d: "batter"
            if d in {"called_strike_challenge", "called_strike_overturned"}
            else "pitcher_catcher"
        )

    print(
        f"  Total pitches: {len(all_pitches):,} | "
        f"ABS challenge events: {len(challenge_pitches):,}"
    )
    return all_pitches, challenge_pitches


# ---------------------------------------------------------------------------
# Team-level aggregation
# ---------------------------------------------------------------------------

def compute_team_challenge_stats(
    batter_df: pd.DataFrame,
    pitcher_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Roll up player-level ABS leaderboard data to the team level.

    Returns a DataFrame with one row per team containing both offensive
    (batter) and defensive (pitcher/catcher) challenge metrics.
    """
    def _team_col(df: pd.DataFrame) -> str:
        for candidate in ("team_abbrev", "team_id", "team", "team_name"):
            if candidate in df.columns:
                return candidate
        raise KeyError("No team column found in dataframe.")

    def _agg(df: pd.DataFrame, prefix: str) -> pd.DataFrame:
        team_col = _team_col(df)
        agg: dict = {}
        for col in ("challenges", "overturns", "expected_challenges",
                    "opp_challenges", "opp_overturns", "delta_win_exp"):
            if col in df.columns:
                agg[col] = "sum"
        for col in ("overturn_rate", "challenge_rate"):
            if col in df.columns:
                agg[col] = "mean"

        grouped = df.groupby(team_col).agg(agg).reset_index()
        grouped.columns = [team_col] + [f"{prefix}_{c}" for c in grouped.columns[1:]]
        grouped = grouped.rename(columns={team_col: "team"})
        return grouped

    bat_team = _agg(batter_df, "bat")
    pit_team = _agg(pitcher_df, "def")

    team_stats = bat_team.merge(pit_team, on="team", how="outer")

    # Combined win expectancy impact
    win_cols = [c for c in team_stats.columns if "delta_win_exp" in c]
    if win_cols:
        team_stats["total_delta_win_exp"] = team_stats[win_cols].fillna(0).sum(axis=1)

    return team_stats.sort_values("total_delta_win_exp", ascending=False)


# ---------------------------------------------------------------------------
# Under-challenger detection
# ---------------------------------------------------------------------------

def find_underchallengers(
    df: pd.DataFrame,
    min_opportunities: int = 5,
    max_pct_of_expected: float = 0.60,
) -> pd.DataFrame:
    """
    Identify players who challenge far less often than expected.

    A player is flagged as an "underchallenger" when their actual challenge
    count is below `max_pct_of_expected` * expected_challenges AND they
    had at least `min_opportunities` expected challenges.

    Parameters
    ----------
    df : pd.DataFrame
        ABS leaderboard (batter or pitcher).
    min_opportunities : int
        Minimum expected challenges to be included.
    max_pct_of_expected : float
        Threshold below which we flag the player (default 0.60 = 60%).

    Returns
    -------
    pd.DataFrame sorted by challenge_deficit descending.
    """
    required = {"challenges", "expected_challenges"}
    if not required.issubset(df.columns):
        print(f"  Columns needed for underchallenger analysis: {required}")
        return df

    df = df.copy()
    df["challenge_deficit"] = df["expected_challenges"] - df["challenges"]
    df["pct_of_expected"] = (
        df["challenges"] / df["expected_challenges"].replace(0, float("nan"))
    )

    mask = (
        (df["expected_challenges"] >= min_opportunities)
        & (df["pct_of_expected"] <= max_pct_of_expected)
    )
    return df[mask].sort_values("challenge_deficit", ascending=False)


# ---------------------------------------------------------------------------
# Count & situation breakdown (from pitch-level data)
# ---------------------------------------------------------------------------

def challenge_by_count(challenge_pitches: pd.DataFrame) -> pd.DataFrame:
    """
    Break down ABS challenges by the ball-strike count at the time of the pitch.

    Returns a pivot-ready DataFrame with columns:
        balls, strikes, n_challenges, n_overturns, overturn_rate
    """
    needed = {"balls", "strikes", "description"}
    if not needed.issubset(challenge_pitches.columns):
        return pd.DataFrame()

    df = challenge_pitches.copy()
    df["is_overturn"] = df["description"].isin(OVERTURN_DESCRIPTIONS)

    count_stats = (
        df.groupby(["balls", "strikes"])
        .agg(n_challenges=("description", "count"),
             n_overturns=("is_overturn", "sum"))
        .reset_index()
    )
    count_stats["overturn_rate"] = (
        count_stats["n_overturns"] / count_stats["n_challenges"]
    )
    count_stats["count_label"] = (
        count_stats["balls"].astype(str) + "-" + count_stats["strikes"].astype(str)
    )
    return count_stats.sort_values(["balls", "strikes"])


def win_exp_by_challenge(challenge_pitches: pd.DataFrame) -> pd.DataFrame:
    """
    Compute average win-expectancy change by challenge type and outcome.

    Requires 'delta_home_win_exp' (or similar) column in pitch data.
    """
    win_col = next(
        (c for c in challenge_pitches.columns if "delta" in c and "win" in c),
        None,
    )
    if win_col is None:
        print("  No delta win expectancy column found in pitch data.")
        return pd.DataFrame()

    df = challenge_pitches.copy()
    df["is_overturn"] = df["description"].isin(OVERTURN_DESCRIPTIONS)

    return (
        df.groupby(["challenger", "is_overturn"])
        .agg(
            n=("description", "count"),
            avg_delta_win_exp=(win_col, "mean"),
            total_delta_win_exp=(win_col, "sum"),
        )
        .reset_index()
    )
