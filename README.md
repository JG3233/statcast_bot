# statcast-bot

MLB statistical analysis tools, currently focused on the 2026 ABS (Automated Ball-Strike) Challenge System.

## Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Contents

- **abs_challenge_analysis.ipynb** -- Main analysis notebook. Fetches ABS leaderboard and Statcast pitch-level data, then produces charts covering batter/pitcher overturn rates, team challenge leverage, count tendencies, win-probability impact, and under-challengers.
- **abs_analysis.py** -- Data-fetching and analysis utilities used by the notebook. Wraps the Baseball Savant ABS leaderboard API and `pybaseball` Statcast queries.

## Data Sources

- [Baseball Savant ABS Leaderboard](https://baseballsavant.mlb.com/leaderboard/abs-challenges)
- [Statcast Search](https://baseballsavant.mlb.com/statcast_search) via [pybaseball](https://github.com/jldbc/pybaseball)
