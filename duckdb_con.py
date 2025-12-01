import duckdb
import pandas as pd
from pathlib import Path
from typing import List, Dict, Any

DB_PATH = Path("nba.duckdb")

#connect to db
def get_connection():
    return duckdb.connect(str(DB_PATH))

def upsert_player_game_logs(records: List[Dict[str, Any]]):
    if not records:
        print("No records to load.")
        return

    df = pd.DataFrame(records)

    #rename columns for cleanliness
    rename_map = {
        "GAME_ID": "game_id",
        "GAME_DATE": "game_date",
        "MATCHUP": "matchup",
        "WL": "win_loss",
        "MIN": "minutes",
        "PTS": "points",
        "REB": "rebounds",
        "AST": "assists",
        "PLUS_MINUS": "plus_minus",
        "PLAYER_ID": "player_id",
        "PLAYER_NAME": "player_name",
        "TEAM_ID": "team_id",
        "TEAM_ABBREVIATION": "team_abbr"
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    con = get_connection()

    #create table of raw data
    con.execute("""
        CREATE TABLE IF NOT EXISTS player_game_logs AS
        SELECT * FROM df LIMIT 0
    """)
    #insert data into table
    con.execute("INSERT INTO player_game_logs SELECT * FROM df")
    print(f"Inserted {len(df)} records into player_game_logs.")
