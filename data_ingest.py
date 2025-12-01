from prefect import flow, task
from typing import List, Dict, Any

from nba_client import NBAClient
from duckdb_con import upsert_player_game_logs

SEASONS = ["2018-19", "2019-20", "2020-21", "2021-22", "2022-23"]

@task(retries=2, retry_delay_seconds=30)
def fetch_season_logs(season: str) -> List[Dict[str, Any]]:
    client = NBAClient()
    return client.fetch_season_player_game_logs(season)

@task
def load_into_duckdb(records: List[Dict[str, Any]]):
    upsert_player_game_logs(records)

@flow(name="nba_ingest_boxscores")
def nba_ingest_boxscores_flow():
    all_records = []
    for season in SEASONS:
        print(f"Fetching logs for {season}...")
        season_records = fetch_season_logs(season)
        all_records.extend(season_records)

    print("Loading into DuckDB...")
    load_into_duckdb(all_records)
    print("Ingestion complete.")

if __name__ == "__main__":
    nba_ingest_boxscores_flow()
