from prefect import flow, task
from duckdb_con import get_connection

#Build cleaner tables from raw data
@task
def build_modeled_tables():
    con = get_connection()

    #create table of cleaned stats aggregated by team and game
    con.execute("""
        CREATE OR REPLACE TABLE raw_team_games AS
        WITH base AS (
            SELECT
                CAST(game_id AS VARCHAR) AS game_id,
                CAST(game_date AS DATE) AS game_date,
                team_abbr,
                player_id,
                player_name,
                points,
                rebounds,
                assists,
                plus_minus,
                win_loss,
                minutes
            FROM player_game_logs
        ),
        teamagg AS (
            SELECT
                game_id,
                game_date,
                team_abbr,
                MAX(win_loss) AS win_loss,
                SUM(points) AS team_points,
                SUM(rebounds) AS team_rebounds,
                SUM(assists) AS team_assists,
                AVG(plus_minus) AS avg_plus_minus,
                SUM(CASE WHEN minutes > 0 THEN 1 ELSE 0 END) AS players_used
            FROM base
            GROUP BY game_id, game_date, team_abbr
        )
        SELECT * FROM teamagg
    """)

    #create table with rest days
    con.execute("""
        CREATE OR REPLACE TABLE team_games_features AS
        WITH sorted AS (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY team_abbr ORDER BY game_date) AS rn
            FROM raw_team_games
        ),
        lagged AS (
            SELECT
                s.*,
                LAG(game_date) OVER (PARTITION BY team_abbr ORDER BY game_date) AS prev_game_date
            FROM sorted s
        )
        SELECT
            *,
            DATE_DIFF('day', prev_game_date, game_date) AS rest_days
        FROM lagged
    """)

    print("Feature tables built successfully.")

#run task
@flow(name="nba_transform_features")
def nba_transform_features_flow():
    build_modeled_tables()

if __name__ == "__main__":
    nba_transform_features_flow()
