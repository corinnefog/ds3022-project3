import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


DB_PATH = "nba.duckdb"

#Load data from DuckDB
def load_data():
    """Load team-game feature data from DuckDB."""
    con = duckdb.connect(DB_PATH)
    df = con.execute("""
        SELECT
            game_date,
            team_abbr,
            team_points,
            team_rebounds,
            team_assists,
            rest_days,
            avg_plus_minus,
            win_loss
        FROM fact_team_game_features
        WHERE rest_days IS NOT NULL
        ORDER BY game_date;
    """).df()

    con.close()

    df = df[(df["rest_days"] >= 0) & (df["rest_days"] <= 5)]

    return df


#Compare rest days to team points
def plot_rest_vs_points(df):
    grouped = (
        df.groupby("rest_days")["team_points"]
        .mean()
        .reset_index()
        .sort_values("rest_days")
    )

    plt.figure(figsize=(8, 5))
    sns.barplot(
        data=grouped,
        x="rest_days",
        y="team_points",
        palette="Blues_r"
    )
    plt.title("Average Team Points by Rest Days")
    plt.xlabel("Rest Days")
    plt.ylabel("Average Points")
    plt.tight_layout()
    plt.savefig("rest_vs_points.png")
    plt.close()

    return grouped


#Compare rest days to average plus/minus
def plot_rest_vs_plus_minus(df):
    grouped = (
        df.groupby("rest_days")["avg_plus_minus"]
        .mean()
        .reset_index()
        .sort_values("rest_days")
    )

    plt.figure(figsize=(8, 5))
    sns.barplot(
        data=grouped,
        x="rest_days",
        y="avg_plus_minus",
        palette="Reds_r"
    )
    plt.title("Average Plus/Minus by Rest Days")
    plt.xlabel("Rest Days")
    plt.ylabel("Average Plus/Minus")
    plt.tight_layout()
    plt.savefig("rest_vs_plus_minus.png")
    plt.close()

    return grouped

#Compare rest days to win rate
def plot_rest_vs_winrate(df):
    df_win = (
        df.assign(win=lambda x: x["win_loss"].eq("W"))
        .groupby("rest_days")["win"]
        .mean()
        .reset_index()
    )

    plt.figure(figsize=(8, 5))
    sns.pointplot(
        data=df_win,
        x="rest_days",
        y="win",
        errorbar=('ci', 95),
        join=False,
        color="black"
    )
    plt.title("Win Probability by Rest Days")
    plt.xlabel("Rest Days")
    plt.ylabel("Win Probability")
    plt.ylim(0, 1)
    plt.tight_layout()
    plt.savefig("rest_vs_winrate.png")
    plt.close()

    return df_win

#rest days compared to point differential boxplot
def plot_rest_boxplot(df):
    df = df.copy()
    # Approximate point differential: subtract team points from opponent mean
    # Data doesn’t include opponent points directly
    df["point_diff"] = df["team_points"] - df.groupby("game_date")["team_points"].transform("mean")

    plt.figure(figsize=(10, 6))
    sns.boxplot(data=df, x="rest_days", y="point_diff", palette="Set3")
    plt.title("Distribution of Point Differential by Rest Days")
    plt.xlabel("Rest Days")
    plt.ylabel("Point Differential")
    plt.tight_layout()
    plt.savefig("point_diff_boxplot.png")
    plt.close()


#Correlations between metrics and rest days
def plot_correlation_matrix(df):
    corr = df[["team_points", "team_rebounds", "team_assists", "avg_plus_minus", "rest_days"]].corr()

    plt.figure(figsize=(8, 6))
    sns.heatmap(corr, annot=True, cmap="coolwarm", fmt=".2f")
    plt.title("Correlation Matrix of Team Metrics & Rest Days")
    plt.tight_layout()
    plt.savefig("correlation_matrix.png")
    plt.close()

    return corr

#Boston Celtics rolling average trend
def plot_team_trend(df, team="BOS"):
    team_df = df[df["team_abbr"] == team].sort_values("game_date")
    team_df["rolling_pts"] = team_df["team_points"].rolling(10).mean()

    plt.figure(figsize=(12, 4))
    sns.lineplot(data=team_df, x="game_date", y="rolling_pts", linewidth=2)
    plt.title(f"{team} 10-Game Rolling Average Points")
    plt.xlabel("Date")
    plt.ylabel("Rolling Avg Points")
    plt.tight_layout()
    plt.savefig(f"{team}_rolling_trend.png")
    plt.close()

    return team_df

#Anomaly detection for biggest under/over performances
def anomaly_detection(df):
    df_sorted = df.sort_values(["team_abbr", "game_date"])
    df_sorted["rolling_avg"] = (
        df_sorted.groupby("team_abbr")["team_points"]
        .transform(lambda x: x.rolling(10, min_periods=4).mean())
    )
    df_sorted["residual"] = df_sorted["team_points"] - df_sorted["rolling_avg"]

    biggest_upsets = df_sorted.nsmallest(10, "residual")
    biggest_overperform = df_sorted.nlargest(10, "residual")

    biggest_upsets.to_csv("biggest_underperformances.csv", index=False)
    biggest_overperform.to_csv("biggest_overperformances.csv", index=False)

    return biggest_upsets, biggest_overperform


#collect and print metrics
def main():
    print("Loading data...")
    df = load_data()

    print("Generating visualizations...")
    rest_points = plot_rest_vs_points(df)
    rest_plus_minus = plot_rest_vs_plus_minus(df)
    winrate = plot_rest_vs_winrate(df)
    plot_rest_boxplot(df)
    corr = plot_correlation_matrix(df)
    plot_team_trend(df, team="BOS")  

    print("Running anomaly detection...")
    upsets, overshoots = anomaly_detection(df)

    print(f"Summarizing findings: {rest_points} {rest_plus_minus} {winrate} {corr}")



if __name__ == "__main__":
    main()

