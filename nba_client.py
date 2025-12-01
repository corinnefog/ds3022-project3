import time
import requests
from typing import List, Dict, Any

class NBAApiError(Exception):
    pass

class NBAClient:
    BASE_URL = "https://stats.nba.com/stats"

    def __init__(self, max_retries: int = 6, backoff_factor: float = 2.0):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json, text/plain, */*",
            "Referer": "https://www.nba.com"
        })
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor

    def _get(self, endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.BASE_URL}/{endpoint}"

        # Strong headers: the NBA API *requires* these
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.nba.com/",
            "Origin": "https://www.nba.com",
            "Connection": "keep-alive",
        }

        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self.session.get(
                    url,
                    params=params,
                    headers=headers,
                    timeout=45,  # increased timeout
                )

                if resp.status_code == 200:
                    return resp.json()

                # Retry on rate limits or server errors
                if resp.status_code in (429, 500, 502, 503, 504):
                    sleep_s = self.backoff_factor ** attempt
                    print(f"[NBAClient] {resp.status_code} - retrying in {sleep_s:.1f}s...")
                    time.sleep(sleep_s)
                    continue

                # All other errors
                raise NBAApiError(f"NBA API error {resp.status_code}: {resp.text[:200]}")

            except requests.exceptions.ReadTimeout:
                sleep_s = self.backoff_factor ** attempt
                print(f"[NBAClient] Timeout (attempt {attempt}) - sleeping {sleep_s:.1f}s...")
                time.sleep(sleep_s)
                continue

            except requests.exceptions.RequestException as e:
                sleep_s = self.backoff_factor ** attempt
                print(f"[NBAClient] Network error: {e} - retrying in {sleep_s:.1f}s...")
                time.sleep(sleep_s)
                continue

        raise NBAApiError("NBA API failed after maximum retries.")

    def fetch_season_player_game_logs(self, season: str) -> List[Dict[str, Any]]:
        params = {
            "Counter": "0",
            "Direction": "ASC",
            "LeagueID": "00",
            "PlayerOrTeam": "P",
            "Season": season,
            "SeasonType": "Regular Season",
            "Sorter": "DATE"
        }
        data = self._get("leaguegamelog", params)

        result = data["resultSets"][0]
        headers = result["headers"]
        rows = result["rowSet"]

        records = []
        for row in rows:
            records.append({h: v for h, v in zip(headers, row)})

        return records
