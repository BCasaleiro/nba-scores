import logging
from collections.abc import Iterator
from csv import DictWriter
from datetime import date
from pathlib import Path

import requests

DATA_DIR = Path(__file__).parent / "data"
API_BASE_URL = "https://www.balldontlie.io/api/v1/games"
API_PAGE_SIZE = 100
TARGET_SEASON = 2023

logger = logging.getLogger(__name__)


def fetch_page(season: int, page_number: int) -> dict:
    response = requests.get(
        f"{API_BASE_URL}?seasons[]={season}&page={page_number}&per_page={API_PAGE_SIZE}"
    )
    response.raise_for_status()
    return response.json()


def download_dataset(season: int) -> Iterator[dict]:
    page_number = 1
    num_pages = 1
    while page_number <= num_pages:
        page = fetch_page(season, page_number=page_number)

        for game in page["data"]:
            if game["postseason"]:
                # Skipping game because postseason=True",
                logger.debug(
                    "Skipping %s vs %s on %s because postseason='%s'",
                    game["home_team"]["abbreviation"],
                    game["visitor_team"]["abbreviation"],
                    game["date"],
                    game["postseason"],
                )
                continue

            yield {
                "date": game["date"][:10],
                "home_team": game["home_team"]["abbreviation"],
                "visitor_team": game["visitor_team"]["abbreviation"],
                "home_team_score": game["home_team_score"],
                "visitor_team_score": game["visitor_team_score"],
            }

        num_pages = page["meta"]["total_pages"]
        logger.info("Fetched page %s of %s", page_number, num_pages)
        page_number += 1


def main():
    today = date.today()

    games = download_dataset(season=TARGET_SEASON)
    games = (x for x in games if date.fromisoformat(x["date"]) < today)
    games_sorted = sorted(games, key=lambda x: x["date"])

    dataset_path = DATA_DIR / f"games_{TARGET_SEASON}.csv"
    with dataset_path.open("w") as dataset_file:
        writer = DictWriter(dataset_file, fieldnames=games_sorted[0].keys())
        writer.writeheader()

        for game in games_sorted:
            writer.writerow(game)

    logger.info("Finished refreshing %s season dataset", TARGET_SEASON)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
