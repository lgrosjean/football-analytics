from pathlib import Path

import ibis

SOURCE = "fbdat"

DATA_DIR = Path(__file__).parents[2] / "data"

BASE_URL = "https://raw.githubusercontent.com/footballcsv/cache.api.fbdat/refs/heads/master/{season}/{league}.csv"

SEASONS = {
    "2122": "2021-22",
    "2223": "2022-23",
    "2324": "2023-24",
    "2425": "2024-25",
}

LEAGUES = [
    "de.1",
    "eng.1",
    "fr.1",
    "it.1",
    "nl.1",
    "pt.1",
    "uefa.cl",
]


def extract(
    league: str = None,
    data_dir: str = DATA_DIR,
    season: str = "2425",
):

    source_dir = Path(data_dir or ".") / SOURCE / season
    source_dir.mkdir(parents=True, exist_ok=True)

    if league is None:
        for l in LEAGUES:
            extract(l, data_dir, season)
        return

    print(f"League: {league} | Season: {season}")

    url = BASE_URL.format(league=league, season=SEASONS[season])

    print(url)

    table = ibis.read_csv(url, table_name=f"{league}_{season}")

    table_path = source_dir / f"{league}.parquet"

    print(table_path)

    table.to_parquet(table_path)


if __name__ == "__main__":
    extract(league="uefa.cl")
