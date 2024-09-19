from pathlib import Path

import ibis

DATA_DIR = Path(__file__).parents[2] / "data"

BASE_URL = "https://www.football-data.co.uk/mmz4281/{season}/{league}.csv"

SEASONS = [
    "2324",
    "2425",
]

LEAGUES = [
    "B1",
    "D1",
    "D2",
    "E0",
    "E1",
    "E2",
    "E3",
    "EC",
    "F1",
    "F2",
    "G1",
    "I1",
    "I2",
    "N1",
    "P1",
    "SC0",
    "SC1",
    "SC2",
    "SC3",
    "SP1",
    "SP2",
    "T1",
]


def extract(
    league: str = None,
    data_dir: str = DATA_DIR,
    season: str = "2425",
):
    source_dir = Path(data_dir or ".") / "football-data" / season
    source_dir.mkdir(parents=True, exist_ok=True)

    if league is None:
        for l in LEAGUES:
            extract(l, data_dir, season)
        return

    print(f"League: {league} | Season: {season}")

    url = BASE_URL.format(league=league, season=season)

    print(url)

    table = ibis.read_csv(url, table_name=f"{league}_{season}")

    table_path = source_dir / f"{league}.parquet"

    print(table_path)

    table.cast({"Time": str}).to_parquet(table_path)


if __name__ == "__main__":
    extract(league="F1")
