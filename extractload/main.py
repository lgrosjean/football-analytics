from sources import fbdat, football_data

if __name__ == "__main__":

    import sys

    seasons = sys.argv[1:]

    if len(seasons) > 0:

        for season in seasons:
            print(season)
            fbdat.extract(season=season)
            football_data.extract(season=season)

    else:
        fbdat.extract()
        football_data.extract()
