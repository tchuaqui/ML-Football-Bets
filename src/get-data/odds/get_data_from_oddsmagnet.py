import urllib.request
import json
import pandas as pd

years = [2021, 2022]
bet_types = ['win-market']


# Note: List of leagues and teams comes from data/mapping

def get_data_oddsmagnet(league, years, bet_types, teams):
    for year in years:
        for bet_type in bet_types:
            odds_data = []
            odds_columns = []
            for team_home in teams:
                for team_away in teams:
                    if team_home != team_away:
                        link = "https://data.oddsmagnet.com/history/%s/football/%s/%s-v-%s/%s.json" % \
                               (year, league, team_home, team_away, bet_type)
                        try:
                            with urllib.request.urlopen(link) as url:
                                odds = json.loads(url.read().decode())
                                odds_data_new = odds["data"]
                                odds_data += odds_data_new
                                if not odds_columns:
                                    odds_columns = odds["columns"]
                                print("%s/%s-v-%s/%s" % (year, team_home, team_away, bet_type))
                        except:
                            continue  # if url does not return any files then we ignore it
            df_odds = pd.DataFrame(data=odds_data, columns=odds_columns)
            df_odds.to_csv('../../../data/oddsmagnet/%s_%s_%s.csv' % (year, league, bet_type))


print("START")
df_teams = pd.read_csv('../../../data/master-data/teams.csv')
df_teams.dropna(subset=["oddsmagnet_team"], inplace=True)
league_teams = df_teams.groupby('oddsmagnet_league')['oddsmagnet_team'].apply(list)
for league in league_teams.index:
    get_data_oddsmagnet(league, years, bet_types, league_teams.loc[league])
print("FINISH")
