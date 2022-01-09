import urllib.request
import json
import pandas as pd

years = [2021, 2022]
bet_types = ['win-market']
teams = [
    'arsenal', 'aston-villa', 'brentford', 'brighton', 'burnley', 'chelsea', 'crystal-palace', 'everton',
    'leeds-united', 'leicester-city', 'liverpool', 'manchester-city', 'manchester-united', 'norwich-city' 'newcastle',
    'southampton', 'tottenham', 'watford', 'west-ham', 'wolves', 'sheffield-united', 'west-bromwich', 'fulham',
    'bournemouth', 'cardiff-city', 'huddersfield', 'swansea', 'stoke-city', 'hull-city', 'middlesbrough', 'sunderland',
    'queens-park-rangers'
]

print("START")

for year in years:
    odds_data = []
    odds_columns = []
    for bet_type in bet_types:
        for team_home in teams:
            for team_away in teams:
                if team_home != team_away:
                    link = "https://data.oddsmagnet.com/history/%s/football/england-premier-league/%s-v-%s/%s.json" % \
                           (year, team_home, team_away, bet_type)
                    try:
                        with urllib.request.urlopen(link) as url:
                            odds = json.loads(url.read().decode())
                            odds_data_new = odds["data"]
                            odds_data += odds_data_new
                            if not odds_columns:
                                odds_columns = odds["columns"]
                            print("%s/%s-v-%s/%s"%(year, team_home, team_away, bet_type))
                    except:
                        continue
    df_odds = pd.DataFrame(data=odds_data, columns=odds_columns)
    df_odds.to_csv('OddsHistory_%s.csv' % year, index=False)

print("FINISH")

