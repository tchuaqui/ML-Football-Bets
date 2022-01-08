import urllib.request
import json
import pandas as pd

link = "https://data.oddsmagnet.com/history/2021/football/england-premier-league/leeds-united-v-brentford/win-market.json"

with urllib.request.urlopen(link) as url:
    matches = json.loads(url.read().decode())
    matches_df = pd.DataFrame(data = matches["data"], columns= matches["columns"])

