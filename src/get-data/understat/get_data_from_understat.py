import numpy as np
import pandas as pd
import lxml
import requests
from bs4 import BeautifulSoup
import json

# For urls of all seasons of all leagues in understat.com
leagues = ['La_liga', 'EPL', 'Bundesliga', 'Serie_A', 'Ligue_1']
seasons = ['2014', '2015', '2016', '2017', '2018','2019','2020','2021']

def get_understat_data(leagues, seasons):
    base_url = 'https://understat.com/league'
    for league in leagues:
        for season in seasons:
            url = base_url+'/'+league+'/'+season
            res = requests.get(url)
            soup = BeautifulSoup(res.content, "lxml")
            # Data in JSON variable, under <script> tags
            scripts = soup.find_all('script')
            string_with_json_obj = ''

            # Find data for teams
            # Data stored per match (not cumulative)
            for el in scripts:
                if 'teamsData' in str(el):
                    string_with_json_obj = str(el).strip()
            
            # get JSON data and strip unnecessary stuff
            ind_start = string_with_json_obj.index("('")+2
            ind_end = string_with_json_obj.index("')")
            json_data = string_with_json_obj[ind_start:ind_end]
            json_data = json_data.encode('utf8').decode('unicode_escape')

            # convert JSON data into Python dictionary
            data = json.loads(json_data)

            # Separate dictionary with teams and respective id's
            teams = {}
            for id in data.keys():
                teams[id] = data[id]['title']
                columns = list(data[id]['history'][0].keys())  #Store list with all the keys of attributes

            #Create df_teams dictionary of dataframes with all teams in a league/season (non-cumulative data)
            df_teams = {}
            for id, team in teams.items():
                teams_data = []
                for row in data[id]['history']:
                    teams_data.append(list(row.values()))
                df_teams[team] = pd.DataFrame(teams_data, columns=columns)

            #Convert ppda and ppda_allowed to ratios like data displayed on understat.com and drop originals
            for team in df_teams.keys():
                df_teams[team]['ppda_ratio'] = df_teams[team]['ppda'].apply(lambda x: x['att']/x['def'] if x['def']!=0 else 0)
                df_teams[team]['oppda_ratio'] = df_teams[team]['ppda_allowed'].apply(lambda x: x['att']/x['def'] if x['def']!=0 else 0)
                df_teams[team].drop(columns=['ppda','ppda_allowed'], inplace=True)

            #Create df_teams_cumulative dictionary of dataframes with all teams in a league/season (cumulative data)    
            cols_to_sum = ['xG', 'xGA', 'npxG', 'npxGA', 'deep', 'deep_allowed', 'scored', 'missed', 'xpts', 'wins', 'draws', 'loses', 'pts', 'npxGD']
            cols_to_mean = ['ppda_ratio', 'oppda_ratio']

            df_teams_cumulative = {}
            mean_df = {}
            for team, df in df_teams.items():
                sum_df = np.cumsum(df[cols_to_sum])
                for col in cols_to_mean:
                    mean_df[col] = np.cumsum(df[col])/np.array(range(1,len(df)+1))
                df_teams_cumulative[team] = sum_df.join(pd.DataFrame(mean_df, columns = cols_to_mean))

            #Save dataframes in ../../../data/understat/
            for team in df_teams.keys():
                df_teams[team].to_csv('../../../data/understat/'+league+'_'+season+'_'+team+'.csv')
                df_teams_cumulative[team].to_csv('../../../data/understat/'+league+'_'+season+'_'+team+'_cumul.csv')

get_understat_data(leagues, seasons)
