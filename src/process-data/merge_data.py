import numpy as np
import pandas as pd
from datetime import date, timedelta

#Directory of data sources
understat_data_dir = '../../data/understat/'
master_table_dir = '../../data/master-data/teams.csv'
football_data_dir = '../../data/football-data/'
oddsmagnet_data_dir = '../../data/oddsmagnet/'


# Create dictionaries of understat dataframes, each for a season/league

leagues = ['La_liga', 'EPL', 'Bundesliga', 'Serie_A', 'Ligue_1']
seasons = ['2014', '2015', '2016', '2017', '2018','2019','2020','2021']

# Read csv with master data
master_data = pd.read_csv(master_table_dir)
# Create dictionary for all data
data_full = {}

for season in seasons:
    data_full[season] = {}
    for league in leagues:
        # Filter master_data to league
        master_data_league = master_data.loc[master_data['league_id'] == league]
        # Convert to dictionary with team_id key
        master_data_dic = master_data_league.set_index('team_id').T.to_dict('list')
        df_cumul = {}
        df_non_cumul = {}
        df_understat = {}
        for key in master_data_dic.keys():
            # Read understat data and save df_understat dictionary of team dataframes
            try:
                df_cumul[key] = pd.read_csv(understat_data_dir+league+'_'+season+'_'+key+'_cumul.csv')
                df_non_cumul[key] = pd.read_csv(understat_data_dir+league+'_'+season+'_'+key+'.csv')
                # Concatenate understat date, home/away and cumulative data
                df_understat[key] = pd.concat([df_non_cumul[key][['date', 'h_a']], df_cumul[key].iloc[:, 1:]], axis=1)
                # Truncate date to remove hours, minutes, seconds
                df_understat[key]['date'] = df_understat[key]['date'].str.slice(stop=10)
                # Convert date to date format
                df_understat[key]['date'] = df_understat[key]['date'].apply(lambda x: date(int(x[:4]), int(x[5:7]), int(x[-2:])))
            except:
                #print(key+' not in season '+season)
                pass

        # Load match data in league and season
        df_matches = pd.read_csv(football_data_dir+league+'_'+season+'_results.csv')
        df_matches = df_matches[['Date', 'HomeTeam', 'AwayTeam', 'FTHG', 'FTAG']]  #keep only necessary columns
        df_matches.dropna(inplace=True)  #Remove rows with NaNs
        # Change date format to match understat data
        df_matches['Date'] = df_matches['Date'].apply(lambda x: date(int('20'+x[-2:]), int(x[3:5]), int(x[0:2])))
        # Change team names in df_matches to match team_id (same as understat data)
        master_data_dic_matches = master_data_league.set_index('footballdata_team').T.to_dict('list')  # Create dictionary
        df_matches['HomeTeam'] = df_matches['HomeTeam'].apply(lambda x: master_data_dic_matches[x][0])
        df_matches['AwayTeam'] = df_matches['AwayTeam'].apply(lambda x: master_data_dic_matches[x][0])

        # Convert dataframe to array for iteration
        matches_array = df_matches.to_numpy()
        i_matches_to_remove = []
        for i_match in range(len(matches_array)):
            date_match = matches_array[i_match, 0]
            home_team = matches_array[i_match, 1]
            away_team = matches_array[i_match, 2]
            # Create dfs with home team and away team data for match date from understat dataframes
            df_understat_home = df_understat[home_team].loc[(df_understat[home_team]['date'] == date_match) & (df_understat[home_team]['h_a'] == 'h')]
            df_understat_away = df_understat[away_team].loc[(df_understat[away_team]['date'] == date_match) & (df_understat[away_team]['h_a'] == 'a')]
            # Add day to account for matches after midnight (football-data does not seem to have this corrected)
            if df_understat_home.empty or df_understat_away.empty:
                delta_day = timedelta(days=1)
                df_understat_home = df_understat[home_team].loc[(df_understat[home_team]['date'] == date_match + delta_day) & (df_understat[home_team]['h_a'] == 'h')]
                df_understat_away = df_understat[away_team].loc[(df_understat[away_team]['date'] == date_match + delta_day) & (df_understat[away_team]['h_a'] == 'a')]
                if df_understat_home.empty or df_understat_away.empty:  #If still cannot find stats for match, save i_match for later removal of match
                    i_matches_to_remove.append(i_match)
                    continue   # continue to next i_match
            # Drop date and home_away data from dfs
            df_understat_home.drop(columns=['date', 'h_a'], inplace=True)
            df_understat_away.drop(columns=['date', 'h_a'], inplace=True)
            # Rename each label with home_ and away_ prefix
            df_understat_home = df_understat_home.add_prefix('home_').reset_index(drop=True)
            df_understat_away = df_understat_away.add_prefix('away_').reset_index(drop=True)
            #test0
            if len(df_understat_home)!=1 or len(df_understat_away)!=1:
                print('error0')
            # Concatenate home and away dfs
            if 'df_understat_matches' in locals():
                new_row = pd.concat([df_understat_home, df_understat_away], axis=1).reset_index(drop=True)
                #test
                len1 = len(df_understat_matches)
                df_understat_matches = pd.concat([df_understat_matches, new_row], axis=0).reset_index(drop=True)
                #test2
                if len(new_row) != 1:
                    print("error1")
                if (len(df_understat_matches)-len1)!=1:
                    print('error2')
                #
            else:
                #test
                df_understat_matches = pd.concat([df_understat_home, df_understat_away], axis=1)
        # Remove matches with missing data
        if i_matches_to_remove:
            rows_to_remove = df_matches.index[i_matches_to_remove]
            df_matches.drop(rows_to_remove, inplace=True)
        # Concatenate df_matches with df_understat_matches
        df_matches = pd.concat([df_matches.reset_index(drop=True), df_understat_matches.reset_index(drop=True)], axis=1)
        # Check sizes and NaNs
        if (len(df_matches) != len(df_understat_matches)): print('Different sizes!', league, season)
        if df_matches.isnull().values.any(): print('NaNs!',league, season,' ',str(i_match))
        del df_understat_matches
        data_full[season][league] = df_matches

# # Add historical match data (one vs one) using last 3 and last 5 matches
# for key_season, data_season in data_full.items():
#     for key_league, data_league in data_season.items():




