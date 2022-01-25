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
seasons = ['2014','2015','2016','2017','2018','2019','2020','2021']


#Function aggregates all understat non-cumulative data into dictionary of dataframes per team
def aggregate_understat(leagues, seasons, master_data, understat_data_dir):
    understat_columns = ['h_a', 'xG', 'xGA', 'npxG', 'npxGA', 'deep', 'deep_allowed', 'scored',
                         'missed', 'xpts', 'result', 'date', 'wins', 'draws', 'loses', 'pts',
                         'npxGD', 'ppda_ratio', 'oppda_ratio']
    # Read non-cumulative understat data and aggregate it all by team
    team_stats = {}
    for league in leagues:
        # Filter master_data to league
        master_data_league = master_data.loc[master_data['league_id'] == league]
        # Convert to dictionary with team_id key
        master_data_dic = master_data_league.set_index('team_id').T.to_dict('list')
        for team in master_data_dic.keys():
            understat_list = []
            # Read understat non cumulative data
            for season in seasons:
                try:
                    df_understat = pd.read_csv(understat_data_dir+league+'_'+season+'_'+team+'.csv', index_col=[0])
                    # Ensure column order is preserved
                    df_understat = df_understat[understat_columns]
                    # Truncate date to remove hours, minutes, seconds
                    df_understat['date'] = df_understat['date'].str.slice(stop=10)
                    # Convert date to date format
                    df_understat['date'] = df_understat['date'].apply(lambda x: date(int(x[:4]), int(x[5:7]), int(x[-2:])))
                    # Convert df_understat to list and append
                    new_rows = df_understat.values.tolist()
                    for row in new_rows:
                        understat_list.append(row)
                except:
                    #print(team+' not in season '+season)
                    pass
            if understat_list:
                # Dictionary of dataframes per team, with all non-cumulative understat data
                team_stats[team] = pd.DataFrame(understat_list, columns=understat_columns)
    return team_stats

# Function converts all non-cumulative understat data into moving sum/moving average (wherever possible)
# Calling this function requires calling aggregate_understat first
def moving_series_understat(non_cum_team_stats, window):
    cum_team_stats = {}
    # Attributes to apply rolling sum
    cols_to_sum = ['xG', 'xGA', 'npxG', 'npxGA', 'deep', 'deep_allowed', 'scored', 'missed', 'xpts', 'wins', 'draws',
                   'loses', 'pts', 'npxGD']
    # Attributes to apply rolling mean
    cols_to_mean = ['ppda_ratio', 'oppda_ratio']
    # Apply transformations (mean/sum and then shift)
    for team in non_cum_team_stats.keys():
        cum_team_stats[team] = non_cum_team_stats[team].copy()
        cum_team_stats[team][cols_to_sum] = cum_team_stats[team][cols_to_sum].rolling(window).sum()
        cum_team_stats[team][cols_to_mean] = cum_team_stats[team][cols_to_mean].rolling(window).mean()
        cum_team_stats[team][cols_to_sum] = cum_team_stats[team][cols_to_sum].shift(1)
        cum_team_stats[team][cols_to_mean] = cum_team_stats[team][cols_to_mean].shift(1)
    return cum_team_stats


# Function converts unmerged non-cumulative understat data into cumulative data per season only (wherever possible)
# Calling this function does not require calling aggregate_understat
def non_moving_series_understat(leagues, seasons, master_data, understat_data_dir):
    understat_columns = ['h_a', 'xG', 'xGA', 'npxG', 'npxGA', 'deep', 'deep_allowed', 'scored',
                         'missed', 'xpts', 'result', 'date', 'wins', 'draws', 'loses', 'pts',
                         'npxGD', 'ppda_ratio', 'oppda_ratio']
    # Non-cumulative attributes
    cols_noncum = ['h_a', 'result', 'date']
    # Attributes to apply season cumulative sum
    cols_to_sum = ['xG', 'xGA', 'npxG', 'npxGA', 'deep', 'deep_allowed', 'scored', 'missed', 'xpts', 'wins', 'draws',
                   'loses', 'pts', 'npxGD']
    # Attributes to apply season cumulative mean
    cols_to_mean = ['ppda_ratio', 'oppda_ratio']

    # Read non-cumulative understat data
    cum_team_stats = {}
    for league in leagues:
        # Filter master_data to league
        master_data_league = master_data.loc[master_data['league_id'] == league]
        # Convert to dictionary with team_id key
        master_data_dic = master_data_league.set_index('team_id').T.to_dict('list')
        for team in master_data_dic.keys():
            understat_list = []
            column_names = []
            for season in seasons:
                try:
                    df_understat = pd.read_csv(understat_data_dir+league+'_'+season+'_'+team+'.csv', index_col=[0])
                    # Truncate date to remove hours, minutes, seconds
                    df_understat['date'] = df_understat['date'].str.slice(stop=10)
                    # Convert date to date format
                    df_understat['date'] = df_understat['date'].apply(lambda x: date(int(x[:4]), int(x[5:7]), int(x[-2:])))
                    # Apply transformations (mean/sum and then shift)
                    sum_df = np.cumsum(df_understat[cols_to_sum])
                    mean_df = {}
                    for col in cols_to_mean:
                        mean_df[col] = np.cumsum(df_understat[col])/np.array(range(1,len(df_understat)+1))
                    df_mean_sum = sum_df.join(pd.DataFrame(mean_df, columns=cols_to_mean))
                    df_mean_sum[cols_to_sum] = df_mean_sum[cols_to_sum].shift(1)
                    df_mean_sum[cols_to_mean] = df_mean_sum[cols_to_mean].shift(1)
                    # Concatenate noncumulative data with cumulative data per team and season
                    df_new = pd.concat([df_understat[cols_noncum].reset_index(drop=True), df_mean_sum.reset_index(drop=True)], axis=1)
                    # Ensure column order is preserved
                    df_new = df_new[understat_columns]
                    # Convert df_new to list and append
                    new_rows = df_new.values.tolist()
                    for row in new_rows:
                        understat_list.append(row)
                except:
                    #print(team+' not in season '+season)
                    pass
            if understat_list:
                # Dictionary of dataframes per team, with all non-cumulative understat data
                cum_team_stats[team] = pd.DataFrame(understat_list, columns=understat_columns)
    return cum_team_stats

# Function merges cumulative understat and match data and outputs data_full containing this data organised by season and league
# Function also outputs historical match data organised by league and pairs of teams (in alphabethical order)
def merge_match_understat(leagues, seasons, master_data, team_cum_stats, football_data_dir):
    # Save understat merged column values for all data (for later use)
    column_values = ['home_xG', 'home_xGA', 'home_npxG', 'home_npxGA', 'home_deep', 'home_deep_allowed',
                     'home_xpts', 'home_wins', 'home_draws', 'home_loses', 'home_pts', 'home_npxGD',
                     'home_ppda_ratio', 'home_oppda_ratio', 'away_xG', 'away_xGA', 'away_npxG',
                     'away_npxGA', 'away_deep', 'away_deep_allowed', 'away_xpts', 'away_wins',
                     'away_draws', 'away_loses', 'away_pts', 'away_npxGD', 'away_ppda_ratio', 'away_oppda_ratio']

    # Necessary columns in matches dataframes and for historical match data
    nec_columns_matches = ['Date', 'HomeTeam', 'AwayTeam', 'FTHG', 'FTAG']

    # Create dictionary for all data
    data_full = {}
    # Create dictionary for historical match data
    df_for_historical = {}
    for league in leagues:
        df_for_historical[league] = {}

    for season in seasons:
        data_full[season] = {}
        for league in leagues:
            # Filter master_data to league
            master_data_league = master_data.loc[master_data['league_id'] == league]
            # Load match data in league and season
            df_matches = pd.read_csv(football_data_dir+league+'_'+season+'_results.csv')
            # Keep only necessary columns
            df_matches = df_matches[nec_columns_matches]
            # Remove rows with NaNs
            df_matches.dropna(inplace=True)
            # Change date format to match understat data
            df_matches['Date'] = df_matches['Date'].apply(lambda x: date(int('20'+x[-2:]), int(x[3:5]), int(x[0:2])))
            # Change team names in df_matches to match team_id (same as understat data)
            master_data_dic_matches = master_data_league.set_index('footballdata_team').T.to_dict('list')  # Create dictionary
            df_matches['HomeTeam'] = df_matches['HomeTeam'].apply(lambda x: master_data_dic_matches[x][0])
            df_matches['AwayTeam'] = df_matches['AwayTeam'].apply(lambda x: master_data_dic_matches[x][0])

            # Convert dataframe to array for iteration
            matches_array = df_matches.to_numpy()
            # Initialisation of auxiliary lists
            i_matches_to_remove = []
            understat_data = []
            # Iterate over matches
            for i_match in range(len(matches_array)):
                date_match = matches_array[i_match, 0]
                home_team = matches_array[i_match, 1]
                away_team = matches_array[i_match, 2]
                FTHG = matches_array[i_match, 3]
                FTAG = matches_array[i_match, 4]

                # Save historical match data
                key_hist = tuple(sorted([home_team, away_team]))
                if key_hist in df_for_historical[league].keys():
                    df_for_historical[league][key_hist].append([date_match, home_team, away_team, FTHG, FTAG])
                else:
                    df_for_historical[league][key_hist] = [[date_match, home_team, away_team, FTHG, FTAG]]

                # Create dfs with home team and away team data for match date from understat dataframes
                team_home = team_cum_stats[home_team].loc[(team_cum_stats[home_team]['date'] == date_match) &
                                                                (team_cum_stats[home_team]['h_a'] == 'h')]
                team_away = team_cum_stats[away_team].loc[(team_cum_stats[away_team]['date'] == date_match) &
                                                                (team_cum_stats[away_team]['h_a'] == 'a')]
                # Add day to account for matches after midnight (football-data does not seem to have this corrected)
                if team_home.empty or team_away.empty:
                    delta_day = timedelta(days=1)
                    team_home = team_cum_stats[home_team].loc[(team_cum_stats[home_team]['date'] == date_match + delta_day) &
                                                                    (team_cum_stats[home_team]['h_a'] == 'h')]
                    team_away = team_cum_stats[away_team].loc[(team_cum_stats[away_team]['date'] == date_match + delta_day) &
                                                                    (team_cum_stats[away_team]['h_a'] == 'a')]
                    # If still cannot find stats for match, save i_match for later removal of match
                    if team_home.empty or team_away.empty:
                        i_matches_to_remove.append(i_match)
                        continue   # continue to next i_match

                # Drop date, home_away, scored/missed data and result from team_home and team_away
                team_home.drop(columns=['date', 'h_a', 'scored', 'missed', 'result'], inplace=True)
                team_away.drop(columns=['date', 'h_a', 'scored', 'missed', 'result'], inplace=True)
                # Rename each label with home_ and away_ prefix
                team_home = team_home.add_prefix('home_').reset_index(drop=True)
                team_away = team_away.add_prefix('away_').reset_index(drop=True)
                # Concatenate team_home and team_away
                new_row = pd.concat([team_home, team_away], axis=1).reset_index(drop=True)
                # Ensure column order is preserved
                new_row = new_row[column_values]
                # Convert new_row into list and append to list of lists, containing understat data organised by matches
                new_row_list = new_row.values.tolist()
                understat_data.append(new_row_list[0])

            print(season, league, 'Original n of matches: ', len(df_matches))
            # Remove matches with missing data
            if i_matches_to_remove:
                rows_to_remove = df_matches.index[i_matches_to_remove]
                df_matches.drop(rows_to_remove, inplace=True)
            print(season, league, 'matches to remove: ', len(i_matches_to_remove))
            # Create dataframe from understat data organised by matches
            df_understat_matches = pd.DataFrame(understat_data, columns=column_values)
            # Concatenate df_matches with df_understat_matches
            df_matches = pd.concat([df_matches.reset_index(drop=True), df_understat_matches.reset_index(drop=True)], axis=1)
            # Test
            print(season, league, 'Final n of matches: ', len(df_matches))
            #
            # Remove rows with NaNs (understat data may have NaNs due to offset from rolling sums/averages)
            df_matches.dropna(inplace=True)
            # Check NaNs
            if df_matches.isnull().values.any(): print('Error - NaNs!', league, season)
            del df_understat_matches
            data_full[season][league] = df_matches

    # Convert historical match data to dataframe
    for league in df_for_historical.keys():
        for key, df in df_for_historical[league].items():
            df_for_historical[league][key] = pd.DataFrame(df, columns=nec_columns_matches)

    return data_full, df_for_historical

# Function merges historical match data of the last "number_historical_matches" matches to all the other data
def merge_historical_data(df_full, df_historical_data, number_historical_matches):
    # New column values for dataframe
    n_cols = ['FTHG', 'FTAG', 'home_bool']
    # Last entries are more recent matches
    new_cols = [col+str(-i) for i in range(number_historical_matches, 0, -1) for col in n_cols]

    df_full_new = {}
    for season in df_full.keys():
        df_full_new[season] = {}
        for league in df_full[season].keys():
            table_match = []
            df_full_array = df_full[season][league].to_numpy()
            for i_match in range(len(df_full_array)):
                date_match = df_full_array[i_match, 0]
                home_team = df_full_array[i_match, 1]
                away_team = df_full_array[i_match, 2]

                # Get historical match data
                df_h_match = df_historical_data[league][tuple(sorted([home_team, away_team]))]
                df_h_match = df_h_match[df_h_match['Date'] < date_match]
                # Replace HomeTeam column by boolean variable (True when home team in historical match is home team of actual match)
                df_h_match['HomeTeam'] = df_h_match['HomeTeam'].apply(lambda x: x==home_team)
                df_h_match = df_h_match.rename(columns={'HomeTeam': 'home_bool'})
                # Drop date and away team
                df_h_match.drop(columns=['Date', 'AwayTeam'], inplace=True)
                # Re-order columns to match n_cols
                df_h_match = df_h_match[n_cols]

                # Create array to append with historical match data
                df_h_match_array = df_h_match.to_numpy()
                if 0 < np.shape(df_h_match_array)[0] < number_historical_matches:
                    aux = np.empty(3*(number_historical_matches - np.shape(df_h_match_array)[0]))
                    aux[:] = np.NaN
                    df_h_match_array = np.concatenate((aux, df_h_match_array.flatten()))
                elif np.shape(df_h_match_array)[0] == 0:
                    df_h_match_array = np.empty(3*number_historical_matches)
                    df_h_match_array[:] = np.NaN
                else:
                    df_h_match_array = df_h_match_array[-number_historical_matches:, :].flatten()
                table_match.append(df_h_match_array.tolist())

            # Concatenate historical match data with full data dataframe
            df_h = pd.DataFrame(table_match, columns=new_cols)
            df_full_new[season][league] = pd.concat([df_full[season][league].reset_index(drop=True),
                                                 df_h.reset_index(drop=True)], axis=1)
    return df_full_new

# Function to convert dictionaries of dataframes to single dataframe and/or export to csv
def export_dataframe_dic_to_csv(dic_df, name_csv, return_df=True):
    cols = []
    df_total_list = []
    for key1, value1 in dic_df.items():
        for key2, df in value1.items():
            # Save columns
            if not cols:
                cols = df.columns.values.tolist()
            # Append each season/league df to large list
            df_list = df.values.tolist()
            for row in df_list:
                df_total_list.append(row)

    df_total = pd.DataFrame(df_total_list, columns=cols)
    df_total.to_csv('../../data/%s.csv' %(name_csv))
    if return_df:
        return df_total




# Read csv with master data
master_data = pd.read_csv(master_table_dir)

# Aggregate non-cumulative understat data per team
team_stats = aggregate_understat(leagues, seasons, master_data, understat_data_dir)

# Convert to cumulative understat data (either with rolling window or per season)
team_cum_rolling_stats = moving_series_understat(team_stats, 10)
team_cum_season_stats = non_moving_series_understat(leagues, seasons, master_data, understat_data_dir)

# Merge understat data with match data. Output merged understat+match data and historical match data in separate dataframes
df1, data_historical1 = merge_match_understat(leagues, seasons, master_data, team_cum_rolling_stats,
                                                  football_data_dir)

df2, data_historical2 = merge_match_understat(leagues, seasons, master_data, team_cum_season_stats,
                                                  football_data_dir)

# Merge understat+match data with historical match data with desired number of historical matches
df1_merged = merge_historical_data(df1, data_historical1, 3)
df2_merged = merge_historical_data(df2, data_historical2, 3)

# Export to csv and return single dataframe
df1_full = export_dataframe_dic_to_csv(df1_merged, 'full_data_rolling')
df2_full = export_dataframe_dic_to_csv(df2_merged, 'full_data_season')

x = 1