import pandas as pd
import glob
import os

name_csv = 'full_data_odds'


# Function to concat all dataframes in a given directory
def concat_csvs_in_dir(dir):
    os.chdir(dir)
    all_filenames = [i for i in glob.glob('*.{}'.format('csv'))]
    df_combined = pd.concat([pd.read_csv(f) for f in all_filenames])
    return df_combined


def find_result(home_team, away_team, bet_slug):
    # bet_slug = bet outcome from oddsmagnet
    if bet_slug == home_team:
        result = 'win'
    elif bet_slug == away_team:
        result = 'loss'
    elif bet_slug == 'draw':
        result = 'draw'
    else:
        result = None
    return result


# Create dict to map team ids
df_master_data = pd.read_csv('../../data/master-data/teams.csv')
df_keys = df_master_data[['oddsmagnet_team', 'team_id']]
df_keys.dropna(inplace=True)
dict_team_id = dict(zip(df_keys['oddsmagnet_team'], df_keys['team_id']))

# Merge odds CSV files
df_oddsmagnet = concat_csvs_in_dir('../../data/oddsmagnet/')
# Add team ids:
df_oddsmagnet['home_team_id'] = [dict_team_id[team] for team in df_oddsmagnet['home_team']]
df_oddsmagnet['away_team_id'] = [dict_team_id[team] for team in df_oddsmagnet['away_team']]
df_oddsmagnet['result'] = df_oddsmagnet.apply(
    lambda x: find_result(x['home_team'], x['away_team'], x['bet_slug']), axis=1)

# drop rows where updated time > match start time:
df_oddsmagnet = df_oddsmagnet[df_oddsmagnet['start_date']>=df_oddsmagnet['updated']]

# Export file:
df_oddsmagnet.to_csv('../../data/%s.csv' % name_csv)
