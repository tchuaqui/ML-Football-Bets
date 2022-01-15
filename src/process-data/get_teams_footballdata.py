import pandas as pd
import os
import glob

# use glob to get all the csv files
# in the folder
path = '../../../data/football-data'
# path = os.getcwd() +'\\football-data'
csv_files = glob.glob(os.path.join(path, "*.csv"))

teams = []
leagues = []
# loop over the list of csv files
for f in csv_files:
    # read the csv file
    df = pd.read_csv(f)
    if len(f.split('football-data\\')[1].split('_')) == 3:
        league = f.split('football-data\\')[1].split('_')[0]
    else:
        league = '_'.join(f.split('football-data\\')[1].split('_')[0:2])

    teams_new = df['HomeTeam'].values
    teams += [x for x in teams_new]
    leagues += [league for x in df['HomeTeam']]

    # print the location and filename
    print('Location:', f)
    print('File Name:', f.split("\\")[-1])

data = {'league': leagues, 'teams': teams}
df = pd.DataFrame(data)
df.drop_duplicates(subset=None, keep="first", inplace=True)
df.to_csv(r'football_data_teams.csv', index=False)
