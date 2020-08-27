import datetime
import requests
import pandas as pd
import numpy as np
from sqlalchemy import *
from pandas.io.json import json_normalize


### USER FUNCTIONS
def db_insert(con, table, table_from_db, table_name):
    if len(table_from_db) > 0:
        print(f"***** {table_name} already exists")
        if table.equals(table_from_db) != True:
            print(f"***** new data inserted into {table_name}")
            if table_name == "boxscores":
                table.to_sql(table_name, con=con, if_exists="append", index=False, method="multi")
            else:
                print(f"***** {table_name} is the same, replacing the table.")
                table.to_sql(table_name, con=con, if_exists="replace", index=False, method="multi")
    else:
        print(f"***** creating new table called {table_name}")
        table.to_sql(table_name, con=con, if_exists="replace", index=False, method="multi")


def build_table(table_name, table_names, con, current_season, func):
    if table_name in table_names:
        table_from_db = pd.read_sql_query(f'select * from {table_name}', con=con)
        if table_name == "boxscores":
            schedule = pd.read_sql_query('select * from schedule', con=con)
            new_games = schedule[~schedule['gameId'].isin(table_from_db.gameId.unique())]
            table = pd.DataFrame()
        else:
            seasons = [current_season]
            table = table_from_db[table_from_db.season < current_season]
    else:
        if table_name == "boxscores":
            new_games = pd.read_sql_query('select * from schedule', con=con)
        table_from_db = pd.DataFrame()
        table = pd.DataFrame()
        seasons = range(2016, current_season + 1)
    
    if table_name == "boxscores":
        for i in range(len(new_games)):
            table = table.append(func(new_games.iloc[i]['startDateEastern'], new_games.iloc[i]['gameId']), ignore_index=True)
    else:
        for s in seasons:
            table = table.append(func(s), ignore_index=True)

    db_insert(con, table, table_from_db, table_name)


def get_schedule(season):
    response = requests.get("http://data.nba.net/prod/v2/{}/schedule.json".format(season))

    try:
        df = pd.read_json(response.text)
        sched_df = json_normalize(df[df.index == 'standard']['league'][0])
        schedule = sched_df[[
            'gameId', 'seasonStageId', 'startDateEastern',
            'period.current', 'period.type', 'period.maxRegular',
            'hTeam.teamId', 'hTeam.score', 'vTeam.teamId', 'vTeam.score']]
        schedule['OT'] = np.where(schedule['period.current'] > 4, True, False)
        schedule['season'] = season
    except:
        print("***** Could not find a schedule table for {}".format(season))
        schedule = pd.DataFrame()

    return schedule


def get_players(season):
    response = requests.get('http://data.nba.net/prod/v1/{}/players.json'.format(season))

    try:
        df = pd.read_json(response.text)
        players_df = json_normalize(df[df.index == 'standard']['league'][0])
        players_df['fullName'] = players_df.firstName + ' ' + players_df.lastName
        players_df['season'] = season
        players_df = players_df[['personId', 'fullName', 'firstName', 'lastName', 'pos', 'season', 'heightFeet', 'heightInches']]
    except:
        print("***** Could not find a players table for {}".format(season))
        players_df = pd.DataFrame()
    
    return players_df


def get_boxscore(date, game_id):
    response = requests.get("http://data.nba.net/10s/prod/v1/{0}/{1}_boxscore.json".format(date, game_id))

    try:
        df = pd.read_json(response.text)
        df = json_normalize(df[df.index == 'activePlayers']['stats'][0])
        boxscore = df[['personId', 'teamId',
                       'min', 'points', 'fgm', 'fga',
                       'ftm', 'fta', 'tpm', 'tpa', 'offReb', 'defReb',
                       'totReb', 'assists', 'pFouls', 'steals', 'turnovers', 'blocks', 'plusMinus']]
        boxscore['min'] = boxscore['min'].str.split(":", expand = True)[0]
        boxscore['gameId'] = game_id
        boxscore['date'] = date
    except:
        print("***** Could not find a boxscore for {} | {}".format(date, game_id))
        boxscore = pd.DataFrame()

    return boxscore