import time
import datetime
import requests
import pandas as pd
import numpy as np
import scrape_functions
import argparse

from sqlalchemy import *
from pandas.io.json import json_normalize

import settings

if settings.PASSWORD == '':
    print('***** You need to fix the settings.py file to add the credentials.')


### MAIN
parser = argparse.ArgumentParser()
parser.add_argument('--season', action='store')
args = parser.parse_args()

current_season = int(args.season)

con = create_engine("postgresql://{}:{}@{}:5432/{}".format(settings.USERNAME, settings.PASSWORD, settings.HOSTNAME, settings.DBNAME))
inspector = inspect(con)
table_names = inspector.get_table_names()

scrape_functions.build_table('schedule', table_names, con, current_season, scrape_functions.get_schedule)
scrape_functions.build_table('players', table_names, con, current_season, scrape_functions.get_players)
scrape_functions.build_table('boxscores', table_names, con, current_season, scrape_functions.get_boxscore)
