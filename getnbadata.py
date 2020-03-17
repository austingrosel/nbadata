import random as rnd
import time
import datetime
import requests
import pandas as pd
from pandas.io.json import json_normalize
import luigi


class Games(luigi.Task):
	date_scrape = luigi.DateParameter()

	def run(self):
		date = self.date_scrape.strftime("%Y%m%d")
		print('************ DATE:', date)
		response = requests.get('http://data.nba.net/json/cms/noseason/scoreboard/{}/games.json'.format(date))

		try:
			df = pd.read_json(response.text)
			games_df = json_normalize(df[df.index == 'games']['sports_content'][0]['game'])
		except:
			print("***** Could not find a table for {}".format(date))
			games_df = pd.DataFrame()

		with self.output().open('w') as fout:
			games_df.to_csv(fout, index=False)

	def output(self):
		return luigi.LocalTarget('data/games/games_{}.csv'.format(self.date_scrape))


class Data(luigi.Task):
	date = luigi.Parameter()
	game_id = luigi.Parameter()

	def run(self):
		response = requests.get("http://data.nba.net/10s/prod/v1/{0}/{1}_boxscore.json".format(self.date, self.game_id))

		try:
			df = pd.read_json(response.text)
			df = json_normalize(df[df.index == 'activePlayers']['stats'][0])
			boxscore = df[['personId', 'firstName', 'lastName', 'teamId',
							'min', 'points', 'fgm', 'fga',
							'ftm', 'fta', 'tpm', 'tpa', 'offReb', 'defReb',
							'totReb', 'assists', 'pFouls', 'steals', 'turnovers', 'blocks', 'plusMinus']]
			boxscore['min'] = boxscore['min'].str.split(":", expand = True)[0]
			boxscore['gameId'] = self.game_id
		except:
			print("***** Could not find a boxscore for {} | {}".format(self.date, self.game_id))
			boxscore = pd.DataFrame()

		with self.output().open('w') as fout: 
			boxscore.to_csv(fout, index=False)

	def output(self):
		return luigi.LocalTarget('data/boxscores/boxscore_{}_{}.csv'.format(self.date, self.game_id))


class Dynamic(luigi.Task):
	date_interval = luigi.DateIntervalParameter()

	def requires(self):
		return [Games(date) for date in self.date_interval]

	def run(self):
		ids = []
		dates = []
		for t in self.input(): 
			with t.open('r') as f:
				lines = [x.strip() for x in f.readlines()]
				for line in lines:
					try:
						#ids = [line.split(',')[0] for line in lines if line.split(',')[0] != 'id']
						#dates = [line.split(',')[3] for line in lines if line.split(',')[3] != 'date']
						if (line.split(',')[0] != 'id') & (len(line.split(',')[3]) > 0):
							ids.append(line.split(',')[0])
						if (line.split(',')[3] != 'date'):
							dates.append(line.split(',')[3])
					except:
						print("****** could not get valid line.")

		print('*** DATES:', dates)
		print('*** IDS:', ids)
		data_dependent_deps = [Data(date=date, game_id=id) for id, date in zip(ids, dates)]
		yield data_dependent_deps


if __name__ == '__main__':
	luigi.run()
