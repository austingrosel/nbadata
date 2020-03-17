import random as rnd
import time
import datetime
import requests
import pandas as pd
from pandas.io.json import json_normalize
import luigi


class LeagueSchedule(luigi.Task):
	date = luigi.YearParameter()

	def run(self):
		print('************ SCHEDULE YEAR:', self.date.year)
		response = requests.get('http://data.nba.net/prod/v2/{}/schedule.json'.format(self.date.year))

		try:
			df = pd.read_json(response.text)
			sched_df = json_normalize(df[df.index == 'standard']['league'][0])
			sched_df = sched_df[['gameId', 'seasonStageId', 'startDateEastern',
			'period.current', 'period.type', 'period.maxRegular',
			'hTeam.teamId', 'hTeam.score', 'vTeam.teamId', 'vTeam.score']]

		except:
			print("***** Could not find a table for {}".format(self.date.year))
			sched_df = pd.DataFrame()

		with self.output().open('w') as fout:
			sched_df.to_csv(fout, index=False)

	def output(self):
		return luigi.LocalTarget('data/schedule/sched_{}.csv'.format(self.date.year))


class Players(luigi.Task):
	date = luigi.YearParameter()

	def run(self):
		print('************ PLAYER YEAR:', self.date.year)
		response = requests.get('http://data.nba.net/prod/v1/{}/players.json'.format(self.date.year))
		try:
			df = pd.read_json(response.text)
			players_df = json_normalize(df[df.index == 'standard']['league'][0])
			players_df = players_df[['personId', 'lastName','firstName', 'pos', 'heightFeet', 'heightInches']]
		except:
			print("***** Could not find a players table for {}".format(self.date.year))
			players_df = pd.DataFrame()

		with self.output().open('w') as fout:
			players_df.to_csv(fout, index=False)

	def output(self):
		return luigi.LocalTarget('data/players/players_{}.csv'.format(self.date.year))

class RunAll(luigi.Task):
	date_interval = luigi.DateIntervalParameter()

	def requires(self):
		yield [LeagueSchedule(date) for date in self.date_interval]
		yield [Players(date) for date in self.date_interval]


if __name__ == '__main__':
	luigi.run()
