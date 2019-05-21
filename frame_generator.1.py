from mysql_functions import dboperator_instance
from config_functions import config
#from configparser import ConfigParser
from os import walk
import sys
import time

# этот класс выдаёт секундные свечки
class frameGenerator:
	def __init__(self):
		self.currentRateDateTime = None
		self.currentTicksArray = []
		self.currentCandle = []
		self.statistics = {}
		self.filenames = []
		self.currentFile = None
		self.startDateTime = time.time()


	def readFilesDirectory(self):
		for (dirpath, dirnames, filenames) in walk(config.get('various', 'datafilespath')):
			self.filenames.extend(filenames)
		self.filenames.reverse()

	def openNextFile(self):
		self.currentFile = open(config.get('various', 'datafilespath') + self.filenames.pop())
		pass


	# вычислить текущую свечку
	def get_current_candle(self):
		mix = []
		candle = [0, 0, 0, 0]
		ticksCount = len(self.currentTicksArray)

		first_tick = self.currentTicksArray[0]
		medium = round(((first_tick[0] + first_tick[1]) / 2), 5)
		candle_open = medium

		# свечки бывают и из одного тика
		if ticksCount > 1:
			for tick in self.currentTicksArray:
				mix.append(tick[0])
				mix.append(tick[1])
			candle_high = max(mix)
			candle_low = min(mix)
			last_tick = self.currentTicksArray[-1]
			medium = round(((last_tick[0] + last_tick[1]) / 2), 5)
			candle_close = medium
			candle = [candle_open, candle_high, candle_low, candle_close]
		else:
			candle = [medium, first_tick[1], first_tick[0], medium]
		self.currentCandle = candle

		vector = int ((candle[-1] - candle[0]) * 100000)
		sizehl = int ((candle[1] - candle[2]) * 100000)

		# сделать немного статистики
		self.statistics['subTicksParsed'] = self.statistics.get('subTicksParsed', 0) + ticksCount
		self.statistics['subCandlesCalculated'] = self.statistics.get('subCandlesCalculated', 0) + 1
		self.statistics['subLastTickParsed'] = self.limitpos
		self.statistics['subLastRateDateTime'] = self.currentRateDateTime.strftime("%Y-%m-%d %H:%M:%S")

		return {'candle': candle, 'ticksCount': ticksCount, 'vector': vector, 'sizehl': sizehl, 'id':self.limitpos, 'RateDateTime': self.currentRateDateTime}

a = frameGenerator()
a.readFilesDirectory()
print(len(a.filenames))
a.openNextFile()