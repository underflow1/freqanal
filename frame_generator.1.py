from mysql_functions import dboperator_instance
from config_functions import config, sysvar
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
		self.fileNamesList = []
		self.currentFileName = None
		self.currentFileHandler = None
		self.currentLineNumber = 0
		self.startDateTime = time.time()
		self.parsedTicksCount = 0
		self.readFilesDirectory()
		self.openNextFile()

	def readFilesDirectory(self):
		for (dirpath, dirnames, filenames) in walk(config.get('various', 'datafilespath')):
			self.fileNamesList.extend(filenames)
		self.fileNamesList.reverse()

	def openNextFile(self):
		self.currentFileName = config.get('various', 'datafilespath') + sysvar.dirsep + self.fileNamesList.pop()
		self.currentFileHandler = open(self.currentFileName, 'r')
		self.currentLineNumber = 0
		pass

	def readticks(self):
		self.currentTicksArray = []
		ticksList = []
		time_start = time.time()
		line = self.currentFileHandler.readline()
		rateDateTime_prev = None
		ticksLineNumber = 0
		while line:
			if self.currentLineNumber > 0: # SKIP FIRST LINE
				items = line.split("\t", 1)
				rateDateTime = items[0].split('.',1)[0]
				rateValue = items[1].split()
				tick = (items[0].split('.',1)[0], float(rateValue[0]), float(rateValue[1]))
				if ticksLineNumber == 0:
					rateDateTime_prev = rateDateTime
				if rateDateTime == rateDateTime_prev:
					self.currentTicksArray.append(tick)
				else:
					self.parsedTicksCount = self.parsedTicksCount + ticksLineNumber
					self.currentRateDateTime = rateDateTime_prev
					rateDateTime_prev = rateDateTime
					return True
				ticksLineNumber += 1
			line = self.currentFileHandler.readline()
			self.currentLineNumber += 1
		time_end = time.time()
		elapsed = str(time_end - time_start)
		print('File parsed. Time elapsed:', elapsed, 'linecount', self.currentLineNumber)
		return False


	# вычислить текущую свечку
	def get_current_candle(self):
		mix = []
		candle = [0, 0, 0, 0]
		ticksCount = len(self.currentTicksArray)

		first_tick = self.currentTicksArray[0]
		medium = round(((first_tick[1] + first_tick[2]) / 2), 5)
		candle_open = medium

		# свечки бывают и из одного тика
		if ticksCount > 1:
			for tick in self.currentTicksArray:
				mix.append(tick[1])
				mix.append(tick[2])
			candle_high = max(mix)
			candle_low = min(mix)
			last_tick = self.currentTicksArray[-1]
			medium = round(((last_tick[1] + last_tick[2]) / 2), 5)
			candle_close = medium
			candle = [candle_open, candle_high, candle_low, candle_close]
		else:
			candle = [medium, first_tick[2], first_tick[1], medium]
		self.currentCandle = candle

		vector = int ((candle[-1] - candle[0]) * 100000)
		sizehl = int ((candle[1] - candle[2]) * 100000)

		# сделать немного статистики
		self.statistics['subTicksParsed'] = self.statistics.get('subTicksParsed', 0) + ticksCount
		self.statistics['subCandlesCalculated'] = self.statistics.get('subCandlesCalculated', 0) + 1
		self.statistics['subLastTickParsed'] = self.parsedTicksCount
		self.statistics['subLastRateDateTime'] = self.currentRateDateTime

		return {'candle': candle, 'ticksCount': ticksCount, 'vector': vector, 'sizehl': sizehl, 'RateDateTime': self.currentRateDateTime}

a = frameGenerator()

if len(a.fileNamesList) > 0:
	a.readticks()
	print(a.get_current_candle())
else:
	a.openNextFile()