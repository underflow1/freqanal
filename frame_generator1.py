from mysql_functions import dboperator_instance
from datetime import datetime
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
		self.currentFullFileName = None
		self.currentFileHandler = None
		self.currentLineNumber = 0
		self.fileTimeStart = None
		self.fileTimeStop = None
		self.fileTimeElapsed = None
		self.parsedTicksCount = 0
		self.readFilesDirectory()
		self.openNextFile()

	def readFilesDirectory(self):
		for (dirpath, dirnames, filenames) in walk(config.get('various', 'datafilespath')):
			self.fileNamesList.extend(filenames)
		self.fileNamesList.reverse()

	def saveSessionData(self):
		self.sessionDataFile = open(config.get('various', 'sessiondatafile'), 'w') 
		self.sessionDataFile.write(str(self.currentLineNumber))
		self.sessionDataFile.close()

	def openNextFile(self):
		currentFileName = self.fileNamesList.pop()
		self.currentFullFileName = config.get('various', 'datafilespath') + sysvar.dirsep + currentFileName
		self.currentFileHandler = open(self.currentFullFileName, 'r')
		self.currentLineNumber = 0
		print('file opened', currentFileName)
		self.fileTimeStart = time.time()
		pass

	def readticks(self):
		self.currentTicksArray = []
		ticksList = []
		line = self.currentFileHandler.readline()
		rateDateTime_prev = None
		while line:
			if self.currentLineNumber > 0: # SKIP FIRST LINE
				items = line.split("\t", 1)
				rateDateTime = items[0].split('.',1)[0]
				rateValue = items[1].split()
				tick = (items[0].split('.',1)[0], float(rateValue[0]), float(rateValue[1]))
				if len(self.currentTicksArray) == 0: # время первого тика всегда равно предыдущему (даже если это тик всего один)
					rateDateTime_prev = rateDateTime
				if rateDateTime == rateDateTime_prev: # если тики равны, добавляем и читаем дальше
					self.currentTicksArray.append(tick)
				else:
					self.parsedTicksCount += len(self.currentTicksArray)
					self.currentRateDateTime = rateDateTime_prev
					return False
			line = self.currentFileHandler.readline()
			self.currentLineNumber += 1
		self.fileTimeStop = time.time()
		self.fileTimeElapsed = self.fileTimeStart - self.fileTimeStop
		print('file parsed', self.fileTimeElapsed)			
		return True


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

		return {'candle': candle, 'ticksCount': ticksCount, 'vector': vector, 'sizehl': sizehl, 'id': self.parsedTicksCount, 'RateDateTime': datetime.strptime(self.currentRateDateTime, "%Y-%m-%d %H:%M:%S")}

	# выдать следующий результат (свечку и её метаданные)
	def next(self):
		while True:
			fileparsed = self.readticks()
			if not fileparsed:
				result = self.get_current_candle()
				return result
			else:
				print(self.statistics)
				self.openNextFile()
