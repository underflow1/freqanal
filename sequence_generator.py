from mysql_functions import dboperator_instance

# этот класс выдаёт секундные свечки
class sequenceGenerator:
	def __init__(self, limitpos, limitsize):
		self.limitsize = limitsize
		self.limitpos = limitpos
		self.distinctRateDateTime = []
		self.currentRateDateTime = None
		self.currentTicksArray = []
		self.currentCandle = []
		self.statistics = {}

	# загрузить в виртуальную таблицу пачку тиков
	def load_ticks_bunch_virtual(self):
		dboperator_instance.cursor.execute('DROP TABLE if exists ticks_tmp')
		query = 'CREATE TEMPORARY TABLE IF NOT EXISTS ticks_tmp AS (SELECT * FROM python_mysql.ticks LIMIT $limitpos, $limitsize) '
		args = {'limitpos': self.limitpos, 'limitsize': self.limitsize}
		query = dboperator_instance.prepareQuery(query, args)
		dboperator_instance.cursor.execute(query)
		#print('Bunch loaded')

	# вычленить список уникальных секунд в пачке 
	def get_Distinct_DateRateTime(self):
		query = 'SELECT DISTINCT RateDateTime FROM ticks_tmp'
		dboperator_instance.cursor.execute(query)
		result = dboperator_instance.cursor.fetchall()
		localdistinctRateDateTime = []
		for tupleitem in result:
			for item in tupleitem:	
				localdistinctRateDateTime.append(item)
		# вот тут начинается хитрый механизм: когда текущий список уникальных секунд подходит к концу
		# нам надо загрузить новый список, но может получится так, что последняя секунда в текущем списке
		# совпадает с первой секундой нового списка, поэтому, чтобы у нас не получились две одинаковые свечки,
		# первую секунду в новом списке надо удалить (в случае совпадения конечно же)
		if len(self.distinctRateDateTime) == 1:
			last_distinctRateDateTime = self.distinctRateDateTime[0]
			new_distinctRateDateTime = localdistinctRateDateTime[0]
			if last_distinctRateDateTime == new_distinctRateDateTime:
				localdistinctRateDateTime.pop(0)
		self.distinctRateDateTime.extend(localdistinctRateDateTime)
		# я переворачиваю массив чтобы POPать его с конца 
		# pop последнего элемента происходит быстрее чем первого или любого другого
		# поэтому перевернуть один раз и попать с конца получается выгоднее
		self.distinctRateDateTime.reverse()
		#print('Distinct DateRateTime loaded ' + str(len(localdistinctRateDateTime)) )
	
	# прочитать следующую секунду
	def get_Next_RateDateTime(self):
		if len(self.distinctRateDateTime) < 2:
			self.load_ticks_bunch_virtual()
			self.get_Distinct_DateRateTime()
		self.currentRateDateTime = self.distinctRateDateTime.pop()
		pass

	# загрузить тики за текущую дату в массив
	def get_ticks_current(self):
		query = ' SELECT RateBid, RateAsk FROM ticks_tmp WHERE RateDateTime = $currentRateDateTime ORDER BY idticks; '
		args = {'currentRateDateTime': self.currentRateDateTime}
		query = dboperator_instance.prepareQuery(query, args)	
		dboperator_instance.cursor.execute(query)	
		result = dboperator_instance.cursor.fetchall()
		self.currentTicksArray = result

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
		
		# сделать немного статистики
		self.statistics['subTicksParsed'] = self.statistics.get('subTicksParsed', 0) + ticksCount
		self.statistics['subCandlesCalculated'] = self.statistics.get('subCandlesCalculated', 0) + 1

		# после каждого успешного вычисления свечки, сдвигаем начальную позицию выборки пачки на количество обработанных тиков
		self.limitpos = self.limitpos + ticksCount

		return {'candle': candle, 'ticksCount': ticksCount, 'RateDateTime': self.currentRateDateTime}

	# выдать следующий результат (свечку и её метаданные)
	def next(self):
		self.get_Next_RateDateTime()
		self.get_ticks_current()
		result = self.get_current_candle()
		return result

pass