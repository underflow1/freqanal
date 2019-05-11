import time
from datetime import timedelta
from frame_generator import frameGenerator
from gestalt_generator import gestaltGenerator
from mysql_functions import dboperator_instance

deals = []

def parseDeals(frame):
	dealnumber = 0
	for deal in deals:
		age = frame['RateDateTime'] - deal[2]
		if deal[4] > 0:
			abc = -1
		else:
			abc = 1
		if deal[0] == 0 and  age > timedelta(seconds = 60):
			deal.append(int((deal[3] - frame['candle'][3]) * abc * 100000))
			deal[0] = 2

		if deal[0] == 2 and  age > timedelta(seconds = 120):
			deal.append(int((deal[3] - frame['candle'][3]) * abc * 100000))
			deal[0] = 3

		if deal[0] == 3 and  age > timedelta(seconds = 180):
			deal.append(int((deal[3] - frame['candle'][3]) * abc * 100000))
			deal[0] = 4

		if deal[0] == 4 and  age > timedelta(seconds = 240):
			deal.append(int((deal[3] - frame['candle'][3]) * abc * 100000))
			deal[0] = 5

		if deal[0] == 5 and  age > timedelta(seconds = 300):
			deal.append(int((deal[3] - frame['candle'][3]) * abc * 100000))
			deal[0] = 10

		if deal[0] == 10 and  age > timedelta(seconds = 600):
			deal.append(int((deal[3] - frame['candle'][3]) * abc * 100000))
			deal[0] = 11

		if deal[0] == 11:
			query = ' INSERT INTO deals VALUES ( $id, $RateDateTime, $close, $gestalt, $wa, $power, $1, $2, $3, $4, $5, $10) '
			args = {'id': deal[1], 'RateDateTime': deal[2], 'close': deal[3], 'wa': deal[4], 'power': deal[5], 'gestalt': str(deal[6]), '1': deal[7], '2': deal[8], '3': deal[9], '4': deal[10], '5': deal[11], '10': deal[12], }
			query = dboperator_instance.prepareQuery(query, args)
			try:
				dboperator_instance.cursor.execute(query)
			except Exception as e:
				print(e)
			else:
				dboperator_instance.connection.commit()
			deals.pop(dealnumber)
		dealnumber += 1


frame = frameGenerator()
gestalt = gestaltGenerator()
start_time = time.time()
savecounter = 0
gestaltblind = 0
for count in range(10000000):
	deal = []
	savecounter += 1
	fr = frame.next()
	parseDeals(fr)
	gestalt.addFrame(fr)
	wa = gestalt.calculateGestaltWeightAverage()
	gestaltblind = gestaltblind + 1
	if wa and wa >= 5 and gestaltblind > 10:
		gestaltblind = 0
		#print(gestalt.getGestaltSnaphot(), 'wa is:', wa, 'power is:', gestalt.calculateGestaltPower(), 'snapshots count:', gestalt.snaphotsCount, '\t\t\t\t\t\t\t\t')
		deal.append(0)
		deal.append(fr['id'])
		deal.append(fr['RateDateTime'])
		deal.append(fr['candle'][3])
		deal.append(wa)
		deal.append(gestalt.calculateGestaltPower())
		deal.append(gestalt.getGestaltSnaphot())
		print(gestalt.snaphotsCount, deal, '\t\t\t')
		deals.append(deal)
	if savecounter == 10:
		savecounter = 0
		frame.saveSessionData()
		print('Длительность', int(time.time() - start_time), frame.statistics, end='\r')
		pass
end_time = time.time()

print('\nВремя выполнения '+ str(end_time - start_time))
