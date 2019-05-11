import time
from frame_generator import frameGenerator
from gestalt_generator import gestaltGenerator

frame = frameGenerator()
gestalt = gestaltGenerator()
start_time = time.time()
savecounter = 0
gestaltblind = 0
for count in range(1000000):
	savecounter += 1
	fr = frame.next()
	gestalt.addFrame(fr)
	wa = gestalt.calculateGestaltWeightAverage()
	gestaltblind = gestaltblind + 1
	if wa and wa >= 10 and gestaltblind > 60:
		gestaltblind = 0
		print(gestalt.getGestaltSnaphot(), 'wa is:', wa, 'power is:', gestalt.calculateGestaltPower(), 'snapshots count:', gestalt.snaphotsCount, '\t\t\t\t\t\t\t\t')
	if savecounter == 10:
		savecounter = 0
		frame.saveSessionData()
		print('Длительность', int(time.time() - start_time), frame.statistics, end='\r')
		pass
end_time = time.time()

print('\nВремя выполнения '+ str(end_time - start_time))
print(gestalt.snaphotsCount)