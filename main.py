import time
from sequence_generator import sequenceGenerator

sequence = sequenceGenerator()
start_time = time.time()
savecounter = 0
for count in range(100000):
	savecounter += 1
	sequence.next()
	if savecounter == 10:
		savecounter = 0
		sequence.saveSessionData()
		print('Длительность', int(time.time() - start_time), sequence.statistics, end='\r')
end_time = time.time()

print('\nВремя выполнения '+ str(end_time - start_time))
print(sequence.statistics, end='\r')