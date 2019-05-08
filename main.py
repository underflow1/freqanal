import time
from sequence_generator import sequenceGenerator
from config import parser

sequence = sequenceGenerator(10, parser.getint('constraints', 'limitsize'))
start_time = time.time()
for count in range(20000):
#	print(sequence.next())
#	print(sequence.statistics)
#	pass
	sequence.next()
end_time = time.time()
print(sequence.statistics)
print('Время выполнения '+ str(end_time - start_time))

pass