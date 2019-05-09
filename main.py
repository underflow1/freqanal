import time
from sequence_generator import sequenceGenerator
#from config import parser

sequence = sequenceGenerator()
start_time = time.time()
for count in range(2000):
#	print(sequence.next())
	
	sequence.next()
	sequence.printSessionDetails()
	pass
end_time = time.time()
sequence.printSessionDetails()
print('Время выполнения '+ str(end_time - start_time))

pass