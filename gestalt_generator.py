from config_functions import config

class gestaltGenerator:
	def __init__(self):
		self.gestalt = []
		self.gestaltReady = False
		self.snaphotsCount = 0
		pass
	
	def addFrame(self, frame):
		self.gestalt.append(frame)
		if len(self.gestalt) < config.getint('constraints', 'gestaltsize'):
			self.gestaltReady = False
		else: 
			if len(self.gestalt) > config.getint('constraints', 'gestaltsize'):
				self.gestalt.pop(0)
			self.gestaltReady = True


	def calculateGestaltWeightAverage(self):
		weight = 0
		sum_vector = 0
		sum_weight = 0
		if self.gestaltReady:
			for frame in self.gestalt:
				weight += 1
				sum_vector = sum_vector + abs(frame['vector']) * weight 
				sum_weight = sum_weight + weight
			WeightAverage = int(sum_vector / sum_weight)
			return WeightAverage
	
	def calculateGestaltPower(self):
		power = 0
		for frame in self.gestalt:
			power = power + frame['vector']
		return power

	def getGestaltSnaphot(self):
		snapshot = []
		self.snaphotsCount += 1
		for frame in self.gestalt:
			snapshot.append(frame['vector'])
		return snapshot
		
