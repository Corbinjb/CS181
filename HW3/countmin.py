import mmh3
import math

def compute_width_depth(delta, epsilon):
	return int(2/epsilon), int(math.log(1 / delta))

class CountMinSketch(object):
	def __init__(self, width, depth, CMS):
		super(CountMinSketch, self).__init__()
		self.depth = depth
        self.width = width
		self.CMS = [[0] * width] * depth

	def increment(self, item):
		for i in range(self.depth):
			ind = mmh3.hash(item, i) % self.width
			self.CMS[i][ind] += 1
		return self

	def estimate(self, item):
		hash = []
		for i in range(self.depth):
			ind = mmh3.hash(item, i) % self.width
			hash.append(self.CMS[i][ind])
		return min(hashs)

	def merge(self, CountMin):
		for i in range(self.depth):
			for j in range(self.width):
				self.CMS[i][j] += CountMin.CMS[i][j]
		return self
