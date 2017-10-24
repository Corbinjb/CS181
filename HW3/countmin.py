from bitarray import bitarray
import math
# 3rd party
import mmh3


def compute_width_depth(delta, epsilon):
	return int(2/epsilon), int(math.log(1 / delta))

class CountMinSketch(object):
	def __init__(self, width, depth):
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
		return min(hash)

	def merge(self, CountMin):
		sketch3 = CountMinSketch(self.width, self.depth)
		for i in range(self.depth):
			for j in range(self.width):
				sketch3.CMS[i][j] = self.CMS[i][j] + CountMin.CMS[i][j]
		return sketch3


width, depth = compute_width_depth(0.01,0.01)
sketch = CountMinSketch(width, depth)
sketch.increment("apple")
sketch.increment("apple")
sketch.increment("pear")
sketch.increment("apple")
sketch.increment("pear")
sketch.increment("apple")
sketch.increment("pear")
sketch.increment("apple")


sketch2 = CountMinSketch(width, depth)
sketch2.increment("apple")
sketch2.increment("apple")
sketch2.increment("pear")
sketch2.increment("apple")
sketch2.increment("pear")
sketch2.increment("apple")
sketch2.increment("pear")
sketch2.increment("apple")

sketch3 = sketch.merge(sketch2)

print "Number of apples:", sketch3.estimate("apple")
print "Number of pear:", sketch3.estimate("pear")
print "Number of ornage:", sketch3.estimate("orange")
