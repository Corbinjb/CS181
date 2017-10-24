"""
Count frequent tweet hashtags using Spark Streaming and Count-Min Sketch.

"""


import sys
import time

import pyspark
import pyspark.streaming

import countmin as cm

# parameters

N = 10000000
W = 6
P = 18

DELTA = 1e-3
EPSILON = 1e-3

# common setup

WIDTH, DEPTH = cm.compute_width_depth(delta=DELTA, epsilon=EPSILON)
# SEEDS = cm.make_seeds()


def make_sketch():
    """Create a new empty sketch using global parameters."""
    return cm.CountMinSketch(width=WIDTH, depth=DEPTH)
    # return cm.CountMinSketch(width=WIDTH, depth=DEPTH, seeds=SEEDS)


def do_count(items):
    """Count all items in a partition and return a single sketch."""
    sketch = make_sketch()
    for item in items:
        sketch.increment(item.lower())
    return [sketch]


def add_sketches(s1, s2):
    """Combine two sketches (reduce phase)."""
    accumulator = make_sketch()
    accumulator.merge(s1)
    accumulator.merge(s2)
    return accumulator


def main():
    print "WIDTH = %i, DEPTH = %i" % (WIDTH, DEPTH)
    # print "SEEDS = %s" % (SEEDS,)

    totals = make_sketch()

    def accumulate_rdd(rdd):
        sketches = rdd.collect()
        print "accumulating %d sketches from RDD" % len(sketches)
        for sketch in sketches:
            totals.merge(sketch)

        print "#GBBO", totals.estimate("#GBBO")
        print "#MelhorClipeTVZLuan", totals.estimate("#MelhorClipeTVZLuan")
        print "#HAVANAtheMOVIEoutNOW", totals.estimate("#HAVANAtheMOVIEoutNOW")


    sc = pyspark.SparkContext(master="local[%i]" % W)
    sc.setLogLevel("ERROR")
    ssc = pyspark.streaming.StreamingContext(sc, 10)

    socket_stream = ssc.socketTextStream("127.0.0.1", 5555)

    lines = socket_stream.window(10)

    (lines
         .flatMap(lambda text: text.split(" "))
         .filter(lambda word: word.lower().startswith('#'))
         .map(lambda x : x.encode('utf-8'))
         .mapPartitions(do_count)
         .reduce(add_sketches)
         .foreachRDD(accumulate_rdd)
    )

    ssc.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        ssc.stop()


if __name__ == "__main__":
    main()
