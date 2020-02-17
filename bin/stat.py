
import sys
import numpy as np

data = np.loadtxt(sys.argv[1])
print("average=" + str(np.average(data)))
print("std=" + str(np.std(data)))
print("median=" + str(np.median(data)))
print("min=" + str(np.min(data)))
print("max=" + str(np.max(data)))
print("95th%=" + str(np.percentile(data, 95)))
print("99th%=" + str(np.percentile(data, 99)))
print("99.9th%=" + str(np.percentile(data, 99.9)))

