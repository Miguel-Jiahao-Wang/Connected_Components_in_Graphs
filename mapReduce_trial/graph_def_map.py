
import sys


for line in sys.stdin:
    edge = line.strip().split()
    print(edge[0] + "\t" + edge[1])

#Map to define the graph from a txt file
