import sys
from collections import defaultdict
from cracker2spark_v1 import Graph, Tree

G = Graph()
node = None

for line in sys.stdin:
    edge = line.strip().split("\t")

    if node == edge[0]:
        #graph |= {int(v)}
        G.addEdge(int(edge[0]), int(edge[1]))

    else:
        if node:
            print("{}\t{}".format( node, ";".join(map(str, G[node])) ))
        G.addEdge(int(edge[0]), int(edge[1]))
        node = edge[0]

if node == edge[0]:
    print("{}\t{}".format( node, ";".join(map(str, G[node])) ))
