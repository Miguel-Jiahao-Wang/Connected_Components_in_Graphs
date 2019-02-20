from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("pyspark")
sc = SparkContext(conf=conf)

def Min_Selection_Step(G): #dictionary format RDD
    v_min = G.map(lambda x: (x[0], min(x[1] | {x[0]})))
    NN_G_u = G.map(lambda x: (x[0], (x[1] | {x[0]})))
    addEdge1 = v_min.cogroup(NN_G_u).map(lambda x :(x[0], ( list(x[1][0]), list(x[1][1])))) #if it is possible to reduce to one MapReduce job
    H = addEdge1.flatMap(lambda x: [(x[1][0][0], y) for y in x[1][1][0]]).map(lambda x: (x[1], x[0])).groupByKey().map(lambda x: (x[0], set(x[1])))#.filter(lambda x: len(x[1]) > 1)
    return H

def Pruning_Step(H, T):
    H_filtered = H.filter(lambda x: len(x[1]) > 1)
    v_min_filtered = H_filtered.map(lambda x: (x[0], min(x[1])))
    NN_H_u = H_filtered.map(lambda x: (x[0], x[1] - {min(x[1])} ))
    addEdge2 = v_min_filtered.cogroup(NN_H_u).map(lambda x :(x[0], ( list(x[1][0]), list(x[1][1]))))
    G = addEdge2.flatMap(lambda x: [(x[1][0][0], y) for y in x[1][1][0]]).flatMap(lambda x: [x, (x[1], x[0])]).groupByKey().map(lambda x: (x[0], set(x[1])))
    
    
    #deactiviation
    deactiveNodes = H.filter(lambda x: x[0] not in x[1]).map(lambda x: (x[0], None))
    v_min = H.map(lambda x: (x[0], min(x[1])))
    addEdge3 = deactiveNodes.join(v_min).map(lambda x: (x[1][1], x[0]))
    T = T.union(addEdge3)

    
    return [G, T]

def findSeeds(T):
    T_rev = T.map(lambda x:(x[1], x[0]))
    A = T.keys().distinct().map(lambda x:(x,1))
    B = T_rev.keys().distinct().map(lambda x:(x,1))
    return A.leftOuterJoin(B).filter(lambda x: not x[1][1]).map(lambda x:x[0])


def Cracker_pyspark(G):
    n = 0
    T = sc.parallelize([])
    while G.take(1):
        n += 1
        print(n)
        H = Min_Selection_Step(G)
        G, T = Pruning_Step(H, T)
    
    return T

def Seed_Propragation(T, seed): 
    seed = seed.map(lambda x: (x, x))  
    T_seed = sc.parallelize([(-1, (None, -1))])                       
    
    while T_seed.map(lambda x: (x[1])).lookup(None):
        T_seed = seed.rightOuterJoin(T)
        seed = T_seed.map(lambda x: (x[1][1], x[1][0])).union(seed)
        
    return T_seed