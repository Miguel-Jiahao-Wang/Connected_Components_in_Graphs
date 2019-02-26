#Finding seeds; Broadcasting
import time

def Min_Selection_Step(G): #dictionary format RDD
    v_min = G.map(lambda x: (x[0], min(x[1] | {x[0]})))
    NN_G_u = G.map(lambda x: (x[0], (x[1] | {x[0]})))

    #Broadcasting
    v_min_bc = sc.broadcast(dict(v_min.collect()))
    addEdge = NN_G_u.map(lambda x: (x[0], (x[1], v_min_bc.value[x[0]]))).flatMap(lambda x: [(y, x[1][1]) for y in x[1][0]])
    #Without broadcasting
    #addEdge = NN_G_u.join(v_min).flatMap(lambda x: [(y, x[1][1]) for y in x[1][0]])
    H = addEdge.groupByKey().mapValues(lambda x: set(x))
    return H

def Pruning_Step(H, T):
    #H = H.cache()
    #minimum node of the neighborhood: shared for following parts
    v_min = H.mapValues(lambda x: min(x))
    v_min_bc = sc.broadcast(dict(v_min.collect())) #Broadcasting v_min

    #---------------G construction-------------------
    H_filtered = H.filter(lambda x: len(x[1]) > 1)
    #NN_H_u = H_filtered.map(lambda x: (x[0], x[1] - {min(x[1])} ))
    NN_H_u = H_filtered.mapValues(lambda x: x - {min(x)} )
    #With Broadcasting
    addEdge2=NN_H_u.map(lambda x:(x[0],(x[1],v_min_bc.value[x[0]]))).flatMap(lambda x:[(x[1][1],y) for y in x[1][0]])
    #Without broadcasting
    #addEdge2 = NN_H_u.join(v_min).flatMap(lambda x: [(x[1][1], y) for y in x[1][0]])
    G = addEdge2.flatMap(lambda x: [x, (x[1], x[0])]).groupByKey().mapValues(lambda x: set(x))

    #---------------Tree construction--------------
    deactiveNodes = H.filter(lambda x: x[0] not in x[1]).mapValues(lambda x: None)
    #Without broadcasting
    #addEdge3 = deactiveNodes.join(v_min).map(lambda x: (x[1][1], x[0]))
    #With Broadcasting
    addEdge3 = deactiveNodes.map(lambda x: (x[0], (x[1], v_min_bc.value[x[0]]))).map(lambda x: (x[1][1], x[0]))
    T = T.union(addEdge3)

    return [G, T]

#------------------Finding seeds------------------
def findSeeds(T):
    keys = T
    values = T.map(lambda x:(x[1], x[0]))
    return keys.subtractByKey(values).keys().distinct()


#-------------Seed propagation-----------
def Seed_Propragation(T, seed):
    seed = seed.map(lambda x: (x, x))
    T_seed = sc.parallelize([(-1, (None, -1))])

    while T_seed.values().lookup(None):
        T_seed = seed.rightOuterJoin(T)
        seed = T_seed.map(lambda x: (x[1][1], x[1][0])).union(seed)

    return T_seed

#-----------------------------Main------------------------
def Cracker(G):
    n = 0
    T = sc.parallelize([])

    while G.take(1):
        n += 1
        H = Min_Selection_Step(G)
        G, T = Pruning_Step(H, T)
    Seeds = findSeeds(T)
    #T_persisted = T.persist()
    #Seeds_persisted = Seeds.keys().persist()

    #T_prop = Seed_Propragation(T_persisted, Seeds_persisted)
    #T_prop = Seed_Propragation(T, Seeds)


    return Seeds.count()

#-----------Function call-----------------
init = time.time()

data_raw = sc.textFile("hdfs:///user/hadoop/wc/input/GRAF_120MB_int.txt")
G = data_raw.map(lambda x: x.split(',')).map(lambda x: (int(x[0]), int(x[1]))).flatMap(lambda x: [x, (x[1], x[0])]).groupByKey().mapValues(lambda x: set(x))



#Cracker with findSeeds
T_prop = Cracker(G)
print(T_prop)

end = time.time()
print("Time employed: %f" % (end - init))
