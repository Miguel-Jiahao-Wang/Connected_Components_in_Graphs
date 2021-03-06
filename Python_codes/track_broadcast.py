#Tracking seeds; Broadcasting
import time

def Min_Selection_Step(G): #dictionary format RDD
    v_min = G.map(lambda x: (x[0], min(x[1] | {x[0]})))
    NN_G_u = G.map(lambda x: (x[0], x[1] | {x[0]}))
    #Broadcasting
    v_min_bc = sc.broadcast(dict(v_min.collect()))
    addEdge = NN_G_u.map(lambda x: (x[0], (x[1], v_min_bc.value[x[0]])) )
    addEdge1 = addEdge.flatMap(lambda x: [(y, x[1][1]) for y in x[1][0]])
    #Without broadcasting
    #addEdge1 = NN_G_u.join(v_min).flatMap(lambda x: [(y, x[1][1]) for y in x[1][0]])

    H = addEdge1.groupByKey().mapValues(lambda x: set(x))
    return H

def Pruning_Step(H, T, Seeds):
    #H = H.cache()
    #minimum node of the neighborhood: shared for following parts
    v_min = H.mapValues(lambda x: min(x))
    v_min_bc = sc.broadcast(dict(v_min.collect())) #Broadcasting v_min

    #---------------G construction-------------------
    H_filtered = H.filter(lambda x: len(x[1]) > 1)
    NN_H_u = H_filtered.mapValues(lambda x: x - {min(x)} )
    #With Broadcasting
    addEdge2=NN_H_u.map(lambda x:(x[0],(x[1],v_min_bc.value[x[0]]))).flatMap(lambda x:[(x[1][1],y) for y in x[1][0]])
    #Without broadcasting
    #addEdge2 = NN_H_u.join(v_min).flatMap(lambda x: [(x[1][1], y) for y in x[1][0]])
    G = addEdge2.flatMap(lambda x: [x, (x[1], x[0])]).groupByKey().mapValues(lambda x: set(x))

    #---------------Tree construction--------------
    #The deactivated Nodes do not appear in G_{t+1}
    deactiveNodes = H.filter(lambda x: x[0] not in x[1]).mapValues(lambda x: False)
    #Without broadcasting
    #addEdge3 = deactiveNodes.join(v_min).map(lambda x: (x[1][1], x[0]))
    #With Broadcasting
    addEdge3 = deactiveNodes.map(lambda x: (x[0], (x[1], v_min_bc.value[x[0]]))).map(lambda x: (x[1][1], x[0]))
    T = T.union(addEdge3)

    #--------------Find Seed-----------------
    #Elements in H with neighborhood from G_{t+1}
    NN_G_H = H.cogroup(G).mapValues(lambda x: (list(x[0]), list(x[1])) ).mapValues(lambda x: set_join(x) )

    #Not sure is necessary to use True/False
    #deactivated = NN_G_H.cogroup(deactiveNodes).map(lambda x: (x[0], (list(x[1][0]), list(x[1][1])) ))
    #seed = deactivated.filter(lambda x: (len(x[1][0]) <= 1) & (x[0] in x[1][0]) & x[1][1])

    seed = NN_G_H.filter(lambda x: (len(x[1]) <= 1) & (x[0] in x[1]))
    Seeds = Seeds.union(seed)

    return [G, T, Seeds]


def set_join(value):
    if not value[1]:
        return value[0][0]
    else:
        return value[0][0] | value[1][0]

#-------------Seed propagation-----------

def Seed_Propragation_lite(T, Seeds):
    
    seed = Seeds.map(lambda x:(x, None))
    needProp = T.subtractByKey(seed)    
    noProp = T.join(seed).map(lambda x: (x[1][0], x[0]))
    T_prop = noProp
    while needProp.take(1):
        noProp = needProp.leftOuterJoin(noProp).values().filter(lambda x: x[1] is not None)
        needProp = needProp.map(lambda x: (x[1], x[0])).subtractByKey(noProp).map(lambda x: (x[1], x[0]))
        T_prop = T_prop.union(noProp)
    
    return T_prop

#-----------------------------Main------------------------
def Cracker(G):
    n = 0
    T = sc.parallelize([])
    Seeds = sc.parallelize([])

    while G.take(1):
        n += 1
        H = Min_Selection_Step(G)
        G, T, Seeds = Pruning_Step(H, T, Seeds)

    #T_persisted = T.persist()
    #Seeds_persisted = Seeds.keys().persist()

    #T_prop = Seed_Propragation(T_persisted, Seeds_persisted)
    
    
    #T_prop = Seed_Propragation(T, Seeds.keys())

    return [T, Seeds.keys()]

#-----------Function call-----------------
init = time.time()
data_raw = sc.textFile("hdfs:///user/hadoop/wc/input/GRAF_2MB_int.txt")
G = data_raw.map(lambda x: x.split(',')).map(lambda x: (int(x[0]), int(x[1]))).flatMap(lambda x: [x, (x[1], x[0])]).groupByKey().mapValues(lambda x: set(x))


#Cracker with findSeeds
T, Seeds = Cracker(G)
T.persist()
Seeds.persist()
seed_time = time.time()
 #time of seed prop
prop_time = time.time()
T_prop = Seed_Propragation_lite(T, Seeds)
T_prop.collect()

end = time.time()
print("Time SeedProp: %f" % (end - prop_time))
print("Time employed: %f" % (seed_time - init))
