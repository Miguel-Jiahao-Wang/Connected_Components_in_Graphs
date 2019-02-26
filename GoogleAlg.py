def create_edges(line):
    a = [int(x) for x in line.split(",")]
    edges_list=[]
    edges_list.append((a[0],a[1]))
    edges_list.append((a[1],a[0]))
    return edges_list

# adj_list.txt is a txt file containing adjacency list of the graph.
adjacency_list = sc.textFile("graph3.txt")

edges_rdd = adjacency_list.flatMap(lambda line : create_edges(line)).distinct()

def largeStarInit(record):
    a, b = record
    yield (a,b)
    yield (b,a)

def largeStar(record):
    a, b = record
    t_list = list(b)
    t_list.append(a)
    list_min = min(t_list)
    for x in b:
        if a < x:
            yield (x,list_min)

def smallStarInit(record):
    a, b = record
    if b<=a:
        yield (a,b)
    else:
        yield (b,a)

def smallStar(record):
    a, b = record
    t_list = list(b)
    t_list.append(a)
    list_min = min(t_list)
    for x in t_list:
        if x!=list_min:
            yield (x,list_min)

#Handle case for single nodes
def single_vertex(line):
    a = [int(x) for x in line.split(",")]
    edges_list=[]
    if len(a)==1:
        edges_list.append((a[0],a[0]))
    return edges_list

iteration_num =0 
while 1==1:
    if iteration_num==0:
        print "iter", iteration_num
        large_star_rdd = edges_rdd.groupByKey().flatMap(lambda x : largeStar(x))
        small_star_rdd = large_star_rdd.flatMap(lambda x : smallStarInit(x)).groupByKey().flatMap(lambda x : smallStar(x)).distinct()
        iteration_num += 1
        
    else:
        print "iter", iteration_num
        large_star_rdd = small_star_rdd.flatMap(lambda x: largeStarInit(x)).groupByKey().flatMap(lambda x : largeStar(x)).distinct()
        small_star_rdd = large_star_rdd.flatMap(lambda x : smallStarInit(x)).groupByKey().flatMap(lambda x : smallStar(x)).distinct()
        iteration_num += 1
    #check Convergence

    changes = (large_star_rdd.subtract(small_star_rdd).union(small_star_rdd.subtract(large_star_rdd))).collect()
    if len(changes) == 0 :
        break

single_vertex_rdd = adjacency_list.flatMap(lambda line : single_vertex(line)).distinct()

answer = single_vertex_rdd.collect() + large_star_rdd.collect()

sol = large_star_rdd.map(lambda x: (x[1] , x[0])).groupByKey().mapValues(list).collect()

print answer[:10]