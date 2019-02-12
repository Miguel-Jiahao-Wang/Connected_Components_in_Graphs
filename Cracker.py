#The Cracker algorithm

def cracker(G):
    #G = (V,E): undirected graph--> [(V, E=(V1,V2)), ...]
    #Returns:

    #u.Active = True
    T = [V, 0] # <--- It seems like MapReduce Syntax; maybe all this code needs to be transformed into Spark
    t = 1
    G_iter = G[:]  #Graph after each iteration

    while len(G_iter) != 0:
        H_iter = Min_Selection_Step(u) #for all u belonging to G_iter
        G_iter = Pruning_Step(u,T) #for all u belonging to H_iter
        t += 1

    G_result = Seed_Propagation(T)

    return G_result



#Min Selection Step
def Min_Selection_Step(u): #I feel like u should be full graph
    #u: (node, edge) from G_iter
    #Returns: DIRECTED graph with edges pointing to local minimum of each node

    min_graph = [] #It will contain all new edges connecting to local minimum
    for node_edge in u:

        NN = neighbors(node_edge[0], u) #Node id of all connected neighbors
        NN = NN.append(node_edge[0]) #Including itself as NN
        id_min = min(set(NN)) #Find neighbor of itself with min id;
        #Set may be needed in case there are nodes pointing to themselves: if not change "neighbors" code

        for node in NN:
            min_graph.append( (node, (node,id_min)) ) #id_min must be second! DIRECTED!

    return min_graph #H_iter

#Nearest Neighbor function used in Min_Selection_Step
#NN defined as all those with an inmediat edge
def neighbors(node, graph):
    #node: node id
    return [edge_end(node, node_edge[1]) \
            for node_edge in graph if node in node_edge[1]]

def edge_end(node, edge):
    #Finds the end of the edge; Used in neighbors
    if node != edge[0]:      return edge[0]
    else:                    return edge[1]
