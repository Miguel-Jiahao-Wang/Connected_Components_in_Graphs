from collections import defaultdict

def addEdge(graph,u,v, bidirection = True):
    graph[u] |= {v}
    #if bidirection and v != None:
    if bidirection:
        graph[v] |= {u} #bidirectional



def get_nodes(graph):
        return(set(graph.keys()))

def fillOrder(graph,v,visited, stack):
    # Mark the current node as visited
    visited[v]= True
    #Recur for all the vertices adjacent to this vertex
    for i in graph[v]:
        #if i != None and visited[i]==False:
        if visited[i]==False:
            fillOrder(graph, i, visited, stack)
    stack = stack.append(v)

def addBranch(tree, u, v):
    tree[u] |= {v}

def setSeed(seed, u, v):
    seed |= {u}
    seed -= {v}

def Min_Selection_Step(G):
    stack = []
    visited = {x:False for x in get_nodes(G)}
    for i in get_nodes(G):
        if visited[i]==False:
            fillOrder(G, i, visited, stack)

    visited = {x:False for x in get_nodes(G)}

    H_next = defaultdict(set) #Creating graph

    while stack:
        u = stack.pop()
        #if self.graph[u] == {None}:
        #    H.addEdge(u, u, bidirection = False)
        if visited[u]==False:
            NN_G_vertice = G[u]
            v_min = min(NN_G_vertice | {u})

            for v in NN_G_vertice | {u}:
                addEdge(H_next, v, v_min, bidirection = False)

    return H_next

def Pruning_Step(H, tree, seed):
    #Remove nodes that are guaranteed to not be seeds and add them to the propagation tree
    stack = []
    #To fill the stack
    visited ={x:False for x in get_nodes(H)}
    for i in get_nodes(H):
        if visited[i]==False:
            fillOrder(H, i, visited, stack)

    #Setting all nodes as not visited
    visited ={x:False for x in get_nodes(H)}
    G_next = defaultdict(set)

    while stack:
        u = stack.pop()
        if visited[u] == False:
            NN_H_vertice = H[u]
            v_min = min(NN_H_vertice)

            #
            if len(NN_H_vertice) > 1: #Linking two possible seeds of u and removing u
            #E.g: 8 linked to 3 and 5. Then, 3 and 5 are linked and 8 is not considered anymore
                for v in (NN_H_vertice - {v_min}):
                    addEdge(G_next, v_min, v, bidirection = True)

            if u not in NN_H_vertice:
                addBranch(tree, v_min, u)
                setSeed(seed, v_min, u)

    return(G_next)

def cracker(graph):
    Tree = defaultdict(set)
    Seed = set()

    G_next = graph.copy()
    while(len(G_next)) > 0:
        H_next = Min_Selection_Step(G_next)
        G_next = Pruning_Step(H_next, Tree, Seed)
    return Tree, Seed
