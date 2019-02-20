#This class represents a directed graph using adjacency list representation 

from collections import defaultdict 

class Graph: 
    def __init__(self): 
        self.graph = defaultdict(set) # default dictionary to store graph 
        #self.T = Tree() #propagation tree
        
    def get_vertices(self):
        return(set(self.graph.keys()))
   
    # function to add an edge to graph 
    def addEdge(self,u,v, bidirection = True): 
        
        self.graph[u] |= {v}
        if bidirection:
            self.graph[v] |= {u} #bidirectional
   
    def fillOrder(self,v,visited, stack): 
        # Mark the current node as visited  
        visited[v]= True
        #Recur for all the vertices adjacent to this vertex 
        for i in self.graph[v]: 
            if visited[i]==False: 
                self.fillOrder(i, visited, stack) 
        stack = stack.append(v) 
      
   
    def Min_Selection_Step(self):
                
        
        stack = [] 
        visited ={x:False for x in self.get_vertices()}
        for i in self.get_vertices(): 
            if visited[i]==False: 
                self.fillOrder(i, visited, stack) 
          
        visited ={x:False for x in self.get_vertices()}
      
        H = Graph()
                
        while stack: 
            u = stack.pop() 
            if visited[u]==False: 
                NN_G_vertice = self.graph[u]
                v_min = min(NN_G_vertice | {u})
            
                for v in NN_G_vertice | {u}:

                    H.addEdge(v, v_min, bidirection = False)
                                         
        return(H)
    
    def Pruning_Step(self, tree): 
        stack = [] 
        visited ={x:False for x in self.get_vertices()}

        for i in self.get_vertices(): 
            if visited[i]==False: 
                self.fillOrder(i, visited, stack) 

          
        visited ={x:False for x in self.get_vertices()}
        G_next = Graph() 
        
        while stack: 
            u = stack.pop() 
            if visited[u]==False:
                
                NN_H_vertice = self.graph[u]
                v_min = min(NN_H_vertice)
                if len(NN_H_vertice) > 1:
                    
                    for v in (NN_H_vertice - {v_min}):
                        G_next.addEdge(v_min, v, bidirection = True)
                
                if u not in NN_H_vertice:
                    tree.addEdge(v_min, u)
                    tree.setSeed(v_min, u)
        
        return(G_next)

    
class Tree:
    
    def __init__(self): 
        self.tree = defaultdict(set) 
        self.seed = set()
    def addEdge(self,u,v):    
        self.tree[u] |= {v}   
    def setSeed(self,u,v):
        self.seed |= {u}
        self.seed -= {v} 

def Cracker_python(G):
    tree = Tree()
    t = 0
    #print("G1:", G.graph)
    while len(G.graph) > 0:
        t += 1
        H = G.Min_Selection_Step()
        #print("H{t}:".format(t = t), H.graph)
        G = H.Pruning_Step(tree)
        #print("Tree seed:", tree.seed)
        #print("G{t}:".format(t = t+1), G.graph)
        
    #print(tree.tree)
    return(tree)
