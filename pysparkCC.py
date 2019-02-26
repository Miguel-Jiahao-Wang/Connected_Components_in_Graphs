#btc_raw = sc.textFile("graph_datasets/btc_02.txt")
#export PYSPARK_SUBMIT_ARGS="--master local[2] pyspark-shell"

#from pyspark import SparkContext, SparkConf
#conf = SparkConf().setAppName("pyspark")
#sc = SparkContext(conf=conf)


class Graph:
    #sc = SparkContext.getOrCreate()

    def __init__(self):
        self.graph = None
        #sc.parallelize()
        self.vertices = None

    def addGraph(self, RDD , bidirection = True):

        if bidirection:
            self.graph = RDD.map(lambda x: x.split(',')).map(lambda x: (int(x[0]),int(x[1]))).flatMap(lambda x: [x, (x[1], x[0])]).groupByKey().map(lambda x : (x[0], set(x[1])))
        else:
            self.graph = RDD.map(lambda x: x.split(',')).map(lambda x: (int(x[0]),int(x[1]))).groupByKey().map(lambda x : (x[0], set(x[1])))

    def addEdge(self, u, RDD, RDDfirst = True, bidirection = True): #RDD is neighbors of u
        if RDDfirst: #actually if bidirection, no difference of RDDfirst
            if not bidirection:
                self.graph = RDD.map(lambda x: (x, u)).groupByKey().map(lambda x : (x[0], set(x[1])))
            else:
                self.graph = RDD.map(lambda x: (x, u)).flatMap(lambda x: [x, (x[1], x[0])]).groupByKey().map(lambda x : (x[0], set(x[1])))
        else:
            if not bidirection:
                self.graph = RDD.map(lambda x: (u, x)).groupByKey().map(lambda x : (x[0], set(x[1])))
            else:
                self.graph = RDD.map(lambda x: (u, x)).flatMap(lambda x: [x, (x[1], x[0])]).groupByKey().map(lambda x : (x[0], set(x[1])))



    def get_vertices(self):
        self.vertices =  self.graph.keys()
        return self.vertices




    def Min_Selection_Step(self, H): #H is an empty Graph with defined graph

        vertices = self.get_vertices().collect() #list
        stack = vertices

        visited ={x:False for x in vertices}

        while stack:
            u = stack.pop()
            if visited[u]==False:
                H_u = Graph()

                NN_G_vertice = self.graph.lookup(u)[0] #set
                v_min = min(NN_G_vertice | {u})
            ####################################
                NN = sc.parallelize(list(NN_G_vertice | {u}))

                H_u.addEdge(u = v_min, RDD = NN, RDDfirst = True, bidirection = False)

                H.graph = H.graph.cogroup(H_u.graph).flatMap(lambda x: (x[0], x[1]))

        return(H)

    def Pruning_Step(self):
        self.get_vertices()
        vertices = self.vertices.collect() #list
        stack = vertices

        visited ={x:False for x in vertices}

        G_next = Graph()

        while stack:
            u = stack.pop()
            if visited[u]==False:

                NN_H_vertice = self.graph.lookup(u)[0]
                v_min = min(NN_H_vertice)
                if len(NN_H_vertice) > 1:
                    NN = sc.parallelize(list(NN_H_vertice - {v_min}))

                G_next.addEdge(u = v_min, RDD = NN, RDDfirst = False, bidirection = True)

        return(G_next)
