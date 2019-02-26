from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import *

sc = SparkContext()
sqlContext = SQLContext(sc)

#-------------Min_Selection_Step--------------------------
def Min_Selection_Step(df_G):
    #Including node inside NN_H and finding the min_id inside the NN_H
    NN_min = df_G.select(array_union(col("NN"), array(col("Node"))).alias("NN")\
                        ).withColumn("v_min", array_min( col("NN") ))
    #All edges that needs to be added to G_{t+1}
    addEdge = NN_min.select(explode(NN_min.NN).alias("Node"), "v_min")
    #Grouping all node_id
    dfH = addEdge.groupBy("Node").agg(collect_set("v_min").alias("NN"))
    return dfH

#-------------Pruning_Step--------------------------
#----------Useful functions
#Function to check if int is inside an array
#Used in deactiveNodes
def isInside(node, NN):
    return (node in NN)

#Turning the function into Pyspark User Defined Function
isInside_udf = udf(isInside, BooleanType())

#Function to join two list
#Used to find seeds
def joinList(H_NN, G_NN):
    #I need to think if H_NN can be empty
    if G_NN != None:
        return list(set(G_NN + H_NN))
    else:
        return H_NN

#Turning the function into Pyspark User Defined Function
joinList_udf = udf(joinList, ArrayType(IntegerType()))

#------------Main Pruning part
def Pruning_Step(dfH, T, Seeds):

    #---------------G construction-------------------
    H_filtered = dfH.filter(size(col("NN")) > 1) #NN with more than 1 element
    NN_H_min = H_filtered.select("NN", array_min(col("NN")).alias("v_min")) #NN and min_id
    NN_H_u = NN_H_min.select(array_except(col("NN"), array(col("v_min"))).alias("NN_u"), "v_min") #NN-min_id, min_id
    addEdge = NN_H_u.select(explode(NN_H_u.NN_u).alias("Node"), "v_min") #New edges
    addEdge_inv = addEdge.select(col("v_min").alias("Node"), col("Node").alias("v_min")) #Inverse direction of edges
    allEdges = addEdge.union(addEdge_inv) #All edges that need to be in the new graph

    G = allEdges.groupBy("Node").agg(collect_set("v_min").alias("NN"))

    #---------------Tree construction--------------
    #The deactivated Nodes do not appear in G_{t+1}
    deactiveNodes = dfH.select("Node", array_min(col("NN")).alias("v_min"), \
                               isInside_udf(col("Node"), col("NN")).alias("Active")).filter(col("Active") == False)
    #Tree in (Parent, Child) format; the one used in RDD
    addEdge = deactiveNodes.select(col("v_min").alias("Parent"), col("Node").alias("Child"))
    T = T.union(addEdge)

    #--------------Find Seed-----------------
    #Without broadcasting
    NN_H_G = dfH.join(G, dfH.Node == G.Node, how="left").select(dfH.Node, dfH.NN.alias("H_NN"), G.NN.alias("G_NN"))
    #With broadcasting
    #NN_H_G = dfH.join(broadcast(G), dfH.Node == G.Node, how="left").select(dfH.Node, dfH.NN.alias("H_NN"), G.NN.alias("G_NN"))
    joined_NN = NN_H_G.select("Node", joinList_udf(col("H_NN"), col("G_NN")).alias("NN"))
    seed = joined_NN.filter( (size(col("NN"))<= 1) & ( isInside_udf(col("Node"), col("NN")) ) )
    Seeds = Seeds.union(seed.select("Node"))

    return G, T, Seeds

#---------------Cracker main functions------------------
def cracker(G):
    n = 0
    empty = sc.parallelize([])
    T = sqlContext.createDataFrame(empty, schemaT)
    Seeds = sqlContext.createDataFrame(empty, schemaS)

    while G.count() != 0:
        n += 1
        H = Min_Selection_Step(G)
        G, T, Seeds = Pruning_Step(H, T, Seeds)

    return T, Seeds

#-------------Seed Propagation: broadcasting----------
def seedPropagation(Tree, Seeds):
    n = 0
    T_seed = Tree.join(broadcast(Seeds), Tree.Parent == Seeds.Node, how= "left").select(col("Child").alias("Node"), col("Node").alias("Seed"))
    Seeds = Seeds.select(Seeds.Node, Seeds.Node)
    while T_seed.filter(T_seed.Seed.isNull()).count() != 0:
        n += 1
        Seeds = T_seed.filter(T_seed.Seed.isNotNull()).union(Seeds)
        T_seed = Tree.join(broadcast(Seeds), Tree.Parent == Seeds.Node, how= "left").select(col("Child").alias("Node"), col("Seed"))

    return T_seed

#------------Running the code-----------------------

T, S = cracker(dfG)

T_prop = seedPropagation(T, S)
T_prop.collect() #Maybe use .show() for better format?
