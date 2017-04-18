"""
spark-submit --packages graphframes:graphframes:0.2.0-spark2.0-s_2.11 graphx2.py input_small.txt > output
"""

from pyspark import SparkContext
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.functions import udf, col
from pyspark.sql.types import *
import sys
import hashlib
from operator import add
from collections import OrderedDict
import __builtin__


sc = SparkContext()
sqlContext = SQLContext(sc)

def parsevertices(r):
	r = r.split("|")
	r[0] = str(r[0])
	r[1] = str(r[1])
	final_list = [(r[0], 0), (r[1], 0)]
	return final_list

def parseedges(r):
	r = r.split("|")
	r[0] = str(r[0])
	r[1] = str(r[1])
	final_list = [(r[0], r[1], 1)]
	return final_list




edge_list_file =  sqlContext.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
vertices = edge_list_file.flatMap(lambda r: parsevertices(r)) # returns an RDD list of (id, vertex, type) by spitting the input line
vertices_df = sqlContext.createDataFrame(vertices,["id","value"]).dropDuplicates()

edges = edge_list_file.flatMap(lambda r: parseedges(r)) #creates an rdd list of (src,dst, weight)
edges_df = sqlContext.createDataFrame(edges,["src", "dst", "weight"])

edges_df = edges_df.groupBy(["src", "dst"]).sum("weight") #aggregating the weights
edges_df = edges_df.withColumnRenamed("sum(weight)", "weight")

edges_df.show()
vertices_df.show()
print "no of rows are ", vertices_df.count()



from graphframes import *
g = GraphFrame(vertices_df, edges_df)

k=20
r = g.pageRank(resetProbability=0.15, tol=0.01)
topk = r.vertices.top(k, key=lambda x: x["pagerank"])
print "Top 20"
print topk.rdd.collect()
print "All verticees"
print r.vertices.rdd.collect()
