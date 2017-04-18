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

def stddev(line):
     list =line[1]
     global theta
     mean =sum(list)/len(list)
     ss = sum((x-mean)**2 for x in list)
     var= ss/len(list)
     dev = ss**0.5
     i=len(list)
     val = list[i-1]
     val1 = (1-theta)*val+theta*dev
     finallist = [(line[0],val1)]
     return finallist


def  calculate_score(i):
	if i==1
		return
	global score
	global ip
	score = ip.flatMap(lambda line: stddev(line))

def calculate_gt(i):
	global score
	global k
	global beta
	global rrecord	
	global vertices_count
	l=k*beta
	vertices_t = score.top(l, key=lambda x: x[1])
	verticesleft = score.subtractByKey(vertices_t)
	"""rrecord = rrecord.union(vertices_t).dropDuplicates()
	if rrecord.count()==vertices_count
		rrecord = rrecord.subtractByKey(rrecord)
	verticesleft = score.subtractByKey(vertices_t)
	verticesleft=verticesleft.subtractByKey(rrecord)"""
	fraction = (1-beta)*k/verticesleft.count()

	l= verticesleft.sample(false,fraction,verticesleft.count())
	vertices_t = vertices_t.union(l)
	v_df = sqlContext.createDataFrame(vertices_t,["id","pagerank"]).dropDuplicates()
    global gt= GraphFrame(v_df, edges_df)


def compute_pr():
	r=gt.pageRank(resetProbability=0.15, tol=0.01)
	r1= r.vertices.select("id","pagerank")
	global ip
	f= r1.union(ip)
	ip = f.redeuceByKey(lambda a,b : a+b)




edge_list_file =  sqlContext.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
vertices = edge_list_file.flatMap(lambda r: parsevertices(r)) # returns an RDD list of (id, vertex, type) by spitting the input line
vertices_df = sqlContext.createDataFrame(vertices,["id","value"]).dropDuplicates()

edges = edge_list_file.flatMap(lambda r: parseedges(r)) #creates an rdd list of (src,dst, weight)
edges_df = sqlContext.createDataFrame(edges,["src", "dst", "weight"])

edges_df = edges_df.groupBy(["src", "dst"]).sum("weight") #aggregating the weights
edges_df = edges_df.withColumnRenamed("sum(weight)", "weight")

edges_df.show()
vertices_df.show()
vertices_count = vertices_df.count()
print "no of rows are ", vertices_df.count()



from graphframes import *
g = GraphFrame(vertices_df, edges_df)

iter_count = 10
theta=0.5
k=2
beta =0.5
r = g.pageRank(resetProbability=0.15, tol=0.01)
score = r.vertices.select("id","pagerank")
ip=r.vertices.select("id","pagerank")
"""score = score.rdd.map(lambda x: x).collect()
score = sc.parallelize(score)
ip=r.vertices.select("id","pagerank") 
ip= ip.rdd.map(lambda x: x).collect() 
ip =sc.parallelize(ip);
"""

 
	
while iter_count != 0:
	calculate_score(iter_count)
	calculate_gt()
	#infer_network()
	compute_pr()
	iter_count = iter_count - 1

print "Top K:"
print score.rdd.top(k,key=lambda x:x[1])
print "All:"
print score.collect()


	
