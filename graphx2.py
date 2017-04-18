"""
spark-submit --packages graphframes:graphframes:0.2.0-spark2.0-s_2.11 graphx2.py edgefile > output

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


NGRAM = 3
THRESHOLD_COUNT = 6
WORD_TYPES = ["OOV","IV"]
sc = SparkContext()
sqlContext = SQLContext(sc)


def parsevertices(r):
	r = r.split("|")
	r[0] = str(r[0])
	r[1] = str(r[1])
	final_list = [(r[0], r[0]), (r[1], r[1])]
	return final_list

def parsevertices_sd(r):
	r = r.split("|")
	r[0] = str(r[0])
	r[1] = str(r[1])
	final_list = [(r[0], 0), (r[1],0)]
	return final_list

def parseedges(r):
	r = r.split("|")
	r[0] = str(r[0])
	r[1] = str(r[1])
	final_list = [(r[0], r[1], 1)]
	return final_list


def create_edge_list(line):
	edgelist = []
	line = line.strip()
	# line = line.decode("utf-8")
	line = line.encode('ascii','ignore')
	line = nlp.clean(line)
	ngramlist = nlp.extract_ngrams(line,NGRAM)
	for ngram in ngramlist:
		ngram = list(ngram)
		index = NGRAM/2
		word = ngram[index]
		ngram[index] = "*"
		edgelist.append(word + "|" + " ".join(ngram))
	return edgelist


def kinflunencial(g):
	r = g.pageRank(resetProbability=0.15, tol=0.01)
	top(2,key = lambda x: x[1])
	print s.collect()

edge_list_file =  sqlContext.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
vertices = edge_list_file.flatMap(lambda r: parsevertices(r)) # returns an RDD list of (id, vertex, type) by spitting the input line
vertices_2 = edge_list_file.flatMap(lambda r: parsevertices_sd(r))
vertices_df = sqlContext.createDataFrame(vertices,["id","value"]).dropDuplicates()
vertices_deviation = sqlContext.createDataFrame(vertices,["id","value"]).dropDuplicates()

edges = edge_list_file.flatMap(lambda r: parseedges(r)) #creates an rdd list of (src,dst, weight)
edges_df = sqlContext.createDataFrame(edges,["src", "dst", "weight"])

edges_df = edges_df.groupBy(["src", "dst"]).sum("weight") #aggregating the weights
edges_df = edges_df.withColumnRenamed("sum(weight)", "weight")

edges_df.show()
vertices_df.show()
print "no of rows are ", vertices_df.count()

#Map Example
#=============
#d = c.rdd.map(lambda x:(x.value, (x.pagerank + getlcscost(n,x.value)))).top(2,key = lambda x: x[1])


from graphframes import *
g = GraphFrame(vertices_df, edges_df)
g1 = g.inDegrees
# g1 = g1.filter(g1.inDegree == 2)
g1.groupBy("inDegree").count().show()
print g1.count()

iter_count = 2
standard_deviation = 0
while iter_count != 0:
	
	#Use r and store the values here
	iter_count = iter_count - 1
