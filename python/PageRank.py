#!/usr/local/spark/bin/spark-submit
from pyspark import SparkConf,SparkContext
import sys
from hdfs.client import Client
#import os
#os.environ['PYSPARK_PYTHON']='/usr/bin/python3'

PRINIT=1
ITERTIME=10
D=0.85

def clcPr(tuple):
    page,(pr,links)=tuple
    for i in links:
        yield (i,pr/len(links))


if __name__=='__main__':
    #main函数
    if len(sys.argv)<3:
        sys.stderr('Usage: PageRank.py <in> <out>')
        sys.exit(2)
    #初始化Spark
    conf = SparkConf().setAppName('PageRank').setMaster('spark://master:7077')
    sc = SparkContext(conf=conf)
    inPath=sys.argv[1]
    outPath=sys.argv[2]
    client=Client('http://10.102.0.197:50070')
    client.delete(sys.argv[2],recursive=True)
    data=sc.textFile(inPath)
    data=data.map(lambda line:line.split('\t'))
    links=data.mapValues(lambda x:x.split(','))
    #初始化pr值
    pr=data.map(lambda line:(line[0],PRINIT))
    for i in range(10):
        pr=pr.join(links).flatMap(clcPr)
        pr=pr.reduceByKey(lambda x,y:x+y)
        pr=pr.mapValues(lambda x:1-D+D*x)
    pr=pr.sortBy(lambda x: x[1], ascending=False,numPartitions=1)
    out=pr.map(lambda x:'({},{:.10f})'.format(x[0],x[1]))
    out.saveAsTextFile(outPath)