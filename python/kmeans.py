#!/usr/local/spark/bin/spark-submit
from pyspark import SparkConf, SparkContext
from hdfs.client import Client
import numpy as np
import re

CLASS_NUM = 6
ITER_TIME = 10


def getNearest(x):
    """计算最近聚类中心"""
    minDistance = float('inf')
    minIndex = 0
    for k, v in enumerate(clusters):
        distance = np.sum(np.square(v - x))
        if distance < minDistance:
            minDistance = distance
            minIndex = k
    return (minIndex, x)


if __name__ == '__main__':
    # 初始化Spark
    global clusters
    conf = SparkConf().setAppName('PageRank')
    sc = SparkContext(conf=conf)
    inPath = 'kmeans/train/X_train.txt'
    outPath = 'kmeans/out/'
    client = Client('http://10.102.0.197:50070')
    client.delete('/user/201600130079/' + outPath, recursive=True)
    data = sc.textFile(inPath)  # 读取数据
    data = data.map(lambda line: np.array(list(map(float, re.split(' +', line)[1:]))))  # 按空格分割特征
    clusters = np.array(data.take(CLASS_NUM))  # 初始化聚类中心
    for i in range(ITER_TIME):
        nearest = data.map(getNearest)
        clusterCount = nearest.countByKey()
        clusters = nearest.reduceByKey(lambda a, b: a + b)
        clusters = clusters.map(lambda x: x[1] / clusterCount[x[0]])
        clusters = clusters.collect()
    sc.parallelize(clusters).repartition(1).saveAsTextFile(outPath)
