#coding=utf-8
__author__ = 'zhaohuilian'
#textFile文件路径根据情况自行修改
# 将tag数据，作为一行，添加到对应的tid数据后面，输出一个新的rdd
from pyspark import SparkContext

sc = SparkContext("local", "Simple App")

thread = sc.textFile("E:/sparktest/thread1.txt")
tag = sc.textFile("E:/sparktest/tag1.txt")

rdd_tag = tag.map(lambda line: (line.split("^")[0], line.split("^")[1])).groupByKey()
rdd_thread = thread.map(lambda line: (line.split("^")[2], line))
rdd = rdd_thread.union(rdd_tag).groupByKey()
print rdd.collect()