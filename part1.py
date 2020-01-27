#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Template for the job scripts of Spark tutorials."""
import os
import sys

# Define parameters
SPARK_HOME = os.environ['SPARK_HOME']
PY4J_ZIP = 'py4j-0.10.7-src.zip'
SPARK_MASTER = 'local[*]'  # Or "spark://[MASTER_IP]:[MASTER_PORT]"
os.environ['PYSPARK_PYTHON'] = 'python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'python3'

# Add pyspark to the path and import this module
sys.path.append(SPARK_HOME + '/python')
sys.path.append(SPARK_HOME + '/python/lib/' + PY4J_ZIP)


from pyspark import SparkConf, SparkContext, StorageLevel  # noqa

# Create a Spark configuration object, needed to create a Spark context
# The master used here is a local one, using every available CPUs
spark_conf = SparkConf().setMaster(SPARK_MASTER)
sc = SparkContext(conf=spark_conf)

def print_top_words_index(item_set):
    """Print the top words index, input format: ((occurences, word), index)."""
    for item in item_set:
        occurences_word, index = item  # Unpack ((occurences, word), index)
        occurences, word = occurences_word  # Unpack (occurences, word)
        print('%d: %s (%d)' % (index, word, occurences))

def score_total(x):
    total = 0
    for k in x:
        total += k
    return total

def assist(x):
    return x * 50

def kills(x):
    return x * 100

def placement(x):
    return 1000 - 10 * (x-1)

# Define here your processes
try:
    # Load the RDD as a text file and persist it (cached into memory or disk)
    readme_rdd = sc.textFile('sample.csv')
    readme_rdd.persist()
    header = readme_rdd.first()

    rdd_subset = readme_rdd.flatMap(lambda line : line.split(' '))\
    .filter( lambda line : line != header)\
    .map(lambda line : line.split(','))\
    .map(lambda line: (line[11],(int(line[14]),1)))
    print('\nWe have %d lines' % rdd_subset.count())
    for x in rdd_subset.take(50) :
        print(x)
    rdd_reduced = rdd_subset.reduceByKey(lambda a,b : (a[0]+b[0],a[1]+b[1]))\
    .filter( lambda line : line[1][1] >= 4)\
    .map(lambda x : (x[0],x[1][0]/x[1][1]))
    print('\nWe have %d lines' % rdd_reduced.count())
    for x in rdd_reduced.take(50) :
        print(x)
    rdd_sorted = rdd_reduced.map(lambda item: (item[1], item[0]))\
    .sortByKey(ascending=True)
    print("\n ---------------There is the first result----------------")
    for x in rdd_sorted.take(50) :
        print(x)

    # (player_name ((team_placement,player_kills,player_assists,player_dmg), 1))
    # (player_name ((team_placement,player_kills,player_assists,player_dmg), games_count))
    # (player_name (average_team_placement,average_player_kills,average_player_assists,average_player_dmg))
    # (player_name, average_total_score)
    rdd_total = readme_rdd.flatMap(lambda line : line.split(' '))\
    .filter( lambda line : line != header)\
    .map(lambda line : line.split(','))\
    .map(lambda line: (line[11],((int(line[14]),int(line[10]),int(line[5]),int(line[9])),1)))\
    .reduceByKey(lambda a,b : ((a[0][0]+b[0][0],a[0][1]+b[0][1],a[0][2]+b[0][2],a[0][3]+b[0][3]),a[1]+b[1]))\
    .filter( lambda line : line[1][1] >= 4)\
    .map(lambda x : (x[0],(x[1][0][0]/x[1][1],x[1][0][1]/x[1][1],x[1][0][2]/x[1][1],x[1][0][3]/x[1][1])))\
    .map(lambda x : (x[0], score_total([placement(x[1][0]),kills(x[1][1]),assist(x[1][2]),x[1][3]])))

    rdd_total_sorted = rdd_total.map(lambda item: (item[1], item[0]))\
    .sortByKey(ascending=False)

    print("\n ---------------There is the result----------------")
    for x in rdd_total_sorted.take(50) :
        print(x)

    # Unpersist the readme RDD
    readme_rdd.unpersist()

# Except an exception, the only thing that it will do is to throw it again
except Exception as e:
    raise e

# NEVER forget to close your current context
finally:
    sc.stop()
