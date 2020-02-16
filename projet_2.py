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

try:
    # Load the RDD as a text file and persist it (cached into memory or disk)
    ratings_rdd = sc.textFile('sample_ratings.csv')
    movies_rdd = sc.textFile('sample_movies.csv')
    header_rating = ratings_rdd.first()

    #séparation des valeurs ratings
    ratings_subset = ratings_rdd.flatMap(lambda line : line.split('\n'))\
    .filter( lambda line : line != header_rating)\
    .map(lambda line : line.split(','))\
    .map(lambda line: (line[1],(float(line[2]),1))) #ajout d'un 1 avec la note pour pouvoir faire le count
    header_movies = movies_rdd.first()
    #séparation des valeurs movies
    movies_subset = movies_rdd.flatMap(lambda line : line.split('\n'))\
    .map(lambda x : x.replace(', ',': '))\
    .filter( lambda line : line != header_movies)\
    .map(lambda line : line.split(','))\
    .map(lambda line : (line[0], line[1], line[2].split('|')))\
    .filter(lambda line : 'Romance' in line[2])
    # filtre par id de movie puis somme des notes et des 1 pour le count
    #division entre count et somme des notes pour note moyenne, on stocke aussi le nombre de commentaires
    ratings_reduced = ratings_subset.reduceByKey(lambda a,b : (a[0]+b[0],a[1]+b[1]))\
    .filter( lambda line : line[1][1] >= 50)\
    .map(lambda x : (x[0],x[1][0]/x[1][1],x[1][1]))\
    .map(lambda x : (x[0],(x[1],x[2])))

    rdd_join = ratings_reduced.join(movies_subset)

    # on trie le rdd en mettant la note moyenne en clef, puis le nom de film puis le nombre de notes
    ratings_sorted = rdd_join.map(lambda item: (item[1][0][0], (item[1][1],item[1][0][1])))\
    .sortByKey(ascending=False)
    print("\n ---------------There is the first result----------------")
    print('\nWe have %d lines' % ratings_sorted.count())
    for x in ratings_sorted.take(50) :
        print(x)
# Except an exception, the only thing that it will do is to throw it again
except Exception as e:
    raise e

# NEVER forget to close your current context
finally:
    sc.stop()
