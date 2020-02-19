#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Template for the job scripts of Spark tutorials."""
import os
import sys

# Define parameters
SPARK_HOME = os.environ['SPARK_HOME']
PY4J_ZIP = 'py4j-0.10.7-src.zip'
SPARK_MASTER = 'local[*]'  # Or "spark://[MASTER_IP]:[MASTER_PORT]"
os.environ['PYSPARK_PYTHON'] = 'python'
os.environ['PYSPARK_DRIVER_PYTHON'] = 'python'

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

def split_genres(elem):
    tmp = []
    for i in elem[2] :
        tmp.append((elem[0],elem[1],i,elem[3]))
    return tmp

try:
    # Load the RDD as a text file and persist it (cached into memory or disk)
    ratings_rdd = sc.textFile('sample_ratings.csv')
    movies_rdd = sc.textFile('sample_movies.csv')
    header_rating = ratings_rdd.first()
    header_movies = movies_rdd.first()

    user = 4    # l'utilisateur d'id 4 a note beaucoup de films, le resultat sera pertinent

    #séparation des valeurs ratings,
    ratings_subset = ratings_rdd.flatMap(lambda line : line.split('\n'))\
    .filter( lambda line : line != header_rating)\
    .map(lambda line : line.split(','))\
    .filter(lambda line: line[0] == '4')\
    .map(lambda line: (line[1], line[2]))

    #séparation des valeurs movies
    movies_subset = movies_rdd.flatMap(lambda line : line.split('\n'))\
    .map(lambda x : x.replace(', ',': '))\
    .filter( lambda line : line != header_movies)\
    .map(lambda line : line.split(','))\
    .map(lambda x : (x[0],(x[1],x[2])))

    # # filtre par id de movie puis somme des notes et des 1 pour le count
    # #division entre count et somme des notes pour note moyenne, on stocke aussi le nombre de commentaires
    # ratings_reduced = ratings_subset.reduceByKey(lambda a,b : (a[0]+b[0],a[1]+b[1]))\
    # .filter( lambda line : line[1][1] >= 50)\
    # .map(lambda x : (x[0],x[1][0]/x[1][1],x[1][1]))\
    # .map(lambda x : (x[0],(x[1],x[2])))

    # on joint les notes de l'utilisateurs avec les titres & genre sur l'id du film
    rdd_join = ratings_subset.join(movies_subset)

    # on applatit les resultats pour plus de lisibilite
    # plus qu'a faire les moyennes par genre recommander les 5 meilleurs de ce genre
    rdd_join_flattened = rdd_join.map(lambda x: (x[0], x[1][1][0], x[1][1][1], x[1][0]))

    rdd_join_split = rdd_join_flattened.map(lambda line : (line[0], line[1], line[2].split('|'),line[3])).flatMap(lambda x : split_genres(x))

    # calcule les moyennes par genre, exclut les genres notés moins de 30 fois
    rdd_genre_average = rdd_join_split.map(lambda line: (line[2], (float(line[3]), 1)))\
    .reduceByKey(lambda a,b : (a[0]+b[0],a[1]+b[1]))\
    .filter(lambda line : line[1][1] >= 40)\
    .map(lambda line : (line[0],line[1][0]/line[1][1]))

    # passage de la note moyenne en clé de tuple pour pouvoir trier par rapport à la note et récupérer le genre le mieux noté
    rdd_sorted_by_note = rdd_genre_average.map(lambda line: (line[1], line[0]))\
    .sortByKey(ascending=False)

    favorite_genre = rdd_sorted_by_note.first()[1]

    print(favorite_genre)

    # print("\n ---------------There is the first result----------------")
    # print('\nWe have %d lines' % rdd_join_flattened.count())
    # for x in rdd_join_flattened.take(50) :
    #     print(x)
    # print("\n ---------------There is the first result----------------")
    # print('\nWe have %d lines' % rdd_join_split.count())
    # for x in rdd_join_split.take(50) :
    #     print(x)
    # print('\nWe have %d lines' % rdd_sorted_by_note.count())
    # for x in rdd_sorted_by_note.take(50) :
    #     print(x)
# Except an exception, the only thing that it will do is to throw it again
except Exception as e:
    raise e

# NEVER forget to close your current context
finally:
    sc.stop()
