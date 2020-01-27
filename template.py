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

# Define here your processes
try:
    # Load the RDD as a text file and persist it (cached into memory or disk)
    readme_rdd = sc.textFile('sample.txt')
    readme_rdd.persist()

    # Count the number of items (= lines)
    print('\nWe have %d lines' % readme_rdd.count())

    # Print the first line
    print('\nThe first line contains : "%s"' % readme_rdd.first())

    # Get the words and their occurences (the two methods are equivalent)
    word_occs_rdd = readme_rdd.flatMap(lambda line: line.split(' '))\
                              .map(lambda line: (line, 1))\
                              .reduceByKey(lambda a, b: a+b)
    print('\nWord occurences : %s' % word_occs_rdd.take(10))

    # Another method which is shorter
    # word_occurences = readme_rdd.flatMap(lambda line: line.split(' '))\
    #                             .countByValue()

    # Sorted word occurences
    sorted_word_occs_rdd = word_occs_rdd.map(lambda item: (item[1], item[0]))\
                                        .sortByKey(ascending=False)
    print('\nTop 50 words : %s' % sorted_word_occs_rdd.take(50))

    # Add the index to the top 50 words, format is
    top_words_w_index_rdd = sorted_word_occs_rdd.zipWithIndex()
    print('\nTop 50 words with index:')
    print_top_words_index(top_words_w_index_rdd.take(50))

    # Remove the empty character
    top_words_rdd = top_words_w_index_rdd.filter(lambda item: item[0][1] != '')
    print('\nTop 50 words with index (empty character filtered out):')
    print_top_words_index(top_words_rdd.take(50))

    # Unpersist the readme RDD
    readme_rdd.unpersist()

# Except an exception, the only thing that it will do is to throw it again
except Exception as e:
    raise e

# NEVER forget to close your current context
finally:
    sc.stop()
