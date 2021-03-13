from pyspark import SparkContext, SparkConf
from multiprocessing import cpu_count
from string import ascii_letters
from operator import add
from time import time
from pathlib import Path as pathlibpath
from os import path as ospath

def word_count_spark(input_filepath, output_filepath=None, n_partitions=cpu_count()):
    # Start Spark cluster
    conf = SparkConf().setAppName('word-count').setMaster('local')
    sc = SparkContext(conf=conf)
    # Run Spark execution
    start = time()
    input_RDD = sc.textFile(input_filepath, n_partitions)
    preproc_RDD = input_RDD.map(lambda line: ''.join([char if char in set(ascii_letters+' ') else '' for char in line]).strip())
    preproc_RDD = preproc_RDD.filter(lambda line: len(line) > 0)
    count_RDD = preproc_RDD.map(lambda line: len(line.split(' ')))
    count = count_RDD.reduce(add)
    spark_duration = time() - start
    # Stop Spark cluster
    sc.stop()

    # Print output metrics
    print(f"""
    Spark execution (@ {input_RDD.getNumPartitions()} partitions):
    ----------------------------------
    word_count = {count}
    time_elapsed = {spark_duration}s
    """)
    
    # Write output to file if filepath provided
    if output_filepath is not None:
        pathlibpath(ospath.dirname(output_filepath)).mkdir(parents=True, exist_ok=True)
        with open(output_filepath, 'w') as writer:
            writer.write(str(count))
        print(f"Output written to {output_filepath}")
    
    # Return the word count
    return count


def word_count(input_filepath, output_filepath=None):
    # Run non-Spark execution for comparison
    start = time()
    count = 0
    with open('data/hamlet.txt', 'r') as reader:
        for line in reader:
            line = ''.join([char if char in set(ascii_letters+' ') else '' for char in line]).strip()
            if len(line) > 0:
                count += len(line.split(' '))
    reg_duration = time() - start
    
    # Print output metrics
    print(f"""
    Regular execution:
    ------------------
    word_count = {count}
    time_elapsed = {reg_duration}s
    """)

    # Write output to file if filepath provided
    if output_filepath is not None:
        pathlibpath(ospath.dirname(output_filepath)).mkdir(parents=True, exist_ok=True)
        with open(output_filepath, 'w') as writer:
            writer.write(str(count))
        print(f"Output written to {output_filepath}")
    
    # Return the word count
    return count

if __name__ == '__main__':
    input_fp = "data/hamlet.txt"
    output_fp = "output/hamletout.txt"

    spark_count = word_count_spark(input_fp, output_fp)
    regular_count = word_count(input_fp)

    assert spark_count == regular_count
    print("Testing equality of results: PASS")