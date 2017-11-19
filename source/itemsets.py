import unittest
import sys
import findspark
findspark.init()
from pyspark import SparkContext
from pyspark import SparkConf
import itertools
def make_pairs(line):
    pairs = []
    for i in range (len (line)):
        j=i+1
        while j in range (len(line)):
            sorted_tuple = tuple(sorted((line[i], line[j])))
            pairs.append(sorted_tuple)
            j+=1
    return pairs

def combine(line):
    if line == []:
        return []
    combinations = []
    union = set()
    for i in range (len (line)):
        for item in line[i]:
            union.add(item)
    s = get_combinations(list(union), len(line[0])+1)
    for item in s:
        combinations.append(tuple(sorted(item)))
    return combinations
def get_combinations(my_list, length):
    l = map(list, itertools.combinations(my_list, length))
    return l
def union_tuple(first, other):
    temp=set()
    for i in range(len(first)):
        temp.add(first[i])
    for i in range(len(other)):
        temp.add(other[i])
    return temp

#spark-class org.apache.spark.deploy.master.Master
#spark-class org.apache.spark.deploy.worker.Worker spark://jdator:7077
def main():
    print "Please specify your spark master"
    print "eg. spark://<IP>:7077"
    master = raw_input()
    print "Please specify your support treshold"
    support = int(raw_input())
    print "Please specify your file path"
    print "The file must be accessible by all spark workers"
    file_path = raw_input()
    run(master, support, file_path)
def run(master, support, file_path):
    #Set up spark
    conf = SparkConf().setAppName("test").setMaster(master)
    sc = SparkContext(conf=conf)
    text_file=sc.textFile(file_path)

    # Separating all words
    words = text_file.flatMap(lambda line: line.split(" "))
    # Counting all words
    counted_words = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a+ b)
    # Filtering out words with occurance<support
    frequent_words = counted_words.filter(lambda (_, x): x>support)
    # Collecting all frequent words in a dictionary
    mapped_singletons = frequent_words.collectAsMap()
    # Separating all lines
    lines = text_file.map(lambda line: line.split("\n"))
    # Separating the words of each line
    lines_of_words=lines.map(lambda line: str(line).strip("[]u' ").split(" "))
    #For each line, filtering out all words with occurance<support
    lines_of_frequent = lines_of_words.map(lambda line: (filter(lambda word: mapped_singletons.has_key(word), line)))

    #Turning the frequent words into sorted pairs
    pairs = lines_of_frequent.flatMap(make_pairs)
    #Counting the pairs
    pair_counts = pairs.map(lambda pair: (pair, 1)).reduceByKey(lambda a, b: a+ b)
    #Filtering out all pairs with occurance<support
    frequent_pairs = pair_counts.filter(lambda (_, x): x>support)
    #Collecting all frequent pairs in a dictionary
    mapped_pairs = frequent_pairs.collectAsMap()
    #Turning each line of frequent singletons into pairs of singletons
    lines_of_pairs = lines_of_frequent.map(make_pairs)
    #For each line, filtering out the infrequent pairs
    lines_of_frequent_pairs = lines_of_pairs.map(lambda line: (filter(lambda pair: mapped_pairs.has_key(pair), line)))

    #Recursively find all combinations of size k, k+1, ... n
    #When there is no combination with occurence>support it returns a list of dictionaries
    print a_priori(lines_of_frequent_pairs, [mapped_pairs], support)
    sc.stop()
def a_priori(lines_of_frequent, result_list, support):
    next_combination = lines_of_frequent.flatMap(combine)
    if next_combination == []:
        return result_list
    next_counts = next_combination.map(lambda entry: (entry, 1)).reduceByKey(lambda a, b: a+ b)
    next_frequent = next_counts.filter((lambda (_, counts): counts>support))
    next_mapped = next_frequent.collectAsMap()
    if not next_mapped:
        return result_list
    partitioned_lines = lines_of_frequent.map(combine)
    next_lines_of_frequent = partitioned_lines.map(
        lambda line: (filter(lambda pair: next_mapped.has_key(pair), line)))
    result_list.append(next_mapped)
    return a_priori(next_lines_of_frequent, result_list, support)

if __name__ == '__main__':
    main()