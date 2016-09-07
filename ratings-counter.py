from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)


# Creating the first RDD with SC

lines = sc.textFile("file:/home/jaafar/anaconda2/SparkCourse/ml-100k/u.data")


# Transformation operation

# Dont transform the RDD in place ! You should assign it to another NEW RDD !
ratings = lines.map(lambda x: x.split()[2])

# Action operation
result = ratings.countByValue()



# Sorting and printing as a collection of key, value

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
