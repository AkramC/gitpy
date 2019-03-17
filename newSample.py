from pyspark.sql import SparkSession
spark= SparkSession.builder.appName("Moving Average").master("local").getOrCreate()
window=3
inputRDD=spark.sparkContext.textFile("file:///D:/AverageData/MovingAverage.txt")
inputList=inputRDD.map(lambda x : x.split(" "))

#groupedlist= [inputList[n:n+window] for n in range(0, len(inputList)-2)]
inputRDDsplitlist=inputRDD.map(lambda inputList : inputList )
#inputRDDsplitlist=inputRDD.map(lambda inputList : [inputList[n:n+window] for n in range(0, len(inputList)-2)])
print(inputRDDsplitlist.collect())