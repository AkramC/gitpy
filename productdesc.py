from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql import Window
spark = SparkSession.builder.appName("product desc").master("local").getOrCreate()
schemaprod = StructType([
StructField("Id",StringType(),True),
StructField("Productdesc",StringType(),True) ])
product = spark.read.schema(schemaprod).csv("D://productdesc.csv")
productexpl=product.withColumn("tshrt",F.split("Productdesc"," ")).select("Id","Productdesc",F.explode("tshrt").alias("words"))
productfilter=productexpl.filter(F.col("words") == "Tshirt").drop("Productdesc")
win=Window.partitionBy("Id")
priducttshrtt=productfilter.select("Id",F.count("Id").over(win).alias("Cnt")).filter(F.col("Cnt") == 2).distinct()
#priducttshrt=productfilter.groupBy("Id").count().filter(F.col("count") == 2)

priducttshrtt.show()