import sys
import os

# Add the project root (src) to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config.sparkConfig import create_spark_session
from pyspark import SparkContext

# Creating Spark Session
#spark = create_spark_session("Stock Analysis")

# Reading Stock Data 
#df = spark.read.option("header", "true").csv("src/data/stock_data_1.csv").toDF("Symbol", "Name","Last Sale","Net Change","% Change", "Market Cap","Country","IPO Year","Volume","Sector","Industry")

data = [1, 2, 3, 4, 5]

#spark context
sc = SparkContext("local", "Parallelize Example")

rdd = sc.parallelize(data)

print(rdd)
print(rdd.collect())

#read csv data
rdd_file = sc.textFile("src/data/stock_data_1.csv")

print(rdd_file)

csvRdd = rdd_file.map(lambda line : line.split(","))

for row in csvRdd.take(10):
    print(row[2])

csvRddLastSale = rdd_file.map(lambda line : line.split(","))
print("row")
for row in csvRddLastSale.take(5):
    print(row[2]*2)


data1 = [2,3,4,5,6,7,8,9,13,14,16,18,43,28]
data2 = [1,2,3, 10,11,12,25]

rdd1 = sc.parallelize(data1)
rdd2 = sc.parallelize(data2)

#union
rddUnion = rdd1.union(rdd2)
print(rddUnion.collect())
print(rddUnion.take(4))
print(rddUnion.first())
print(rddUnion.count())
#intersection
rddInter = rdd1.intersection(rdd2)
print(rddInter.collect())