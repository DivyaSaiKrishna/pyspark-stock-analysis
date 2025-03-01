from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("pyspark_sql").getOrCreate()
from pyspark.sql.window import Window
from pyspark.sql.functions import col, avg, regexp_replace, rank, lead, lag, ntile, udf
from pyspark.sql.types import StringType

print("Create a sample RDD")
rdd = spark.sparkContext.parallelize([(1, "Alice"), (2, "Bob"), (3, "Charlie")])

print("Create a PySpark DataFrame from RDD")
df = spark.createDataFrame(rdd, ["id", "name"])

print("Print the contents of the DataFrame")
df.show()

stockData = spark.read.csv("src/data/stock_data_1.csv", header=True, inferSchema=True)
stockData.show(10)

stockData.count()

stockData.filter(stockData['IPO Year']>2021).show()

appleStockData = spark.read.csv("src/data/APPLE_HistoricalData_2020-2025.csv", header=True, inferSchema=True)
appleStockData.show(10)


amazonStock = spark.read.csv("src/data/Amazon_HistoricalData_2025.csv", header=True, inferSchema=True)
amazonStock.show(10)

print("-----------Inner-------")
joined_df_inner = appleStockData.join(amazonStock, on="Date", how="inner")
joined_df_inner.show()
print("-----------Outer-------")
joined_df_outer = appleStockData.join(amazonStock, on="Date", how="outer")
joined_df_outer.show()

print("-----------Last Sale-------")
#partition with sector
windowSpec = Window.partitionBy('Sector').orderBy('Volume')
#convert 'Last Sale' to float
df_LS = stockData.withColumn("Last Sale", regexp_replace(col("Last Sale"), "[$,]", "").cast("double"))
#average last sale

#stcokSector = stockData.filter(col("Sector").isNotNull())
stockAvgData = df_LS.withColumn("avg_lastSale", avg('Last Sale').over(windowSpec))
#stockAvgData.show()

print("------- Not Null ---------")
stockAvgDataNN = stockAvgData.select("Sector", "avg_lastSale").distinct().filter(col("Sector").isNotNull())

stockAvgDataNN.show()
print("------- Rank ---------")
stockData.withColumn('rank', rank().over(windowSpec)).show()



print("Next month's sales for each category:")
stockData.withColumn('next_Last_Sale', lead('Last Sale', 1).over(windowSpec)).show()

print("Calculating the average last sale:")
stockData.agg(avg(col("Volume")).alias("Average Volume")).show()

stockData.groupBy("Sector").agg(avg("Volume").alias("Average Volume by Sector")).show()

#capitalize string
def capitalizeString(s):
    return s.upper()


capitalize_udf = udf(capitalizeString, StringType())

stockCS = stockData.withColumn("Capitalized Sector", capitalize_udf(stockData["Sector"]))

print("Display the final results of the DataFrame")
stockCS.show()


stockData.createOrReplaceTempView("stock")

stockSQL = spark.sql("SELECT Symbol from stock")
print("--------Stock SQL-------")
stockSQL.show()


stockCount = spark.sql("SELECT COUNT(*)  from stock")
print("----- Stock Count --------")
stockCount.show()
print(" -------- sector count -------")
spark.sql("SELECT Sector, COUNT(*) from stock GROUP By Sector").show()
