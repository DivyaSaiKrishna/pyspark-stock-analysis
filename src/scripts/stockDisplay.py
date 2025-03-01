import sys
import os

# Add the project root (src) to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config.sparkConfig import create_spark_session

# Creating Spark Session
spark = create_spark_session("Stock Analysis")

# Reading Stock Data 
df = spark.read.option("header", "true").csv("src/data/stock_data_1.csv").toDF("Symbol", "Name","Last Sale","Net Change","% Change", "Market Cap","Country","IPO Year","Volume","Sector","Industry")

#dispplay top 5
print(" Top 5")
df.show(5)

#display IPO Year greater than 2023
print(" IPO Year greater than 2023")
df.filter(df["IPO Year"] > 2023).show()

#Display Sector is Techology
print(" Display Sector is Techology")
df.filter(df["Sector"] == "Technology").show()
