# Import

from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.types import *


# Creating Spark Session

spark = SparkSession.builder.appName("Create Delta Table").getOrCreate()


# Creating Delta Table

# Schema

housing_schema = StructType([StructField("longitude",DoubleType(),True),StructField("latitude", DoubleType(),True),
StructField("housingMedianAge",DoubleType(),True),StructField("totalRooms",DoubleType(),True),
StructField("totalBedrooms",DoubleType(),True),StructField("population",DoubleType(), True),
StructField("households",DoubleType(),True),
StructField("medianIncome",DoubleType(),True),
StructField("medianHouseValue",DoubleType(),True)])


DeltaTable.createOrReplace(spark, "housing-dataset").schema(housing_schema)


# loading data into table

# Creating dataframe

housing_df = spark.read.format("csv").load("housing_data")



# Saving as Delta table

housing_df.write.format("delta").mode("overwrite").save("housing-dataset")


# To see data from Delta table 

spark.sql('select * from housing-dataset').show()



 










