# unit test


import pytest
from pyspark.sql import SparkSession
from delta import *

def test_create_delta_table():
    spark = SparkSession.builder.appName("Test_Create_Delta_Table_Job").getOrCreate()

    
    # Define the schema for the housing dataset

   housing_schema = StructType([StructField("longitude",DoubleType(),True),
StructField("latitude", DoubleType(),True),
StructField("housingMedianAge",DoubleType(),True),StructField("totalRooms",DoubleType(),True),
StructField("totalBedrooms",DoubleType(),True),StructField("population",DoubleType(), True),
StructField("households",DoubleType(),True),StructField("medianIncome",DoubleType(),True),
StructField("medianHouseValue",DoubleType(),True)])


   # Create the Delta table

    DeltaTable.createOrReplace(spark, "housing-dataset").schema(housing_schema)

    
    # Assert that delta table is created
    deltaTable = DeltaTable.forPath(spark, "/housing-dataset")
    assert deltaTable is not None
    
    # Assert that schema is correct
    expected_schema = StructType([StructField("longitude",DoubleType(),True),
StructField("latitude", DoubleType(),True),
StructField("housingMedianAge",DoubleType(),True),StructField("totalRooms",DoubleType(),True),
StructField("totalBedrooms",DoubleType(),True),StructField("population",DoubleType(), True),
StructField("households",DoubleType(),True),StructField("medianIncome",DoubleType(),True),
StructField("medianHouseValue",DoubleType(),True)])
    
     assert deltaTable.toDF().schema == expected_schema
    
def test_ingest_data():
    spark = SparkSession.builder.appName("Test_Ingest_Data_Job").getOrCreate()
    housing_df = spark.read.format("csv").load("housing_data_test")

     housing_df.write.format("delta").mode("overwrite").save("housing-dataset")
    
    # Assert that Delta table exists
    deltaTable = DeltaTable.forPath(spark, "/housing-dataset")
    assert deltaTable is not None
    
    # Assert that data is ingested into Delta table
    expected_count = spark.read.format("csv").load("housing_data_test").count()
    actual_count = deltaTable.toDF().count()
    assert actual_count == expected_count
   


# unit test to assert that data is in fact moving into the Delta lake table

import pytest
from pyspark.sql import SparkSession
from delta import *

def test_ingest_data():
    spark = SparkSession.builder.appName("Test_Ingest_Data_Job").getOrCreate()
    
    # Assert that Delta table exists
    deltaTable = DeltaTable.forPath(spark, "/housing-dataset")
    assert deltaTable is not None
    
    # Assert that data is ingested into Delta table
    expected_count = spark.read.format("csv").load("housing_data_test").count()
    actual_count = deltaTable.toDF().count()
    assert actual_count == expected_count
    
    # Assert that the data in Delta table matches the expected data
    expected_df = spark.read.format("csv").load("housing_data_test")
    actual_df = deltaTable.toDF()
    assert expected_df.subtract(actual_df).count() == 0


















