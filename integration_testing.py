Import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from delta import *

def test_create_delta_table():
    spark = SparkSession.builder.appName("Test_Delta_Table_Creation_Job").getOrCreate()
    

    # Define the schema for the housing dataset

   housing_schema = StructType([StructField("longitude",DoubleType(),True),
StructField("latitude", DoubleType(),True),
StructField("housingMedianAge",DoubleType(),True),StructField("totalRooms",DoubleType(),True),
StructField("totalBedrooms",DoubleType(),True),StructField("population",DoubleType(), True),
StructField("households",DoubleType(),True),StructField("medianIncome",DoubleType(),True),
StructField("medianHouseValue",DoubleType(),True)])


   # Create the Delta table

    DeltaTable.createOrReplace(spark, "housing-dataset").schema(housing_schema)

 
    # Assert that Delta table exists
    deltaTable = DeltaTable.forPath(spark, "/housing-dataset")
    assert deltaTable is not None
    
    # Assert that Delta table has expected schema
    
     expected_schema =  StructType([StructField("longitude",DoubleType(),True),
StructField("latitude", DoubleType(),True),
StructField("housingMedianAge",DoubleType(),True),StructField("totalRooms",DoubleType(),True),
StructField("totalBedrooms",DoubleType(),True),StructField("population",DoubleType(), True),
StructField("households",DoubleType(),True),StructField("medianIncome",DoubleType(),True),
StructField("medianHouseValue",DoubleType(),True)])

   assert deltaTable.toDF().schema == expected_schema


def test_data_ingestion():
    spark = SparkSession.builder.appName("Test_Delta_Table_Creation_Job").getOrCreate()
       

        housing_df = spark.read.format("csv").load("housing_data")

        housing_df.write.format("delta").mode("overwrite").save("housing-dataset")
     
    # Asset that data is loaded in table 

        delta_table = DeltaTable.forName(spark, "housing-dataset")
        assert delta_table.toDF().count() > 0


