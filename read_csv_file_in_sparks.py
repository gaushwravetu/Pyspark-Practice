# Databricks notebook source
flight_df = spark.read.format("csv")\
            .option("header","false")\
            .option("inferschema","false")\
            .option("mode","FAILFAST")\
            .load("/FileStore/tables/data.csv")
flight_df.show(5)

# COMMAND ----------

flight_df_header = spark.read.format("csv")\
            .option("header","true")\
            .option("inferschema","false")\
            .option("mode","FAILFAST")\
            .load("/FileStore/tables/data.csv")
flight_df_header.show(5)

# COMMAND ----------

flight_df.printSchema()

# COMMAND ----------

flight_df_header_schema = spark.read.format("csv")\
            .option("header","true")\
            .option("inferschema","true")\
            .option("mode","FAILFAST")\
            .load("/FileStore/tables/data.csv")
flight_df.show(5)

# COMMAND ----------

flight_df_header_schema.printSchema()

# COMMAND ----------

flight_df = spark.read.format("csv")\
            .option("header","false")\
            .option("inferschema","false")\
            .schema(my_schema)\
            .option("mode","FAILFAST")\
            .load("/FileStore/tables/data.csv")
flight_df.show(5)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

my_schema = StructType([StructField("DEST_COUNTRY_NAME", StringType(), True), StructField("ORIGIN_COUNTRY_NAME", StringType(),True),StructField("count", IntegerType(), True)])

# COMMAND ----------

flight_df = spark.read.format("csv")\
            .option("header","false")\
            .option("skiprows",1)\
            .option("inferschema","false")\
            .schema(my_schema)\
            .option("mode","PERMISSIVE")\
            .load("/FileStore/tables/data.csv")
flight_df.show(5)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/data.csv
