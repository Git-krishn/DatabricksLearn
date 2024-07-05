# Databricks notebook source
countries_df = spark.read.csv("/FileStore/tables/countries.csv")

# COMMAND ----------

type(countries_df)

# COMMAND ----------

countries_df.show()

# COMMAND ----------

countries_df.display()

# COMMAND ----------

countries_df = spark.read.csv("/FileStore/tables/countries.csv",header= True)

# COMMAND ----------

countries_df.display()

# COMMAND ----------

countries_df = spark.read.options(header =True, inferSchema = True).csv("/FileStore/tables/countries.csv")

# COMMAND ----------

countries_df.dtypes

# COMMAND ----------

countries_df.schema

# COMMAND ----------

countries_df.describe()

# COMMAND ----------

from pyspark.sql.types import IntegerType, StringType, DoubleType, StructField, StructType

# COMMAND ----------

countries_schema = StructType([StructField("Country_ID", IntegerType(), False), StructField("Name", StringType(), False), StructField("Nationality", StringType(), False), StructField("Country_Code", StringType(), False), StructField("ISO_ALPHA2", StringType(), False), StructField("Capital", StringType(), False),StructField("Population", DoubleType(), False),StructField("Area_KM2",IntegerType(),False),StructField("Region_Id",IntegerType(),True),StructField("Sub_Region_Id",IntegerType(),True),StructField("Intermediate_Region_ID",IntegerType(),True),StructField("Organization_Region_Id",IntegerType(),True)])

# COMMAND ----------

countries_df = spark.read.csv("/FileStore/tables/countries.csv", header = True, schema = countries_schema)

# COMMAND ----------

countries_df = spark.read.options(header =True).schema(countries_schema).csv("/FileStore/tables/countries.csv")
