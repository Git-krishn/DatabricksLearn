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

# COMMAND ----------

countries_sl_json = spark.read.json("/FileStore/tables/countries_single_line.json")

# COMMAND ----------

display(countries_sl_json)

# COMMAND ----------

countries_ml_json = spark.read.json("dbfs:/FileStore/tables/countries_multi_line.json")

# COMMAND ----------

countries_ml_json.display()

# COMMAND ----------

countries_ml_json= spark.read.options(multiLine=True).json("dbfs:/FileStore/tables/countries_multi_line.json")

# COMMAND ----------



# COMMAND ----------

countries_ml_json.display()

# COMMAND ----------

countries_txt = spark.read.csv("dbfs:/FileStore/tables/countries.txt", header= True, sep ="\t")

# COMMAND ----------


