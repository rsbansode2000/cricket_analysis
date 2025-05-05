# Databricks notebook source
# MAGIC %md
# MAGIC **Load CSV data in bronze layer**

# COMMAND ----------

# initilize parameters
dbutils.widgets.removeAll()
dbutils.widgets.dropdown(name = "load_type", defaultValue = "init", choices = ["init", "delta"], label = "load load_type")
load_type = dbutils.widgets.get("load_type")
print(f"Load Type : {load_type}")
bronze_data_path = "dbfs:/FileStore/project/cricket/bronze/"

# COMMAND ----------

# MAGIC %md
# MAGIC setting required properties

# COMMAND ----------

spark.conf.set("spark.databricks.delta.schema.autoMerge.enable", True)
spark.conf.set("spark.databricks.delta.schema.optimizeWrite.enable", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read csv from local storage

# COMMAND ----------

cricket_df = spark.read.format("csv") \
  .option("header", True) \
  .load("dbfs:/FileStore/project/cricket/ODI_data.csv")

# COMMAND ----------

column_drop = ["_c0", "Unnamed: 13"]
cricket_df = cricket_df.withColumnRenamed("0", "zero") \
    .withColumnRenamed("50", 'fifty') \
    .withColumnRenamed("100", 'centuries') \
    .drop(*column_drop)
display(cricket_df.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save bronze data in delta format

# COMMAND ----------

# import
from pyspark.sql import *
from pyspark.sql.functions import *

# COMMAND ----------

if load_type == "init":
    dbutils.fs.rm(bronze_data_path + "/cricket01/" , True)
    write_type = "overwrite"
else :
    write_type = "append"
print("Write type : "+ write_type)

# COMMAND ----------

try :
  cricket_df.write.format("delta") \
  .mode(write_type) \
  .save(bronze_data_path + "/cricket01/")
  print("********************** Data loaded successfully ********************")
except Exception as e:
  print("Got Exception "+ e)