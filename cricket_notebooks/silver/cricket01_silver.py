# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer

# COMMAND ----------

# initilize parameters
dbutils.widgets.removeAll()
dbutils.widgets.dropdown(name = "load_type", defaultValue = "delta", choices = ["init", "delta"], label = "load load_type")
load_type = dbutils.widgets.get("load_type")
print(f"Load Type : {load_type}")
bronze_data_path = "dbfs:/FileStore/project/cricket/bronze/"
# silver_data_path = "dbfs:/FileStore/project/cricket/silver/"

# COMMAND ----------

# imports necessary modules
from pyspark.sql.types import *
from pyspark.sql.functions import split, col, current_timestamp
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC setting required configurations

# COMMAND ----------

spark.conf.set("spark.databricks.delta.schema.autoMerge.enable", True)
spark.conf.set("spark.databricks.delta.schema.optimizeWrite.enable", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read data from bronze layer

# COMMAND ----------

cricket_df = spark.read.format("delta") \
    .load(bronze_data_path + "/cricket01/")

# COMMAND ----------

# MAGIC %md
# MAGIC Dropping duplicate data

# COMMAND ----------

cricket_df = cricket_df.dropDuplicates()

# COMMAND ----------

# MAGIC %md
# MAGIC Renaming columns

# COMMAND ----------

col_to_rename = {
    "Player" : "player",
    "Span" : "span",
    "Mat" : "matches",
    "Inns" : "innings",
    "NO" : "not_out",
    "Runs" : "runs",
    "HS" : "highest_score",
    "Ave" : "batting_average",
    "SR" : "strike_rate",
}
for old_col, new_col in col_to_rename.items():
    cricket_df = cricket_df.withColumnRenamed(old_col, new_col)

# COMMAND ----------

# MAGIC %md
# MAGIC Add load date

# COMMAND ----------

cricket_df = cricket_df.withColumn("load_date", current_timestamp())
display(cricket_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing data to silver table

# COMMAND ----------

# MAGIC %md
# MAGIC read silver table

# COMMAND ----------

# Load the existing Delta table from a specific schema and table name
silver_df = DeltaTable.forName(spark, "cricket01")

# COMMAND ----------

# Check if the source DataFrame has duplicates based on the key column 'name'
cricket_df = cricket_df.dropDuplicates(["player"])
dq_check = cricket_df.groupBy("player").count().filter("count > 1")
dq_check.show()

# COMMAND ----------

if dq_check.count() == 0:
    silver_df.alias("target").merge(
    cricket_df.alias("source"),
    "target.player = source.player"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()
    print("***************** Data Merger Successfully ************************")
else :
    print("Dq check Fail")