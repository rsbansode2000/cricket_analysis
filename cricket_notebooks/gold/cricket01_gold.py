# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC Setting required configurations

# COMMAND ----------

spark.conf.set("spark.databricks.delta.schema.autoMerge.enable", True)
spark.conf.set("spark.databricks.delta.schema.optimizeWrite.enable", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read silver table

# COMMAND ----------

cricket_df = spark.table("cricket01")

# COMMAND ----------

# MAGIC %md
# MAGIC seperate the span column

# COMMAND ----------

cricket_df = cricket_df.withColumn("from", split(col("span"), "-")[0]) \
    .withColumn("to", split(col("span"), "-")[1]).drop("span")

# COMMAND ----------

# MAGIC %md
# MAGIC make highest score as integer

# COMMAND ----------

cricket_df = cricket_df.withColumn("highest_score_not_out", when(col("highest_score").like("%*%"), lit("YES")).otherwise(lit("NO")))
cricket_df = cricket_df.withColumn("highest_score", regexp_replace(col("highest_score"), "[*]", "").cast("int"))

# COMMAND ----------

# MAGIC %md
# MAGIC Seperate country column

# COMMAND ----------

cricket_df = cricket_df.withColumn("country", regexp_extract(col("player"), "\(([\w/]+)\)", 1)) \
        .withColumn("player", trim(regexp_extract(col("player"), r'[\w\s]+', 0)))

# COMMAND ----------

# MAGIC %md
# MAGIC Join country csv

# COMMAND ----------

country_df = spark.read.format("csv") \
    .options(header = True) \
    .load("dbfs:/FileStore/project/cricket/country.csv")
cricket_df = cricket_df.join(country_df,  cricket_df.country == country_df.old_country , how = "left")
cricket_df = cricket_df.withColumn("country", col("new_country")).drop("old_country", "new_country")

# COMMAND ----------

cricket_df = cricket_df.drop("load_date")
cricket_df = cricket_df.withColumn("load_date", current_timestamp())
display(cricket_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write data in Gold Table 
# MAGIC ##### as overwrite as only latest data comming from silver

# COMMAND ----------

try :
  cricket_df.write \
    .format("delta") \
      .mode("overwrite") \
      .option("overwriteSchema", "true") \
      .partitionBy('country') \
    .saveAsTable("cricket01_gold")
  print(f"**************** Data saved in Gold Table at {date.today()} ***************")
except Exception as e:
  print(e)
