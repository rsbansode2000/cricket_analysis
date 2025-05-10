# Databricks notebook source
# import
import pandas as pd
from matplotlib import pyplot as plt

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Top 10 Highest runs scored by batsman

# COMMAND ----------

highest_score_df = spark.sql("""
                             select country, player, runs, innings
                            from cricket01_gold
                            order by runs desc limit 10
                        """).toPandas()
display(highest_score_df)

# COMMAND ----------

plt.figure(figsize=(18,10))
plt.bar(highest_score_df_pd["player"], highest_score_df_pd["runs"])
plt.xlabel("Player Name", fontdict={'color' : 'black', 'fontweight' : 'bold', 'fontsize' : 20})
plt.ylabel("runs", fontdict={'color' : 'black', 'fontweight' : 'bold', 'fontsize' : 20})
plt.grid(True)
plt.title("Top 10 Batsman with most Runs", fontdict={'color' : 'green', 'fontweight' : 'bold', 'fontsize' : 40})

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Top 10 Highest runs scorered by batsman by each country (atlease 10 innings)

# COMMAND ----------

highest_runs_by_country_df = spark.sql("""
                                       with cte as (select country, player, runs, rank() over(partition by country order by runs desc) as rnk 
                                        from default.cricket01_gold where innings >= 10)
                                        select concat(country, ' (', player, ')') as player ,runs as highes_runs from cte
                                        where rnk = 1 and concat(player, ' (', country, ')') is not null
                                        order by player
                                       """).toPandas()
display(highest_runs_by_country_df)                                   

# COMMAND ----------

plt.figure(figsize=(18,10))
plt.barh(highest_runs_by_country_df["player"], highest_runs_by_country_df["highes_runs"])
plt.xlabel("Runs", fontdict={'color' : 'black', 'fontweight' : 'bold', 'fontsize' : 20})
plt.ylabel("country (player)", fontdict={'color' : 'black', 'fontweight' : 'bold', 'fontsize' : 20})
plt.grid(True)
plt.title("Counrty wise most runs", fontdict={'color' : 'green', 'fontweight' : 'bold', 'fontsize' : 40})

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Country wise player with highest batting average (atleast 10 innings)

# COMMAND ----------

highets_batting_avg_country_df = spark.sql("""
                                           select s.player, s.matches, s.innings, s.batting_average
                                                from (select concat(country, ' (', player, ')') as player, matches, innings, batting_average, rank() over(partition by country order by batting_average desc) as rnk
                                                from cricket01_gold
                                                where innings >= 10 and concat(country, ' (', player, ')') is not null) as s
                                                where s.rnk = 1
                                                order by s.player
                                           """).toPandas()
display(highets_batting_avg_country_df)    

# COMMAND ----------

plt.figure(figsize=(18,10))
plt.barh(highets_batting_avg_country_df["player"], highets_batting_avg_country_df["batting_average"])
plt.xlabel("Batting Average", fontdict={'color' : 'black', 'fontweight' : 'bold', 'fontsize' : 20})
plt.ylabel("country (player)", fontdict={'color' : 'black', 'fontweight' : 'bold', 'fontsize' : 20})
plt.grid(True)
plt.title("Counrty wise highest batting average", fontdict={'color' : 'green', 'fontweight' : 'bold', 'fontsize' : 40})

# COMMAND ----------

# MAGIC %md
# MAGIC 4. Country wise player with highest Centuries (atleast 10 innings)

# COMMAND ----------

highest_centuries_by_country_df = spark.sql("""
                                            select concat(s.country, ' (', s.player, ')') as player, s.matches, s.innings, s.centuries
                                            from (select player, matches, innings, centuries, country, rank() over(partition by country order by centuries desc, innings asc) as rnk
                                            from cricket01_gold
                                            where innings >= 10) as s
                                            where s.rnk = 1 and s.country is not null
                                            order by player
                                            """).toPandas()
display(highest_centuries_by_country_df)                                            

# COMMAND ----------

plt.figure(figsize=(18,10))
plt.barh(highest_centuries_by_country_df["player"], highest_centuries_by_country_df["centuries"])
plt.xlabel("Centuries", fontdict={'color' : 'black', 'fontweight' : 'bold', 'fontsize' : 20})
plt.ylabel("country (player)", fontdict={'color' : 'black', 'fontweight' : 'bold', 'fontsize' : 20})
plt.grid(True)
plt.title("Counrty wise highest centuries", fontdict={'color' : 'green', 'fontweight' : 'bold', 'fontsize' : 40})

# COMMAND ----------

# MAGIC %md
# MAGIC 5. Country wise player with highest Fifties (atleast 10 innings)

# COMMAND ----------

highest_fifties_by_country_df = spark.sql("""
                                            select concat(s.country, ' (', s.player, ')') as player, s.matches, s.innings, s.fifty
                                                from (select player, matches, innings, fifty, country, rank() over(partition by country order by fifty desc, innings asc) as rnk
                                                from cricket01_gold
                                                where innings >= 10 and country is not null) as s
                                                where s.rnk = 1
                                                order by player desc
                                            """).toPandas()
display(highest_fifties_by_country_df)

# COMMAND ----------

plt.figure(figsize=(18,10))
plt.barh(highest_fifties_by_country_df["player"], highest_fifties_by_country_df["fifty"])
plt.xlabel("Fifties", fontdict={'color' : 'black', 'fontweight' : 'bold', 'fontsize' : 20})
plt.ylabel("country (player)", fontdict={'color' : 'black', 'fontweight' : 'bold', 'fontsize' : 20})
plt.grid(True)
plt.title("Counrty wise highest Fifties", fontdict={'color' : 'green', 'fontweight' : 'bold', 'fontsize' : 40})