-- Databricks notebook source
-- MAGIC %md
-- MAGIC 1. Highest runs scored by batsman

-- COMMAND ----------

select country, player, runs, innings
from default.cricket01_gold
order by runs desc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Highest runs scorer (atlease 10 innings)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC     a. by player

-- COMMAND ----------

select player, country, runs
from cricket01_gold
order by runs desc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC     b. by each country

-- COMMAND ----------

with cte as (select country, player, runs, rank() over(partition by country order by runs desc) as rnk 
from default.cricket01_gold where innings >= 10)
select country, player, runs as highes_runs from cte
where rnk = 1
order by runs desc;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Country wise player with highest batting average (atleast 10 innings)

-- COMMAND ----------

select s.country, s.player, s.matches, s.innings, s.batting_average
from (select player, matches, innings, batting_average, country, rank() over(partition by country order by batting_average desc) as rnk
from cricket01_gold
where innings >= 10) as s
where s.rnk = 1
order by s.batting_average desc
;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 4. Country wise player with highest Centuries (atleast 10 innings)

-- COMMAND ----------

select s.country, s.player, s.matches, s.innings, s.centuries
from (select player, matches, innings, centuries, country, rank() over(partition by country order by centuries desc, innings asc) as rnk
from cricket01_gold
where innings >= 10) as s
where s.rnk = 1
order by s.centuries desc
;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 5. Country wise player with highest Fifties (atleast 10 innings)

-- COMMAND ----------

select s.country, s.player, s.matches, s.innings, s.fifty
from (select player, matches, innings, fifty, country, rank() over(partition by country order by fifty desc, innings asc) as rnk
from cricket01_gold
where innings >= 10) as s
where s.rnk = 1
order by s.fifty desc
;