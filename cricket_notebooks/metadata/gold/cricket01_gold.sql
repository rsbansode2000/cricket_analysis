-- Databricks notebook source
DROP TABLE IF EXISTS cricket01_gold;
CREATE TABLE cricket01_gold (
  player STRING COMMENT 'Player name',
  country STRING,
  matches INTEGER,
  innings INTEGER,
  not_out INTEGER,
  runs INTEGER,
  highest_score INTEGER,
  highest_score_not_out STRING,
  batting_average DECIMAL(5,2),
  strike_rate DECIMAL(5,2),
  centuries INTEGER,
  fifty INTEGER,
  zero INTEGER,
  `from` STRING,
  `to` STRING,
  load_date TIMESTAMP
)
USING DELTA
PARTITIONED BY (country)
LOCATION 'dbfs:/FileStore/project/cricket/gold/cricket01_gold/'
TBLPROPERTIES (
  'delta.constraints.primaryKey' = 'player',
  'delta.constraints.primaryKey.enabled' = 'true'
);

-- COMMAND ----------

-- %python
-- dbutils.fs.rm("dbfs:/FileStore/project/cricket/gold/cricket01_gold/", True)
