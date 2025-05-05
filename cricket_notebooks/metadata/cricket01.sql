-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS mydb;

-- COMMAND ----------

DROP TABLE IF EXISTS cricket01;
CREATE TABLE cricket01 (
  player STRING COMMENT 'Player name',
  span STRING COMMENT 'span',
  matches INTEGER,
  innings INTEGER,
  not_out INTEGER,
  runs INTEGER,
  highest_score STRING,
  batting_average DECIMAL(5,2),
  strike_rate DECIMAL(5,2),
  centuries INTEGER,
  fifty INTEGER,
  zero INTEGER,
  load_date TIMESTAMP
)
USING DELTA
LOCATION 'dbfs:/FileStore/project/cricket/silver/cricket01/'
TBLPROPERTIES (
  'delta.constraints.primaryKey' = 'player',
  'delta.constraints.primaryKey.enabled' = 'true'
);


-- COMMAND ----------

-- %py
-- dbutils.fs.rm("dbfs:/FileStore/project/cricket/silver/cricket01/", True)