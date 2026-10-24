-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Sample Silver Table for ML Task

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW silver_sample_ml
AS
SELECT 
    *
FROM LIVE.health_silver
LIMIT 2;
