# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img
# MAGIC     src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png"
# MAGIC     alt="Databricks Learning"
# MAGIC   >
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Databricks Streaming and Lakeflow Declarative Pipelines (Formerly DLT)
# MAGIC
# MAGIC This course provides a comprehensive understanding of Spark Structured Streaming and Delta Lake, including computation models, configuration for streaming reads, and maintaining data quality in a streaming environment.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Prerequisites
# MAGIC You should meet the following prerequisites before starting this course:
# MAGIC
# MAGIC - Ability to perform basic code development tasks using the Databricks Data Engineering and Data Science workspace (`create clusters`, `run code in notebooks`, `use basic notebook operations`, `import repos from git`, etc.)
# MAGIC - Intermediate programming experience with PySpark
# MAGIC - Extract data from a variety of file formats and data sources
# MAGIC - Apply common transformations to clean data
# MAGIC - Reshape and manipulate complex data using advanced built-in functions
# MAGIC - Intermediate programming experience with Delta Lake (`create tables`, `perform complete and incremental updates`, `compact files`, `restore previous versions`, etc.)
# MAGIC
# MAGIC
# MAGIC Beginner experience configuring and scheduling data pipelines using the Delta Live Tables UI
# MAGIC
# MAGIC - Beginner experience defining Lakeflow Declarative pipelines using PySpark
# MAGIC - Ingest and process data using Auto Loader and PySpark syntax
# MAGIC - Process Change Data Capture feeds with `APPLY CHANGES INTO` syntax
# MAGIC - Review pipeline event logs and results to troubleshoot Declarative Pipeline syntax
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Course Agenda
# MAGIC The following modules are part of the **Data Engineer Learning Path** by Databricks Academy.
# MAGIC
# MAGIC | # | Module Name |
# MAGIC | --- | --- |
# MAGIC | 1 | [Incremental Processing with Spark Structured Streaming]($./SDLT 1 - Incremental Processing with Spark Structured Streaming/SDLT 1.0 - Module Introduction) |
# MAGIC | 2 | [Streaming ETL Patterns with Lakeflow Declarative Pipelines]($./SDLT 2 - Streaming ETL Patterns with Lakeflow Declarative Pipelines/SDLT 2.0 - Module Introduction) |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Requirements
# MAGIC
# MAGIC Please review the following requirements before starting the lesson:
# MAGIC
# MAGIC - Use Databricks Runtime version: **`16.4.x-scala2.12`** for running all demo and lab notebooks.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
