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
# MAGIC ## Incremental Processing with Spark Structured Streaming
# MAGIC This module is part of the Data Engineer Learning Path by Databricks Academy. 
# MAGIC
# MAGIC #### Lessons
# MAGIC Lecture: Streaming Data Concepts (slides available in .pdf)<br>
# MAGIC Lecture: Introduction to Structured Streaming (slides available in .pdf) <br>
# MAGIC [SDLT 1.1 - Reading from a Streaming Query]($./SDLT 1.1 - Reading from a Streaming Query) <br>
# MAGIC [SDLT 1.2L - Streaming Query Lab]($./SDLT 1.2L - Streaming Query Lab) <br>
# MAGIC Lecture: Aggregations, Time Windows, Watermarks (slides available in .pdf) <br>
# MAGIC [SDLT 1.3L - Stream Aggregations Lab]($./SDLT 1.3L - Stream Aggregations Lab) <br>
# MAGIC [SDLT 1.4 - Windowed Aggregation with Watermark]($./SDLT 1.4 - Windowed Aggregation with Watermark) <br> 
# MAGIC [SDLT 1.5 - Stream-Stream Joins (Optional)]($./SDLT 1.5 - Optional - Stream-Stream Joins) <br> 
# MAGIC
# MAGIC #### Prerequisites
# MAGIC * Ability to perform basic code development tasks using the Databricks Data Engineering & Data Science workspace (create clusters, run code in notebooks, use basic notebook operations, import repos from git, etc)
# MAGIC * Intermediate programming experience with PySpark
# MAGIC   * Extract data from a variety of file formats and data sources
# MAGIC   * Apply a number of common transformations to clean data
# MAGIC   * Reshape and manipulate complex data using advanced built-in functions
# MAGIC * Intermediate programming experience with Delta Lake (create tables, perform complete and incremental updates, compact files, restore previous versions etc.)
# MAGIC
# MAGIC #### Technical Considerations
# MAGIC * This course runs on **`16.4.x-scala2.12`**.
# MAGIC * This course cannot be delivered on Databricks Community Edition.

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
