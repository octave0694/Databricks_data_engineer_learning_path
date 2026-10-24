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
# MAGIC ## Databricks Data Privacy
# MAGIC ---
# MAGIC
# MAGIC ### Prerequisites
# MAGIC
# MAGIC Before beginning this course, you should have the following experience:
# MAGIC
# MAGIC - Proficient in using the Databricks Data Engineering & Data Science workspace:
# MAGIC   - Creating clusters, running code in notebooks, importing repos from Git, etc.
# MAGIC - Intermediate PySpark programming:
# MAGIC   - Extracting data from various file formats and sources
# MAGIC   - Applying common transformations to clean data
# MAGIC   - Manipulating complex data using advanced built-in functions
# MAGIC - Intermediate experience with Delta Lake:
# MAGIC   - Creating tables, performing updates (complete/incremental), file compaction, restoring versions
# MAGIC - Beginner-level experience with Lakeflow Declarative Pipelines UI:
# MAGIC   - Configuring and scheduling data pipelines
# MAGIC - Beginner-level experience defining Lakeflow Declarative Pipelines using Python or SQL:
# MAGIC   - Ingesting and processing data using Auto Loader
# MAGIC   - Applying Change Data Capture with `APPLY CHANGES INTO`
# MAGIC   - Reviewing pipeline logs for troubleshooting
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Course Agenda
# MAGIC
# MAGIC The following modules are part of the **Data Engineer Learning Path** by Databricks Academy.
# MAGIC
# MAGIC | #   | Module Name                                                                 |
# MAGIC |-----|------------------------------------------------------------------------------|
# MAGIC | 1   | [DP 1.1 – Securing Data in Unity Catalog]($./DP 1.1 - Securing Data in Unity Catalog) |
# MAGIC | 2   | [DP 1.2 – PII Data Security]($./DP 1.2 - PII Data Security)                  |
# MAGIC | 3   | [DP 1.3 – Processing Records from CDF and Propagating Changes]($./DP 1.3 - Processing Records from CDF and Propagating Changes) |
# MAGIC | 4   | [DP 1.4L – Propagating Changes with CDF Lab]($./DP 1.4L - Propagating Changes with CDF Lab) |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Requirements
# MAGIC
# MAGIC Please ensure the following before starting:
# MAGIC
# MAGIC - Use Databricks Runtime version: **`16.4.x-scala2.12`** for running all demo and lab notebooks.
# MAGIC
# MAGIC ### Technical Considerations
# MAGIC * This course cannot be delivered on Databricks Free Edition.

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
