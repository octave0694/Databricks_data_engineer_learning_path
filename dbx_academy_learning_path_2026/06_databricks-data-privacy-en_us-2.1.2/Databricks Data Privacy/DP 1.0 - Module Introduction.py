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
# MAGIC
# MAGIC This content provides a comprehensive guide to managing data privacy within Databricks. It covers key topics like Delta Lake architecture, regional data isolation, GDPR/CCPA compliance, and Change Data Feed (CDF) usage. Through practical demos and hands-on labs, participants learn to use Unity Catalog features for securing sensitive data and ensuring compliance, empowering them to safeguard data integrity effectively.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Prerequisites
# MAGIC
# MAGIC - Ability to perform basic code development tasks using the Databricks Data Engineering & Data Science workspace (create clusters, run code in notebooks, use basic notebook operations, import repos from git, etc)
# MAGIC - Intermediate programming experience with PySpark
# MAGIC - Extract data from a variety of file formats and data sources
# MAGIC - Apply a number of common transformations to clean data
# MAGIC - Reshape and manipulate complex data using advanced built-in functions
# MAGIC - Intermediate programming experience with Delta Lake (create tables, perform complete and incremental updates, compact files, restore previous versions etc.)
# MAGIC - Beginner experience configuring and scheduling data pipelines using the Lakeflow Pipelines Editor
# MAGIC - Beginner experience defining Apache Spark™ Declarative Pipelines using PySpark
# MAGIC - Ingest and process data using Auto Loader and PySpark syntax
# MAGIC - Process Change Data Capture feeds with APPLY CHANGES INTO syntax
# MAGIC - Review pipeline event logs and results to troubleshoot DLT syntax
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
# MAGIC - Use Databricks Runtime version: **`17.3.x-scala2.13`** for running all demo and lab notebooks.
# MAGIC
# MAGIC ### Technical Considerations
# MAGIC * This course cannot be delivered on Databricks Free Edition.

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>