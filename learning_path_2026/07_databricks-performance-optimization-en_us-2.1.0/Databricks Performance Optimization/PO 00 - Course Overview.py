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
# MAGIC # Databricks Performance Optimization
# MAGIC
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This course teaches you how to identify, diagnose, and resolve the most common performance bottlenecks in Databricks workloads using Spark and Delta Lake.
# MAGIC
# MAGIC You will learn how common performance problems like file explosion, data skew, excessive shuffling, exploding joins, and inefficient user-defined functions can dramatically slow down your Spark workloads. The course focuses on understanding why these issues occur and how to fix them using built-in Databricks and Delta Lake features.
# MAGIC
# MAGIC You will follow a hands-on workflow through five modules, each targeting a specific performance bottleneck. Topics include over-partitioning and the small file problem, data skipping with ZORDER and Liquid Clustering, shuffle mechanics and broadcast joins, exploding joins with spill analysis, and the performance impact of Python UDFs versus SQL UDFs.
# MAGIC
# MAGIC You will also learn how to use the Spark UI to diagnose performance issues, interpret key metrics like cloud storage requests, file pruning, shuffle sizes, and disk spill, and apply targeted optimizations to resolve each bottleneck.
# MAGIC
# MAGIC By the end of the course, you will be able to optimize workloads and physical layout with Spark and Delta Lake and analyze the Spark UI to assess performance and debug applications.
# MAGIC
# MAGIC ## Terminal Objectives
# MAGIC - Navigate the Spark UI to identify jobs, stages, tasks, and key performance metrics (cloud storage requests, file pruning, shuffle size, spill).
# MAGIC - Explain how over-partitioning causes the small file problem, and apply Databricks features such as auto-optimize and Predictive Optimization to maintain healthy file layouts.
# MAGIC - Compare Z-Ordering and Liquid Clustering, and apply Liquid Clustering to improve data skipping on high-cardinality and multi-column filters.
# MAGIC - Diagnose data skew, shuffle, and disk spill in the Spark UI, and apply mitigations including Adaptive Query Execution, broadcast joins, join reordering, shuffle-partition tuning, and ANALYZE TABLE.
# MAGIC - Identify the performance cost of Python UDFs due to serialization, and apply native higher-order functions or Arrow-optimized / Vectorized UDFs to improve execution efficiency.
# MAGIC - Select the appropriate cluster type, instance family, and Photon configuration for a given workload using Databricks rules-of-thumb and the IFTTT cluster-sizing process.
# MAGIC
# MAGIC
# MAGIC ##### Course update and version can be found in the `Version Info` file.

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Prerequisites
# MAGIC
# MAGIC Before starting this course, learners should be comfortable with the following:
# MAGIC
# MAGIC - **General familiarity with the Databricks workspace** - You can navigate the workspace UI, create clusters, run code in notebooks, use basic notebook operations, and import repos from git.
# MAGIC
# MAGIC - **Intermediate programming experience with PySpark** - You can extract data from a variety of file format data sources, apply common transformations to clean data, and reshape and manipulate complex data using advanced built-in functions.
# MAGIC
# MAGIC - **Intermediate programming experience with Unity Catalog Managed Tables** - You can create tables, perform complete and incremental updates, compact files, and restore previous versions.
# MAGIC
# MAGIC - **Basic knowledge of SQL** - You can read and write SQL queries including SELECT, JOIN, GROUP BY, and aggregate functions.
# MAGIC
# MAGIC - **Access to a Databricks workspace** with classic compute (version = 15.4 LTS).

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Workspace Setup Information
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="border-left: 4px solid #f44336; background: #ffebee; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC <div>
# MAGIC <strong style="color: #c62828; font-size: 1.1em;">Prerequisites</strong>
# MAGIC <p style="margin: 8px 0 0 0; color: #333;">This content assumes you have:</p>
# MAGIC
# MAGIC - <strong>Classic Compute (version = 15.4 LTS)</strong> attached to your notebook
# MAGIC - <strong>Unity Catalog</strong> enabled in your workspace
# MAGIC - Permission to <strong>create catalogs and schemas</strong> in your workspace
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #ff9800;
# MAGIC   background: #fff3e0;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <strong style="display:block; color:#e65100; margin-bottom:6px; font-size: 1.1em;">
# MAGIC     Classic Compute Required
# MAGIC   </strong>
# MAGIC   <div style="color:#333;">
# MAGIC
# MAGIC This course requires **Classic Compute** for all demos and labs. Serverless is enabled by default, but Spark configuration settings used throughout the course (such as disabling disk caching and modifying shuffle partitions) are not supported on Serverless. Please ensure you select your classic compute cluster before running any notebook.
# MAGIC
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B1. Databricks Provided Vocareum Workspace (Recommended)
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #1976d2;
# MAGIC   background: #e3f2fd;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <div style="color:#333;">
# MAGIC
# MAGIC - If you are running this notebook in a <strong>Databricks Academy provided Vocareum workspace</strong>, your Unity Catalog catalog is already created for you.
# MAGIC
# MAGIC - Your catalog name matches your Vocareum username and looks like: <strong>labuser12345</strong> (series of unique numbers)
# MAGIC
# MAGIC - If a <strong>Marketplace</strong> dataset is required, the share is already installed and available in the workspace.
# MAGIC
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B2. Databricks Free Edition or Outside Workspaces (*as is*)
# MAGIC
# MAGIC ##### Databricks Free Edition or Other Workspaces may work for this, but it is provided **as is** and support is not guaranteed.
# MAGIC
# MAGIC Some features may not be available depending on the capabilities of Databricks Free Edition or your Workspace.
# MAGIC
# MAGIC Please read below to setup your environment
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #1976d2;
# MAGIC   background: #e3f2fd;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC <div style="color:#333;">
# MAGIC
# MAGIC #### Catalog Information
# MAGIC
# MAGIC - If you are running this notebook in your own Databricks workspace or Databricks Free Edition, the setup will <strong>create a Unity Catalog catalog and schema for you</strong>.
# MAGIC
# MAGIC - The <strong>Create Catalog</strong> permission is required.
# MAGIC
# MAGIC - The catalog name is derived from your Databricks username and follows this pattern: <strong>labuser_username</strong>
# MAGIC
# MAGIC <br></br>
# MAGIC #### Access Marketplace Data
# MAGIC
# MAGIC The lab on Data Skipping and Liquid Clustering uses the **Airline Performance Data** shared through the Databricks Marketplace, provided by **Databricks**. The name of the share is **Airline Performance Data**.
# MAGIC
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #ff9800;
# MAGIC   background: #fff3e0;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC
# MAGIC   <strong style="display:block; color:#e65100; margin-bottom:6px; font-size: 1.1em;">
# MAGIC     Troubleshooting Setup - Your Workspace Can't Create Catalogs
# MAGIC   </strong>
# MAGIC <details>
# MAGIC   <div style="color:#333;">
# MAGIC
# MAGIC If you do not have permission to create a new catalog but already have one available, you can explicitly specify an existing catalog by setting the variable in the common classroom setup notebook: `./Includes/Classroom-Setup-Common`
# MAGIC   </div>
# MAGIC </details>
# MAGIC </div>
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #f44336;
# MAGIC   background: #ffebee;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC <strong style="display:block; color:#c62828; margin-bottom:6px; font-size: 1.1em;">Do Not Run in Production Environments</strong>
# MAGIC
# MAGIC <div style="color:#333;">
# MAGIC <ul>
# MAGIC <li>Only run this course in <strong>development or sandbox workspaces</strong>.</li>
# MAGIC <li>Do not run in production environments. The setup scripts creates catalogs, schemas and tables in your workspace.</li>
# MAGIC </ul>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## C. Course Agenda
# MAGIC
# MAGIC The following modules are part of the **Data Engineer Learning Path** by Databricks Academy.
# MAGIC
# MAGIC <div style="max-width: 1200px; margin: 0 auto; font-family: sans-serif;">
# MAGIC <div style="background: #F9F7F4; border-radius: 10px; padding: 20px 24px; box-shadow: 0 2px 8px rgba(27,49,57,0.06);">
# MAGIC
# MAGIC <style>
# MAGIC .agenda-table td, .agenda-table th {
# MAGIC   font-size: 14pt !important;
# MAGIC }
# MAGIC </style>
# MAGIC
# MAGIC <table class="agenda-table" style="width: 100%; border-collapse: collapse; line-height: 1.5;">
# MAGIC   <thead>
# MAGIC     <tr style="background: #1B5162; color: white;">
# MAGIC       <th style="padding: 10px 14px; text-align: center; border: 1px solid #EEEDE9; width: 50px;">#</th>
# MAGIC       <th style="padding: 10px 14px; text-align: center; border: 1px solid #EEEDE9; width: 80px;">Type</th>
# MAGIC       <th style="padding: 10px 14px; text-align: left; border: 1px solid #EEEDE9;">Module Name</th>
# MAGIC       <th style="padding: 10px 14px; text-align: left; border: 1px solid #EEEDE9;">Key Topics</th>
# MAGIC     </tr>
# MAGIC   </thead>
# MAGIC   <tbody>
# MAGIC     <tr style="background: white;">
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center; font-weight: 700; color: #1B5162;">1</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center;"><span style="background: #e3f2fd; color: #1976d2; padding: 2px 8px; border-radius: 4px; font-weight: 600;">Demo</span></td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9;">PO 1.1 - File Explosion</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; color: #618794;">Over-partitioning, small file problem, Spark UI analysis</td>
# MAGIC     </tr>
# MAGIC     <tr style="background: #F9F7F4;">
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center; font-weight: 700; color: #1B5162;">2</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center;"><span style="background: #fff3e0; color: #e65100; padding: 2px 8px; border-radius: 4px; font-weight: 600;">Lab</span></td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9;">PO 1.2L - Data Skipping and Liquid Clustering</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; color: #618794;">ZORDER, Liquid Clustering, data skipping, file pruning</td>
# MAGIC     </tr>
# MAGIC     <tr style="background: white;">
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center; font-weight: 700; color: #1B5162;">3</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center;"><span style="background: #e3f2fd; color: #1976d2; padding: 2px 8px; border-radius: 4px; font-weight: 600;">Demo</span></td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9;">PO 1.3 - Shuffle</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; color: #618794;">Shuffle mechanics, broadcast joins, aggregations, AQE</td>
# MAGIC     </tr>
# MAGIC     <tr style="background: #F9F7F4;">
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center; font-weight: 700; color: #1B5162;">4</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center;"><span style="background: #fff3e0; color: #e65100; padding: 2px 8px; border-radius: 4px; font-weight: 600;">Lab</span></td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9;">PO 1.4L - Exploding Join</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; color: #618794;">Exploding joins, disk spill, shuffle partitions, join reordering, ANALYZE TABLE</td>
# MAGIC     </tr>
# MAGIC     <tr style="background: white;">
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center; font-weight: 700; color: #1B5162;">5</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center;"><span style="background: #e3f2fd; color: #1976d2; padding: 2px 8px; border-radius: 4px; font-weight: 600;">Demo</span></td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9;">PO 1.5 - User-Defined Functions</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; color: #618794;">Python UDFs, SQL UDFs, repartitioning, Photon optimization</td>
# MAGIC     </tr>
# MAGIC     <tr style="background: #F9F7F4;">
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center; font-weight: 700; color: #1B5162;">6</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center;"><span style="background: #fff3e0; color: #e65100; padding: 2px 8px; border-radius: 4px; font-weight: 600;">Summary</span></td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9;">PO 99 - Summary and Next Steps</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; color: #618794;">Additional Resources</td>
# MAGIC     </tr>
# MAGIC   </tbody>
# MAGIC </table>
# MAGIC
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ### Requirements
# MAGIC
# MAGIC Please review the following requirements before starting the lesson:
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #1976d2;
# MAGIC   background: #e3f2fd;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <div style="color:#333;">
# MAGIC
# MAGIC - Use Databricks Runtime version: **`15.4.x-scala2.12`** (Classic Compute) for running all demo and lab notebooks.
# MAGIC
# MAGIC - **Serverless** is not supported for this course, as Spark configuration settings are modified throughout the demos and labs.
# MAGIC
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>