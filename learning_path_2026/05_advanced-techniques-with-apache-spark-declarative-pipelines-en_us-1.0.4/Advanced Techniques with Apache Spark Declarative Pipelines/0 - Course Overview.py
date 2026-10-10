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
# MAGIC # Advanced Techniques with Apache Spark™ Declarative Pipelines
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This course provides a hands-on exploration of **Apache Spark™ Declarative Pipelines (SDP)** —Databricks modern framework for building production-grade streaming pipelines. You will learn advanced pipeline design patterns, data quality enforcement, and cross-platform integration essential for real-world lakehouse engineering.
# MAGIC
# MAGIC The course starts with **Multi-Flow pipelines**, ingesting from multiple sources into a single target table using explicit flows. You will then explore **Liquid Clustering** for adaptive data layout optimization and **Data Quality Expectations** for enforcing business rules at every pipeline stage.
# MAGIC
# MAGIC Next, you will learn the **Multiplex Streaming pattern** for processing mixed-schema event streams, connecting with **Delta Sinks** and **Iceberg Reads via Delta UniForm** for cross-platform access to live streaming data.
# MAGIC
# MAGIC The course also covers **Change Data Capture (CDC)** —including **SCD Type 1** (current state) and **SCD Type 2** (full history tracking)—automated with `AUTO CDC INTO` in SDP.
# MAGIC
# MAGIC Finally, you will implement advanced data quality techniques, including range-based constraints, NULL-tolerant expressions, schema evolution handling, and the **Quarantine Pattern** for zero-data-loss pipelines with full audit trails.
# MAGIC
# MAGIC Through lectures and hands-on demos, you will:
# MAGIC
# MAGIC - Build **multi-flow pipelines** ingesting data from multiple retail subsidiaries into a single Bronze streaming table
# MAGIC - Apply **Liquid Clustering** and **Data Quality Expectations** to Silver and Gold tables
# MAGIC - Implement the **Multiplex pattern** with Delta Sinks and Iceberg UniForm for cross-platform access
# MAGIC - Automate **SCD Type 2 history tracking** using `AUTO CDC INTO`
# MAGIC - Design **quarantine-based quality pipelines** that preserve invalid records for audit and remediation
# MAGIC
# MAGIC By the end of this course, you will be equipped to design and build enterprise-grade streaming pipelines on Databricks that are reliable, observable, and production-ready.
# MAGIC
# MAGIC
# MAGIC ## Terminal Objectives
# MAGIC - Build **multi-flow Bronze streaming tables** consolidating CSV and JSON data from multiple sources using explicit **`CREATE FLOW`** definitions
# MAGIC
# MAGIC - Create **incremental Gold materialized views** and apply **Unity Catalog tags** to Bronze, Silver, and Gold objects for governance and discoverability
# MAGIC
# MAGIC - Implement the **Multiplex pattern** to ingest a mixed-schema event stream into a single Bronze table using the **`VARIANT`** data type and fan out into domain-specific Silver tables
# MAGIC
# MAGIC - Build a **Delta Sink** using **`dp.create_sink`** and **`@dp.append_flow`**, then enable **Iceberg reads via Delta UniForm** for cross-platform analytics access
# MAGIC
# MAGIC - Automate a **CDC pipeline** using **`AUTO CDC INTO`** with **SCD Type 2** to track customer INSERT, UPDATE, and DELETE events with full history, surfaced via Gold materialized views
# MAGIC
# MAGIC - Implement **advanced data quality expectations** covering NOT NULL, numeric range, and NULL-tolerant constraints, and handle **schema evolution** in Bronze using **`schemaHints`** and **`_rescued_data`**
# MAGIC
# MAGIC - Apply the **Quarantine Pattern** using **inverse logic** to route invalid records without data loss, and monitor per-constraint violation metrics in the **Pipelines UI**
# MAGIC
# MAGIC
# MAGIC
# MAGIC #####NOTE: Course updates and version details can be found in the `Version Info` file.

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Prerequisites
# MAGIC
# MAGIC Before starting this course, learners should be comfortable with the following:
# MAGIC
# MAGIC 1. **Spark Declarative Pipelines** — Completion of the **"Build Data Pipelines with Apache Spark Declarative Pipelines"** course or familiarity with `CREATE OR REFRESH STREAMING TABLE`, `CONSTRAINTS`, and the Pipelines UI.
# MAGIC
# MAGIC 2. **Delta Lake Fundamentals** — Understanding Delta tables, and how Delta manages data files and transaction logs.
# MAGIC
# MAGIC 3. **Streaming Concepts** — Knowledge of micro-batch streaming, checkpointing, and event-time processing in SDP.
# MAGIC
# MAGIC 4. **SQL Proficiency** — Ability to read and write SQL, including `SELECT`, `JOIN`, `MERGE`, `CASE WHEN`, and common aggregate functions.
# MAGIC
# MAGIC 5. **Python in Databricks Notebooks** — Comfort with reading and running Python code in Databricks notebooks.
# MAGIC
# MAGIC 6. **Unity Catalog Basics** — Understanding catalogs, schemas, tables, and volumes in Unity Catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Workspace Setup Information

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
# MAGIC ### B2. Databricks Free Edition (*as is*)
# MAGIC
# MAGIC ##### Databricks Free Edition may work for this course, but it is provided **as is** and support is not guaranteed.  
# MAGIC
# MAGIC Some features may not be available depending on the capabilities of Databricks Free Edition.
# MAGIC
# MAGIC Please read below to setup your environment for Databricks Free Edition
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
# MAGIC If you are running this in your own Workspace, complete the following steps to get your own copy of the Marketplace data. If you already have this share simply add that name to the variable below.
# MAGIC
# MAGIC 1. Open **Databricks Marketplace** in a new tab.  
# MAGIC
# MAGIC 2. Search for `Simulated Retail Customer Data`.  
# MAGIC
# MAGIC 3. Select the tile titled **Simulated Retail Customer Data (Databricks provided)**.  
# MAGIC
# MAGIC 4. Click **Get instant access**.  
# MAGIC
# MAGIC 5. **Enter a unique catalog name** for your share to avoid receiving a duplicate catalog error in shared Workspaces. For example: `dbacademy_retail_yourname`.  
# MAGIC
# MAGIC 6. Review and accept the terms, then click **Get instant access** to complete the setup.
# MAGIC
# MAGIC 7. Update the variable `your_marketplace_share_catalog_name` in cell below to point to your shared catalog from Marketplace.
# MAGIC   </div>
# MAGIC </details>
# MAGIC </div>
# MAGIC
# MAGIC </div>
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
# MAGIC <li>Do not run in production environments. The setup scripts creates catalogs, schemas and pipelines in your workspace.</li>
# MAGIC </ul>
# MAGIC
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B3. Bring Your Own Workspace (BYOW)
# MAGIC
# MAGIC If you are running this course in your **own Databricks workspace**, complete the following before starting Module 1:
# MAGIC
# MAGIC 1. **Databricks Runtime** — Use **DBR 15.4 LTS** or above (required for Spark Declarative Pipelines and Delta UniForm features).
# MAGIC
# MAGIC 2. **Unity Catalog** — Ensure Unity Catalog is enabled and you have `CREATE CATALOG` or `USE CATALOG` privileges.
# MAGIC
# MAGIC 3. **Marketplace Dataset** — This course uses the `dbacademyretail` shared catalog from Databricks Marketplace. Attach it to your workspace before running any demo notebooks.
# MAGIC
# MAGIC 4. **Cluster Policy** — Use a Single-Node or Standard cluster with Photon enabled for best performance with Liquid Clustering demos.
# MAGIC
# MAGIC 5. **Pipeline Settings** — All pipeline notebooks are pre-configured with catalog and schema variables. Update the `mycatalog` variable in each demo notebook to match your target catalog.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>