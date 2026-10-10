# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC ![Databricks Academy](https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/databricks_academy.png)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Lecture - Introduction to Data Engineering in Databricks
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC This lecture introduces how Databricks supports efficient data engineering by combining optimized storage techniques, unified data governance through **Unity Catalog**, and **Lakeflow's integrated capabilities** for data ingestion, transformation, and orchestration. It then focuses on **Apache Spark™ Declarative Pipelines**, a low-code, declarative framework for building, managing, and automating reliable batch and streaming data pipelines using SQL or Python.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lecture, you will be able to:
# MAGIC
# MAGIC - **Understand** the components of Data Engineering with Lakeflow in Databricks with Connect, Spark Declarative Pipelines, and Jobs
# MAGIC - **Explain** how Apache Spark™ Declarative Pipelines incrementally processes data using Streaming Tables and Materialized Views in batch or streaming Jobs

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## A. Data Engineering Platform Overview
# MAGIC
# MAGIC It all begins with optimized storage using Delta Lake, Parquet, or Iceberg, built upon by unified governance with Unity Catalog, and powered by Lakeflow to deliver end-to-end, high-quality data engineering for analytics and AI.
# MAGIC
# MAGIC <div style="max-width:1000px;margin:0 auto;font-family:sans-serif;color:#0b2026;line-height:1.2;"><div style="display:flex;justify-content:center;position:relative;z-index:2;margin-bottom:-1px;"><div style="border:1px solid #FFD3CB;border-bottom:none;background:#fff;border-radius:10px 10px 0 0;padding:6px 22px;"><span style="font-size:18pt;font-weight:800;letter-spacing:-0.5px;">databricks</span> <span style="font-size:18pt;font-weight:800;color:#FF5F46;margin-left:6px;">LAKEFLOW</span></div></div><div style="border:1px solid #FFD3CB;border-radius:10px;padding:14px;"><div style="text-align:center;color:#FF5F46;font-weight:800;font-size:14pt;letter-spacing:0.5px;margin-bottom:12px;">UNIFIED DATA ENGINEERING FOR THE DATA INTELLIGENCE PLATFORM</div><div style="display:flex;gap:12px;margin-bottom:12px;flex-wrap:wrap;"><div style="flex:1;min-width:200px;border:1px solid #EEEDE9;border-radius:6px;overflow:hidden;box-shadow:0 1px 4px rgba(27,49,57,0.06);"><div style="background:#FF5F46;color:#fff;font-weight:800;font-size:14pt;text-align:center;letter-spacing:0.5px;min-height:56px;display:flex;align-items:center;justify-content:center;padding:8px 10px;">CONNECT</div><div style="padding:11px 14px;text-align:center;font-weight:700;font-size:14pt;">Efficient ingestion connectors</div></div><div style="flex:1;min-width:200px;border:1px solid #EEEDE9;border-radius:6px;overflow:hidden;box-shadow:0 1px 4px rgba(27,49,57,0.06);"><div style="background:#FF5F46;color:#fff;font-weight:800;font-size:14pt;text-align:center;letter-spacing:0.5px;min-height:56px;display:flex;align-items:center;justify-content:center;padding:8px 10px;">APACHE SPARK DECLARATIVE PIPELINES (SDP)</div><div style="padding:11px 14px;text-align:center;font-weight:700;font-size:14pt;">Accelerated ETL development</div></div><div style="flex:1;min-width:200px;border:1px solid #EEEDE9;border-radius:6px;overflow:hidden;box-shadow:0 1px 4px rgba(27,49,57,0.06);"><div style="background:#FF5F46;color:#fff;font-weight:800;font-size:14pt;text-align:center;letter-spacing:0.5px;min-height:56px;display:flex;align-items:center;justify-content:center;padding:8px 10px;">JOBS</div><div style="padding:11px 14px;text-align:center;font-weight:700;font-size:14pt;">Reliable orchestration for analytics and AI</div></div></div><div style="background:#FF5F46;color:#fff;border-radius:6px;text-align:center;padding:13px 14px;"><div style="font-weight:800;font-size:16pt;letter-spacing:0.5px;">INDUSTRY LEADING DATA PROCESSING ENGINE</div><div style="font-weight:700;font-size:14pt;margin-top:3px;">(Apache Spark + Structured Streaming)</div></div></div><div style="display:flex;margin-top:8px;border:1px solid #EEEDE9;border-radius:6px;overflow:hidden;"><div style="background:#618794;color:#fff;font-weight:800;font-size:14pt;letter-spacing:0.5px;padding:10px 14px;width:240px;display:flex;align-items:center;">UNIFIED GOVERNANCE</div><div style="flex:1;background:#fff;display:flex;align-items:center;justify-content:center;font-weight:800;font-size:14pt;padding:8px;">Unity Catalog</div></div><div style="display:flex;margin-top:6px;border:1px solid #EEEDE9;border-radius:6px;overflow:hidden;"><div style="background:#618794;color:#fff;font-weight:800;font-size:14pt;letter-spacing:0.5px;padding:10px 14px;width:240px;display:flex;align-items:center;">OPTIMIZED STORAGE</div><div style="flex:1;background:#fff;display:flex;align-items:center;justify-content:center;gap:24px;font-weight:800;font-size:14pt;padding:8px;flex-wrap:wrap;"><span>Delta Lake</span><span style="color:#DCE0E2;font-weight:400;">|</span><span>Parquet</span><span style="color:#DCE0E2;font-weight:400;">|</span><span>Iceberg</span></div></div></div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC It all begins with optimized storage using Delta Lake, Parquet, or Iceberg.
# MAGIC
# MAGIC Built on top of this storage layer is unified governance with Unity Catalog. Unity Catalog is a centralized data catalog that provides access control, auditing, data lineage, quality monitoring, and data discovery across Databricks workspaces.
# MAGIC
# MAGIC Databricks then offers Lakeflow, an end-to-end data engineering solution that empowers data engineers, software developers, SQL developers, analysts, and data scientists to deliver high-quality data for downstream analytics, AI, and operational applications. Lakeflow provides a unified platform for data ingestion, transformation, and orchestration, and includes the following components:
# MAGIC
# MAGIC - **Lakeflow Connect**: A set of efficient ingestion connectors that simplify data ingestion from popular enterprise applications, databases, cloud storage, message buses, and local files.
# MAGIC
# MAGIC - **Apache Spark™ Declarative Pipelines**: A framework for building batch and streaming data pipelines using SQL and Python, designed to accelerate ETL development.
# MAGIC
# MAGIC - **Lakeflow Jobs**: A workflow automation tool for Databricks that orchestrates data processing tasks and workflows. It enables coordination of multiple tasks within complex workflows, allowing for the scheduling, optimization, and management of repeatable processes.
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## B. Apache Spark™ Declarative Pipelines
# MAGIC
# MAGIC
# MAGIC <div style="text-align: center; margin-top: 20px;">
# MAGIC   <img
# MAGIC     src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/lecture_data_engineering_databricks/spark_declarative_pipeline.png"
# MAGIC     alt="Apache Spark™ Declarative Pipeline"
# MAGIC     style="width: 1000px; max-width: 100%; height: auto;">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC This course will focus on simplifying batch and streaming ETL with automated reliability, optimizations, and built-in data quality with Apache Spark™ Declarative Pipelines.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## C. It’s Difficult to Build and Operate Reliable Data Pipelines

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### C1. Challenges in Building Reliable Data Pipelines

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; box-sizing: border-box; width: 1000px; margin: 0 auto; padding: 12px; background: #ffffff;">
# MAGIC  <!-- Top row -->
# MAGIC  <div style="display: flex; gap: 10px; margin-bottom: 10px;">
# MAGIC   <div style="flex: 1; border: 2px solid #B0173C; border-radius: 8px; padding: 10px 12px; display: flex; align-items: center; gap: 8px;">
# MAGIC    <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/version_control.png" alt="Version Control icon" style="height: 24px; background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1);">
# MAGIC    <span style="font-size: 16px; font-weight: 600;">Version Control</span>
# MAGIC   </div>
# MAGIC   <div style="flex: 1; border: 2px solid #B0173C; border-radius: 8px; padding: 10px 12px; display: flex; align-items: center; gap: 8px;">
# MAGIC    <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/deployment_infrastructure.png" alt="Deployment Infrastructure icon" style="height: 24px; background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1);">
# MAGIC    <span style="font-size: 16px; font-weight: 600;">Deployment Infrastructure</span>
# MAGIC   </div>
# MAGIC  </div>
# MAGIC  <!-- Second row -->
# MAGIC  <div style="display: flex; gap: 10px; margin-bottom: 10px;">
# MAGIC   <div style="flex: 1; border: 2px solid #4C7780; border-radius: 8px; padding: 10px 12px; display: flex; align-items: center; gap: 8px;">
# MAGIC    <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/quality_checks.png" alt="Quality Checks icon" style="height: 24px; background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1);">
# MAGIC    <span style="font-size: 16px; font-weight: 600;">Quality Checks</span>
# MAGIC   </div>
# MAGIC   <div style="flex: 1; border: 2px solid #4C7780; border-radius: 8px; padding: 10px 12px; display: flex; align-items: center; gap: 8px;">
# MAGIC    <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/governance.png" alt="Governance icon" style="height: 24px; background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1);">
# MAGIC    <span style="font-size: 16px; font-weight: 600;">Governance</span>
# MAGIC   </div>
# MAGIC   <div style="flex: 1; border: 2px solid #F3A72B; border-radius: 8px; padding: 10px 12px; display: flex; align-items: center; gap: 8px;">
# MAGIC    <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/data_discovery.png" alt="Data Discovery icon" style="height: 24px; background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1);">
# MAGIC    <span style="font-size: 16px; font-weight: 600;">Data Discovery</span>
# MAGIC   </div>
# MAGIC  </div>
# MAGIC  <!-- Middle band -->
# MAGIC  <div style="display: grid; grid-template-columns: 1.2fr 2fr 1.2fr; gap: 10px; margin-bottom: 14px; align-items: stretch;">
# MAGIC   <!-- Left column -->
# MAGIC   <div style="display: flex; flex-direction: column; gap: 10px;">
# MAGIC    <div style="border: 2px solid #F3A72B; border-radius: 8px; padding: 10px 12px; display: flex; flex-direction: column; gap: 8px;">
# MAGIC     <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/backfill_handling_gear.png" alt="Backfill Handling icon" style="height: 28px; background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1); align-self: flex-start;">
# MAGIC     <span style="font-size: 16px; font-weight: 600; line-height: 1.2;">Backfill<br>Handling</span>
# MAGIC    </div>
# MAGIC    <div style="border: 2px solid #F3A72B; border-radius: 8px; padding: 10px 12px; display: flex; flex-direction: column; gap: 8px;">
# MAGIC     <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/dependency_management.png" alt="Dependency Management icon" style="height: 28px; align-self: flex-start; background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1);">
# MAGIC     <span style="font-size: 15px; font-weight: 600; line-height: 1.2;">Dependency<br>Management</span>
# MAGIC    </div>
# MAGIC   </div>
# MAGIC   <!-- Center -->
# MAGIC   <div style="border: 2px solid #16333A; border-radius: 8px; padding: 18px; display: flex; align-items: center; justify-content: center; text-align: center;">
# MAGIC    <span style="font-size: 20px; font-weight: 600; color: #FF6347; line-height: 1.3;">[your transformation logic]</span>
# MAGIC   </div>
# MAGIC   <!-- Right column -->
# MAGIC   <div style="display: flex; flex-direction: column; gap: 10px;">
# MAGIC    <div style="border: 2px solid #F3A72B; border-radius: 8px; padding: 10px 12px; display: flex; flex-direction: column; gap: 8px;">
# MAGIC     <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/daily_partition_computation.png" alt="Daily Partition Computation icon" style="height: 28px; align-self: flex-start; background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1);">
# MAGIC     <span style="font-size: 15px; font-weight: 600; line-height: 1.2;">Daily Partition<br>Computation</span>
# MAGIC    </div>
# MAGIC    <div style="border: 2px solid #F3A72B; border-radius: 8px; padding: 10px 12px; display: flex; flex-direction: column; gap: 8px;">
# MAGIC     <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/checkpointing_retries.png" alt="Checkpointing &amp; Retries icon" style="height: 28px; align-self: flex-start; background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1);">
# MAGIC     <span style="font-size: 15px; font-weight: 600; line-height: 1.2;">Checkpointing<br>&amp; Retries</span>
# MAGIC    </div>
# MAGIC   </div>
# MAGIC  </div>
# MAGIC  <!-- Bottom platform -->
# MAGIC  <div style="border: 2px solid #16333A; border-radius: 14px; padding: 16px 14px 18px; margin-top: 12px; position: relative; overflow: visible;">
# MAGIC   <div style="display: flex; justify-content: center; gap: 14px; flex-wrap: wrap; align-items: center;">
# MAGIC    <div style="width: 92px; height: 92px; border-radius: 50%; background: #ffffff; box-shadow: 0 2px 8px rgba(0,0,0,0.08); display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 4px;">
# MAGIC     <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/streaming_icon.png" alt="Streaming icon" style="height: 24px; background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1);">
# MAGIC     <span style="font-size: 12px;">Streaming</span>
# MAGIC    </div>
# MAGIC    <div style="width: 118px; height: 118px; border-radius: 50%; background: #ffffff; box-shadow: 0 2px 8px rgba(0,0,0,0.08); display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 4px;">
# MAGIC     <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/data_lake.png" alt="Data Lake icon" style="height: 28px; background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1);">
# MAGIC     <span style="font-size: 13px;">Data Lake</span>
# MAGIC    </div>
# MAGIC    <div style="width: 104px; height: 104px; border-radius: 50%; background: #ffffff; box-shadow: 0 2px 8px rgba(0,0,0,0.08); display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 4px;">
# MAGIC     <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/orchestration.png" alt="Orchestration icon" style="height: 24px; background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1);">
# MAGIC     <span style="font-size: 12px;">Orchestration</span>
# MAGIC    </div>
# MAGIC    <div style="width: 92px; height: 92px; border-radius: 50%; background: #ffffff; box-shadow: 0 2px 8px rgba(0,0,0,0.08); display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 4px;">
# MAGIC     <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/data_science.png" alt="Data Science icon" style="height: 22px; background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1);">
# MAGIC     <span style="font-size: 12px;">Data Science</span>
# MAGIC    </div>
# MAGIC    <div style="width: 78px; height: 78px; border-radius: 50%; background: #ffffff; box-shadow: 0 2px 8px rgba(0,0,0,0.08); display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 3px;">
# MAGIC     <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/machine_learning.png" alt="Machine Learning icon" style="height: 18px; background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1);">
# MAGIC     <span style="font-size: 10px; text-align:center;">Machine<br>Learning</span>
# MAGIC    </div>
# MAGIC    <div style="width: 118px; height: 118px; border-radius: 50%; background: #ffffff; box-shadow: 0 2px 8px rgba(0,0,0,0.08); display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 4px;">
# MAGIC     <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/generative_ai.png" alt="Generative AI icon" style="height: 28px; background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1);">
# MAGIC     <span style="font-size: 13px;">Generative AI</span>
# MAGIC    </div>
# MAGIC    <div style="width: 110px; height: 110px; border-radius: 50%; background: #ffffff; box-shadow: 0 2px 8px rgba(0,0,0,0.08); display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 4px;">
# MAGIC     <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/data_warehouse.png" alt="Data Warehouse icon" style="height: 24px; background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1);">
# MAGIC     <span style="font-size: 12px;">Data Warehouse</span>
# MAGIC    </div>
# MAGIC    <div style="width: 84px; height: 84px; border-radius: 50%; background: #ffffff; box-shadow: 0 2px 8px rgba(0,0,0,0.08); display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 3px;">
# MAGIC     <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/BI.png" alt="BI icon" style="height: 20px; background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1);">
# MAGIC     <span style="font-size: 11px;">BI</span>
# MAGIC    </div>
# MAGIC   </div>
# MAGIC  </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC Your data engineers just want to focus on transforming and aggregating data—getting it into the right shape for the business.
# MAGIC
# MAGIC - But first, they have to figure out where the data lives—spread across both the data lake and the data warehouse.
# MAGIC - Then, they need to make it work for BI reporting, data science, and machine learning.
# MAGIC - They’re supporting streaming pipelines for new use cases, enabling Generative AI projects, and managing orchestration—all at once.
# MAGIC - And that’s before they even get to version control, CI/CD, and deployment infrastructure.
# MAGIC - On top of that, they’re handling data quality checks, governance, and discovery—each with its own complexity.
# MAGIC - Finally, there’s the operational heavy lifting: hand-coding backfills, managing dependencies, partitions, checkpointing, retries…
# MAGIC
# MAGIC All of this when what you really care about is just delivering reliable data.
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### C2. Why Reliable Data Pipelines Are Hard to Build
# MAGIC
# MAGIC It's difficult to build and operate reliable data pipelines for a variety of reasons.
# MAGIC
# MAGIC <style>
# MAGIC   .flip-grid {
# MAGIC     display: flex;
# MAGIC     justify-content: space-between;
# MAGIC     gap: 32px;
# MAGIC     max-width: 1100px;
# MAGIC     margin: 32px auto;
# MAGIC     font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
# MAGIC   }
# MAGIC
# MAGIC   .flip-card {
# MAGIC     background: transparent;
# MAGIC     width: 320px;
# MAGIC     height: 220px;
# MAGIC     perspective: 1000px;
# MAGIC     flex-shrink: 0;
# MAGIC   }
# MAGIC
# MAGIC   .flip-card-inner {
# MAGIC     position: relative;
# MAGIC     width: 100%;
# MAGIC     height: 100%;
# MAGIC     transition: transform 0.9s;
# MAGIC     transform-style: preserve-3d;
# MAGIC   }
# MAGIC
# MAGIC   .flip-card:hover .flip-card-inner {
# MAGIC     transform: rotateY(180deg);
# MAGIC   }
# MAGIC
# MAGIC   .flip-card-front,
# MAGIC   .flip-card-back {
# MAGIC     position: absolute;
# MAGIC     width: 100%;
# MAGIC     height: 100%;
# MAGIC     -webkit-backface-visibility: hidden;
# MAGIC     backface-visibility: hidden;
# MAGIC     border-radius: 8px;
# MAGIC     box-sizing: border-box;
# MAGIC     display: flex;
# MAGIC     align-items: center;
# MAGIC     justify-content: center;
# MAGIC     text-align: center;
# MAGIC     padding: 24px 28px;
# MAGIC     border: 2px solid rgba(0, 0, 0, 0.08);
# MAGIC     box-shadow: 0 4px 10px rgba(0,0,0,0.08);
# MAGIC   }
# MAGIC
# MAGIC   .flip-card-front {
# MAGIC     background: #ffffff;
# MAGIC     color: #111111;
# MAGIC     font-size: 20px;
# MAGIC     font-weight: 600;
# MAGIC   }
# MAGIC
# MAGIC   .flip-card-back {
# MAGIC     background: #ff6b4a;
# MAGIC     color: #ffffff;
# MAGIC     transform: rotateY(180deg);
# MAGIC     font-size: 16px;
# MAGIC     line-height: 1.4;
# MAGIC     text-align: left;
# MAGIC     justify-content: center;
# MAGIC     flex-direction: column;
# MAGIC   }
# MAGIC
# MAGIC   .flip-card-back strong {
# MAGIC     display: block;
# MAGIC     margin-bottom: 8px;
# MAGIC   }
# MAGIC
# MAGIC   @media screen and (max-width: 900px) {
# MAGIC     .flip-grid {
# MAGIC       flex-direction: column;
# MAGIC       align-items: center;
# MAGIC     }
# MAGIC
# MAGIC     .flip-card {
# MAGIC       width: 100%;
# MAGIC       max-width: 360px;
# MAGIC     }
# MAGIC   }
# MAGIC </style>
# MAGIC
# MAGIC <div class="flip-grid">
# MAGIC   <div class="flip-card">
# MAGIC     <div class="flip-card-inner">
# MAGIC       <div class="flip-card-front">
# MAGIC         Labor-Intensive<br />Development
# MAGIC       </div>
# MAGIC       <div class="flip-card-back">
# MAGIC         <strong>Slow &amp; Error-Prone Pipelines:</strong>
# MAGIC         Delays data products and insights, slowing business impact.
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="flip-card">
# MAGIC     <div class="flip-card-inner">
# MAGIC       <div class="flip-card-front">
# MAGIC         Operational<br />Complexity
# MAGIC       </div>
# MAGIC       <div class="flip-card-back">
# MAGIC         <strong>High Operational Costs:</strong>
# MAGIC         Inefficiencies drive downtime, engineering toil, and wasted resources.
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div class="flip-card">
# MAGIC     <div class="flip-card-inner">
# MAGIC       <div class="flip-card-front">
# MAGIC         Siloed Batch and<br />Streaming
# MAGIC       </div>
# MAGIC       <div class="flip-card-back">
# MAGIC         <strong>Limited Flexibility:</strong>
# MAGIC         Adapting to evolving latency, cost, and SLA needs is cumbersome and expensive.
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## D. Spark Declarative Pipelines: Reliable Data Pipelines Made Easy
# MAGIC
# MAGIC Apache Spark™ Declarative Pipelines let your team focus on what really matters — writing and managing transformation logic.
# MAGIC
# MAGIC - No more getting bogged down in orchestration, infrastructure, or edge-case operations.
# MAGIC - Just clean, reliable data pipelines.
# MAGIC - All delivered with less effort and more confidence.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; box-sizing: border-box; width: 1000px; margin: 0 auto; padding: 12px; background: #ffffff;">
# MAGIC  <!-- Top row (dimmed) -->
# MAGIC  <div style="display: flex; gap: 10px; margin-bottom: 10px; opacity: 0.25;">
# MAGIC   <div style="flex: 1; border: 2px solid #B0173C; border-radius: 8px; padding: 10px 12px; display: flex; align-items: center; gap: 8px;">
# MAGIC    <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/version_control.png" alt="Version Control icon" style="height: 24px; background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1);">
# MAGIC    <span style="font-size: 16px; font-weight: 600;">Version Control</span>
# MAGIC   </div>
# MAGIC   <div style="flex: 1; border: 2px solid #B0173C; border-radius: 8px; padding: 10px 12px; display: flex; align-items: center; gap: 8px;">
# MAGIC    <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/deployment_infrastructure.png" alt="Deployment Infrastructure icon" style="height: 24px; background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1);">
# MAGIC    <span style="font-size: 16px; font-weight: 600;">Deployment Infrastructure</span>
# MAGIC   </div>
# MAGIC  </div>
# MAGIC  <!-- Second row (dimmed) -->
# MAGIC  <div style="display: flex; gap: 10px; margin-bottom: 10px; opacity: 0.25;">
# MAGIC   <div style="flex: 1; border: 2px solid #4C7780; border-radius: 8px; padding: 10px 12px; display: flex; align-items: center; gap: 8px;">
# MAGIC    <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/quality_checks.png" alt="Quality Checks icon" style="height: 24px; background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1);">
# MAGIC    <span style="font-size: 16px; font-weight: 600;">Quality Checks</span>
# MAGIC   </div>
# MAGIC   <div style="flex: 1; border: 2px solid #4C7780; border-radius: 8px; padding: 10px 12px; display: flex; align-items: center; gap: 8px;">
# MAGIC    <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/governance.png" alt="Governance icon" style="height: 24px; background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1);">
# MAGIC    <span style="font-size: 16px; font-weight: 600;">Governance</span>
# MAGIC   </div>
# MAGIC   <div style="flex: 1; border: 2px solid #F3A72B; border-radius: 8px; padding: 10px 12px; display: flex; align-items: center; gap: 8px;">
# MAGIC    <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/data_discovery.png" alt="Data Discovery icon" style="height: 24px; background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1);">
# MAGIC    <span style="font-size: 16px; font-weight: 600;">Data Discovery</span>
# MAGIC   </div>
# MAGIC  </div>
# MAGIC  <!-- Middle band -->
# MAGIC  <div style="display: grid; grid-template-columns: 1.2fr 2fr 1.2fr; gap: 10px; margin-bottom: 14px; align-items: stretch;">
# MAGIC   <!-- Left column: Backfill + Dependency (dimmed) -->
# MAGIC   <div style="display: flex; flex-direction: column; gap: 10px; opacity: 0.25;">
# MAGIC    <div style="border: 2px solid #F3A72B; border-radius: 8px; padding: 10px 12px; display: flex; flex-direction: column; gap: 8px;">
# MAGIC     <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/backfill_handling_gear.png" alt="Backfill Handling icon" style="height: 28px; background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1); align-self: flex-start;">
# MAGIC     <span style="font-size: 16px; font-weight: 600; line-height: 1.2;">Backfill<br>Handling</span>
# MAGIC    </div>
# MAGIC    <div style="border: 2px solid #F3A72B; border-radius: 8px; padding: 10px 12px; display: flex; flex-direction: column; gap: 8px;">
# MAGIC     <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/dependency_management.png" alt="Dependency Management icon" style="height: 28px; align-self: flex-start; background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1);">
# MAGIC     <span style="font-size: 15px; font-weight: 600; line-height: 1.2;">Dependency<br>Management</span>
# MAGIC    </div>
# MAGIC   </div>
# MAGIC   <!-- Center: transformation logic (highlighted) -->
# MAGIC   <div style="border: 2px solid #FF5F46; border-radius: 10px; padding: 18px; display: flex; align-items: center; justify-content: center; background:#FFF6F4; box-shadow: 0 0 0 3px rgba(255,95,70,0.12);">
# MAGIC    <span style="font-size: 20px; font-weight: 700; color: #0B2026; text-align:center; line-height:1.3;">
# MAGIC     With Spark Declarative Pipelines<br>
# MAGIC     <span style="color:#FF5F46;">simply focus on</span><br>
# MAGIC     <span style="color:#0B2026;">[your transformation logic]</span>
# MAGIC    </span>
# MAGIC   </div>
# MAGIC   <!-- Right column: Daily Partition + Checkpointing (dimmed) -->
# MAGIC   <div style="display: flex; flex-direction: column; gap: 10px; opacity: 0.25;">
# MAGIC    <div style="border: 2px solid #F3A72B; border-radius: 8px; padding: 10px 12px; display: flex; flex-direction: column; gap: 8px;">
# MAGIC     <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/daily_partition_computation.png" alt="Daily Partition Computation icon" style="height: 28px; align-self: flex-start; background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1);">
# MAGIC     <span style="font-size: 15px; font-weight: 600; line-height: 1.2;">Daily Partition<br>Computation</span>
# MAGIC    </div>
# MAGIC    <div style="border: 2px solid #F3A72B; border-radius: 8px; padding: 10px 12px; display: flex; flex-direction: column; gap: 8px;">
# MAGIC     <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/checkpointing_retries.png" alt="Checkpointing &amp; Retries icon" style="height: 28px; align-self: flex-start; background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1);">
# MAGIC     <span style="font-size: 15px; font-weight: 600; line-height: 1.2;">Checkpointing<br>&amp; Retries</span>
# MAGIC    </div>
# MAGIC   </div>
# MAGIC  </div>
# MAGIC  <!-- Bottom platform (dimmed) -->
# MAGIC  <div style="border: 2px solid #16333A; border-radius: 14px; padding: 16px 14px 18px; margin-top: 12px; position: relative; overflow: visible; opacity: 0.25;">
# MAGIC   <div style="display: flex; justify-content: center; gap: 14px; flex-wrap: wrap; align-items: center;">
# MAGIC
# MAGIC    <div style="width: 92px; height: 92px; border-radius: 50%; background: #ffffff; box-shadow: 0 2px 8px rgba(0,0,0,0.08); display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 4px;">
# MAGIC     <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/streaming_icon.png" alt="Streaming icon" style="height: 24px; background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1);">
# MAGIC     <span style="font-size: 12px;">Streaming</span>
# MAGIC    </div>
# MAGIC    <div style="width: 118px; height: 118px; border-radius: 50%; background: #ffffff; box-shadow: 0 2px 8px rgba(0,0,0,0.08); display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 4px;">
# MAGIC     <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/data_lake.png" alt="Data Lake icon" style="height: 28px; background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1);">
# MAGIC     <span style="font-size: 13px;">Data Lake</span>
# MAGIC    </div>
# MAGIC    <div style="width: 104px; height: 104px; border-radius: 50%; background: #ffffff; box-shadow: 0 2px 8px rgba(0,0,0,0.08); display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 4px;">
# MAGIC     <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/orchestration.png" alt="Orchestration icon" style="height: 24px; background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1);">
# MAGIC     <span style="font-size: 12px;">Orchestration</span>
# MAGIC    </div>
# MAGIC    <div style="width: 92px; height: 92px; border-radius: 50%; background: #ffffff; box-shadow: 0 2px 8px rgba(0,0,0,0.08); display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 4px;">
# MAGIC     <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/data_science.png" alt="Data Science icon" style="height: 22px; background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1);">
# MAGIC     <span style="font-size: 12px;">Data Science</span>
# MAGIC    </div>
# MAGIC    <div style="width: 78px; height: 78px; border-radius: 50%; background: #ffffff; box-shadow: 0 2px 8px rgba(0,0,0,0.08); display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 3px;">
# MAGIC     <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/machine_learning.png" alt="Machine Learning icon" style="height: 18px; background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1);">
# MAGIC     <span style="font-size: 10px; text-align:center;">Machine<br>Learning</span>
# MAGIC    </div>
# MAGIC    <div style="width: 118px; height: 118px; border-radius: 50%; background: #ffffff; box-shadow: 0 2px 8px rgba(0,0,0,0.08); display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 4px;">
# MAGIC     <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/generative_ai.png" alt="Generative AI icon" style="height: 28px; background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1);">
# MAGIC     <span style="font-size: 13px;">Generative AI</span>
# MAGIC    </div>
# MAGIC    <div style="width: 110px; height: 110px; border-radius: 50%; background: #ffffff; box-shadow: 0 2px 8px rgba(0,0,0,0.08); display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 4px;">
# MAGIC     <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/data_warehouse.png" alt="Data Warehouse icon" style="height: 24px; background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1);">
# MAGIC     <span style="font-size: 12px;">Data Warehouse</span>
# MAGIC    </div>
# MAGIC    <div style="width: 84px; height: 84px; border-radius: 50%; background: #ffffff; box-shadow: 0 2px 8px rgba(0,0,0,0.08); display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 3px;">
# MAGIC     <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/BI.png" alt="BI icon" style="height: 20px; background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1);">
# MAGIC     <span style="font-size: 11px;">BI</span>
# MAGIC    </div>
# MAGIC   </div>
# MAGIC  </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### D1. Introducing Declarative Pipelines: Reliable data pipelines made easy
# MAGIC
# MAGIC With Spark Declarative Pipelines, we're making it dramatically easier to build and manage reliable data pipelines at scale. The following sections walk through how Spark Declarative Pipelines address each of these challenges.
# MAGIC
# MAGIC <div style="text-align: center; margin-top: 20px;">
# MAGIC   <img
# MAGIC     src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/lecture_lakeflow_spark_declaratives/demo_pipeline.png"
# MAGIC     alt="Demo of Pipeline Overview"
# MAGIC     style="width: 1000px; max-width: 100%; height: auto;">
# MAGIC </div>
# MAGIC
# MAGIC <div style="display: flex; gap: 16px; max-width: 1000px; margin: 24px auto 0 auto;">
# MAGIC   <!-- Column 1 -->
# MAGIC   <div style="flex: 1; background: #f9f9f9; border-radius: 10px; overflow: hidden;
# MAGIC               box-shadow: 0 2px 8px rgba(0,0,0,0.08);">
# MAGIC     <div style="background: #00A972; color: #F9F7F4; font-weight: bold;
# MAGIC                 font-size: 12pt; padding: 14px 16px; text-align: center;">
# MAGIC       Simplified Pipeline Authoring
# MAGIC     </div>
# MAGIC     <div style="padding: 12px; display: flex; flex-direction: column; gap: 10px;">
# MAGIC       <div class="kb-card">
# MAGIC         <div class="kb-title">Write SQL or Python</div>
# MAGIC         <div class="kb-text">Define ingestion & transformation in familiar languages</div>
# MAGIC       </div>
# MAGIC       <div class="kb-card">
# MAGIC         <div class="kb-title">No orchestration logic</div>
# MAGIC         <div class="kb-text">Lakeflow manages the execution plan for you</div>
# MAGIC       </div>
# MAGIC       <div class="kb-card">
# MAGIC         <div class="kb-title">Zero boilerplate</div>
# MAGIC         <div class="kb-text">Error handling & dependency management built-in</div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- Column 2 -->
# MAGIC   <div style="flex: 1; background: #f9f9f9; border-radius: 10px; overflow: hidden;
# MAGIC               box-shadow: 0 2px 8px rgba(0,0,0,0.08);">
# MAGIC     <div style="background: #00A972; color: #F9F7F4; font-weight: bold;
# MAGIC                 font-size: 12pt; padding: 14px 16px; text-align: center;">
# MAGIC       Intelligent Optimization at Scale
# MAGIC     </div>
# MAGIC     <div style="padding: 12px; display: flex; flex-direction: column; gap: 10px;">
# MAGIC       <div class="kb-card">
# MAGIC         <div class="kb-title">Auto-scaling</div>
# MAGIC         <div class="kb-text">Pipelines scale automatically as data volumes grow</div>
# MAGIC       </div>
# MAGIC       <div class="kb-card">
# MAGIC         <div class="kb-title">Self-healing</div>
# MAGIC         <div class="kb-text">Automatic recovery from failures — no manual steps</div>
# MAGIC       </div>
# MAGIC       <div class="kb-card">
# MAGIC         <div class="kb-title">Lower overhead</div>
# MAGIC         <div class="kb-text">Better reliability with much less operational effort</div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- Column 3 -->
# MAGIC   <div style="flex: 1; background: #f9f9f9; border-radius: 10px; overflow: hidden;
# MAGIC               box-shadow: 0 2px 8px rgba(0,0,0,0.08);">
# MAGIC     <div style="background: #00A972; color: #F9F7F4; font-weight: bold;
# MAGIC                 font-size: 12pt; padding: 14px 16px; text-align: center;">
# MAGIC       Unified Batch and Streaming
# MAGIC     </div>
# MAGIC     <div style="padding: 12px; display: flex; flex-direction: column; gap: 10px;">
# MAGIC       <div class="kb-card">
# MAGIC         <div class="kb-title">One platform</div>
# MAGIC         <div class="kb-text">Process historical and real-time data seamlessly</div>
# MAGIC       </div>
# MAGIC       <div class="kb-card">
# MAGIC         <div class="kb-title">Workload-aware</div>
# MAGIC         <div class="kb-text">Adapts and optimizes for performance and cost</div>
# MAGIC       </div>
# MAGIC       <div class="kb-card">
# MAGIC         <div class="kb-title">No rewrites</div>
# MAGIC         <div class="kb-text">Same code handles both batch and streaming</div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <style>
# MAGIC   .kb-card {
# MAGIC     background: #ffffff;
# MAGIC     border-radius: 7px;
# MAGIC     padding: 12px 14px;
# MAGIC     border-left: 3px solid #00A972;
# MAGIC     box-shadow: 0 1px 4px rgba(0,0,0,0.06);
# MAGIC   }
# MAGIC   .kb-title {
# MAGIC     font-weight: bold;
# MAGIC     font-size: 11.5pt;
# MAGIC     color: #0b2026;
# MAGIC     margin-bottom: 4px;
# MAGIC   }
# MAGIC   .kb-text {
# MAGIC     font-size: 10.5pt;
# MAGIC     color: #618794;
# MAGIC     line-height: 1.6;
# MAGIC   }
# MAGIC </style>
# MAGIC
# MAGIC <div style="margin-top: 20px; font-size: 11.5pt; color: #0b2026; line-height: 1.8;">
# MAGIC   Overall, this is about empowering your team to move faster with fewer headaches while delivering production-grade data pipelines that just work.
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## E. Connecting to Data Sources
# MAGIC
# MAGIC It all starts with getting your data into Databricks — and that's where Lakeflow Connect plays a crucial role.
# MAGIC
# MAGIC <div style="text-align: center; margin-top: 20px;">
# MAGIC   <img
# MAGIC     src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/lecture_lakeflow_spark_declaratives/connecting_data_sources.png"
# MAGIC     alt="Connecting Data Sources"
# MAGIC     style="width: 1000px; max-width: 100%; height: auto;">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC It all starts with getting your data into Databricks, and that’s where Lakeflow Connect plays a crucial role.
# MAGIC
# MAGIC You can bring in data from a variety of sources. Whether it is stored in cloud object stores such as S3, ADLS, or GCS, streaming from message queues like Kafka, Pub/Sub, or Kinesis, pulled from traditional databases including SQL Server or Postgres, or coming from SaaS applications like Salesforce or Workday, Lakeflow Connect makes ingestion simple and reliable.
# MAGIC
# MAGIC Once the data is connected, Spark Declarative Pipelines can handle ingestion and transformation efficiently. This allows you to build data pipelines that follow the medallion architecture, progressing data through bronze, silver, and gold layers with confidence in reliability and scalability.
# MAGIC
# MAGIC No matter what your data source, you can ingest, transform, and operationalize your data quickly, all within the Databricks environment.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## F. Simplifying Batch and Streaming ETL in the Medallion Architecture
# MAGIC
# MAGIC <div style="text-align: center; margin-top: 20px;">
# MAGIC   <img
# MAGIC     src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/lecture_lakeflow_spark_declaratives/declarative_pipelines.png"
# MAGIC     alt="Demo of Pipeline Overview"
# MAGIC     style="width: 1000px; max-width: 100%; height: auto;">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC With Spark Declarative Pipelines, you can focus your ETL efforts by ingesting data using either batch or streaming method.
# MAGIC
# MAGIC 1. <b>Batch ingestion</b> is designed for large volumes of data that arrive all at once. 
# MAGIC 2. <b>Streaming ingestion</b>, on the other hand, handles data that flows continuously in small increments, enabling near real-time processing.
# MAGIC
# MAGIC Once your data is ingested, you’ll follow the medallion architecture to organize and refine it:
# MAGIC
# MAGIC - <b>Bronze</b> is the raw data ingestion layer in your lakehouse. Here, data is brought in as-is, sometimes with additional metadata columns added to provide context about the ingestion process.
# MAGIC
# MAGIC - <b>Silver</b> is where cleaning and transformation happen, turning raw data into a more usable and refined table.
# MAGIC
# MAGIC - <b>Gold</b> represents the final business-level aggregates designed for downstream use cases such as reporting, machine learning, AI, streaming analytics, and more.
# MAGIC
# MAGIC This structured approach helps ensure data quality and usability across your organization’s analytics and operational needs. Spark Declarative Pipelines make this entire process easy to create, monitor, and optimize, so you can focus on driving insights rather than managing complexity.
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## G. Incremental Processing in Declarative Pipelines

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### G1. Overview - Incremental Processing in Declarative Pipelines
# MAGIC
# MAGIC Let’s review at a high level how Declarative Pipelines make implementing incremental processing easy. This is the fundamental reason to use them.
# MAGIC
# MAGIC
# MAGIC <div style="max-width: 1000px; margin: 0 auto; padding: 12px 0;">
# MAGIC   <!-- Pipeline Flow -->
# MAGIC   <div style="display: flex; align-items: flex-start; justify-content: center; gap: 24px; flex-wrap: nowrap;">
# MAGIC     <!-- Raw Data Source -->
# MAGIC     <div style="display: flex; flex-direction: column; align-items: center; min-width: 170px;">
# MAGIC       <div style="background: #f0f4f8; border: 2px dashed #618794; border-radius: 12px;
# MAGIC                   padding: 18px 16px; text-align: center; width: 170px; height: 350px; box-sizing:border-box;">
# MAGIC         <div style="font-size: 10pt; font-style: italic; color: #618794; margin-bottom: 10px;">Raw Data Source</div>
# MAGIC         <div style="background: #4299E0; color: white; font-size: 9pt; font-weight: bold;
# MAGIC                     padding: 4px 10px; border-radius: 4px; margin-bottom: 10px; display: inline-block;">
# MAGIC           File / Stream
# MAGIC         </div>
# MAGIC         <div style="display: flex; flex-direction: column; gap: 6px; margin-top: 6px;">
# MAGIC           <div style="height: 8px; background: #c8dff5; border: 1px dashed #4299E0; border-radius: 3px;"></div>
# MAGIC           <div style="height: 8px; background: #c8dff5; border: 1px dashed #4299E0; border-radius: 3px;"></div>
# MAGIC           <div style="height: 8px; background: #c8dff5; border: 1px dashed #4299E0; border-radius: 3px;"></div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC       <!-- Ingest modes -->
# MAGIC       <div style="margin-top: 8px; background: #fff; border: 1px solid #d0d8db;
# MAGIC                   border-radius: 8px; padding: 6px 10px; font-size: 9pt;
# MAGIC                   color: #0b2026; line-height: 1.5; text-align: left; width: 170px;">
# MAGIC         <div><b>Batch</b><br><span style="color:#618794;">Scheduled processing</span></div>
# MAGIC         <div style="margin-top:4px;"><b>Streaming</b><br><span style="color:#618794;">Continuous data flow</span></div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <!-- Bronze -->
# MAGIC     <div style="display: flex; flex-direction: column; align-items: center; min-width: 120px; margin-top: 4px;">
# MAGIC       <div style="background: #fff8f0; border: 1.5px solid #CD7F32; border-radius: 10px;
# MAGIC                   padding: 8px 10px 10px 10px; text-align: center; width: 120px; box-sizing:border-box;">
# MAGIC         <img
# MAGIC           src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/bronze_streaming_table.png"
# MAGIC           alt="Bronze streaming table icon"
# MAGIC           style="display:block; margin:0 auto 4px auto; max-width:80px; height:auto; background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1);">
# MAGIC         <div style="font-size:8.5pt; color:#8B5E3C; background:#fdebd0;
# MAGIC                     border-radius:20px; padding:2px 8px; display:inline-block;">
# MAGIC           Streaming Table
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <!-- Silver -->
# MAGIC     <div style="display: flex; flex-direction: column; align-items: center; min-width: 120px; margin-top: 4px;">
# MAGIC       <div style="background: #f5f5f5; border: 1.5px solid #9E9E9E; border-radius: 10px;
# MAGIC                   padding: 8px 10px 10px 10px; text-align: center; width: 120px; box-sizing:border-box;">
# MAGIC         <img
# MAGIC           src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/silver_streaming_table.png"
# MAGIC           alt="Silver streaming table icon"
# MAGIC           style="display:block; margin:0 auto 4px auto; max-width:80px; height:auto; background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1);">
# MAGIC         <div style="font-size:8.5pt; color:#555; background:#e8e8e8;
# MAGIC                     border-radius:20px; padding:2px 8px; display:inline-block;">
# MAGIC           Streaming Table
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <!-- Gold -->
# MAGIC     <div style="display: flex; flex-direction: column; align-items: center; min-width: 120px; margin-top: 4px;">
# MAGIC       <div style="background: #fffbea; border: 1.5px solid #FFAB00; border-radius: 10px;
# MAGIC                   padding: 8px 10px 10px 10px; text-align: center; width: 120px; box-sizing:border-box;">
# MAGIC         <img
# MAGIC           src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/gold_materialized_view.png"
# MAGIC           alt="Gold materialized view icon"
# MAGIC           style="display:block; margin:0 auto 4px auto; max-width:80px; height:auto; background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1);">
# MAGIC         <div style="font-size:8.5pt; color:#7a5800; background:#fff0b3;
# MAGIC                     border-radius:20px; padding:2px 8px; display:inline-block;">
# MAGIC           Materialized View
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC Your data starts out living in some raw data source(s). Whether cloud storage, databases, SaaS applications, or message queues.
# MAGIC
# MAGIC From there, you choose how to ingest that data: batch or streaming. One of the great benefits is the flexibility to switch between these modes easily if your needs change.
# MAGIC
# MAGIC Typically, you’ll build a bronze, silver, and gold ETL pipeline. In this simple example, we’ll create:
# MAGIC
# MAGIC - A bronze streaming table, A silver streaming table and A gold materialized view
# MAGIC
# MAGIC Later in this course, we’ll dive deeper into the fundamentals of streaming tables and materialized views. For now, focus on understanding the overall architecture and how data incrementally flows through these data types in the pipeline.
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### G2. Run 1 - Incremental Processing in Declarative Pipelines
# MAGIC
# MAGIC Here is a high-level view of the first run in this pipeline.
# MAGIC
# MAGIC <div style="max-width: 1200px; margin: 0 auto; padding: 12px 0;">
# MAGIC  <!-- Pipeline Flow -->
# MAGIC  <div style="display: flex; align-items: flex-start; justify-content: center; gap: 32px; flex-wrap: nowrap;">
# MAGIC   <!-- Raw Data Source -->
# MAGIC   <div style="display: flex; flex-direction: column; align-items: center; min-width: 190px;">
# MAGIC     <div style="background: #f0f4f8; border: 2px dashed #618794; border-radius: 12px;
# MAGIC                 padding: 18px 16px; text-align: center; width: 190px; height: 350px;
# MAGIC                 box-sizing:border-box; position:relative;">
# MAGIC       <div style="font-size: 10pt; font-style: italic; color: #618794; margin-bottom: 10px;">Raw data source</div>
# MAGIC       <div style="position:absolute; left:-2px; top:58px; background:#4299E0; color:#fff; font-size:9pt; font-weight:bold;
# MAGIC                   padding:4px 10px; border-radius:3px 0 0 3px;">
# MAGIC         File 1
# MAGIC       </div>
# MAGIC       <div style="display: flex; flex-direction: column; gap: 6px; margin-top: 40px;
# MAGIC                   border:1px dashed #c4ccd6; padding:10px; border-radius:8px;">
# MAGIC         <div style="height: 8px; background: #c8dff5; border-radius: 3px;"></div>
# MAGIC         <div style="height: 8px; background: #c8dff5; border-radius: 3px;"></div>
# MAGIC         <div style="height: 8px; background: #c8dff5; border-radius: 3px;"></div>
# MAGIC       </div>
# MAGIC       <!-- connector from dotted box → right → down (toward Bronze append level) -->
# MAGIC       <div style="position:absolute; right:-4px; top:112px; width:26px; height:2px; background:#02A36F;"></div>
# MAGIC       <div style="position:absolute; right: -4px; top:112px; width:2px; height:210px; background:#02A36F;"></div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <!-- Bronze -->
# MAGIC   <div style="display: flex; flex-direction: column; align-items: center; min-width: 180px; margin-right: 40px;">
# MAGIC     <!-- header icon + type -->
# MAGIC     <div style="background: #fff8f0; border: 1.5px solid #CD7F32; border-radius: 10px;
# MAGIC                 padding: 8px 10px 10px 10px; text-align: center; width: 150px; box-sizing:border-box;">
# MAGIC       <img
# MAGIC         src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/bronze_streaming_table.png"
# MAGIC         alt="Bronze streaming table icon"
# MAGIC         style="display:block; margin:0 auto 4px auto; max-width:80px; height:auto;
# MAGIC                background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1);">
# MAGIC       <div style="font-size:9pt; color:#0b2026; font-weight:600;">
# MAGIC         Streaming table
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <!-- custom image slot between header and append box -->
# MAGIC     <img
# MAGIC       src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/streaming.png"
# MAGIC       alt="Bronze step image"
# MAGIC       style="display:block; margin:5px auto 0 auto; max-width:60px; height:auto;">
# MAGIC     <!-- streaming table box -->
# MAGIC     <div style="margin-top: 12px; width: 180px; height: 210px; border:2px solid #1B3139;
# MAGIC                 border-radius:10px; background:#ffffff; box-sizing:border-box; padding:16px 14px; position:relative;">
# MAGIC       <!-- connector final segment from Raw into Bronze append box -->
# MAGIC       <div style="position:absolute; left:-40px; top:130px; width:40px; height:2px; background:#02A36F;"></div>
# MAGIC       <div style="border:1.5px dashed #c4ccd6; border-radius:8px; padding:10px; margin-bottom:12px;">
# MAGIC         <div style="height:8px; background:#c8dff5; border-radius:3px; margin-bottom:5px;"></div>
# MAGIC         <div style="height:8px; background:#c8dff5; border-radius:3px; margin-bottom:5px;"></div>
# MAGIC         <div style="height:8px; background:#c8dff5; border-radius:3px;"></div>
# MAGIC       </div>
# MAGIC       <div style="font-size:11pt; font-weight:700; color:#0B2026; text-align:center;">
# MAGIC         Append
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <!-- Silver -->
# MAGIC   <div style="display: flex; flex-direction: column; align-items: center; min-width: 200px; margin-right: 40px;">
# MAGIC     <!-- header icon + type -->
# MAGIC     <div style="background: #f5f5f5; border: 1.5px solid #9E9E9E; border-radius: 10px;
# MAGIC                 padding: 8px 10px 10px 10px; text-align: center; width: 150px; box-sizing:border-box;">
# MAGIC       <img
# MAGIC         src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/silver_streaming_table.png"
# MAGIC         alt="Silver streaming table icon"
# MAGIC         style="display:block; margin:0 auto 4px auto; max-width:80px; height:auto;
# MAGIC                background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1);">
# MAGIC       <div style="font-size:9pt; color:#0b2026; font-weight:600;">
# MAGIC         Streaming table
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <!-- outer highlight box -->
# MAGIC     <div style="margin-top: 18px; width: 200px; height: 230px; border:2px solid #FFAB00;
# MAGIC     border-radius:10px; background:#fffdf6; box-sizing:border-box; padding:10px 10px 14px 10px; position:relative;">
# MAGIC     <!-- custom image slot between header and append box -->
# MAGIC     <img
# MAGIC       src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/streaming.png"
# MAGIC       alt="Silver step image"
# MAGIC       style="display:block; margin:5px auto 0 auto; max-width:60px; height:auto;">
# MAGIC       <!-- inner streaming table -->
# MAGIC       <div style="width: 100%; height: 100%; border:2px solid #1B3139; border-radius:8px;
# MAGIC                   background:#ffffff; box-sizing:border-box; padding:28px 14px 16px 14px; position:relative;">
# MAGIC         <div style="border:1.5px dashed #c4ccd6; border-radius:8px; padding:10px; margin-bottom:12px;">
# MAGIC           <div style="height:8px; background:#c8dff5; border-radius:3px; margin-bottom:5px;"></div>
# MAGIC           <div style="height:8px; background:#c8dff5; border-radius:3px; margin-bottom:5px;"></div>
# MAGIC           <div style="height:8px; background:#c8dff5; border-radius:3px;"></div>
# MAGIC         </div>
# MAGIC         <div style="font-size:11pt; font-weight:700; color:#0B2026; text-align:center;">
# MAGIC           Append
# MAGIC         </div>
# MAGIC       </div>
# MAGIC       <!-- Transform label -->
# MAGIC       <div style="position:absolute; left:-70px; top:50%; transform:translateY(-50%);
# MAGIC                   font-size:12pt; color:#1B5162; white-space:nowrap;">Transform →&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <!-- Gold -->
# MAGIC   <div style="display: flex; flex-direction: column; align-items: center; min-width: 220px;">
# MAGIC     <!-- header icon + type -->
# MAGIC     <div style="background: #fffbea; border: 1.5px solid #FFAB00; border-radius: 10px;
# MAGIC                 padding: 8px 10px 10px 10px; text-align: center; width: 160px; box-sizing:border-box;">
# MAGIC       <img
# MAGIC         src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/gold_materialized_view.png"
# MAGIC         alt="Gold materialized view icon"
# MAGIC         style="display:block; margin:0 auto 4px auto; max-width:80px; height:auto;
# MAGIC                background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1);">
# MAGIC       <div style="font-size:9pt; color:#0b2026; font-weight:600;">
# MAGIC         Materialized View
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <!-- custom image slot between header and aggregation box -->
# MAGIC     <img
# MAGIC       src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/materialized.png"
# MAGIC       alt="Gold step image"
# MAGIC       style="display:block; margin:5px auto 0 auto; max-width:60px; height:auto;">
# MAGIC     <!-- aggregation box -->
# MAGIC     <div style="margin-top: 24px; width: 220px; border:2px solid #1B3139; border-radius:10px;
# MAGIC                 background:#ffffff; box-sizing:border-box; padding:18px 16px;">
# MAGIC       <div style="border:1.5px solid #DCE0E2; border-radius:8px; padding:8px 10px; margin-bottom:12px;">
# MAGIC         <div style="height:9px; background:#4299E0; border-radius:3px; margin-bottom:4px;"></div>
# MAGIC         <div style="height:9px; background:#e05c3a; border-radius:3px; margin-bottom:4px;"></div>
# MAGIC         <div style="height:9px; background:#1B5162; border-radius:3px; margin-bottom:4px;"></div>
# MAGIC         <div style="height:9px; background:#00A972; border-radius:3px; margin-bottom:4px;"></div>
# MAGIC         <div style="height:9px; background:#FFAB00; border-radius:3px;"></div>
# MAGIC       </div>
# MAGIC       <div style="font-size:11pt; color:#0B2026; font-weight:600; text-align:left;">
# MAGIC         Aggregations and other queries from a streaming table
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <!-- serverless optimization callout + arrow (aligned to meet vertical from Silver) -->
# MAGIC     <div style="margin-top: 10px; font-size:10.5pt; color:#0B2026; text-align:right; width:220px; position:relative;">
# MAGIC       <span style="font-style:italic;">Built in optimizations on Serverless</span>
# MAGIC       <div style="position:absolute; left:0; bottom:-14px; width:2px; height:20px; background:#FFAB00;"></div>
# MAGIC       <div style="position:absolute; left:-1px; bottom:-18px; width:0; height:0; border-left:4px solid transparent; border-right:4px solid transparent; border-top:6px solid #FFAB00;"></div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC  </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="margin-top:16px; background:#f0f4f8; border-radius:8px; padding:14px 20px;
# MAGIC               font-size:11pt; color:#0b2026; line-height:1.8; border-left:4px solid #00A972;">
# MAGIC     <b>Run 1 Summary:</b> Raw data — appended to Bronze — transformed and appended to Silver —
# MAGIC     processed into the Gold Materialized View with built-in Serverless optimizations.
# MAGIC   </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC In the first run of this pipeline, raw data will be ingested from the data source and appended to the bronze streaming table.
# MAGIC
# MAGIC Next, the pipeline transforms the new data from the bronze layer according to your transformation logic and appends the results to the silver streaming table.
# MAGIC
# MAGIC Finally, the materialized view processes the data at the gold level. Depending on various factors, this processing can happen incrementally or as a full refresh, ensuring your gold-level data is always up to date and ready for downstream use. Materialized views contain a variety of built in optimizations when run on Serverless.
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### G3. Run 2 - Incremental Processing in Declarative Pipelines
# MAGIC
# MAGIC Here is a high-level view of the second run in this pipeline.
# MAGIC
# MAGIC <div style="max-width: 1200px; margin: 0 auto; padding: 12px 0;">
# MAGIC  <!-- Pipeline Flow -->
# MAGIC  <div style="display: flex; align-items: flex-start; justify-content: center; gap: 32px; flex-wrap: nowrap;">
# MAGIC   <!-- Raw Data Source -->
# MAGIC   <div style="display: flex; flex-direction: column; align-items: center; min-width: 190px;">
# MAGIC     <div style="background: #f0f4f8; border: 2px dashed #618794; border-radius: 12px;
# MAGIC                 padding: 18px 16px; text-align: center; width: 190px; height: 350px;
# MAGIC                 box-sizing:border-box; position:relative;">
# MAGIC       <div style="font-size: 10pt; font-style: italic; color: #618794; margin-bottom: 10px;">Raw data source</div>
# MAGIC       <div style="position:absolute; left:-2px; top:100px; background:#4299E0; color:#fff; font-size:9pt; font-weight:bold;
# MAGIC                   padding:4px 10px; border-radius:3px 0 0 3px;">
# MAGIC         File 2
# MAGIC       </div>
# MAGIC       <!-- Existing data (grey) + new data (blue) -->
# MAGIC       <div style="display: flex; flex-direction: column; gap: 8px; margin-top: 28px;">
# MAGIC         <!-- Existing data -->
# MAGIC         <div style="background:#f5f5f5; border-radius:8px; padding:10px 8px;">
# MAGIC           <div style="height:8px; background:#d4d4d4; border-radius:3px; margin-bottom:4px;"></div>
# MAGIC           <div style="height:8px; background:#d4d4d4; border-radius:3px; margin-bottom:4px;"></div>
# MAGIC           <div style="height:8px; background:#d4d4d4; border-radius:3px;"></div>
# MAGIC         </div>
# MAGIC         <!-- New data -->
# MAGIC         <div style="border:1px dashed #c4ccd6; border-radius:8px; padding:10px;">
# MAGIC           <div style="height:8px; background:#4f8fdc; border-radius:3px; margin-bottom:4px;"></div>
# MAGIC           <div style="height:8px; background:#4f8fdc; border-radius:3px; margin-bottom:4px;"></div>
# MAGIC           <div style="height:8px; background:#4f8fdc; border-radius:3px;"></div>
# MAGIC         </div>
# MAGIC         <div style="font-size:10pt; color:#0B2026; text-align:left; padding-left:2px;">
# MAGIC           New data
# MAGIC         </div>
# MAGIC       </div>
# MAGIC       <!-- connector from new-data box → right → down (toward Bronze append level) -->
# MAGIC       <div style="position:absolute; right:-4px; top:160px; width:26px; height:2px; background:#02A36F;"></div>
# MAGIC       <div style="position:absolute; right:-4px; top:160px; width:2px; height:162px; background:#02A36F;"></div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <!-- Bronze -->
# MAGIC   <div style="display: flex; flex-direction: column; align-items: center; min-width: 180px; margin-right: 40px;">
# MAGIC     <!-- header icon + type -->
# MAGIC     <div style="background: #fff8f0; border: 1.5px solid #CD7F32; border-radius: 10px;
# MAGIC                 padding: 8px 10px 10px 10px; text-align: center; width: 150px; box-sizing:border-box;">
# MAGIC       <img
# MAGIC         src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/bronze_streaming_table.png"
# MAGIC         alt="Bronze streaming table icon"
# MAGIC         style="display:block; margin:0 auto 4px auto; max-width:80px; height:auto;
# MAGIC                background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1);">
# MAGIC       <div style="font-size:9pt; color:#0b2026; font-weight:600;">
# MAGIC         Streaming table
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <!-- custom image slot between header and append box -->
# MAGIC     <img
# MAGIC       src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/streaming.png"
# MAGIC       alt="Bronze step image"
# MAGIC       style="display:block; margin:5px auto 0 auto; max-width:60px; height:auto;">
# MAGIC     <!-- streaming table box -->
# MAGIC     <div style="margin-top: 12px; width: 180px; height: 210px; border:2px solid #1B3139;
# MAGIC                 border-radius:10px; background:#ffffff; box-sizing:border-box; padding:16px 14px; position:relative;">
# MAGIC       <!-- connector final segment from Raw into Bronze append box -->
# MAGIC       <div style="position:absolute; left:-40px; top:130px; width:40px; height:2px; background:#02A36F;"></div>
# MAGIC       <!-- Existing rows (grey) -->
# MAGIC       <div style="border:1.5px solid #DCE0E2; border-radius:8px; padding:10px; margin-bottom:8px;">
# MAGIC         <div style="height:8px; background:#d4d4d4; border-radius:3px; margin-bottom:4px;"></div>
# MAGIC         <div style="height:8px; background:#d4d4d4; border-radius:3px; margin-bottom:4px;"></div>
# MAGIC         <div style="height:8px; background:#d4d4d4; border-radius:3px;"></div>
# MAGIC       </div>
# MAGIC       <!-- New append rows (blue) -->
# MAGIC       <div style="border:1.5px dashed #c4ccd6; border-radius:8px; padding:10px; margin-bottom:8px;">
# MAGIC         <div style="height:8px; background:#4f8fdc; border-radius:3px; margin-bottom:4px;"></div>
# MAGIC         <div style="height:8px; background:#4f8fdc; border-radius:3px; margin-bottom:4px;"></div>
# MAGIC         <div style="height:8px; background:#4f8fdc; border-radius:3px;"></div>
# MAGIC       </div>
# MAGIC       <div style="font-size:11pt; font-weight:700; color:#0B2026; text-align:center;">
# MAGIC         Append
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <!-- Silver -->
# MAGIC   <div style="display: flex; flex-direction: column; align-items: center; min-width: 200px; margin-right: 40px;">
# MAGIC     <!-- header icon + type -->
# MAGIC     <div style="background: #f5f5f5; border: 1.5px solid #9E9E9E; border-radius: 10px;
# MAGIC                 padding: 8px 10px 10px 10px; text-align: center; width: 150px; box-sizing:border-box;">
# MAGIC       <img
# MAGIC         src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/silver_streaming_table.png"
# MAGIC         alt="Silver streaming table icon"
# MAGIC         style="display:block; margin:0 auto 4px auto; max-width:80px; height:auto;
# MAGIC                background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1);">
# MAGIC       <div style="font-size:9pt; color:#0b2026; font-weight:600;">
# MAGIC         Streaming table
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <!-- outer highlight box -->
# MAGIC     <div style="margin-top: 18px; width: 200px; height: 270px; border:2px solid #FFAB00;
# MAGIC                 border-radius:10px; background:#fffdf6; box-sizing:border-box; padding:10px 10px 14px 10px; position:relative;">
# MAGIC       <!-- custom image slot inside highlight, above inner table -->
# MAGIC       <img
# MAGIC         src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/streaming.png"
# MAGIC         alt="Silver step image"
# MAGIC         style="display:block; margin:2px auto 0 auto; max-width:60px; height:auto;">
# MAGIC       <!-- inner streaming table (existing + append) -->
# MAGIC       <div style="width: 100%; height: 100%; border:2px solid #1B3139; border-radius:8px;
# MAGIC                   background:#ffffff; box-sizing:border-box; padding:24px 14px 16px 14px; position:relative;">
# MAGIC         <!-- Existing rows -->
# MAGIC         <div style="border:1.5px solid #DCE0E2; border-radius:8px; padding:10px; margin-bottom:8px;">
# MAGIC           <div style="height:8px; background:#d4d4d4; border-radius:3px; margin-bottom:4px;"></div>
# MAGIC           <div style="height:8px; background:#d4d4d4; border-radius:3px; margin-bottom:4px;"></div>
# MAGIC           <div style="height:8px; background:#d4d4d4; border-radius:3px;"></div>
# MAGIC         </div>
# MAGIC         <!-- New append rows -->
# MAGIC         <div style="border:1.5px dashed #c4ccd6; border-radius:8px; padding:10px; margin-bottom:8px;">
# MAGIC           <div style="height:8px; background:#4f8fdc; border-radius:3px; margin-bottom:4px;"></div>
# MAGIC           <div style="height:8px; background:#4f8fdc; border-radius:3px; margin-bottom:4px;"></div>
# MAGIC           <div style="height:8px; background:#4f8fdc; border-radius:3px;"></div>
# MAGIC         </div>
# MAGIC         <div style="font-size:11pt; font-weight:700; color:#0B2026; text-align:center;">
# MAGIC           Append
# MAGIC         </div>
# MAGIC       </div>
# MAGIC       <!-- Transform label -->
# MAGIC       <div style="position:absolute; left:-70px; top:50%; transform:translateY(-50%);
# MAGIC                   font-size:12pt; color:#1B5162; white-space:nowrap;">
# MAGIC         Transform →&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <!-- Gold -->
# MAGIC   <div style="display: flex; flex-direction: column; align-items: center; min-width: 220px;">
# MAGIC     <!-- header icon + type -->
# MAGIC     <div style="background: #fffbea; border: 1.5px solid #FFAB00; border-radius: 10px;
# MAGIC                 padding: 8px 10px 10px 10px; text-align: center; width: 160px; box-sizing:border-box;">
# MAGIC       <img
# MAGIC         src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/gold_materialized_view.png"
# MAGIC         alt="Gold materialized view icon"
# MAGIC         style="display:block; margin:0 auto 4px auto; max-width:80px; height:auto;
# MAGIC                background: transparent; mix-blend-mode: multiply; filter: contrast(1.15) brightness(1);">
# MAGIC       <div style="font-size:9pt; color:#0b2026; font-weight:600;">
# MAGIC         Materialized View
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <!-- custom image slot between header and aggregation box -->
# MAGIC     <img
# MAGIC       src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/materialized.png"
# MAGIC       alt="Gold step image"
# MAGIC       style="display:block; margin:5px auto 0 auto; max-width:60px; height:auto;">
# MAGIC     <!-- aggregation box -->
# MAGIC     <div style="margin-top: 24px; width: 220px; border:2px solid #1B3139; border-radius:10px;
# MAGIC                 background:#ffffff; box-sizing:border-box; padding:18px 16px;">
# MAGIC       <div style="border:1.5px solid #DCE0E2; border-radius:8px; padding:8px 10px; margin-bottom:12px;">
# MAGIC         <div style="height:9px; background:#4299E0; border-radius:3px; margin-bottom:4px;"></div>
# MAGIC         <div style="height:9px; background:#e05c3a; border-radius:3px; margin-bottom:4px;"></div>
# MAGIC         <div style="height:9px; background:#1B5162; border-radius:3px; margin-bottom:4px;"></div>
# MAGIC         <div style="height:9px; background:#00A972; border-radius:3px; margin-bottom:4px;"></div>
# MAGIC         <div style="height:9px; background:#FFAB00; border-radius:3px;"></div>
# MAGIC       </div>
# MAGIC       <div style="font-size:11pt; color:#0B2026; font-weight:600; text-align:left;">
# MAGIC         Aggregations and other queries from a streaming table
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <!-- serverless optimization callout + arrow -->
# MAGIC     <div style="margin-top: 10px; font-size:10.5pt; color:#0B2026; text-align:right; width:220px; position:relative;">
# MAGIC       <span style="font-style:italic;">Built in optimizations on Serverless</span>
# MAGIC       <div style="position:absolute; right:40px; bottom:-14px; width:2px; height:20px; background:#FFAB00;"></div>
# MAGIC       <div style="position:absolute; right:39px; bottom:-18px; width:0; height:0;
# MAGIC                   border-left:4px solid transparent; border-right:4px solid transparent; border-top:6px solid #FFAB00;"></div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC  </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC   <div style="margin-top:16px; background:#f0f4f8; border-radius:8px; padding:14px 20px;
# MAGIC               font-size:11pt; color:#0b2026; line-height:1.8; border-left:4px solid #00A972;">
# MAGIC     <b>Run 2 Summary:</b> Only new data (File 2) is incrementally ingested into Bronze — incrementally transformed and appended into Silver — then the full Silver table is processed into the Gold Materialized View with built-in Serverless optimizations.
# MAGIC   </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC When the pipeline runs a second time, it uses checkpoints to track what data has already been ingested and processed (Auto Loader).
# MAGIC
# MAGIC In this scenario, let's imagine new data has arrived in the raw data source.
# MAGIC
# MAGIC - In this case, only the new data is incrementally ingested from the data source and appended to the bronze streaming table. The pipeline ignores data it has already processed to avoid processing overhead from the previously ingested data.
# MAGIC - Next, only the new data from the bronze table is incrementally transformed and appended to the silver streaming table, ignoring data it has already transformed in the first run, again avoiding processing overhead from the previous transformation.
# MAGIC - Finally, the materialized view processes the full silver streaming table at the gold level. Depending on various factors, this processing can be incremental or a full refresh, ensuring your gold-level data stays accurate and up to date for your downstream consumers.
# MAGIC
# MAGIC This incremental processing approach improves efficiency by only handling new data that hasn’t been processed before. It avoids unnecessary reprocessing of existing data. Materialized views add even more value by providing built-in optimizations, making it faster and more efficient to aggregate or query the streaming tables.
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## H. Creating a Spark Declarative Pipeline
# MAGIC
# MAGIC There are two easy ways to create a Spark Declarative Pipeline directly from the Databricks workspace. 
# MAGIC
# MAGIC Both ways will take you to the same pipeline creation flow, where you can start defining your ingestion and transformation logic using SQL or Python.
# MAGIC
# MAGIC **Select each tab to explore both methods.**
# MAGIC
# MAGIC <div style="width: 100%; margin: auto; font-family: sans-serif;">
# MAGIC
# MAGIC <style>
# MAGIC .four-grid {
# MAGIC     display: flex; flex-direction: column; gap: 60px;
# MAGIC     justify-content: center; align-items: center;
# MAGIC }
# MAGIC .ing-box {
# MAGIC     width: 80%; min-height: 400px; background: #F9F7F4;
# MAGIC     border: none; border-radius: 8px;
# MAGIC     box-shadow: 0 2px 8px rgba(27,49,57,0.06);
# MAGIC     overflow: hidden; display: flex; flex-direction: column;
# MAGIC     gap: 12px; padding: 20px; text-align: center;
# MAGIC     position: relative; box-sizing: border-box;
# MAGIC }
# MAGIC .ing-box::before {
# MAGIC     content: ""; position: absolute; top: 0; left: 0; width: 100%; height: 8px;
# MAGIC }
# MAGIC .ing-box.method1::before { background: #1B5FA8; }
# MAGIC .ing-box.method2::before { background: #00A972; }
# MAGIC .ing-box-title { font-size: 16pt; font-weight: bold; text-align: center; }
# MAGIC .ing-box-content {
# MAGIC     display: flex; align-items: center; justify-content: center;
# MAGIC     gap: 24px; width: 100%;
# MAGIC }
# MAGIC .ing-box-text { font-size: 14pt; max-width: 460px; text-align: left; line-height: 1.6; }
# MAGIC .ing-box-text ol { text-align: left; padding-left: 18px; margin: 0 0 14px 0; }
# MAGIC .ing-box-text li { margin-bottom: 10px; }
# MAGIC .ing-box-text li:last-child { margin-bottom: 0; }
# MAGIC .ing-example {
# MAGIC     padding: 12px 14px; border-radius: 8px; font-size: 13pt;
# MAGIC     line-height: 1.6; font-weight: 400; margin-top: 10px;
# MAGIC }
# MAGIC .method1 .ing-example { background: rgba(27,95,168,0.08); border-left: 4px solid #1B5FA8; }
# MAGIC .method2 .ing-example { background: rgba(0,169,114,0.08); border-left: 4px solid #00A972; }
# MAGIC .step-badge {
# MAGIC     display: inline-block; width: 28px; height: 28px; border-radius: 50%;
# MAGIC     font-size: 13pt; font-weight: bold; color: #fff;
# MAGIC     text-align: center; line-height: 28px; margin-right: 8px; flex-shrink: 0;
# MAGIC }
# MAGIC .method1 .step-badge { background: #1B5FA8; }
# MAGIC .method2 .step-badge { background: #00A972; }
# MAGIC .step-row { display: flex; align-items: flex-start; gap: 10px; margin-bottom: 12px; }
# MAGIC </style>
# MAGIC
# MAGIC <!-- Tabs -->
# MAGIC <div style="display: flex; border-bottom: 2px solid #EEEDE9; margin-bottom: 0;">
# MAGIC   <button class="dbtab" onclick="showEtlTab(1)" style="padding: 10px 18px; border: none; border-bottom: 3px solid #1B5FA8; background: none; font-size: 14pt; font-weight: bold; color: #1B5FA8; cursor: pointer; margin-bottom: -2px;">Method 1: Workspace Menu</button>
# MAGIC   <button class="dbtab" onclick="showEtlTab(2)" style="padding: 10px 18px; border: none; border-bottom: 3px solid transparent; background: none; font-size: 14pt; font-weight: bold; color: #888; cursor: pointer; margin-bottom: -2px;">Method 2: Jobs &amp; Pipelines</button>
# MAGIC </div>
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC <!-- TAB 1 -->
# MAGIC <div class="dbpanel" style="display: block;">
# MAGIC   <div class="four-grid">
# MAGIC     <div class="ing-box method1">
# MAGIC       <div class="ing-box-title">Create an ETL Pipeline — From the Workspace Menu</div>
# MAGIC       <div class="ing-box-content">
# MAGIC         <div>
# MAGIC           <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/lecture_lakeflow_spark_declaratives/method1_create_pipeline.png"
# MAGIC                alt="Method 1 — Workspace options menu showing ETL Pipeline"
# MAGIC                style="width: 800px; max-width: 100%; height: auto; border-radius: 6px;
# MAGIC                       box-shadow: 0 2px 10px rgba(0,0,0,0.10);">
# MAGIC         </div>
# MAGIC         <div class="ing-box-text">
# MAGIC           <div class="step-row">
# MAGIC             <span class="step-badge">1</span>
# MAGIC             <span>From <strong>anywhere in your workspace</strong>, click the <strong>options (⋮) button</strong> in the navigation bar.</span>
# MAGIC           </div>
# MAGIC           <div class="step-row">
# MAGIC             <span class="step-badge">2</span>
# MAGIC             <span>Select <strong>Create</strong> from the context menu.</span>
# MAGIC           </div>
# MAGIC           <div class="step-row">
# MAGIC             <span class="step-badge">3</span>
# MAGIC             <span>Then choose <strong>ETL Pipeline</strong> from the submenu.</span>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- TAB 2 -->
# MAGIC <div class="dbpanel" style="display: none;">
# MAGIC   <div class="four-grid">
# MAGIC     <div class="ing-box method2">
# MAGIC       <div class="ing-box-title">Create an ETL Pipeline — From Jobs &amp; Pipelines</div>
# MAGIC       <div class="ing-box-content">
# MAGIC         <div>
# MAGIC           <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/lecture_lakeflow_spark_declaratives/method2_create_pipeline.png"
# MAGIC                alt="Method 2 — Jobs and Pipelines section with ETL Pipeline option"
# MAGIC                style="width: 800px; max-width: 100%; height: auto; border-radius: 6px;
# MAGIC                       box-shadow: 0 2px 10px rgba(0,0,0,0.10);">
# MAGIC         </div>
# MAGIC         <div class="ing-box-text">
# MAGIC           <div class="step-row">
# MAGIC             <span class="step-badge">1</span>
# MAGIC             <span>Go directly to the <strong>Jobs &amp; Pipelines</strong> section in the main navigation bar.</span>
# MAGIC           </div>
# MAGIC           <div class="step-row">
# MAGIC             <span class="step-badge">2</span>
# MAGIC             <span>Click the <strong>Create</strong> button in the top-right corner.</span>
# MAGIC           </div>
# MAGIC           <div class="step-row">
# MAGIC             <span class="step-badge">3</span>
# MAGIC             <span>Then select <strong>ETL Pipeline</strong> — <em>Build and run an ETL pipeline with SQL and Python</em>.</span>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC <script>
# MAGIC function showEtlTab(n) {
# MAGIC   var tabs   = document.getElementsByClassName("dbtab");
# MAGIC   var panels = document.getElementsByClassName("dbpanel");
# MAGIC   var colors = ["#1B5FA8", "#00A972"];
# MAGIC   for (var i = 0; i < tabs.length; i++) {
# MAGIC     tabs[i].style.color        = "#888";
# MAGIC     tabs[i].style.borderBottom = "3px solid transparent";
# MAGIC     panels[i].style.display    = "none";
# MAGIC   }
# MAGIC   tabs[n-1].style.color        = colors[n-1];
# MAGIC   tabs[n-1].style.borderBottom = "3px solid " + colors[n-1];
# MAGIC   panels[n-1].style.display    = "block";
# MAGIC }
# MAGIC window.onload = function() { showEtlTab(1); };
# MAGIC </script>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## I. Conclusion
# MAGIC
# MAGIC - Databricks combines optimized storage, Unity Catalog governance, and Lakeflow capabilities for ingestion, transformation, and orchestration.
# MAGIC - Apache Spark™ Declarative Pipelines let teams build batch and streaming pipelines in SQL or Python while focusing on transformation logic.
# MAGIC - Declarative pipelines simplify incremental processing, automate medallion architecture workflows, and scale with changing business needs.
# MAGIC
# MAGIC ### Next Steps
# MAGIC
# MAGIC In the next section, you will perform a demonstration on Course Setup and Creating a Pipeline.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC &copy; <span id="dbx-year"></span> Databricks, Inc. All rights reserved.
# MAGIC Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
# MAGIC <script>
# MAGIC   document.getElementById("dbx-year").textContent = new Date().getFullYear();
# MAGIC </script>
# MAGIC