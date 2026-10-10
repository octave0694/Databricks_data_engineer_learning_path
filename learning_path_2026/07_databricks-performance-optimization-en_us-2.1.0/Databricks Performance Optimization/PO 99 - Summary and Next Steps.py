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
# MAGIC # Summary and Next Steps

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="max-width: 1200px; margin: 0 auto; font-family: sans-serif; color: #0b2026;">
# MAGIC
# MAGIC <!-- Title -->
# MAGIC <div style="text-align: center; margin-bottom: 28px;">
# MAGIC   <div style="font-size: 15pt; color: #618794; margin-top: 6px;">The performance optimization toolkit</div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- ===== ROW 1: Data Layout → Query Execution → Diagnose ===== -->
# MAGIC <div style="display: flex; justify-content: center; align-items: stretch; gap: 0;">
# MAGIC
# MAGIC   <!-- 1. Data Layout -->
# MAGIC   <div style="flex: 1; max-width: 280px; background: #F9F7F4; border-radius: 10px; padding: 18px 16px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); text-align: center; border-top: 6px solid #4299E0;">
# MAGIC     <div style="font-size: 15pt; font-weight: 700; color: #0b2026; margin-bottom: 10px;">Data Layout</div>
# MAGIC     <div style="font-size: 14pt; color: #618794; line-height: 1.6; text-align: left;">
# MAGIC       <div style="margin-bottom: 4px;">Partitioning strategy</div>
# MAGIC       <div style="margin-bottom: 4px;">File sizing</div>
# MAGIC       <div style="margin-bottom: 4px;">ZORDER</div>
# MAGIC       <div>Liquid Clustering</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- Arrow -->
# MAGIC   <div style="display: flex; align-items: center; padding: 0 10px; font-size: 26pt; color: #618794;">&#10132;</div>
# MAGIC
# MAGIC   <!-- 2. Query Execution -->
# MAGIC   <div style="flex: 1.1; max-width: 300px; background: #1B5162; border-radius: 10px; padding: 18px 16px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); text-align: center; border-top: 6px solid #FF5F46;">
# MAGIC     <div style="font-size: 15pt; font-weight: 700; color: white; margin-bottom: 10px;">Query Execution</div>
# MAGIC     <div style="display: flex; flex-direction: column; gap: 5px;">
# MAGIC       <div style="background: rgba(255,255,255,0.15); border-radius: 5px; padding: 6px 10px; font-size: 14pt; color: white;">Shuffle &amp; Broadcast Joins</div>
# MAGIC       <div style="background: rgba(255,255,255,0.15); border-radius: 5px; padding: 6px 10px; font-size: 14pt; color: white;">Join Ordering &amp; Spill</div>
# MAGIC       <div style="background: rgba(255,255,255,0.15); border-radius: 5px; padding: 6px 10px; font-size: 14pt; color: white;">ANALYZE TABLE &amp; CBO</div>
# MAGIC       <div style="background: rgba(255,255,255,0.15); border-radius: 5px; padding: 6px 10px; font-size: 14pt; color: white;">UDF Optimization</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- Arrow -->
# MAGIC   <div style="display: flex; align-items: center; padding: 0 10px; font-size: 26pt; color: #618794;">&#10132;</div>
# MAGIC
# MAGIC   <!-- 3. Diagnose & Optimize -->
# MAGIC   <div style="flex: 1; max-width: 280px; background: #F9F7F4; border-radius: 10px; padding: 18px 16px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); text-align: center; border-top: 6px solid #00A972;">
# MAGIC     <div style="font-size: 15pt; font-weight: 700; color: #0b2026; margin-bottom: 10px;">Diagnose &amp; Optimize</div>
# MAGIC     <div style="font-size: 14pt; color: #618794; line-height: 1.6;">
# MAGIC       Use the Spark UI to identify bottlenecks, measure improvements, and validate optimizations
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC <!-- ===== ITERATE LOOP BAR ===== -->
# MAGIC <div style="margin: 16px auto 0 auto; max-width: 1050px; display: flex; align-items: center; justify-content: center; gap: 12px;">
# MAGIC   <div style="flex: 1; height: 4px; background: #FF5F46; border-radius: 2px;"></div>
# MAGIC   <div style="background: #FF5F46; color: white; padding: 6px 22px; border-radius: 20px; font-size: 14pt; font-weight: 700; white-space: nowrap;">
# MAGIC     MEASURE &amp; IMPROVE
# MAGIC   </div>
# MAGIC   <div style="flex: 1; height: 4px; background: #FF5F46; border-radius: 2px;"></div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- ===== COURSE JOURNEY TABLE ===== -->
# MAGIC <div style="margin-top: 20px; background: #F9F7F4; border-radius: 10px; padding: 20px 24px; box-shadow: 0 2px 8px rgba(27,49,57,0.06);">
# MAGIC
# MAGIC <div style="font-size: 16pt; font-weight: 700; color: #1B5162; margin-bottom: 14px;">Your Journey Through the Course</div>
# MAGIC
# MAGIC <style>
# MAGIC .journey-table td, .journey-table th {
# MAGIC   font-size: 14pt !important;
# MAGIC }
# MAGIC </style>
# MAGIC
# MAGIC <table class="journey-table" style="width: 100%; border-collapse: collapse; line-height: 1.5;">
# MAGIC   <thead>
# MAGIC     <tr style="background: #1B5162; color: white;">
# MAGIC       <th style="padding: 10px 14px; text-align: center; border: 1px solid #EEEDE9; width: 50px;">Step</th>
# MAGIC       <th style="padding: 10px 14px; text-align: left; border: 1px solid #EEEDE9;">What You Did</th>
# MAGIC       <th style="padding: 10px 14px; text-align: left; border: 1px solid #EEEDE9;">Key Takeaway</th>
# MAGIC       <th style="padding: 10px 14px; text-align: center; border: 1px solid #EEEDE9; width: 60px;"></th>
# MAGIC     </tr>
# MAGIC   </thead>
# MAGIC   <tbody>
# MAGIC     <tr style="background: white;">
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center; font-weight: 700; color: #1B5162;">1</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Explored over-partitioning and the small file problem</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; color: #618794;">Let Spark handle file layout; avoid unnecessary partitioning</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center; font-size: 16pt; color: #00A972;">&#10003;</td>
# MAGIC     </tr>
# MAGIC     <tr style="background: #F9F7F4;">
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center; font-weight: 700; color: #1B5162;">2</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Compared ZORDER and Liquid Clustering for data skipping</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; color: #618794;">Liquid Clustering is flexible and supports multiple columns without degradation</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center; font-size: 16pt; color: #00A972;">&#10003;</td>
# MAGIC     </tr>
# MAGIC     <tr style="background: white;">
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center; font-weight: 700; color: #1B5162;">3</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Identified shuffle in joins and applied broadcast joins</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; color: #618794;">Broadcast joins eliminate expensive data movement for small table joins</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center; font-size: 16pt; color: #00A972;">&#10003;</td>
# MAGIC     </tr>
# MAGIC     <tr style="background: #F9F7F4;">
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center; font-weight: 700; color: #1B5162;">4</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Diagnosed exploding joins and spill, then tuned join strategies</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; color: #618794;">Join order, shuffle partitions, and ANALYZE TABLE can dramatically reduce spill and runtime</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center; font-size: 16pt; color: #00A972;">&#10003;</td>
# MAGIC     </tr>
# MAGIC     <tr style="background: white;">
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center; font-weight: 700; color: #1B5162;">5</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Compared Python UDFs and SQL UDFs for performance</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; color: #618794;">Prefer native Spark/SQL functions; repartition when UDFs are necessary</td>
# MAGIC       <td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align: center; font-size: 16pt; color: #00A972;">&#10003;</td>
# MAGIC     </tr>
# MAGIC   </tbody>
# MAGIC </table>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC <!-- ===== CLOSING CALLOUT ===== -->
# MAGIC <div style="margin-top: 20px; padding: 20px 28px; background: #FFF6F4; border: 3px solid #FF5F46; border-radius: 10px; text-align: center;">
# MAGIC   <div style="font-size: 17pt; font-weight: 700; color: #FF5F46; margin-bottom: 8px;">Performance optimization is not a one-time task.</div>
# MAGIC   <div style="font-size: 15pt; color: #0b2026; line-height: 1.6;">The best-performing workloads are maintained through continuous monitoring. Use the Spark UI to diagnose bottlenecks, apply targeted optimizations, validate improvements with metrics, and repeat as data volumes and query patterns evolve.</div>
# MAGIC </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC
# MAGIC ##### EXPAND FOR ADDITIONAL NOTES
# MAGIC
# MAGIC <details>
# MAGIC
# MAGIC #### The Complete Optimization Toolkit
# MAGIC
# MAGIC This visual shows the three pillars of performance optimization you explored across all demos and labs. Each builds on the others to deliver fast, efficient workloads.
# MAGIC
# MAGIC #### Data Layout
# MAGIC
# MAGIC How your data is physically organized on disk has a direct impact on query speed. You learned that:
# MAGIC
# MAGIC - **Over-partitioning** creates thousands of small files, leading to excessive cloud storage requests and slow queries. Letting Spark handle file layout avoids this problem entirely.
# MAGIC - **ZORDER** colocates related data in the same files, enabling data skipping for filtered queries — but only on the Z-ordered column.
# MAGIC - **Liquid Clustering** replaces both partitioning and ZORDER with a flexible, multi-column approach. It supports redefining clustering keys without rewriting data and delivers strong file pruning across multiple query patterns.
# MAGIC
# MAGIC #### Query Execution
# MAGIC
# MAGIC Even with good data layout, query execution strategy matters:
# MAGIC
# MAGIC - **Shuffle joins** redistribute data across nodes, which is expensive for large tables. **Broadcast joins** send small tables to all nodes, avoiding the shuffle entirely.
# MAGIC - **Exploding joins** caused by duplicate keys can multiply row counts by 100x, causing massive spill to disk. Reordering joins (smaller first) and using `ANALYZE TABLE` to enable the cost-based optimizer can eliminate spill.
# MAGIC - **Python UDFs** bypass Spark's optimizer and process data serially by default. SQL UDFs are optimized by Catalyst and Photon. When Python UDFs are necessary, repartitioning enables parallel execution.
# MAGIC
# MAGIC #### Diagnose & Optimize
# MAGIC
# MAGIC The Spark UI is your primary diagnostic tool. Throughout this course you learned to read:
# MAGIC
# MAGIC - **Cloud storage request counts** and **response sizes** to understand I/O efficiency
# MAGIC - **Files read** and **files pruned** to verify data skipping effectiveness
# MAGIC - **Shuffle read/write sizes** to identify expensive data movement
# MAGIC - **Disk spill metrics** to detect memory pressure
# MAGIC - **Query plans (DAGs)** to trace the full execution path and verify optimization effects
# MAGIC
# MAGIC </details>
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Predictive Optimization
# MAGIC
# MAGIC <br></br>
# MAGIC <div style="max-width: 1200px; margin: 0 auto; font-family: sans-serif;">
# MAGIC
# MAGIC <div style="background: #F9F7F4; border-radius: 10px; padding: 24px 28px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); border-top: 6px solid #FF5F46;">
# MAGIC
# MAGIC   <div style="font-size: 18pt; font-weight: 700; color: #0b2026; margin-bottom: 14px;">Let Databricks Optimize for You Automatically</div>
# MAGIC
# MAGIC   <div style="font-size: 15pt; color: #0b2026; line-height: 1.7; margin-bottom: 16px;">
# MAGIC     Now that you understand the fundamentals of performance optimization, from data layout and clustering to join strategies and UDF tuning, explore <strong>Predictive Optimization</strong> to automate many of these tasks.
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="font-size: 15pt; color: #0b2026; line-height: 1.7; margin-bottom: 18px;">
# MAGIC     Predictive Optimization automatically identifies tables that would benefit from maintenance operations and runs them for you. It handles <code style="background: #EEEDE9; padding: 2px 6px; border-radius: 3px;">OPTIMIZE</code>, <code style="background: #EEEDE9; padding: 2px 6px; border-radius: 3px;">VACUUM</code>, and <code style="background: #EEEDE9; padding: 2px 6px; border-radius: 3px;">ANALYZE TABLE</code> based on your table's usage patterns, removing the need for manual maintenance scheduling.
# MAGIC   </div>
# MAGIC
# MAGIC   <div style="display: flex; gap: 10px; margin-top: 12px;">
# MAGIC     <a href="https://docs.databricks.com/aws/en/optimizations/predictive-optimization" target="_blank" style="display: inline-block; background: #1B5162; color: white; font-size: 15pt; font-weight: 700; padding: 10px 24px; border-radius: 8px; text-decoration: none;">
# MAGIC       AWS &rarr;
# MAGIC     </a>
# MAGIC     <a href="https://learn.microsoft.com/en-us/azure/databricks/optimizations/predictive-optimization" target="_blank" style="display: inline-block; background: #1B5162; color: white; font-size: 15pt; font-weight: 700; padding: 10px 24px; border-radius: 8px; text-decoration: none;">
# MAGIC       Azure &rarr;
# MAGIC     </a>
# MAGIC     <a href="https://docs.databricks.com/gcp/en/optimizations/predictive-optimization" target="_blank" style="display: inline-block; background: #1B5162; color: white; font-size: 15pt; font-weight: 700; padding: 10px 24px; border-radius: 8px; text-decoration: none;">
# MAGIC       GCP &rarr;
# MAGIC     </a>
# MAGIC   </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Additional Resources
# MAGIC
# MAGIC Explore the following resources to learn more about Databricks performance optimization and stay up to date with the latest platform updates.
# MAGIC
# MAGIC ### A1. Documentation
# MAGIC
# MAGIC - Optimization recommendations on Databricks:
# MAGIC [AWS](https://docs.databricks.com/aws/en/optimizations/) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/optimizations/) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/optimizations/)
# MAGIC
# MAGIC - Partitioning recommendations - When and how to use partitioning:
# MAGIC [AWS](https://docs.databricks.com/aws/en/tables/partitions) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/tables/partitions) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/tables/partitions)
# MAGIC
# MAGIC - Use liquid clustering for tables - Replaces table partitioning and ZORDER:
# MAGIC [AWS](https://docs.databricks.com/aws/en/delta/clustering) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/delta/clustering) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/delta/clustering)
# MAGIC
# MAGIC - Data skipping with Z-order indexes for Delta Lake:
# MAGIC [AWS](https://docs.databricks.com/aws/en/delta/data-skipping) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/delta/data-skipping) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/delta/data-skipping)
# MAGIC
# MAGIC - Optimize performance with caching on Databricks:
# MAGIC [AWS](https://docs.databricks.com/aws/en/optimizations/disk-cache) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/optimizations/disk-cache) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/optimizations/disk-cache)
# MAGIC
# MAGIC ### A2. Blog and Announcements
# MAGIC
# MAGIC - [Announcing Automatic Liquid Clustering: Optimized data layout for up to 10x faster queries](https://www.databricks.com/blog/announcing-automatic-liquid-clustering) - Automatic Liquid Clustering eliminates the need to manually select clustering keys by analyzing query patterns and optimizing data layout automatically.
# MAGIC
# MAGIC - [Arrow-optimized Python UDFs in Apache Spark 3.5](https://www.databricks.com/blog/arrow-optimized-python-udfs-apache-sparktm-35) - How Arrow-optimized UDFs improve the efficiency of data exchange between the Spark runtime and the UDF process.
# MAGIC
# MAGIC - [Top 5 Performance Tips](https://www.databricks.com/blog/2022/03/10/top-5-databricks-performance-tips.html) - Practical tips for optimizing Spark workloads on Databricks.
# MAGIC
# MAGIC - [Accelerate Feature Engineering With Photon](https://www.databricks.com/blog/accelerate-feature-engineering-photon) - Learn how the Photon Engine in Databricks Machine Learning Runtime speeds up Spark jobs and feature engineering workloads by 2x or more.
# MAGIC
# MAGIC ### A3. Additional Features Outside the Scope of this Course
# MAGIC
# MAGIC - Predictive optimization for Unity Catalog managed tables:
# MAGIC [AWS](https://docs.databricks.com/aws/en/optimizations/predictive-optimization) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/optimizations/predictive-optimization) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/optimizations/predictive-optimization)
# MAGIC
# MAGIC - Adaptive Query Execution:
# MAGIC [Apache Spark AQE Documentation](https://spark.apache.org/docs/latest/sql-performance-tuning.html#adaptive-query-execution)
# MAGIC
# MAGIC - ANALYZE TABLE for statistics:
# MAGIC [AWS](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-aux-analyze-table) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-aux-analyze-table) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/sql/language-manual/sql-ref-syntax-aux-analyze-table)
# MAGIC
# MAGIC - Photon runtime:
# MAGIC [AWS](https://docs.databricks.com/aws/en/compute/photon) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/compute/photon) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/compute/photon)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Next Steps
# MAGIC
# MAGIC Continue building your Databricks skills with additional training and certification resources.
# MAGIC
# MAGIC ### B1. Continue Your Learning
# MAGIC
# MAGIC Expand your data and AI knowledge through Databricks self-paced and instructor-led training. These courses help you deepen your technical skills and gain hands-on experience with the Databricks platform.
# MAGIC
# MAGIC Visit the [Databricks Training and Certification](https://www.databricks.com/learn/training/home)
# MAGIC
# MAGIC - [Advanced Data Engineering with Databricks](https://www.databricks.com/training/catalog/advanced-data-engineering-with-databricks-971) - Build production data pipelines at scale with Delta Lake and Structured Streaming.
# MAGIC
# MAGIC - [Data Engineering with Databricks](https://www.databricks.com/learn/partners/partner-courses-and-public-schedule/data-engineering-databricks) - Learn the fundamentals of data engineering on the Databricks Lakehouse Platform.
# MAGIC
# MAGIC - [Apache Spark Programming with Databricks](https://www.databricks.com/training/catalog/apache-spark-programming-with-databricks-134) - Develop proficiency in using Apache Spark on the Databricks platform.
# MAGIC
# MAGIC ### B2. Earn a Certification
# MAGIC
# MAGIC Validate your Databricks expertise by earning an official credential. Certifications demonstrate your ability to apply Databricks technologies in real-world data and AI workloads.
# MAGIC
# MAGIC Visit the [Databricks Certification and Badging](https://www.databricks.com/learn/training/certification)

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>