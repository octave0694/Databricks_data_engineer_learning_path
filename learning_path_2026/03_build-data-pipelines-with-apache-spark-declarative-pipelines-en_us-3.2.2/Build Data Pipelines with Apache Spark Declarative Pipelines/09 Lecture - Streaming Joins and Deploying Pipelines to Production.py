# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ![DBAcademy](https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/databricks_academy.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # Lecture - Streaming Joins and Deploying Pipelines to Production
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC In this lecture, you will learn the fundamentals of streaming joins in Apache Spark™ Declarative Pipelines, including stream-snapshot joins, joins through materialized views, and stream-stream joins. You will also learn how to operationalize a pipeline for production using scheduling, notifications, monitoring, and event logs.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lecture, you will be able to:
# MAGIC
# MAGIC - **Deploy** a Apache Spark™ Declarative Pipeline in production by modifying configuration options like mode, schedule, email notifications and more
# MAGIC - **Analyze** event logs and pipeline metrics to examine the entirety of a pipeline
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## A. What Are Streaming Joins?
# MAGIC
# MAGIC When performing joins with streaming tables in Declarative Pipelines, it is important to understand the different join types and how they behave.
# MAGIC
# MAGIC <div style="max-width:1200px; margin:0 auto; font-family:'Segoe UI',sans-serif;">
# MAGIC   <div style="margin:20px 0;">
# MAGIC     <div style="font-size:14pt; font-weight:800; color:#618794; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:16px; text-align:center;">
# MAGIC       Join Types Covered
# MAGIC     </div>
# MAGIC   <div style="display:grid; grid-template-columns:repeat(auto-fit, minmax(260px, 1fr)); gap:16px; margin:20px 0;">
# MAGIC     <!-- Block 1 -->
# MAGIC     <div style="background:#F9F7F4; border-radius:12px; padding:20px 22px; border:2px solid #2272B4; box-shadow:0 3px 12px rgba(34,114,180,0.10);">
# MAGIC       <div style="font-weight:800; font-size:11pt; color:#0B2026; margin-bottom:14px; border-bottom:2px solid #EEEDE9; padding-bottom:10px;">
# MAGIC         <div style="font-weight:800; font-size:12pt; color:#0B2026;">Stream-Snapshot Join</div>
# MAGIC         <div style="font-size:10pt; color:#618794; margin-top:3px;">Streaming table joined with a static table
# MAGIC </div>
# MAGIC       </div>
# MAGIC       <div style="display:flex; align-items:flex-start; gap:14px; margin-bottom:14px;">
# MAGIC         <div style="font-size:10.5pt; color:#1B3139; line-height:1.8; flex:1;">
# MAGIC           The goal is to <strong>incrementally join new data</strong> from a streaming table with a
# MAGIC           <strong>static lookup table</strong> to create another streaming table.
# MAGIC         </div>
# MAGIC         <img
# MAGIC           src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/lecture_streaming_joins/stream_snapshot_goal.png"
# MAGIC           alt="Stream-Snapshot Join Diagram"
# MAGIC           style="width:120px; height:auto; border-radius:8px; border:1.5px solid #DCE0E2; flex-shrink:0; object-fit:contain; background:white;">
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <!-- Block 2 -->
# MAGIC     <div style="background:#F9F7F4; border-radius:12px; padding:20px 22px; border:2px solid #2272B4; box-shadow:0 3px 12px rgba(34,114,180,0.10);">
# MAGIC       <div style="font-weight:800; font-size:11pt; color:#0B2026; margin-bottom:14px; border-bottom:2px solid #EEEDE9; padding-bottom:10px;">
# MAGIC         <div style="font-weight:800; font-size:12pt; color:#0B2026;">Streaming via Materialized View</div>
# MAGIC           <div style="font-size:10pt; color:#618794; margin-top:3px;">Two streaming tables joined in a materialized view</div>
# MAGIC       </div>
# MAGIC       <div style="display:flex; align-items:flex-start; gap:14px; margin-bottom:14px;">
# MAGIC         <div style="font-size:10.5pt; color:#1B3139; line-height:1.8; flex:1;">
# MAGIC         The goal is to take <strong>all rows from two streaming tables</strong> and join them together each time the pipeline is run.
# MAGIC       </div>
# MAGIC       <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/lecture_streaming_joins/streaming_table_goal.png" alt="Streaming Table Join Diagram"
# MAGIC         style="width:120px; height:auto; border-radius:8px; border:1.5px solid #DCE0E2; flex-shrink:0; object-fit:contain;">
# MAGIC     </div>
# MAGIC     </div>
# MAGIC     <!-- Block 3 -->
# MAGIC     <div style="background:#F9F7F4; border-radius:12px; padding:20px 22px; border:2px solid #2272B4; box-shadow:0 3px 12px rgba(34,114,180,0.10);">
# MAGIC       <div style="font-weight:800; font-size:11pt; color:#0B2026; margin-bottom:14px; border-bottom:2px solid #EEEDE9; padding-bottom:10px;">
# MAGIC         <div style="font-weight:800; font-size:12pt; color:#0B2026;">Stream-Stream Join</div>
# MAGIC           <div style="font-size:10pt; color:#618794; margin-top:3px;">Incremental join of two live streams (advanced)</div>
# MAGIC       </div>
# MAGIC       <div style="display:flex; align-items:flex-start; gap:14px; margin-bottom:14px;">
# MAGIC         <div style="font-size:10.5pt; color:#1B3139; line-height:1.8; flex:1;">
# MAGIC         The goal is to <b>incrementally</b> join new data from two tables, <b>past data is not used</b>
# MAGIC       </div>
# MAGIC       <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/lecture_streaming_joins/stream_stream_joins.png" alt="Stream-Stream Join Diagram"
# MAGIC         style="width:120px; height:auto; border-radius:8px; border:1.5px solid #DCE0E2; flex-shrink:0; object-fit:contain;">
# MAGIC     </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #####EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC <b>Stream-Snapshot Join Overview</b>
# MAGIC
# MAGIC Let’s start with the most straightforward: Joining a Streaming Table to a Static Table (sometimes called a Stream-Snapshot Join or Stream-static join depending on the scenario)
# MAGIC
# MAGIC In this pattern, we incrementally join new data from a streaming table to a static lookup table to create another streaming table.
# MAGIC
# MAGIC For example, imagine you’re ingesting a stream of transactions that includes a country_code column. You can join that with a static reference table that maps each code to a full country name.
# MAGIC
# MAGIC This is useful when you want to enrich your streaming data with reference information that doesn’t change often.
# MAGIC
# MAGIC <b>Joining Streaming Tables with a Materialized View</b>
# MAGIC
# MAGIC Another supported pattern is joining two streaming tables using a materialized view.
# MAGIC
# MAGIC The goal here is to join all rows from both streaming tables each time the pipeline is run.
# MAGIC
# MAGIC This type of join is typically used when both input datasets are changing continuously, and you need to combine them regularly to produce a unified, up-to-date result.
# MAGIC
# MAGIC For example, imagine one stream contains customer activity and another contains product catalog updates. You could join them using a materialized view to enrich activity data with the most recent product information.
# MAGIC
# MAGIC Because both sides are streaming, a materialized view is required to handle this join efficiently and keep the results current.
# MAGIC
# MAGIC The materialized view will process all new rows from both tables and incrementally refresh, depending on pipeline configuration and compute mode.
# MAGIC
# MAGIC <b>Stream-Stream Joins</b>
# MAGIC
# MAGIC The final type of streaming join is a stream-stream join, which is designed to incrementally join new data from two streaming tables as it arrives.
# MAGIC
# MAGIC In this pattern, only the new incoming data from each stream is joined, past data is not considered during each run.
# MAGIC
# MAGIC These joins are useful for detecting relationships between events that occur close together in time, such as joining clickstream data with real-time ad impressions.
# MAGIC
# MAGIC However, because stream-stream joins often involve windowing logic, watermarking, and other advanced streaming concepts, they are outside the scope of this course.
# MAGIC
# MAGIC We recommend reviewing additional Databricks documentation or advanced streaming resources if your use case requires this type of real-time event correlation.
# MAGIC
# MAGIC </details>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Stream-Snapshot Join Overview
# MAGIC
# MAGIC Let’s walk through how this type of join actually works in practice.
# MAGIC
# MAGIC As new data arrives in your streaming table, it is joined in real time with the static reference table.
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="max-width: 1000px; margin: 0 auto; font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;">
# MAGIC
# MAGIC   <style>
# MAGIC     .ssj-wrap {
# MAGIC       display: flex;
# MAGIC       align-items: flex-start;
# MAGIC       justify-content: center;
# MAGIC       gap: 32px;
# MAGIC       margin: 20px 0;
# MAGIC     }
# MAGIC
# MAGIC     .ssj-col {
# MAGIC       display: flex;
# MAGIC       flex-direction: column;
# MAGIC       align-items: center;
# MAGIC       gap: 6px;
# MAGIC     }
# MAGIC
# MAGIC     .ssj-col img {
# MAGIC       width: 56px;
# MAGIC       height: 40px;
# MAGIC       object-fit: contain;
# MAGIC       background: transparent;
# MAGIC       mix-blend-mode: multiply;
# MAGIC       filter: contrast(1.05) brightness(1.02);
# MAGIC     }
# MAGIC
# MAGIC     .ssj-title-out {
# MAGIC       font-size: 12pt;
# MAGIC       font-weight: 700;
# MAGIC       color: #0B2026;
# MAGIC       text-align: center;
# MAGIC     }
# MAGIC
# MAGIC     .ssj-card {
# MAGIC       width: 260px;
# MAGIC       height: 210px;
# MAGIC       border-radius: 6px;
# MAGIC       border: 2px solid #1B3139;  /* black border box */
# MAGIC       background: #FFFFFF;
# MAGIC       box-sizing: border-box;
# MAGIC       padding: 12px 14px;
# MAGIC     }
# MAGIC
# MAGIC     .ssj-card.small {
# MAGIC       width: 230px;
# MAGIC     }
# MAGIC
# MAGIC     /* Streaming table inner dashed box + Append (top aligned) */
# MAGIC     .ssj-append-label {
# MAGIC       font-size: 11pt;
# MAGIC       font-weight: 700;
# MAGIC       color:#0B2026;
# MAGIC       margin-bottom:6px;
# MAGIC       text-align:center;
# MAGIC     }
# MAGIC
# MAGIC     .ssj-append-area {
# MAGIC       border-radius: 4px;
# MAGIC       border: 1.5px dashed #C4CCD6;
# MAGIC       padding: 8px 8px 10px 8px;
# MAGIC       box-sizing: border-box;
# MAGIC     }
# MAGIC
# MAGIC     .ssj-bar-blue {
# MAGIC       height: 10px;
# MAGIC       background:#4299E0;
# MAGIC       border-radius:3px;
# MAGIC       margin-bottom:5px;
# MAGIC     }
# MAGIC     .ssj-bar-blue:last-child { margin-bottom:0; }
# MAGIC
# MAGIC     /* Joined streaming table rows */
# MAGIC     .ssj-row-joined {
# MAGIC       display:flex;
# MAGIC       margin-bottom:5px;
# MAGIC       border-radius:3px;
# MAGIC       overflow:hidden;
# MAGIC       height:10px;
# MAGIC     }
# MAGIC     .ssj-row-joined .left {
# MAGIC       flex:1;
# MAGIC       background:#4299E0;
# MAGIC     }
# MAGIC     .ssj-row-joined .right {
# MAGIC       flex:1;
# MAGIC       background:#00A972;
# MAGIC     }
# MAGIC     .ssj-row-joined:last-child { margin-bottom:0; }
# MAGIC
# MAGIC     /* Static table bars */
# MAGIC     .ssj-static-bar {
# MAGIC       height:10px;
# MAGIC       background:#00A972;
# MAGIC       border-radius:3px;
# MAGIC       margin-bottom:4px;
# MAGIC     }
# MAGIC     .ssj-static-bar:last-child { margin-bottom:0; }
# MAGIC
# MAGIC     /* Connectors */
# MAGIC     .ssj-arrow-horizontal {
# MAGIC       width: 80px;
# MAGIC       height: 0;
# MAGIC       border-top: 2px solid #1B3139;
# MAGIC       position: relative;
# MAGIC       margin-top: 120px;
# MAGIC     }
# MAGIC     .ssj-arrow-horizontal::after {
# MAGIC       content:"";
# MAGIC       position:absolute;
# MAGIC       right:-1px;
# MAGIC       top:-6px;
# MAGIC       border-top:5px solid transparent;
# MAGIC       border-bottom:5px solid transparent;
# MAGIC       border-left:8px solid #1B3139;
# MAGIC     }
# MAGIC
# MAGIC     .ssj-arrow-to-join {
# MAGIC       width: 80px;
# MAGIC       height: 0;
# MAGIC       border-top: 2px solid #1B3139;
# MAGIC       position: relative;
# MAGIC       margin-top: 120px;
# MAGIC       margin-left: -8px;
# MAGIC     }
# MAGIC     .ssj-arrow-to-join::after {
# MAGIC       content:"";
# MAGIC       position:absolute;
# MAGIC       right:-1px;
# MAGIC       top:-6px;
# MAGIC       border-top:5px solid transparent;
# MAGIC       border-bottom:5px solid transparent;
# MAGIC       border-left:8px solid #1B3139;
# MAGIC     }
# MAGIC
# MAGIC     /* Ingest arrow */
# MAGIC     .sj-ingest-arrow {
# MAGIC       width: 100px;
# MAGIC       height: 40px;
# MAGIC       border-radius: 6px;
# MAGIC       background: #4299E0;
# MAGIC       clip-path: polygon(0 20%, 60% 20%, 60% 0, 100% 50%, 60% 100%, 60% 80%, 0 80%);
# MAGIC       margin-top: 100px;
# MAGIC     }
# MAGIC   </style>
# MAGIC
# MAGIC   <div class="ssj-wrap">
# MAGIC     <!-- Left: ingest arrow using CSS shape -->
# MAGIC     <div class="sj-ingest-arrow"></div>
# MAGIC     <!-- Streaming Table -->
# MAGIC     <div class="ssj-col">
# MAGIC       <!-- Icon and name OUTSIDE the box -->
# MAGIC       <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/streaming.png" alt="Streaming table icon">
# MAGIC       <div class="ssj-title-out">Streaming Table</div>
# MAGIC       <!-- Black border box with dashed inner box + Append at top -->
# MAGIC       <div class="ssj-card">
# MAGIC         <div class="ssj-append-area">
# MAGIC           <div class="ssj-bar-blue"></div>
# MAGIC           <div class="ssj-bar-blue"></div>
# MAGIC           <div class="ssj-bar-blue"></div>
# MAGIC         </div>
# MAGIC         <div class="ssj-append-label">Append</div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <!-- Arrow to Joined Streaming Table -->
# MAGIC     <div class="ssj-arrow-horizontal"></div>
# MAGIC     <!-- Joined Streaming Table -->
# MAGIC     <div class="ssj-col">
# MAGIC       <!-- Icon and name OUTSIDE the box -->
# MAGIC       <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/streaming.png" alt="Streaming table icon">
# MAGIC       <div class="ssj-title-out">Joined Streaming Table</div>
# MAGIC       <!-- Black border box -->
# MAGIC       <div class="ssj-card">
# MAGIC         <div class="ssj-row-joined">
# MAGIC           <div class="left"></div>
# MAGIC           <div class="right"></div>
# MAGIC         </div>
# MAGIC         <div class="ssj-row-joined">
# MAGIC           <div class="left"></div>
# MAGIC           <div class="right"></div>
# MAGIC         </div>
# MAGIC         <div class="ssj-row-joined">
# MAGIC           <div class="left"></div>
# MAGIC           <div class="right"></div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <!-- Arrow from Static Table into Joined Streaming Table -->
# MAGIC     <div class="ssj-arrow-to-join"></div>
# MAGIC     <!-- Static Table -->
# MAGIC     <div class="ssj-col">
# MAGIC       <!-- Icon and name OUTSIDE the box -->
# MAGIC       <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/static_table_icon.png" alt="Static table icon">
# MAGIC       <div class="ssj-title-out">Static Table</div>
# MAGIC       <!-- Black border box -->
# MAGIC       <div class="ssj-card small">
# MAGIC         <div class="ssj-static-bar"></div>
# MAGIC         <div class="ssj-static-bar"></div>
# MAGIC         <div class="ssj-static-bar"></div>
# MAGIC         <div class="ssj-static-bar"></div>
# MAGIC         <div class="ssj-static-bar"></div>
# MAGIC         <div class="ssj-static-bar"></div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #####EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC That means every new record in the streaming table is matched against all available data in the static table, ensuring a complete and accurate join each time.
# MAGIC
# MAGIC The result of that join is then appended to your final streaming output table, continuing the pipeline flow.
# MAGIC
# MAGIC This is a great pattern for enriching streaming data with context, like converting country codes, product IDs, or user roles into readable formats using static reference data.
# MAGIC
# MAGIC It’s efficient, reliable, and works well since only one side of the join, the streaming table, is changing over time.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC A key detail to understand here is that as new data is appended to your source streaming table, only the new rows are joined with the entire static lookup table.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="max-width: 1100px; margin: 0 auto; font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;">
# MAGIC
# MAGIC   <style>
# MAGIC     .ssj-wrap {
# MAGIC       display: flex;
# MAGIC       align-items: flex-start;
# MAGIC       justify-content: center;
# MAGIC       gap: 32px;
# MAGIC       margin: 20px 0;
# MAGIC     }
# MAGIC
# MAGIC     .ssj-col {
# MAGIC       display: flex;
# MAGIC       flex-direction: column;
# MAGIC       align-items: center;
# MAGIC       gap: 6px;
# MAGIC     }
# MAGIC
# MAGIC     .ssj-col img {
# MAGIC       width: 56px;
# MAGIC       height: 40px;
# MAGIC       object-fit: contain;
# MAGIC       background: transparent;
# MAGIC       mix-blend-mode: multiply;
# MAGIC       filter: contrast(1.05) brightness(1.02);
# MAGIC     }
# MAGIC
# MAGIC     .ssj-title-out {
# MAGIC       font-size: 12pt;
# MAGIC       font-weight: 700;
# MAGIC       color: #0B2026;
# MAGIC       text-align: center;
# MAGIC     }
# MAGIC
# MAGIC     .ssj-card {
# MAGIC       width: 260px;
# MAGIC       height: 210px;
# MAGIC       border-radius: 6px;
# MAGIC       border: 2px solid #1B3139;
# MAGIC       background: #FFFFFF;
# MAGIC       box-sizing: border-box;
# MAGIC       padding: 12px 14px;
# MAGIC     }
# MAGIC
# MAGIC     .ssj-card.small {
# MAGIC       width: 230px;
# MAGIC     }
# MAGIC
# MAGIC     /* Streaming table inner dashed box + Append */
# MAGIC     .ssj-append-area {
# MAGIC       border-radius: 4px;
# MAGIC       border: 1.5px dashed #C4CCD6;
# MAGIC       padding: 8px 8px 10px 8px;
# MAGIC       box-sizing: border-box;
# MAGIC       margin-top: 46px;
# MAGIC     }
# MAGIC
# MAGIC     .ssj-bar-blue {
# MAGIC       height: 10px;
# MAGIC       background:#4299E0;
# MAGIC       border-radius:3px;
# MAGIC       margin-bottom:5px;
# MAGIC     }
# MAGIC     .ssj-bar-blue:last-child { margin-bottom:0; }
# MAGIC
# MAGIC     .ssj-append-label {
# MAGIC       font-size: 11pt;
# MAGIC       font-weight: 700;
# MAGIC       color:#0B2026;
# MAGIC       margin-top:10px;
# MAGIC       text-align:center;
# MAGIC     }
# MAGIC
# MAGIC     /* Grey header rows in tables */
# MAGIC     .ssj-grey-row {
# MAGIC       height:10px;
# MAGIC       background:#D3D6DA;
# MAGIC       border-radius:3px;
# MAGIC       margin-bottom:4px;
# MAGIC     }
# MAGIC
# MAGIC     /* Joined streaming table rows */
# MAGIC     .ssj-row-joined {
# MAGIC       display:flex;
# MAGIC       margin-bottom:5px;
# MAGIC       border-radius:3px;
# MAGIC       overflow:hidden;
# MAGIC       height:10px;
# MAGIC     }
# MAGIC     .ssj-row-joined .left {
# MAGIC       flex:1;
# MAGIC       background:#4299E0;
# MAGIC     }
# MAGIC     .ssj-row-joined .right {
# MAGIC       flex:1;
# MAGIC       background:#00A972;
# MAGIC     }
# MAGIC     .ssj-row-joined:last-child { margin-bottom:0; }
# MAGIC
# MAGIC     /* Static table bars */
# MAGIC     .ssj-static-bar {
# MAGIC       height:10px;
# MAGIC       background:#00A972;
# MAGIC       border-radius:3px;
# MAGIC       margin-bottom:4px;
# MAGIC     }
# MAGIC     .ssj-static-bar:last-child { margin-bottom:0; }
# MAGIC
# MAGIC     /* Connectors */
# MAGIC     .ssj-arrow-horizontal {
# MAGIC       width: 80px;
# MAGIC       height: 0;
# MAGIC       border-top: 2px solid #1B3139;
# MAGIC       position: relative;
# MAGIC       margin-top: 120px;
# MAGIC     }
# MAGIC     .ssj-arrow-horizontal::after {
# MAGIC       content:"";
# MAGIC       position:absolute;
# MAGIC       right:-1px;
# MAGIC       top:-6px;
# MAGIC       border-top:5px solid transparent;
# MAGIC       border-bottom:5px solid transparent;
# MAGIC       border-left:8px solid #1B3139;
# MAGIC     }
# MAGIC
# MAGIC     .ssj-arrow-to-join {
# MAGIC       width: 80px;
# MAGIC       height: 0;
# MAGIC       border-top: 2px solid #1B3139;
# MAGIC       position: relative;
# MAGIC       margin-top: 120px;
# MAGIC       margin-left: -8px;
# MAGIC     }
# MAGIC     .ssj-arrow-to-join::after {
# MAGIC       content:"";
# MAGIC       position:absolute;
# MAGIC       right:-1px;
# MAGIC       top:-6px;
# MAGIC       border-top:5px solid transparent;
# MAGIC       border-bottom:5px solid transparent;
# MAGIC       border-left:8px solid #1B3139;
# MAGIC     }
# MAGIC
# MAGIC     /* Ingest arrow */
# MAGIC     .sj-ingest-arrow {
# MAGIC       width: 100px;
# MAGIC       height: 40px;
# MAGIC       border-radius: 6px;
# MAGIC       background: #4299E0;
# MAGIC       clip-path: polygon(0 20%, 60% 20%, 60% 0, 100% 50%, 60% 100%, 60% 80%, 0 80%);
# MAGIC       margin-top: 100px;
# MAGIC     }
# MAGIC   </style>
# MAGIC
# MAGIC   <div class="ssj-wrap">
# MAGIC     <!-- Left: ingest arrow using CSS shape -->
# MAGIC     <div class="sj-ingest-arrow"></div>
# MAGIC     <!-- Streaming Table -->
# MAGIC     <div class="ssj-col">
# MAGIC       <!-- Icon and name OUTSIDE the box -->
# MAGIC       <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/streaming.png" alt="Streaming table icon">
# MAGIC       <div class="ssj-title-out">Streaming Table</div>
# MAGIC       <!-- Card -->
# MAGIC       <div class="ssj-card">
# MAGIC         <div class="ssj-grey-row"></div>
# MAGIC         <div class="ssj-grey-row"></div>
# MAGIC         <div class="ssj-grey-row" style="margin-bottom:10px;"></div>
# MAGIC         <div class="ssj-append-area">
# MAGIC           <div class="ssj-bar-blue"></div>
# MAGIC           <div class="ssj-bar-blue"></div>
# MAGIC           <div class="ssj-bar-blue"></div>
# MAGIC         </div>
# MAGIC         <div class="ssj-append-label">Append</div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <!-- Arrow to Joined Streaming Table -->
# MAGIC     <div class="ssj-arrow-horizontal"></div>
# MAGIC     <!-- Joined Streaming Table -->
# MAGIC     <div class="ssj-col">
# MAGIC       <!-- Icon and name OUTSIDE the box -->
# MAGIC       <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/streaming.png" alt="Joined streaming table icon">
# MAGIC       <div class="ssj-title-out">Joined Streaming Table</div>
# MAGIC       <!-- Card -->
# MAGIC       <div class="ssj-card">
# MAGIC         <div class="ssj-grey-row"></div>
# MAGIC         <div class="ssj-grey-row"></div>
# MAGIC         <div class="ssj-grey-row" style="margin-bottom:10px;"></div>
# MAGIC         <div class="ssj-row-joined">
# MAGIC           <div class="left"></div>
# MAGIC           <div class="right"></div>
# MAGIC         </div>
# MAGIC         <div class="ssj-row-joined">
# MAGIC           <div class="left"></div>
# MAGIC           <div class="right"></div>
# MAGIC         </div>
# MAGIC         <div class="ssj-row-joined">
# MAGIC           <div class="left"></div>
# MAGIC           <div class="right"></div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <!-- Arrow from Static Table into Joined Streaming Table -->
# MAGIC     <div class="ssj-arrow-to-join"></div>
# MAGIC     <!-- Static Table -->
# MAGIC     <div class="ssj-col">
# MAGIC       <!-- Icon and name OUTSIDE the box -->
# MAGIC       <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/static_table_icon.png" alt="Static table icon">
# MAGIC       <div class="ssj-title-out">Static Table</div>
# MAGIC       <!-- Card -->
# MAGIC       <div class="ssj-card small">
# MAGIC         <div class="ssj-static-bar"></div>
# MAGIC         <div class="ssj-static-bar"></div>
# MAGIC         <div class="ssj-static-bar"></div>
# MAGIC         <div class="ssj-static-bar"></div>
# MAGIC         <div class="ssj-static-bar"></div>
# MAGIC         <div class="ssj-static-bar"></div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #####EXPAND FOR ADDITIONAL NOTES
# MAGIC
# MAGIC <details>
# MAGIC
# MAGIC This is an incremental join, meaning the static table doesn’t need to be reprocessed, only the incoming streaming data is evaluated.
# MAGIC
# MAGIC This keeps the process highly efficient, and ensures that each new record is immediately enriched with the latest reference data from the static table.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #####Documentation
# MAGIC
# MAGIC - <a href="https://docs.databricks.com/gcp/en/transform/join#stream-stream-joins" style="color: #1976d2; text-decoration: underline;">Stream-snapshot joins</a>

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Joining Streaming Tables with a Materialized View
# MAGIC
# MAGIC Let’s walk through how this join actually works in practice.
# MAGIC
# MAGIC As new data is appended to both streaming tables, the materialized view detects the changes and responds accordingly.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="max-width: 1100px; margin: 0 auto; font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;">
# MAGIC
# MAGIC   <style>
# MAGIC     .mv-wrap {
# MAGIC       display: flex;
# MAGIC       align-items: flex-start;
# MAGIC       justify-content: center;
# MAGIC       gap: 32px;
# MAGIC       margin: 24px 0;
# MAGIC     }
# MAGIC
# MAGIC     .mv-col {
# MAGIC       display: flex;
# MAGIC       flex-direction: column;
# MAGIC       align-items: center;
# MAGIC       gap: 6px;
# MAGIC     }
# MAGIC
# MAGIC     .mv-col img {
# MAGIC       width: 56px;
# MAGIC       height: 40px;
# MAGIC       object-fit: contain;
# MAGIC       background: transparent;
# MAGIC       mix-blend-mode: multiply;
# MAGIC       filter: contrast(1.05) brightness(1.02);
# MAGIC     }
# MAGIC
# MAGIC     .mv-title {
# MAGIC       font-size: 13pt;
# MAGIC       font-weight: 700;
# MAGIC       color: #0B2026;
# MAGIC       text-align: center;
# MAGIC     }
# MAGIC
# MAGIC     .mv-card {
# MAGIC       width: 280px;
# MAGIC       height: 220px;
# MAGIC       border-radius: 6px;
# MAGIC       border: 2px solid #1B3139;
# MAGIC       background: #FFFFFF;
# MAGIC       box-sizing: border-box;
# MAGIC       padding: 12px 16px;
# MAGIC     }
# MAGIC
# MAGIC     /* Dashed “streaming” area */
# MAGIC     .mv-dashed {
# MAGIC       border-radius: 4px;
# MAGIC       border: 1.5px dashed #C4CCD6;
# MAGIC       padding: 8px 8px 10px 8px;
# MAGIC       box-sizing: border-box;
# MAGIC       margin-bottom: 10px;
# MAGIC       margin-top: 4px;
# MAGIC     }
# MAGIC
# MAGIC     .mv-bar-blue {
# MAGIC       height: 10px;
# MAGIC       background:#4299E0;
# MAGIC       border-radius:3px;
# MAGIC       margin-bottom:5px;
# MAGIC     }
# MAGIC     .mv-bar-blue:last-child { margin-bottom:0; }
# MAGIC
# MAGIC     .mv-bar-yellow {
# MAGIC       height: 10px;
# MAGIC       background:#FFB020;
# MAGIC       border-radius:3px;
# MAGIC       margin-bottom:5px;
# MAGIC     }
# MAGIC     .mv-bar-yellow:last-child { margin-bottom:0; }
# MAGIC
# MAGIC     .mv-append {
# MAGIC       font-size: 12pt;
# MAGIC       font-weight: 700;
# MAGIC       color:#0B2026;
# MAGIC       text-align:center;
# MAGIC       margin-top: 4px;
# MAGIC     }
# MAGIC
# MAGIC     /* Joined MV rows: half blue, half yellow */
# MAGIC     .mv-row-joined {
# MAGIC       display:flex;
# MAGIC       margin-bottom:6px;
# MAGIC       border-radius:3px;
# MAGIC       overflow:hidden;
# MAGIC       height:10px;
# MAGIC       margin-top: 8px;
# MAGIC     }
# MAGIC     .mv-row-joined .left {
# MAGIC       flex:1;
# MAGIC       background:#4299E0;
# MAGIC     }
# MAGIC     .mv-row-joined .right {
# MAGIC       flex:1;
# MAGIC       background:#FFB020;
# MAGIC     }
# MAGIC     .mv-row-joined:last-child { margin-bottom:0; }
# MAGIC
# MAGIC     /* Connector arrows */
# MAGIC     .mv-arrow-horizontal {
# MAGIC       width: 90px;
# MAGIC       height: 0;
# MAGIC       border-top: 2px solid #1B3139;
# MAGIC       position: relative;
# MAGIC       margin-top: 120px;
# MAGIC     }
# MAGIC     .mv-arrow-horizontal::after {
# MAGIC       content:"";
# MAGIC       position:absolute;
# MAGIC       right:-1px;
# MAGIC       top:-6px;
# MAGIC       border-top:5px solid transparent;
# MAGIC       border-bottom:5px solid transparent;
# MAGIC       border-left:8px solid #1B3139;
# MAGIC     }
# MAGIC
# MAGIC     /* Ingest arrow */
# MAGIC     .sj-ingest-arrow {
# MAGIC       width: 100px;
# MAGIC       height: 40px;
# MAGIC       border-radius: 6px;
# MAGIC       background: #4299E0;
# MAGIC       clip-path: polygon(0 20%, 60% 20%, 60% 0, 100% 50%, 60% 100%, 60% 80%, 0 80%);
# MAGIC       margin-top: 100px;
# MAGIC     }
# MAGIC
# MAGIC     /* Side ingest arrows */
# MAGIC     .mv-arrow-left {
# MAGIC       width: 80px;
# MAGIC       height: 40px;
# MAGIC       border-radius: 6px;
# MAGIC       background: #4299E0;
# MAGIC       clip-path: polygon(0 50%, 40% 0, 40% 25%, 100% 25%, 100% 75%, 40% 75%, 40% 100%);
# MAGIC       margin-top: 110px;
# MAGIC     }
# MAGIC
# MAGIC     .mv-arrow-right {
# MAGIC       width: 80px;
# MAGIC       height: 40px;
# MAGIC       border-radius: 6px;
# MAGIC       background: #FFB020;
# MAGIC       clip-path: polygon(0 25%, 60% 25%, 60% 0, 100% 50%, 60% 100%, 60% 75%, 0 75%);
# MAGIC       margin-top: 110px;
# MAGIC     }
# MAGIC   </style>
# MAGIC
# MAGIC   <div style="display:flex; align-items:flex-start; justify-content:center; gap:20px;">
# MAGIC     <!-- Left outer arrow -->
# MAGIC     <div class="mv-arrow-left"></div>
# MAGIC     <div class="mv-wrap">
# MAGIC       <!-- Left Streaming Table -->
# MAGIC       <div class="mv-col">
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/streaming.png" alt="Streaming table icon">
# MAGIC         <div class="mv-title">Streaming Table</div>
# MAGIC         <div class="mv-card">
# MAGIC           <div class="mv-dashed">
# MAGIC             <div class="mv-bar-blue"></div>
# MAGIC             <div class="mv-bar-blue"></div>
# MAGIC             <div class="mv-bar-blue"></div>
# MAGIC           </div>
# MAGIC           <div class="mv-append">Append</div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC       <!-- Arrow to Joined MV -->
# MAGIC       <div class="mv-arrow-horizontal"></div>
# MAGIC       <!-- Joined Materialized View -->
# MAGIC       <div class="mv-col">
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/materialized.png" alt="Joined materialized view icon">
# MAGIC         <div class="mv-title">Joined Materialized View</div>
# MAGIC         <div class="mv-card">
# MAGIC           <div class="mv-row-joined">
# MAGIC             <div class="left"></div>
# MAGIC             <div class="right"></div>
# MAGIC           </div>
# MAGIC           <div class="mv-row-joined">
# MAGIC             <div class="left"></div>
# MAGIC             <div class="right"></div>
# MAGIC           </div>
# MAGIC           <div class="mv-row-joined">
# MAGIC             <div class="left"></div>
# MAGIC             <div class="right"></div>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC       <!-- Arrow back to right Streaming Table -->
# MAGIC       <div class="mv-arrow-horizontal" style="transform:scaleX(-1);"></div>
# MAGIC       <!-- Right Streaming Table -->
# MAGIC       <div class="mv-col">
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/streaming.png" alt="Streaming table icon">
# MAGIC         <div class="mv-title">Streaming Table</div>
# MAGIC         <div class="mv-card">
# MAGIC           <div class="mv-dashed">
# MAGIC             <div class="mv-bar-yellow"></div>
# MAGIC             <div class="mv-bar-yellow"></div>
# MAGIC             <div class="mv-bar-yellow"></div>
# MAGIC           </div>
# MAGIC           <div class="mv-append">Append</div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <!-- Right outer arrow -->
# MAGIC     <div class="mv-arrow-right"></div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #####EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC Each time the pipeline runs, the materialized view will efficiently compute the join by processing all the current data from both streaming tables.
# MAGIC
# MAGIC This ensures that every new record from either side is matched correctly, producing an up-to-date and complete output.
# MAGIC
# MAGIC The use of a materialized view allows the join to scale efficiently, leveraging incremental refresh where possible to avoid unnecessary recomputation.
# MAGIC
# MAGIC This join pattern is ideal when both data sources are live and frequently updated, and you want to keep your results synchronized across both.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC As new data is added to the streaming tables, the materialized view will again efficiently compute all the data in the streaming tables and joins the rows.
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="max-width: 1100px; margin: 0 auto; font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;">
# MAGIC
# MAGIC   <style>
# MAGIC     .mv2-wrap {
# MAGIC       display: flex;
# MAGIC       align-items: flex-start;
# MAGIC       justify-content: center;
# MAGIC       gap: 32px;
# MAGIC       margin: 24px 0;
# MAGIC     }
# MAGIC
# MAGIC     .mv2-col {
# MAGIC       display: flex;
# MAGIC       flex-direction: column;
# MAGIC       align-items: center;
# MAGIC       gap: 6px;
# MAGIC     }
# MAGIC
# MAGIC     .mv2-col img {
# MAGIC       width: 56px;
# MAGIC       height: 40px;
# MAGIC       object-fit: contain;
# MAGIC       background: transparent;
# MAGIC       mix-blend-mode: multiply;
# MAGIC       filter: contrast(1.05) brightness(1.02);
# MAGIC     }
# MAGIC
# MAGIC     .mv2-title {
# MAGIC       font-size: 13pt;
# MAGIC       font-weight: 700;
# MAGIC       color: #0B2026;
# MAGIC       text-align: center;
# MAGIC     }
# MAGIC
# MAGIC     .mv2-card {
# MAGIC       width: 260px;
# MAGIC       height: 210px;
# MAGIC       border-radius: 6px;
# MAGIC       border: 2px solid #1B3139;
# MAGIC       background: #FFFFFF;
# MAGIC       box-sizing: border-box;
# MAGIC       padding: 10px 14px 14px 14px;
# MAGIC     }
# MAGIC
# MAGIC     .mv2-card.small {
# MAGIC       width: 260px;
# MAGIC     }
# MAGIC
# MAGIC     /* Dashed areas for streaming tables */
# MAGIC     .mv2-dashed {
# MAGIC       border-radius: 4px;
# MAGIC       border: 1.5px dashed #C4CCD6;
# MAGIC       padding: 8px 8px 10px 8px;
# MAGIC       box-sizing: border-box;
# MAGIC       margin-bottom: 8px;
# MAGIC     }
# MAGIC
# MAGIC     .mv2-bar-blue {
# MAGIC       height: 10px;
# MAGIC       background:#4299E0;
# MAGIC       border-radius:3px;
# MAGIC       margin-bottom:5px;
# MAGIC     }
# MAGIC     .mv2-bar-blue:last-child { margin-bottom:0; }
# MAGIC
# MAGIC     .mv2-bar-yellow {
# MAGIC       height: 10px;
# MAGIC       background:#FFB020;
# MAGIC       border-radius:3px;
# MAGIC       margin-bottom:5px;
# MAGIC     }
# MAGIC     .mv2-bar-yellow:last-child { margin-bottom:0; }
# MAGIC
# MAGIC     .mv2-append {
# MAGIC       font-size: 12pt;
# MAGIC       font-weight: 700;
# MAGIC       color:#0B2026;
# MAGIC       text-align:center;
# MAGIC       margin-top: 4px;
# MAGIC     }
# MAGIC
# MAGIC     /* Joined MV rows: half blue, half yellow */
# MAGIC     .mv2-row-joined {
# MAGIC       display:flex;
# MAGIC       margin-bottom:6px;
# MAGIC       border-radius:3px;
# MAGIC       overflow:hidden;
# MAGIC       height:10px;
# MAGIC       margin-top: 4px;
# MAGIC     }
# MAGIC     .mv2-row-joined .left {
# MAGIC       flex:1;
# MAGIC       background:#4299E0;
# MAGIC     }
# MAGIC     .mv2-row-joined .right {
# MAGIC       flex:1;
# MAGIC       background:#FFB020;
# MAGIC     }
# MAGIC     .mv2-row-joined:last-child { margin-bottom:0; }
# MAGIC
# MAGIC     /* Connectors between boxes */
# MAGIC     .mv2-arrow-horizontal {
# MAGIC       width: 90px;
# MAGIC       height: 0;
# MAGIC       border-top: 2px solid #1B3139;
# MAGIC       position: relative;
# MAGIC       margin-top: 120px;
# MAGIC     }
# MAGIC     .mv2-arrow-horizontal::after {
# MAGIC       content:"";
# MAGIC       position:absolute;
# MAGIC       right:-1px;
# MAGIC       top:-6px;
# MAGIC       border-top:5px solid transparent;
# MAGIC       border-bottom:5px solid transparent;
# MAGIC       border-left:8px solid #1B3139;
# MAGIC     }
# MAGIC
# MAGIC     /* Outer ingest arrows */
# MAGIC     .mv2-arrow-left {
# MAGIC       width: 100px;
# MAGIC       height: 40px;
# MAGIC       border-radius: 6px;
# MAGIC       background: #4299E0;
# MAGIC       clip-path: polygon(0 20%, 60% 20%, 60% 0, 100% 50%, 60% 100%, 60% 80%, 0 80%);
# MAGIC       margin-top: 110px;
# MAGIC     }
# MAGIC
# MAGIC     .mv2-arrow-right {
# MAGIC       width: 100px;
# MAGIC       height: 40px;
# MAGIC       border-radius: 6px;
# MAGIC       background: #FFB020;
# MAGIC       clip-path: polygon(0 20%, 60% 20%, 60% 0, 100% 50%, 60% 100%, 60% 80%, 0 80%);
# MAGIC       margin-top: 110px;
# MAGIC       transform: scaleX(-1);
# MAGIC     }
# MAGIC   </style>
# MAGIC
# MAGIC   <div style="display:flex; align-items:flex-start; justify-content:center; gap:20px;">
# MAGIC     <!-- Left outer arrow -->
# MAGIC     <div class="mv2-arrow-left"></div>
# MAGIC     <div class="mv2-wrap">
# MAGIC       <!-- Left Streaming Table (blue) -->
# MAGIC       <div class="mv2-col">
# MAGIC         <!-- Replace src with streaming-table icon -->
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/streaming.png" alt="Streaming table icon">
# MAGIC         <div class="mv2-title">Streaming Table</div>
# MAGIC         <div class="mv2-card">
# MAGIC           <div class="mv2-dashed">
# MAGIC             <div class="mv2-bar-blue"></div>
# MAGIC             <div class="mv2-bar-blue"></div>
# MAGIC           </div>
# MAGIC           <div class="mv2-dashed">
# MAGIC             <div class="mv2-bar-blue"></div>
# MAGIC             <div class="mv2-bar-blue"></div>
# MAGIC           </div>
# MAGIC           <div class="mv2-append">Append</div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC       <!-- Arrow to Joined Materialized View -->
# MAGIC       <div class="mv2-arrow-horizontal"></div>
# MAGIC       <!-- Joined Materialized View -->
# MAGIC       <div class="mv2-col">
# MAGIC         <!-- Replace src with materialized-view icon -->
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/materialized.png" alt="Joined materialized view icon">
# MAGIC         <div class="mv2-title">Joined Materialized View</div>
# MAGIC         <div class="mv2-card small">
# MAGIC           <div class="mv2-row-joined">
# MAGIC             <div class="left"></div>
# MAGIC             <div class="right"></div>
# MAGIC           </div>
# MAGIC           <div class="mv2-row-joined">
# MAGIC             <div class="left"></div>
# MAGIC             <div class="right"></div>
# MAGIC           </div>
# MAGIC           <div class="mv2-row-joined">
# MAGIC             <div class="left"></div>
# MAGIC             <div class="right"></div>
# MAGIC           </div>
# MAGIC           <div class="mv2-row-joined">
# MAGIC             <div class="left"></div>
# MAGIC             <div class="right"></div>
# MAGIC           </div>
# MAGIC           <div class="mv2-row-joined">
# MAGIC             <div class="left"></div>
# MAGIC             <div class="right"></div>
# MAGIC           </div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC       <!-- Arrow from MV back to right Streaming Table -->
# MAGIC       <div class="mv2-arrow-horizontal" style="transform:scaleX(-1);"></div>
# MAGIC       <!-- Right Streaming Table (yellow) -->
# MAGIC       <div class="mv2-col">
# MAGIC         <!-- Replace src with streaming-table icon -->
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/streaming.png" alt="Streaming table icon">
# MAGIC         <div class="mv2-title">Streaming Table</div>
# MAGIC         <div class="mv2-card">
# MAGIC           <div class="mv2-dashed">
# MAGIC             <div class="mv2-bar-yellow"></div>
# MAGIC             <div class="mv2-bar-yellow"></div>
# MAGIC           </div>
# MAGIC           <div class="mv2-dashed">
# MAGIC             <div class="mv2-bar-yellow"></div>
# MAGIC             <div class="mv2-bar-yellow"></div>
# MAGIC           </div>
# MAGIC           <div class="mv2-append">Append</div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <!-- Right outer arrow -->
# MAGIC     <div class="mv2-arrow-right"></div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## D. Stream-Stream Joins
# MAGIC
# MAGIC The final type of streaming join is a stream-stream join, which is designed to incrementally join new data from two streaming tables as it arrives.
# MAGIC
# MAGIC - In this pattern, only the new incoming data from each stream is joined, past data is not considered during each run.
# MAGIC
# MAGIC - These joins are useful for detecting relationships between events that occur close together in time, such as joining clickstream data with real-time ad impressions.
# MAGIC
# MAGIC However, because stream-stream joins often involve windowing logic, watermarking, and other advanced streaming concepts, they are outside the scope of this course.
# MAGIC
# MAGIC We recommend reviewing additional Databricks documentation or advanced streaming resources if your use case requires this type of real-time event correlation.
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #####Documentation
# MAGIC
# MAGIC - <a href="https://docs.databricks.com/gcp/en/transform/join#stream-stream-joins" style="color: #1976d2; text-decoration: underline;">Stream-stream joins</a>
# MAGIC - <a href="https://docs.databricks.com/aws/en/dlt/stateful-processing"  style="color: #1976d2; text-decoration: underline;">Optimize stateful processing in Apache Spark™ Declarative Pipelines with watermarks</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## E. Comparing the Join Types
# MAGIC
# MAGIC The table below summarizes all three join patterns at a glance. Use this as a quick reference when deciding which join type fits your pipeline's requirements.
# MAGIC
# MAGIC <div style="max-width:1000px; margin:0 auto; font-family:'Segoe UI',sans-serif;">
# MAGIC
# MAGIC <!-- Comparison Table -->
# MAGIC <div style="border-radius:12px; overflow:hidden; border:2px solid #EEEDE9; box-shadow:0 3px 12px rgba(0,0,0,0.06); margin:20px 0;">
# MAGIC   <div style="background:#0B2026; padding:12px 20px; color:white; font-weight:800; font-size:11pt;">All Three Join Types — At a Glance</div>
# MAGIC   <table style="width:100%; border-collapse:collapse; font-family:'Segoe UI',sans-serif;">
# MAGIC     <thead>
# MAGIC       <tr style="background:#F9F7F4;">
# MAGIC         <th style="padding:12px 16px; text-align:left; font-size:9.5pt; color:#618794; text-transform:uppercase; letter-spacing:0.06em; border-bottom:2px solid #EEEDE9;">Join Type</th>
# MAGIC         <th style="padding:12px 16px; text-align:left; font-size:9.5pt; color:#618794; text-transform:uppercase; letter-spacing:0.06em; border-bottom:2px solid #EEEDE9;">Sources</th>
# MAGIC         <th style="padding:12px 16px; text-align:left; font-size:9.5pt; color:#618794; text-transform:uppercase; letter-spacing:0.06em; border-bottom:2px solid #EEEDE9;">Output Type</th>
# MAGIC         <th style="padding:12px 16px; text-align:left; font-size:9.5pt; color:#618794; text-transform:uppercase; letter-spacing:0.06em; border-bottom:2px solid #EEEDE9;">Data Processed</th>
# MAGIC         <th style="padding:12px 16px; text-align:left; font-size:9.5pt; color:#618794; text-transform:uppercase; letter-spacing:0.06em; border-bottom:2px solid #EEEDE9;">In Scope?</th>
# MAGIC       </tr>
# MAGIC     </thead>
# MAGIC     <tbody>
# MAGIC       <tr style="border-bottom:1px solid #EEEDE9;">
# MAGIC         <td style="padding:12px 16px;"><span style="background:#2272B4; color:white; font-weight:800; padding:3px 12px; border-radius:999px; font-size:9.5pt;">Stream-Snapshot</span></td>
# MAGIC         <td style="padding:12px 16px; color:#1B3139; font-size:10pt;">Streaming + Static</td>
# MAGIC         <td style="padding:12px 16px; color:#1B3139; font-size:10pt;">Streaming Table</td>
# MAGIC         <td style="padding:12px 16px; color:#1B3139; font-size:10pt;">New rows only</td>
# MAGIC         <td style="padding:12px 16px; color:#00A972; font-weight:700; font-size:10pt;">Yes</td>
# MAGIC       </tr>
# MAGIC       <tr style="border-bottom:1px solid #EEEDE9; background:#F9F7F4;">
# MAGIC         <td style="padding:12px 16px;"><span style="background:#00A972; color:white; font-weight:800; padding:3px 12px; border-radius:999px; font-size:9.5pt;">MV Join</span></td>
# MAGIC         <td style="padding:12px 16px; color:#1B3139; font-size:10pt;">Streaming + Streaming</td>
# MAGIC         <td style="padding:12px 16px; color:#1B3139; font-size:10pt;">Materialized View</td>
# MAGIC         <td style="padding:12px 16px; color:#1B3139; font-size:10pt;">All rows each run</td>
# MAGIC         <td style="padding:12px 16px; color:#00A972; font-weight:700; font-size:10pt;">Yes</td>
# MAGIC       </tr>
# MAGIC       <tr>
# MAGIC         <td style="padding:12px 16px;"><span style="background:#618794; color:white; font-weight:800; padding:3px 12px; border-radius:999px; font-size:9.5pt;">Stream-Stream</span></td>
# MAGIC         <td style="padding:12px 16px; color:#1B3139; font-size:10pt;">Streaming + Streaming</td>
# MAGIC         <td style="padding:12px 16px; color:#1B3139; font-size:10pt;">Streaming Table</td>
# MAGIC         <td style="padding:12px 16px; color:#1B3139; font-size:10pt;">New rows only (windowed)</td>
# MAGIC         <td style="padding:12px 16px; color:#98102A; font-weight:700; font-size:10pt;">Advanced only</td>
# MAGIC       </tr>
# MAGIC     </tbody>
# MAGIC   </table>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## F. The Complete Pipeline — What We Have Built

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="text-align: center; margin-top: 20px;">
# MAGIC   <img
# MAGIC     src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/lecture_deploying_pipeline_production/order_status_flow_pipeline.png"
# MAGIC     alt="LakeFlow Connect Unified Ingestion"
# MAGIC     style="width: 1200px; max-width: 100%; height: auto;">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #####EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC So far, we’ve completed the first flow of our pipeline:
# MAGIC
# MAGIC Orders Flow: <b>orders JSON files → orders_bronze → orders_silver → gold_orders_by_date</b>
# MAGIC
# MAGIC Now we’re ready to add the second flow to the pipeline, the Status flow.
# MAGIC
# MAGIC Here’s what this new flow will do:
# MAGIC
# MAGIC - Ingest status JSON files to the status_bronze streaming table
# MAGIC - Transform the bronze streaming table into the status_silver streaming table
# MAGIC - Join status_silver with orders_silver to create a new materialized view: full_order_info_gold
# MAGIC - From there, we’ll build two additional materialized views:
# MAGIC <ul>
# MAGIC <li>cancelled_orders
# MAGIC <li>Delivered_orders
# MAGIC </ul>
# MAGIC
# MAGIC This flow gives us rich insight into order lifecycle states, and demonstrates how we can join flows together and branch logic for more advanced analytics.
# MAGIC
# MAGIC After we complete the Status Flow, our next step is to move the pipeline from development to production.
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## G. Schedule, Notifications, and Monitoring
# MAGIC
# MAGIC Use these key steps to operationalize the pipeline. When you're ready to move your Apache Spark™ Declarative Pipeline into production, there are four key operational tasks to ensure it's reliable, automated, and monitored:
# MAGIC
# MAGIC <div style="max-width: 1100px; margin: 20px auto 0 auto; font-family: sans-serif; color: #0b2026;">
# MAGIC   <div style="display: flex; gap: 16px; align-items: stretch;">
# MAGIC     <!-- Schedule the Pipeline -->
# MAGIC     <div style="flex: 1; border: 2px solid #FF5F46; border-radius: 10px; padding: 20px;">
# MAGIC       <div style="text-align: center; margin-bottom: 12px;">
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/scheduling_icon.png"
# MAGIC              alt="Schedule Pipeline icon"
# MAGIC              style="max-height: 80px; max-width: 100px; width: auto; height: auto;">
# MAGIC       </div>
# MAGIC       <div style="font-size: 16pt; font-weight: 700; margin-bottom: 12px;">Schedule the Pipeline</div>
# MAGIC       <ul style="font-size: 14pt; line-height: 1.7; padding-left: 18px; margin: 0;">
# MAGIC         <li>Define a schedule for automatic execution (e.g., every 15 minutes, hourly, daily, continuous).</li>
# MAGIC         <li>This ensures your data is refreshed regularly without manual triggers.</li>
# MAGIC       </ul>
# MAGIC     </div>
# MAGIC     <!-- Set Up Email Notifications -->
# MAGIC     <div style="flex: 1; border: 2px solid #FF5F46; border-radius: 10px; padding: 20px;">
# MAGIC       <div style="text-align: center; margin-bottom: 12px;">
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/email_notifications_icon.png"
# MAGIC              alt="Email Notifications icon"
# MAGIC              style="max-height: 80px; max-width: 100px; width: auto; height: auto;">
# MAGIC       </div>
# MAGIC       <div style="font-size: 16pt; font-weight: 700; margin-bottom: 12px;">Set Up Email Notifications</div>
# MAGIC       <div style="font-size: 14pt; line-height: 1.6;">
# MAGIC         Configure alerts to notify you or your team of:
# MAGIC         <ul style="padding-left: 18px; margin: 4px 0 10px;">
# MAGIC           <li>Successful runs</li>
# MAGIC           <li>Failures</li>
# MAGIC           <li>Starts</li>
# MAGIC         </ul>
# MAGIC         <div>Helps with proactive monitoring and response.</div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <!-- Monitor with Event Log -->
# MAGIC     <div style="flex: 1; border: 2px solid #FF5F46; border-radius: 10px; padding: 20px;">
# MAGIC       <div style="text-align: center; margin-bottom: 12px;">
# MAGIC         <img src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/monitor_pipelines_icon.png"
# MAGIC              alt="Event Log icon"
# MAGIC              style="max-height: 80px; max-width: 100px; width: auto; height: auto;">
# MAGIC       </div>
# MAGIC       <div style="font-size: 16pt; font-weight: 700; margin-bottom: 12px;">Monitor with the Event Log</div>
# MAGIC       <ul style="font-size: 14pt; line-height: 1.7; padding-left: 18px; margin: 0;">
# MAGIC         <li>Use the Event Log to view detailed runtime information.</li>
# MAGIC         <li>Track execution status, row counts, errors, warnings, and performance metrics.</li>
# MAGIC       </ul>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## H. Scheduling the Pipeline
# MAGIC
# MAGIC Once your pipeline is moved to production, the next step is to schedule it to run automatically.
# MAGIC
# MAGIC <div style="max-width:1000px; margin:0 auto; font-family:'Segoe UI',sans-serif;">
# MAGIC
# MAGIC <style>
# MAGIC .tab-btn9 { padding:10px 24px; border:none; border-bottom:3px solid transparent; background:none; font-size:12pt; font-weight:700; color:#618794; cursor:pointer; transition:color 0.2s; font-family:'Segoe UI',sans-serif; }
# MAGIC .tab-btn9.active-tab9 { color:#2272B4; border-bottom:3px solid #2272B4; }
# MAGIC .tab-panel9 { display:none; }
# MAGIC .tab-panel9.active-panel9 { display:block; }
# MAGIC </style>
# MAGIC
# MAGIC <div style="border-bottom:2px solid #EEEDE9; margin:20px 0 0 0; display:flex;">
# MAGIC   <button class="tab-btn9 active-tab9" onclick="showTab9(1)">Triggered Mode</button>
# MAGIC   <button class="tab-btn9" onclick="showTab9(2)">Continuous Mode</button>
# MAGIC </div>
# MAGIC <br>
# MAGIC
# MAGIC <!-- Triggered -->
# MAGIC <div class="tab-panel9 active-panel9" id="tab9-1">
# MAGIC   <div style="border-radius:12px; overflow:hidden; border:2px solid #2272B4; box-shadow:0 3px 12px rgba(34,114,180,0.12);">
# MAGIC     <div style="background:#2272B4; padding:14px 22px; color:white; font-weight:800; font-size:12pt;">
# MAGIC       Triggered Mode
# MAGIC       <span style="background:white; color:#2272B4; font-size:8.5pt; font-weight:800; padding:3px 10px; border-radius:999px; margin-left:10px;">Ideal for batch workloads</span>
# MAGIC     </div>
# MAGIC     <div style="padding:22px 24px; background:#F9F7F4; display:grid; grid-template-columns:1fr 1fr; gap:14px;">
# MAGIC       <div style="background:white; border-radius:8px; border-left:4px solid #2272B4; padding:14px 16px;">
# MAGIC         <div style="font-weight:800; font-size:10.5pt; color:#0B2026; margin-bottom:6px;">How It Works</div>
# MAGIC         <div style="font-size:10pt; color:#5A6F77; line-height:1.75;">
# MAGIC           Pipelines can be triggered <strong style="color:#0B2026;">manually</strong> or set to run on a
# MAGIC           <strong style="color:#0B2026;">recurring schedule</strong> (e.g., every 15 minutes, hourly, daily).
# MAGIC         </div>
# MAGIC       </div>
# MAGIC       <div style="background:white; border-radius:8px; border-left:4px solid #2272B4; padding:14px 16px;">
# MAGIC         <div style="font-weight:800; font-size:10.5pt; color:#0B2026; margin-bottom:6px;">What Happens</div>
# MAGIC         <div style="font-size:10pt; color:#5A6F77; line-height:1.75;">
# MAGIC           The pipeline refreshes selected tables using the data
# MAGIC           <strong style="color:#0B2026;">available at the start of execution</strong>. Once updates are complete, the pipeline
# MAGIC           <strong style="color:#0B2026;">stops</strong>.
# MAGIC         </div>
# MAGIC       </div>
# MAGIC       <div style="background:white; border-radius:8px; border-left:4px solid #00A972; padding:14px 16px; grid-column:1/-1;">
# MAGIC         <div style="font-weight:800; font-size:10.5pt; color:#0B2026; margin-bottom:6px;">Best For</div>
# MAGIC         <div style="font-size:10pt; color:#5A6F77; line-height:1.75;">
# MAGIC           Batch processing or pipelines that do not need to run constantly. The pipeline runs, completes its work, and stops —
# MAGIC           making it cost-efficient for scheduled, non-continuous workloads.
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- Continuous -->
# MAGIC <div class="tab-panel9" id="tab9-2">
# MAGIC   <div style="border-radius:12px; overflow:hidden; border:2px solid #00A972; box-shadow:0 3px 12px rgba(0,169,114,0.12);">
# MAGIC     <div style="background:#00A972; padding:14px 22px; color:white; font-weight:800; font-size:12pt;">
# MAGIC       Continuous Mode
# MAGIC       <span style="background:white; color:#00A972; font-size:8.5pt; font-weight:800; padding:3px 10px; border-radius:999px; margin-left:10px;">Ideal for streaming workloads</span>
# MAGIC     </div>
# MAGIC     <div style="padding:22px 24px; background:#F9F7F4; display:grid; grid-template-columns:1fr 1fr; gap:14px;">
# MAGIC       <div style="background:white; border-radius:8px; border-left:4px solid #00A972; padding:14px 16px;">
# MAGIC         <div style="font-weight:800; font-size:10.5pt; color:#0B2026; margin-bottom:6px;">How It Works</div>
# MAGIC         <div style="font-size:10pt; color:#5A6F77; line-height:1.75;">
# MAGIC           Pipelines <strong style="color:#0B2026;">continuously process new data</strong> to keep streaming tables and materialized
# MAGIC           views up to date in near real-time.
# MAGIC         </div>
# MAGIC       </div>
# MAGIC       <div style="background:white; border-radius:8px; border-left:4px solid #00A972; padding:14px 16px;">
# MAGIC         <div style="font-weight:800; font-size:10.5pt; color:#0B2026; margin-bottom:6px;">What Happens</div>
# MAGIC         <div style="font-size:10pt; color:#5A6F77; line-height:1.75;">
# MAGIC           The system <strong style="color:#0B2026;">monitors dependencies</strong> and updates only when source data changes —
# MAGIC           ensuring efficiency without unnecessary reprocessing.
# MAGIC         </div>
# MAGIC       </div>
# MAGIC       <div style="background:white; border-radius:8px; border-left:4px solid #FFAB00; padding:14px 16px; grid-column:1/-1;">
# MAGIC         <div style="font-weight:800; font-size:10.5pt; color:#0B2026; margin-bottom:6px;">Best For</div>
# MAGIC         <div style="font-size:10pt; color:#5A6F77; line-height:1.75;">
# MAGIC           Streaming use cases where <strong style="color:#0B2026;">freshness and responsiveness are critical</strong>. The pipeline
# MAGIC           stays alive and processes data as it arrives — keeping results current at all times.
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <script>
# MAGIC function showTab9(n) {
# MAGIC   document.querySelectorAll('.tab-btn9').forEach(function(t,i){
# MAGIC     t.classList.toggle('active-tab9', i===n-1);
# MAGIC   });
# MAGIC   document.querySelectorAll('.tab-panel9').forEach(function(p,i){
# MAGIC     p.classList.toggle('active-panel9', i===n-1);
# MAGIC   });
# MAGIC }
# MAGIC </script>
# MAGIC
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## I. Email Notifications
# MAGIC <br>
# MAGIC
# MAGIC <div style="max-width:700px; margin:0 auto; font-family:'Segoe UI',sans-serif;">
# MAGIC   <div style="display:flex; gap:24px; align-items:flex-start; flex-wrap:wrap;">
# MAGIC     <!-- Left: Text -->
# MAGIC     <div style="flex:1 1 0; min-width:260px; font-size:15px; color:#0B2026; line-height:1.7;">
# MAGIC       <br>
# MAGIC       As part of scheduling your pipeline, you can configure email notifications to keep stakeholders informed of pipeline activity.
# MAGIC       <br><br>
# MAGIC       Notifications can be set for any combination of three events.
# MAGIC     </div>
# MAGIC     <!-- Right: Image -->
# MAGIC     <div style="flex:1 1 0; min-width:260px; text-align:left;">
# MAGIC       <img
# MAGIC         src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/lecture_deploying_pipeline_production/email_notification_setting.png"
# MAGIC         alt="Email notification settings"
# MAGIC         style="max-width:100%; height:auto; border-radius:4px;">
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <div style="max-width:1000px; margin:0 auto; font-family:'Segoe UI',sans-serif;">
# MAGIC <div style="margin:20px 0;">
# MAGIC   <div style="background:#F9F7F4; border:2px solid #EEEDE9; border-radius:12px; padding:24px; margin-bottom:16px;">
# MAGIC     <div style="font-weight:800; font-size:11pt; color:#0B2026; margin-bottom:18px; padding-bottom:10px; border-bottom:2px solid #EEEDE9;">Configure Notifications for Any Combination of These Events</div>
# MAGIC     <div style="display:grid; grid-template-columns:repeat(3,1fr); gap:12px;">
# MAGIC       <div style="background:white; border-radius:10px; border:2px solid #2272B4; overflow:hidden;">
# MAGIC         <div style="background:#2272B4; padding:10px 16px; font-weight:800; font-size:10pt; color:white; text-align:center;">Pipeline Starts</div>
# MAGIC         <div style="padding:14px 16px; font-size:10pt; color:#5A6F77; line-height:1.75; text-align:center;">
# MAGIC           Receive an alert the moment a pipeline run begins — useful for tracking execution timing.
# MAGIC         </div>
# MAGIC       </div>
# MAGIC       <div style="background:white; border-radius:10px; border:2px solid #00A972; overflow:hidden;">
# MAGIC         <div style="background:#00A972; padding:10px 16px; font-weight:800; font-size:10pt; color:white; text-align:center;">Pipeline Succeeds</div>
# MAGIC         <div style="padding:14px 16px; font-size:10pt; color:#5A6F77; line-height:1.75; text-align:center;">
# MAGIC           Get confirmation when a run completes successfully — gives confidence that data is fresh.
# MAGIC         </div>
# MAGIC       </div>
# MAGIC       <div style="background:white; border-radius:10px; border:2px solid #98102A; overflow:hidden;">
# MAGIC         <div style="background:#98102A; padding:10px 16px; font-weight:800; font-size:10pt; color:white; text-align:center;">Pipeline Fails</div>
# MAGIC         <div style="padding:14px 16px; font-size:10pt; color:#5A6F77; line-height:1.75; text-align:center;">
# MAGIC           Be immediately alerted to failures — enables fast response and minimizes data freshness gaps.
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div style="margin-top:14px; background:#FFAB00; border-radius:8px; padding:14px 18px; font-size:10.5pt; color:#98102A; line-height:1.75; text-align:center;">
# MAGIC       You can choose <strong style="color:white;">any combination</strong> of these three events — or all three — depending on your monitoring needs.
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## J. Monitoring with the Pipeline Event Log
# MAGIC <br>
# MAGIC
# MAGIC <div style="max-width:900px; margin:0 auto; font-family:'Segoe UI',sans-serif;">
# MAGIC   <div style="display:flex; gap:24px; align-items:flex-start; flex-wrap:wrap;">
# MAGIC     <!-- Left: Text -->
# MAGIC     <div style="flex:1 1 0; min-width:260px; font-size:15px; color:#0B2026; line-height:1.7;">
# MAGIC       Another essential task when running pipelines in production is monitoring their health and performance.
# MAGIC     </div>
# MAGIC     <!-- Right: Image -->
# MAGIC     <div style="flex:1 1 0; min-width:600px; text-align:left;">
# MAGIC       <img
# MAGIC         src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/lecture_deploying_pipeline_production/monitoring_event_log.png"
# MAGIC         alt="Monioring event log"
# MAGIC         style="max-width:100%; height:auto; border-radius:4px;">
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### J1. Pipeline Event Log
# MAGIC
# MAGIC Spark Declarative Pipelines provide a pipeline event log that captures all critical information, including:
# MAGIC
# MAGIC <div style="max-width:1000px; margin:0 auto; font-family:'Segoe UI',sans-serif;">
# MAGIC <div style="margin:20px 0;">
# MAGIC   <div style="background:#F9F7F4; border-radius:12px; border:2px solid #EEEDE9; overflow:hidden; margin-bottom:16px;">
# MAGIC     <div style="display:grid; grid-template-columns:repeat(2,1fr); gap:0;">
# MAGIC       <div style="padding:18px 20px; border-right:2px solid #EEEDE9; border-bottom:2px solid #EEEDE9;">
# MAGIC         <div style="font-weight:800; font-size:10.5pt; color:#0B2026; margin-bottom:6px; padding-bottom:6px; border-bottom:2px solid #EEEDE9;">Audit Logs</div>
# MAGIC         <div style="font-size:10pt; color:#5A6F77; line-height:1.75;">Track <strong style="color:#1B3139;">who did what and when</strong> within the pipeline. Provides a full history of actions taken against your pipeline and its datasets.</div>
# MAGIC       </div>
# MAGIC       <div style="padding:18px 20px; border-bottom:2px solid #EEEDE9;">
# MAGIC         <div style="font-weight:800; font-size:10.5pt; color:#0B2026; margin-bottom:6px; padding-bottom:6px; border-bottom:2px solid #EEEDE9;">Data Quality Checks</div>
# MAGIC         <div style="font-size:10pt; color:#5A6F77; line-height:1.75;">Monitor the <strong style="color:#1B3139;">results of any expectations or constraints</strong> applied during processing — including pass/fail counts and violation details.</div>
# MAGIC       </div>
# MAGIC       <div style="padding:18px 20px; border-right:2px solid #EEEDE9;">
# MAGIC         <div style="font-weight:800; font-size:10.5pt; color:#0B2026; margin-bottom:6px; padding-bottom:6px; border-bottom:2px solid #EEEDE9;">Pipeline Progress</div>
# MAGIC         <div style="font-size:10pt; color:#5A6F77; line-height:1.75;">See the <strong style="color:#1B3139;">status and progress of each pipeline run</strong> — including row counts processed, stages completed, and current execution state.</div>
# MAGIC       </div>
# MAGIC       <div style="padding:18px 20px;">
# MAGIC         <div style="font-weight:800; font-size:10.5pt; color:#0B2026; margin-bottom:6px; padding-bottom:6px; border-bottom:2px solid #EEEDE9;">Data Lineage</div>
# MAGIC         <div style="font-size:10pt; color:#5A6F77; line-height:1.75;">Understand <strong style="color:#1B3139;">how data flows and transforms</strong> through your pipeline — from source ingestion through bronze, silver, and gold layers.</div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC This comprehensive event log helps you quickly diagnose issues, ensure data integrity, and maintain full visibility into your pipeline operations.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### J2. Querying the Declarative Pipeline Event Log
# MAGIC <br>
# MAGIC
# MAGIC <div style="max-width:1000px; margin:0 auto; font-family:'Segoe UI',sans-serif;">
# MAGIC <div style="display:grid; grid-template-columns:1fr 1fr; gap:14px; margin-bottom:16px;">
# MAGIC <div style="background:#F9F7F4; border-radius:12px; padding:20px 22px; border:2px solid #2272B4;">
# MAGIC     <div style="font-weight:800; font-size:11pt; color:#0B2026; margin-bottom:14px; border-bottom:2px solid #EEEDE9; padding-bottom:10px;">Publish to Metastore (Recommended)</div>
# MAGIC     <div style="font-size:10.5pt; color:#1B3139; line-height:1.8;">
# MAGIC       Publish the event log as a UC table using the advanced settings
# MAGIC       <ul>
# MAGIC       <li>Specify the table location (catalog, schema)
# MAGIC       <li>Define the table name
# MAGIC       </ul>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <div style="background:#F9F7F4; border-radius:12px; padding:20px 22px; border:2px solid #EEEDE9;">
# MAGIC     <div style="font-weight:800; font-size:11pt; color:#0B2026; margin-bottom:14px; border-bottom:2px solid #EEEDE9; padding-bottom:10px;">Default Behavior</div>
# MAGIC     <div style="font-size:10.5pt; color:#1B3139; line-height:1.8;">
# MAGIC       By default, the event log is written as a <strong>hidden UC table</strong> located in the pipeline's default catalog and schema.
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC <div style="background:#F9F7F4; border:2px solid #EEEDE9; border-radius:12px; overflow:hidden; margin-bottom:16px;">
# MAGIC   <div style="background:#1B3139; padding:12px 20px; color:white; font-weight:800; font-size:10.5pt;">Querying the Event Log</div>
# MAGIC   <div style="padding:18px 22px;">
# MAGIC     <div style="font-size:10.5pt; color:#1B3139; line-height:1.75; margin-bottom:12px;">
# MAGIC       Once published, you can query the event log just like any other UC table, enabling you to build custom reports, dashboards, or perform deeper analysis on pipeline activity and health.
# MAGIC     </div>
# MAGIC <div class="code-block" data-language="sql">
# MAGIC SELECT * FROM &amp;lt;catalog&amp;gt;.&amp;lt;schema&amp;gt;.&amp;lt;event_log_table_name&amp;gt;
# MAGIC </div>
# MAGIC
# MAGIC <link href="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/themes/prism.min.css" rel="stylesheet" />
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/prism.min.js"></script>
# MAGIC <script src="https://cdnjs.cloudflare.com/ajax/libs/prism/1.29.0/components/prism-sql.min.js"></script>
# MAGIC
# MAGIC <script>
# MAGIC (function() {
# MAGIC   document.querySelectorAll('.code-block').forEach(function(block) {
# MAGIC     if (block.getAttribute('data-processed')) return;
# MAGIC     block.setAttribute('data-processed', 'true');
# MAGIC     var lang = block.getAttribute('data-language') || 'sql';
# MAGIC     var code = block.textContent.trim();
# MAGIC     var id = 'code-' + Math.random().toString(36).substr(2, 9);
# MAGIC     block.innerHTML = 
# MAGIC       '<div style="position:relative;margin:16px 0;">' +
# MAGIC         '<button class="copy-btn" style="position:absolute;top:8px;right:8px;padding:4px 12px;font-size:12px;background:#ddd;color:#333;border:1px solid #ccc;border-radius:4px;cursor:pointer;z-index:10;">Copy</button>' +
# MAGIC         '<pre style="background:#f8f8f8;border-radius:8px;padding:16px;padding-top:40px;overflow-x:auto;margin:0;border:1px solid #e0e0e0;"><code id="' + id + '" class="language-' + lang + '" style="font-family:Consolas,Monaco,monospace;font-size:14px;"></code></pre>' +
# MAGIC       '</div>';
# MAGIC     var codeEl = document.getElementById(id);
# MAGIC     codeEl.textContent = code;
# MAGIC     Prism.highlightElement(codeEl);
# MAGIC     block.querySelector('.copy-btn').onclick = function() {
# MAGIC       var t = document.createElement('textarea');
# MAGIC       t.value = code;
# MAGIC       document.body.appendChild(t);
# MAGIC       t.select();
# MAGIC       document.execCommand('copy');
# MAGIC       document.body.removeChild(t);
# MAGIC       this.textContent = '✓ Copied!';
# MAGIC       setTimeout(() => this.textContent = 'Copy', 2000);
# MAGIC     };
# MAGIC   });
# MAGIC })();
# MAGIC </script>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## K. Conclusion
# MAGIC
# MAGIC In this lecture, you learned how streaming joins and production deployment work together in Apache Spark™ Declarative Pipelines:
# MAGIC
# MAGIC - Streaming joins support different patterns: stream-snapshot joins for lookup enrichment, streaming table joins through materialized views, and stream-stream joins with windowing and watermarking.
# MAGIC - Choosing the right join type depends on whether sources are static or streaming, and whether the workflow needs enrichment or synchronization.
# MAGIC - Production deployment relies on scheduling, notifications, monitoring, and event logs for reliable, observable pipelines with audit logs, quality results, progress, and lineage.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; <span id="dbx-year"></span> Databricks, Inc. All rights reserved.
# MAGIC Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/>
# MAGIC <a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> |
# MAGIC <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> |
# MAGIC <a href="https://help.databricks.com/" target="_blank">Support</a>
# MAGIC <script>
# MAGIC   document.getElementById("dbx-year").textContent = new Date().getFullYear();
# MAGIC </script>
# MAGIC