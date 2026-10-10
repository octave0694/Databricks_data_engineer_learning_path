# Databricks notebook source
# MAGIC %md
# MAGIC ![Databricks Academy](https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/databricks_academy.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # Lecture - Simplified Pipeline Development and Common Pipeline Settings
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC In this lecture, you will learn how the multi-file editor simplifies pipeline development and debugging and how common pipeline settings customize pipeline execution.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lecture, you will be able to:
# MAGIC
# MAGIC - **Identify** and configure pipeline settings such as compute, data assets, trigger modes, and advanced options
# MAGIC - **Develop** a functional Declarative Pipeline using the new pipeline editor and SQL-based syntax for the pipeline

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## A. Simplified Pipeline Development

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A1. What Is the Multi-File Editor?
# MAGIC
# MAGIC The multi-file editor in Apache Spark™ Declarative Pipelines makes developing and debugging your ETL pipelines easier and more efficient.
# MAGIC
# MAGIC Instead of managing one large file, your pipeline is organized as a set of files visible in the pipeline assets browser, letting you edit code and configure your pipeline components all in one place.
# MAGIC
# MAGIC
# MAGIC <div style="border-left: 4px solid #1976d2; background: #e3f2fd; padding: 16px 20px; border-radius: 4px; margin: 16px 0;">
# MAGIC <div style="display: flex; align-items: flex-start; gap: 12px;">
# MAGIC <div>
# MAGIC <strong style="color: #0d47a1; font-size: 1.1em;">Announced at Data & AI Summit 2025</strong>
# MAGIC <p style="margin: 8px 0 0 0; color: #333;">Write pipeline code using <strong>.sql</strong> or <strong>.py</strong> files (recommended), or use notebooks — your choice. These features help you build robust pipelines faster, streamline your development workflow, debug with integrated tools, and validate without full runs.</p>
# MAGIC </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### A2. Key Features of the Multi-File Editor
# MAGIC
# MAGIC The multi-file editor in Apache Spark™ Declarative Pipelines makes developing and debugging your ETL pipelines easier and more efficient.
# MAGIC
# MAGIC Instead of managing one large file, your pipeline is organized as a set of files visible in the pipeline assets browser, letting you edit code and configure your pipeline components all in one place.
# MAGIC
# MAGIC
# MAGIC #####Click each number to reveal the key features of the Multi-File Editor.
# MAGIC <br>
# MAGIC
# MAGIC <div style="max-width: 1120px; margin: 0 auto; font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;">
# MAGIC
# MAGIC <style>
# MAGIC   .hotspot-root {
# MAGIC     position: relative;
# MAGIC     max-width: 100%;
# MAGIC   }
# MAGIC   .hotspot-wrap {
# MAGIC     position: relative;
# MAGIC     display: inline-block;
# MAGIC     max-width: 100%;
# MAGIC   }
# MAGIC   .hotspot-img {
# MAGIC     width: 1100px;
# MAGIC     max-width: 100%;
# MAGIC     height: auto;
# MAGIC     border-radius: 4px;
# MAGIC     border: 1px solid #d0d7dd;
# MAGIC     display: block;
# MAGIC   }
# MAGIC   /* Transparent, clickable overlays positioned over the numbers already
# MAGIC      baked into the screenshot. Centered on each number via translate(). */
# MAGIC   .hotspot-dot {
# MAGIC     position: absolute;
# MAGIC     transform: translate(-50%, -50%);
# MAGIC     width: 40px;
# MAGIC     height: 40px;
# MAGIC     border-radius: 999px;
# MAGIC     background: transparent;
# MAGIC     color: transparent;
# MAGIC     font-size: 0;
# MAGIC     cursor: pointer;
# MAGIC     border: 2px solid transparent;
# MAGIC     transition: box-shadow 0.15s, border-color 0.15s;
# MAGIC   }
# MAGIC   .hotspot-dot:hover {
# MAGIC     border-color: rgba(27,81,98,0.9);
# MAGIC     box-shadow: 0 0 0 4px rgba(27,81,98,0.18);
# MAGIC   }
# MAGIC
# MAGIC   .hotspot-panel {
# MAGIC     margin-top: 16px;
# MAGIC     padding: 14px 16px 12px 16px;
# MAGIC     border-radius: 10px;
# MAGIC     border: 1px solid #d0d7dd;
# MAGIC     background: #F9F7F4;
# MAGIC     box-shadow: 0 2px 6px rgba(15,23,42,0.06);
# MAGIC     font-size: 13px;
# MAGIC     color: #0B2026;
# MAGIC     display: none;
# MAGIC   }
# MAGIC   .hotspot-panel-title {
# MAGIC     font-size: 15px;
# MAGIC     font-weight: 700;
# MAGIC     color: #1B3139;
# MAGIC     margin-bottom: 6px;
# MAGIC   }
# MAGIC   .hotspot-panel-body {
# MAGIC     line-height: 1.6;
# MAGIC   }
# MAGIC   .hotspot-panel-footer {
# MAGIC     margin-top: 8px;
# MAGIC     font-size: 11px;
# MAGIC     color: #64748b;
# MAGIC     font-style: italic;
# MAGIC   }
# MAGIC
# MAGIC   /* hide radios */
# MAGIC   .hotspot-radio {
# MAGIC     display: none;
# MAGIC   }
# MAGIC
# MAGIC   /* active state styling via :checked */
# MAGIC   #hs1:checked ~ .hotspot-wrap label[for="hs1"],
# MAGIC   #hs2:checked ~ .hotspot-wrap label[for="hs2"],
# MAGIC   #hs3:checked ~ .hotspot-wrap label[for="hs3"],
# MAGIC   #hs4:checked ~ .hotspot-wrap label[for="hs4"],
# MAGIC   #hs5:checked ~ .hotspot-wrap label[for="hs5"],
# MAGIC   #hs6:checked ~ .hotspot-wrap label[for="hs6"] {
# MAGIC     border-color: #1B5162;
# MAGIC     box-shadow: 0 0 0 4px rgba(27,81,98,0.25);
# MAGIC   }
# MAGIC
# MAGIC   #hs1:checked ~ .hotspot-panels .panel1 { display: block; }
# MAGIC   #hs2:checked ~ .hotspot-panels .panel2 { display: block; }
# MAGIC   #hs3:checked ~ .hotspot-panels .panel3 { display: block; }
# MAGIC   #hs4:checked ~ .hotspot-panels .panel4 { display: block; }
# MAGIC   #hs5:checked ~ .hotspot-panels .panel5 { display: block; }
# MAGIC   #hs6:checked ~ .hotspot-panels .panel6 { display: block; }
# MAGIC </style>
# MAGIC <div class="hotspot-root">
# MAGIC   <!-- Radios to drive state -->
# MAGIC   <input type="radio" name="hs" id="hs1" class="hotspot-radio" checked>
# MAGIC   <input type="radio" name="hs" id="hs2" class="hotspot-radio">
# MAGIC   <input type="radio" name="hs" id="hs3" class="hotspot-radio">
# MAGIC   <input type="radio" name="hs" id="hs4" class="hotspot-radio">
# MAGIC   <input type="radio" name="hs" id="hs5" class="hotspot-radio">
# MAGIC   <input type="radio" name="hs" id="hs6" class="hotspot-radio">
# MAGIC   <!-- Image with hotspots -->
# MAGIC   <div class="hotspot-wrap">
# MAGIC     <img
# MAGIC       src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/lecture_simplified_pipeline_development/key_features.png"
# MAGIC       alt="Pipeline key features"
# MAGIC       class="hotspot-img">
# MAGIC     <!-- Approximate positions (adjust top/left as needed) -->
# MAGIC     <label class="hotspot-dot" for="hs1" style="top: 25.5%; left: 13.3%;">1</label>
# MAGIC     <label class="hotspot-dot" for="hs2" style="top: 7.8%; left: 31.6%;">2</label>
# MAGIC     <label class="hotspot-dot" for="hs3" style="top: 13.7%; left: 50.0%;">3</label>
# MAGIC     <label class="hotspot-dot" for="hs4" style="top: 25.5%; left: 64.0%;">4</label>
# MAGIC     <label class="hotspot-dot" for="hs5" style="top: 79.3%; left: 19.4%;">5</label>
# MAGIC     <label class="hotspot-dot" for="hs6" style="top: 73.2%; left: 76.3%;">6</label>
# MAGIC   </div>
# MAGIC   <!-- Panels driven by radios -->
# MAGIC   <div class="hotspot-panels">
# MAGIC     <div class="hotspot-panel panel1">
# MAGIC       <div class="hotspot-panel-title">Pipeline Asset Browser</div>
# MAGIC       <div class="hotspot-panel-body">To easily navigate pipeline files</div>
# MAGIC     </div>
# MAGIC     <div class="hotspot-panel panel2">
# MAGIC       <div class="hotspot-panel-title">Multi-File Code Editor</div>
# MAGIC       <div class="hotspot-panel-body">Designed for step-by-step pipeline development</div>
# MAGIC     </div>
# MAGIC     <div class="hotspot-panel panel3">
# MAGIC       <div class="hotspot-panel-title">Pipeline-Specific Toolbars</div>
# MAGIC       <div class="hotspot-panel-body">For quick access to common actions</div>
# MAGIC     </div>
# MAGIC     <div class="hotspot-panel panel4">
# MAGIC       <div class="hotspot-panel-title">Interactive DAG</div>
# MAGIC       <div class="hotspot-panel-body">(Directed Acyclic Graph) for visualizing pipeline dependencies</div>
# MAGIC     </div>
# MAGIC     <div class="hotspot-panel panel5">
# MAGIC       <div class="hotspot-panel-title">Data Previews</div>
# MAGIC       <div class="hotspot-panel-body">To inspect intermediate results
# MAGIC </div>
# MAGIC     </div>
# MAGIC     <div class="hotspot-panel panel6">
# MAGIC       <div class="hotspot-panel-title">Execution Insights, Debugging &amp; Dry Run</div>
# MAGIC       <div class="hotspot-panel-body">To monitor pipeline runs
# MAGIC       <ul>
# MAGIC       <li>Easier debugging with integrated tools
# MAGIC       <li>Faster validation through dry run capabilities, so you can check your pipeline without running full executions
# MAGIC       </li>
# MAGIC       </ul>
# MAGIC </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #####Documentation
# MAGIC
# MAGIC
# MAGIC <a href="https://docs.databricks.com/aws/en/dlt/dlt-multi-file-editor" style="color: #1976d2; text-decoration: underline;">Develop and debug ETL pipelines with the multi-file editor in Apache Spark™ Declarative Pipelines</a>
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## B. Common Pipeline Settings

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B1. Overview — Accessing Pipeline Settings
# MAGIC
# MAGIC Declarative Pipelines come with a variety of settings you can easily configure to customize how your pipeline behaves.
# MAGIC <div style="text-align: center; margin-top: 20px;">
# MAGIC   <img
# MAGIC     src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/lecture_common_pipeline_settings/pipeline_settings.png" alt="Accessing Pipeline Seetings"
# MAGIC     style="width: 1100px; max-width: 100%; height: auto;">
# MAGIC </div>
# MAGIC
# MAGIC <div style="max-width:1100px; margin:0 auto; font-family:'Segoe UI',sans-serif;">
# MAGIC
# MAGIC <div style="display:flex; gap:16px; margin:20px 0; align-items:stretch;">
# MAGIC
# MAGIC   <!-- Left: Key message -->
# MAGIC   <div style="flex:1; background:#1B5162; border-radius:12px; padding:24px 28px; color:white; display:flex; flex-direction:column; justify-content:center;">
# MAGIC     <div style="font-size:13pt; font-weight:800; color:#FFAB00; margin-bottom:12px; text-transform:uppercase; letter-spacing:0.05em;">Edit Settings Directly in the Editor</div>
# MAGIC     <div style="font-size:11.5pt; line-height:1.85; color:#c8dde4;">
# MAGIC       With the new multi-file editor, accessing and editing pipeline settings is <strong style="color:white;">simple and intuitive</strong>.
# MAGIC       <ul>
# MAGIC       <li><strong style="color:white;">Quick Access Settings:</strong> Just click the small <strong style="color:#FFAB00;">gear</strong> icon in your editor window to quickly open the pipeline settings popup. No need to leave your development environment.
# MAGIC       <br><br>
# MAGIC       <li><strong style="color:white;">Seamless Configuration:</strong> From there, you can adjust key settings and options directly, helping you tailor your pipeline to your specific requirements without interrupting your workflow.
# MAGIC       </ul>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- Right: 3 core settings -->
# MAGIC   <div style="width:320px; flex-shrink:0; display:flex; flex-direction:column; gap:10px;">
# MAGIC   <div style="background:#F9F7F4; border:2px solid #EEEDE9; border-radius:10px; padding:14px 18px;">
# MAGIC     <div style="font-size:9pt; font-weight:800; color:#5A6F77; text-transform:uppercase; letter-spacing:0.06em; margin-bottom:10px;">
# MAGIC       3 Most Common Settings
# MAGIC     </div>
# MAGIC     <div style="display:flex; flex-direction:column; gap:10px;">
# MAGIC       <!-- Compute -->
# MAGIC       <div style="display:flex; align-items:center; gap:12px; background:white; border-radius:8px; padding:10px 12px; border-left:4px solid #2272B4;">
# MAGIC         <img
# MAGIC           src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/compute_icon.png"
# MAGIC           alt="Compute icon"
# MAGIC           style="width:50px; height:50px; flex-shrink:0; background:transparent; mix-blend-mode:multiply; filter:contrast(1.05) brightness(1.02);">
# MAGIC         <div>
# MAGIC           <div style="font-weight:800; font-size:10pt; color:#1B3139;">Compute</div>
# MAGIC           <div style="font-size:9pt; color:#5A6F77;">Resources &amp; environment</div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC       <!-- Code Assets -->
# MAGIC       <div style="display:flex; align-items:center; gap:12px; background:white; border-radius:8px; padding:10px 12px; border-left:4px solid #00A972;">
# MAGIC         <img
# MAGIC           src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/code_assets_icon.png"
# MAGIC           alt="Code assets icon"
# MAGIC           style="width:50px; height:50px; flex-shrink:0; background:transparent; mix-blend-mode:multiply; filter:contrast(1.05) brightness(1.02);">
# MAGIC         <div>
# MAGIC           <div style="font-weight:800; font-size:10pt; color:#1B3139;">Code Assets</div>
# MAGIC           <div style="font-size:9pt; color:#5A6F77;">Files and code modules</div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC       <!-- Configuration -->
# MAGIC       <div style="display:flex; align-items:center; gap:12px; background:white; border-radius:8px; padding:10px 12px; border-left:4px solid #FFAB00;">
# MAGIC         <img
# MAGIC           src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/icons/configurations_icon.png"
# MAGIC           alt="Configuration icon"
# MAGIC           style="width:50px; height:50px; flex-shrink:0; background:transparent; mix-blend-mode:multiply; filter:contrast(1.05) brightness(1.02);">
# MAGIC         <div>
# MAGIC           <div style="font-weight:800; font-size:10pt; color:#1B3139;">Configuration</div>
# MAGIC           <div style="font-size:9pt; color:#5A6F77;">Pipeline parameters</div>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC We'll focus on these three settings now, and explore additional options during the upcoming demonstration.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### B2. Compute
# MAGIC
# MAGIC Let’s begin with Compute, which lets you select the resources and environment where your pipeline will run. 
# MAGIC
# MAGIC ##### Click each tab to explore the two compute options available for your pipeline.
# MAGIC
# MAGIC <div style="max-width:1000px; margin:0 auto; font-family:'Segoe UI',sans-serif;">
# MAGIC
# MAGIC <style>
# MAGIC .compute-tab { padding:10px 20px; border:none; border-bottom:3px solid transparent; background:none; font-size:12pt; font-weight:700; color:#888; cursor:pointer; margin-bottom:-2px; transition:color 0.2s; }
# MAGIC .compute-tab.active-tab { color:#2272B4; border-bottom:3px solid #2272B4; }
# MAGIC .compute-panel { display:none; }
# MAGIC .compute-panel.active-panel { display:block; }
# MAGIC </style>
# MAGIC
# MAGIC <div style="border-bottom:2px solid #EEEDE9; margin-bottom:0; display:flex;">
# MAGIC   <button class="compute-tab active-tab" onclick="showComputeTab(1)">Serverless (Recommended)</button>
# MAGIC   <button class="compute-tab" onclick="showComputeTab(2)">Classic (Fixed Size)</button>
# MAGIC </div>
# MAGIC <br>
# MAGIC
# MAGIC <!-- TAB 1: Serverless -->
# MAGIC <div class="compute-panel active-panel" id="compute-1">
# MAGIC   <div style="background:#F9F7F4; border-radius:12px; overflow:hidden; border:2px solid #2272B4; box-shadow:0 3px 12px rgba(34,114,180,0.12);">
# MAGIC     <div style="background:#2272B4; padding:14px 20px; color:white; font-weight:800; font-size:12pt; display:flex; align-items:center; gap:10px;">
# MAGIC       Serverless Compute
# MAGIC       <span style="background:#FFAB00; color:#1B3139; font-size:8.5pt; font-weight:800; padding:3px 10px; border-radius:999px; margin-left:8px;">DATABRICKS RECOMMENDED</span>
# MAGIC     </div>
# MAGIC     <div style="padding:20px 24px; display:grid; grid-template-columns:1fr 1fr; gap:14px;">
# MAGIC       <div style="background:white; border-radius:8px; padding:14px 16px; border-left:4px solid #2272B4;">
# MAGIC         <div style="font-weight:800; font-size:10.5pt; color:#1B3139; margin-bottom:6px;">Optimized Cost & Performance</div>
# MAGIC         <div style="font-size:10pt; color:#5A6F77; line-height:1.7;">Serverless optimizes costs while maintaining strong performance.</div>
# MAGIC       </div>
# MAGIC       <div style="background:white; border-radius:8px; padding:14px 16px; border-left:4px solid #00A972;">
# MAGIC         <div style="font-weight:800; font-size:10.5pt; color:#1B3139; margin-bottom:6px;">Focus on Code, Not Infrastructure</div>
# MAGIC         <div style="font-size:10pt; color:#5A6F77; line-height:1.7;">It allows you to focus fully on your code without needing to manage or provision infrastructure.</div>
# MAGIC       </div>
# MAGIC       <div style="background:white; border-radius:8px; padding:14px 16px; border-left:4px solid #FFAB00;">
# MAGIC         <div style="font-weight:800; font-size:10.5pt; color:#1B3139; margin-bottom:6px;">Incremental Refresh for MVs</div>
# MAGIC         <div style="font-size:10pt; color:#5A6F77; line-height:1.7;">Support for incremental refresh of materialized views.</div>
# MAGIC       </div>
# MAGIC       <div style="background:white; border-radius:8px; padding:14px 16px; border-left:4px solid #FF5F46;">
# MAGIC         <div style="font-weight:800; font-size:10.5pt; color:#1B3139; margin-bottom:6px;">Cost-Based Optimizer</div>
# MAGIC         <div style="font-size:10pt; color:#5A6F77; line-height:1.7;">A cost-based optimizer that enables fast and efficient transformations of materialized views, improving pipeline efficiency and speed.</div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC     <div style="margin:0 24px 20px 24px; background:white; border-left:4px solid #2272B4; border-radius:6px; padding:12px 16px; font-size:10pt; color:#1B3139; line-height:1.7;"><strong>Performance Optimized Setting:</strong> For time-sensitive Lakeflow Jobs, there's an optional <strong>Serverless Performance Optimized</strong> setting to boost responsiveness.
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <!-- TAB 2: Classic -->
# MAGIC <div class="compute-panel" id="compute-2">
# MAGIC   <div style="background:#F9F7F4; border-radius:12px; overflow:hidden; border:2px solid #CD7F32; box-shadow:0 3px 12px rgba(48,63,71,0.12);">
# MAGIC     <div style="background:#CD7F32; padding:14px 20px; color:white; font-weight:800; font-size:12pt;">
# MAGIC       Classic Compute (Fixed Size)
# MAGIC     </div>
# MAGIC     <div style="padding:20px 24px; display:grid; grid-template-columns:1fr 1fr; gap:14px;">
# MAGIC       <div style="background:white; border-radius:8px; padding:14px 16px; border-left:4px solid #303F47;">
# MAGIC         <div style="font-weight:800; font-size:10.5pt; color:#1B3139; margin-bottom:6px;">Fixed-Size Clusters</div>
# MAGIC         <div style="font-size:10pt; color:#5A6F77; line-height:1.7;">Classic Compute uses fixed-size clusters. Users need appropriate permissions to create compute resources for Declarative Pipelines.</div>
# MAGIC       </div>
# MAGIC       <div style="background:white; border-radius:8px; padding:14px 16px; border-left:4px solid #5A6F77;">
# MAGIC         <div style="font-weight:800; font-size:10.5pt; color:#1B3139; margin-bottom:6px;">Permission & Policy Control</div>
# MAGIC         <div style="font-size:10pt; color:#5A6F77; line-height:1.7;">Workspace administrators can set up cluster policies to control and provide access to compute resources for users.</div>
# MAGIC       </div>
# MAGIC       <div style="background:white; border-radius:8px; padding:14px 16px; border-left:4px solid #00A972; grid-column:1/-1;">
# MAGIC         <div style="font-weight:800; font-size:10.5pt; color:#1B3139; margin-bottom:6px;">Enhanced Autoscaling — Enabled by Default</div>
# MAGIC         <div style="font-size:10pt; color:#5A6F77; line-height:1.7;">Enhanced autoscaling is enabled by default for all new pipelines using Classic Compute. It automatically adjusts cluster size based on workload volume, optimizing resource utilization and helping control costs without manual intervention.</div>
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC <script>
# MAGIC function showComputeTab(n) {
# MAGIC   document.querySelectorAll('.compute-tab').forEach(function(t,i){ t.classList.toggle('active-tab', i===n-1); });
# MAGIC   document.querySelectorAll('.compute-panel').forEach(function(p,i){ p.classList.toggle('active-panel', i===n-1); });
# MAGIC }
# MAGIC </script>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #####Documentation
# MAGIC - <a href="https://docs.databricks.com/aws/en/dlt/serverless-dlt#select-a-performance-mode" style="color: #1976d2; text-decoration: underline;">Serverless Performance Optimized</a>
# MAGIC - <a href="https://docs.databricks.com/aws/en/dlt/serverless-dlt#serverless-pipeline-features" style="color: #1976d2; text-decoration: underline;">Serverless Pipeline Features</a>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ### B3. Code Assets
# MAGIC
# MAGIC Code Assets manage the files and code modules that make up your pipeline.
# MAGIC .
# MAGIC
# MAGIC <div style="text-align: center; margin-top: 20px;">
# MAGIC   <img
# MAGIC     src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/lecture_common_pipeline_settings/code_assets.png"
# MAGIC     alt="Code Assets Overview"
# MAGIC     style="width: 1000px; max-width: 100%; height: auto;">
# MAGIC </div>
# MAGIC
# MAGIC <div style="max-width:1000px; margin:0 auto; font-family:'Segoe UI',sans-serif;">
# MAGIC
# MAGIC <div style="display:grid; grid-template-columns:1fr 1fr; gap:16px; margin:20px 0;">
# MAGIC
# MAGIC   <!-- Pipeline Root Folder -->
# MAGIC   <div style="border-radius:10px; overflow:hidden; border:2px solid #00A972; box-shadow:0 3px 12px rgba(0,169,114,0.12);">
# MAGIC   <div style="background:#00A972; padding:12px 18px; color:white; font-weight:800; font-size:11pt;">
# MAGIC       Pipeline Root Folder
# MAGIC   </div>
# MAGIC   <div style="background:#F9F7F4; padding:18px 20px;">
# MAGIC       <div style="font-size:10.5pt; color:#1B3139; line-height:1.8;">
# MAGIC         The <b>Pipeline Root Folder</b> is set to automatically and includes all relevant files within that folder for your pipeline project (unless otherwise specified by the user). <br><br>This can be a <b>Git folder</b>, enabling easy version control and collaboration.
# MAGIC       </div>
# MAGIC   </div>
# MAGIC   </div>
# MAGIC
# MAGIC   <!-- Source Code Section -->
# MAGIC   <div style="border-radius:10px; overflow:hidden; border:2px solid #2272B4; box-shadow:0 3px 12px rgba(34,114,180,0.12);">
# MAGIC   <div style="background:#2272B4; padding:12px 18px; color:white; font-weight:800; font-size:11pt;">
# MAGIC       Source Code Section
# MAGIC   </div>
# MAGIC   <div style="background:#F9F7F4; padding:18px 20px;">
# MAGIC       <div style="font-size:10.5pt; color:#1B3139; line-height:1.8;">
# MAGIC         The <strong>Source Code Section</strong> lets you specify which subfolders or individual files to include in your pipeline, typically sub folders and files within the root folder.
# MAGIC         <br><br>
# MAGIC         These can be:<br>
# MAGIC         Python scripts | SQL files | Notebooks | etc.
# MAGIC       </div>
# MAGIC   </div>
# MAGIC   </div>
# MAGIC
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #####EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC Code Assets setting controls what code files your pipeline uses during execution.<br>
# MAGIC Together, these settings ensure your pipeline runs with the exact code it needs, organized and versioned as part of your project.
# MAGIC </details>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ### B4. Configuration (Parameters)
# MAGIC
# MAGIC A pipeline's configuration is a map of **key-value pairs** that can be used to parameterize your code.
# MAGIC
# MAGIC <div style="max-width:1000px; margin:0 auto; font-family:'Segoe UI',sans-serif;">
# MAGIC <div style="display:grid; grid-template-columns:1fr 1fr; gap:16px; margin:20px 0;">
# MAGIC   <!-- Benefits -->
# MAGIC   <div style="background:#F9F7F4; border-radius:12px; padding:20px 22px; border:2px solid #FFAB00; box-shadow:0 3px 12px rgba(255,171,0,0.12);">
# MAGIC     <div style="font-weight:800; font-size:11pt; color:#1B3139; margin-bottom:14px; border-bottom:2px solid #EEEDE9; padding-bottom:10px;">
# MAGIC       Why Use Configuration Parameters?
# MAGIC     </div>
# MAGIC     <div style="display:flex; flex-direction:column; gap:12px;">
# MAGIC       <div style="display:flex; align-items:flex-start; gap:10px;">
# MAGIC         <span style="background:#FFAB00; color:#1B3139; width:24px; height:24px; border-radius:50%; display:flex; align-items:center; justify-content:center; font-weight:800; font-size:11px; flex-shrink:0;">1</span>
# MAGIC         <div style="font-size:10.5pt; color:#1B3139; line-height:1.7;">
# MAGIC           <strong>Improves readability &amp; maintainability</strong> by centralizing key values.
# MAGIC         </div>
# MAGIC       </div>
# MAGIC       <div style="display:flex; align-items:flex-start; gap:10px;">
# MAGIC         <span style="background:#FFAB00; color:#1B3139; width:24px; height:24px; border-radius:50%; display:flex; align-items:center; justify-content:center; font-weight:800; font-size:11px; flex-shrink:0;">2</span>
# MAGIC         <div style="font-size:10.5pt; color:#1B3139; line-height:1.7;">
# MAGIC           <strong>Enables you to reuse</strong> common parameters across multiple pipeline files, avoiding hardcoding values.
# MAGIC         </div>
# MAGIC       </div>
# MAGIC       <div style="display:flex; align-items:flex-start; gap:10px;">
# MAGIC         <span style="background:#FFAB00; color:#1B3139; width:24px; height:24px; border-radius:50%; display:flex; align-items:center; justify-content:center; font-weight:800; font-size:11px; flex-shrink:0;">3</span>
# MAGIC         <div style="font-size:10.5pt; color:#1B3139; line-height:1.7;">
# MAGIC           <strong>Makes pipelines flexible</strong> and easier to update without changing code in multiple places.
# MAGIC         </div>
# MAGIC       </div>
# MAGIC       <div style="font-size:10.5pt; color:#5A6F77; line-height:1.7; margin-bottom:12px;">
# MAGIC     For example, you might have a parameter called <code>source</code> that points to a specific volume path.
# MAGIC     <br><br>
# MAGIC     In your SQL code, you can reference this parameter using the syntax <code style="background:#e8f0fe; padding:2px 6px; border-radius:4px; color:#1B3139;">&#36;{source}</code>, which dynamically inserts the value at runtime.
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC   </div>
# MAGIC   <!-- SQL Reference Syntax (updated) -->
# MAGIC   <div style="background:#F9F7F4; border-radius:12px; padding:20px 22px; border:2px solid #2272B4; box-shadow:0 3px 12px rgba(34,114,180,0.12); overflow:hidden;">
# MAGIC   <div style="font-weight:800; font-size:11pt; color:#1B3139; margin-bottom:14px; border-bottom:2px solid #EEEDE9; padding-bottom:10px;">
# MAGIC       SQL Reference Syntax
# MAGIC   </div>
# MAGIC   <div style="max-width: 900px; margin: 0 auto; font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; position:relative; padding-left:22px;">
# MAGIC       <svg xmlns="http://www.w3.org/2000/svg" width="20" height="218" viewBox="0 0 20 218" aria-hidden="true" style="position:absolute; left:0; top:70px; display:block; pointer-events:none;">
# MAGIC         <path d="M 16 0 L 4 0 L 4 200 Q 4 212 14 212" fill="none" stroke="#FF5F46" stroke-width="2"/>
# MAGIC         <polygon points="18,212 12,208 12,216" fill="#FF5F46"/>
# MAGIC       </svg>
# MAGIC       <div style="border:1px solid #D0D7DD; border-radius:8px; padding:12px 16px; margin-bottom:24px; background:#FFFFFF; overflow:hidden;">
# MAGIC         <div style="font-size:13px; font-weight:600; color:#1B3139; margin-bottom:8px;">Configuration</div>
# MAGIC         <div style="display:flex; align-items:center; gap:12px; margin-bottom:8px;">
# MAGIC           <div style="flex:0 0 80px; font-size:11px; font-weight:600; color:#5A6F77;">Key*</div>
# MAGIC           <div style="flex:1; font-size:11px; font-weight:600; color:#5A6F77; padding-left: 80px">Value</div>
# MAGIC           <div style="flex:0 0 24px;"></div>
# MAGIC         </div>
# MAGIC         <div style="display:flex; align-items:center; gap:12px; min-width:0;">
# MAGIC           <div style="flex:0 0 160px;">
# MAGIC             <div style="border:1px solid #CDD4DB; border-radius:6px; padding:6px 8px; font-size:13px; color:#0B2026; background:#FDFDFD;">
# MAGIC               source
# MAGIC             </div>
# MAGIC           </div>
# MAGIC           <div style="flex:1; min-width:0; overflow:hidden;">
# MAGIC             <div style="border:1px solid #CDD4DB; border-radius:6px; padding:6px 8px; font-size:13px; color:#0B2026; background:#FDFDFD; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; max-width:100%; box-sizing:border-box;" title="/Volumes/dbacademy/ops/dbacademy_warehouse">
# MAGIC               /Volumes/dbacademy/ops/dbacademy_warehouse
# MAGIC             </div>
# MAGIC           </div>
# MAGIC           <div style="flex:0 0 24px; text-align:center; color:#9CA3AF; font-size:14px;">🗑</div>
# MAGIC         </div>
# MAGIC         <button style="margin-top:10px; padding:4px 10px; border-radius:4px; border:1px solid #D0D7DD; background:#F9F9FB; font-size:12px; color:#1B3139; cursor:default;">
# MAGIC           Add parameter
# MAGIC         </button>
# MAGIC       </div>
# MAGIC       <div style="border:1px solid #D0D7DD; border-radius:8px; padding:14px 16px; background:#FFFFFF;">
# MAGIC         <div style="position:relative; margin:16px 0;">
# MAGIC           <button type="button" id="b4-copy-btn" onclick="copyB4Sql()" style="position:absolute; top:8px; right:8px; padding:4px 12px; font-size:12px; background:#ddd; color:#333; border:1px solid #ccc; border-radius:4px; cursor:pointer; z-index:10;">Copy</button>
# MAGIC           <pre style="background:#f8f8f8; border-radius:8px; padding:16px; padding-top:40px; overflow-x:auto; margin:0; border:1px solid #e0e0e0;"><code id="b4-sql-code" style="font-family:Consolas,Monaco,monospace; font-size:14px; color:#1B3139; white-space:pre;">CREATE STREAMING TABLE bronze
# MAGIC SELECT *
# MAGIC FROM STREAM read_files(
# MAGIC   "<span style='background:#e8f0fe; padding:0 2px; border-radius:3px;'>&#36;{source}</span>/orders",
# MAGIC   format =&gt; 'JSON'
# MAGIC );</code></pre>
# MAGIC         </div>
# MAGIC       </div>
# MAGIC       <script>
# MAGIC       if (typeof copyB4Sql !== 'function') {
# MAGIC         function copyB4Sql() {
# MAGIC           var btn = document.getElementById('b4-copy-btn');
# MAGIC           var sql = 'CREATE STREAMING TABLE bronze\nSELECT *\nFROM STREAM read_files(\n  "' + '$' + '{source}/orders",\n  format => \'JSON\'\n);';
# MAGIC           function showCopied() {
# MAGIC             btn.textContent = '✓ Copied!';
# MAGIC             setTimeout(function() { btn.textContent = 'Copy'; }, 2000);
# MAGIC           }
# MAGIC           function fallback() {
# MAGIC             var t = document.createElement('textarea');
# MAGIC             t.value = sql;
# MAGIC             document.body.appendChild(t);
# MAGIC             t.select();
# MAGIC             document.execCommand('copy');
# MAGIC             document.body.removeChild(t);
# MAGIC             showCopied();
# MAGIC           }
# MAGIC           if (navigator.clipboard && navigator.clipboard.writeText) {
# MAGIC             navigator.clipboard.writeText(sql).then(showCopied).catch(fallback);
# MAGIC           } else {
# MAGIC             fallback();
# MAGIC           }
# MAGIC         }
# MAGIC       }
# MAGIC       </script>
# MAGIC   </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ### B5. Additional Pipeline Settings
# MAGIC
# MAGIC Beyond the core settings we've discussed, there are several other pipeline settings you’ll encounter throughout the course.
# MAGIC
# MAGIC #####Place cursor over the highlighted headings to know more about additional pipeline settings.
# MAGIC <br>
# MAGIC
# MAGIC <div style="max-width: 700px; margin: 0 auto; font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;">
# MAGIC
# MAGIC <style>
# MAGIC   .pipeline-hotspot-wrap {
# MAGIC     position: relative;
# MAGIC     display: inline-block;
# MAGIC     max-width: 100%;
# MAGIC   }
# MAGIC   .pipeline-hotspot-img {
# MAGIC     width: 100%;
# MAGIC     height: auto;
# MAGIC     border-radius: 4px;
# MAGIC     border: 1px solid #d0d7dd;
# MAGIC     display: block;
# MAGIC   }
# MAGIC
# MAGIC   .hs-box {
# MAGIC     position: absolute;
# MAGIC     border: 2px solid rgba(255,95,70,0.9);
# MAGIC     background: rgba(255,95,70,0.06);
# MAGIC     border-radius: 6px;
# MAGIC     box-sizing: border-box;
# MAGIC     cursor: pointer;
# MAGIC   }
# MAGIC   .hs-label {
# MAGIC     position: absolute;
# MAGIC     top: -11px;
# MAGIC     left: -11px;
# MAGIC     width: 22px;
# MAGIC     height: 22px;
# MAGIC     border-radius: 999px;
# MAGIC     background: #FF5F46;
# MAGIC     color: #ffffff;
# MAGIC     font-size: 16px;
# MAGIC     font-weight: 800;
# MAGIC     display: flex;
# MAGIC     align-items: center;
# MAGIC     justify-content: center;
# MAGIC     box-shadow: 0 2px 6px rgba(15,23,42,0.35);
# MAGIC   }
# MAGIC
# MAGIC   .hs-tooltip {
# MAGIC     position: absolute;
# MAGIC     left: 100%;
# MAGIC     top: 0;
# MAGIC     margin-left: 10px;
# MAGIC     min-width: 260px;
# MAGIC     max-width: 320px;
# MAGIC     background: #F9F7F4;
# MAGIC     border-radius: 8px;
# MAGIC     border: 1px solid #D0D7DD;
# MAGIC     padding: 10px 12px;
# MAGIC     font-size: 16px;
# MAGIC     color: #0B2026;
# MAGIC     box-shadow: 0 3px 10px rgba(15,23,42,0.12);
# MAGIC     line-height: 1.6;
# MAGIC     display: none;
# MAGIC     z-index: 5;
# MAGIC   }
# MAGIC   .hs-tooltip-title {
# MAGIC     font-weight: 800;
# MAGIC     margin-bottom: 2px;
# MAGIC     color: #1B3139;
# MAGIC   }
# MAGIC   .hs-box:hover .hs-tooltip {
# MAGIC     display: block;
# MAGIC   }
# MAGIC
# MAGIC   /* Approximate highlight positions (tuned for this screenshot) */
# MAGIC   .hs-1 { top: 2%;  left: 4%;  width: 30%; height: 5%; }   /* Pipeline settings */
# MAGIC   .hs-2 { top: 26%; left: 4%;  width: 19%; height: 5%; }   /* Code assets */
# MAGIC   .hs-3 { top: 43%; left: 4%;  width: 43%; height: 5%; }  /* Default location */
# MAGIC   .hs-4 { top: 59.5%; left: 4%;  width: 15%; height: 5%; }   /* Compute */
# MAGIC   .hs-5 { top: 70.5%; left: 4%;  width: 20%; height: 5%; }  /* Configuration */
# MAGIC   .hs-6 { top: 86%; left: 4%;  width: 8%; height: 4%; }   /* Tags */
# MAGIC
# MAGIC   @media (max-width: 800px) {
# MAGIC     .hs-tooltip {
# MAGIC       left: 0;
# MAGIC       top: 100%;
# MAGIC       margin-left: 0;
# MAGIC       margin-top: 6px;
# MAGIC     }
# MAGIC   }
# MAGIC </style>
# MAGIC
# MAGIC <div class="pipeline-hotspot-wrap">
# MAGIC   <img
# MAGIC     src="https://files.training.databricks.com/binder/prod_main/build-data-pipelines-with-apache-spark-declarative-pipelines-en_us-3.2.2/images/20261007T191223Z/Build Data Pipelines with Apache Spark Declarative Pipelines/Includes/images/lecture_common_pipeline_settings/pipeline_settings_details.png"
# MAGIC     alt="Pipeline settings panel"
# MAGIC     class="pipeline-hotspot-img">
# MAGIC   <!-- 1. Pipeline settings -->
# MAGIC   <div class="hs-box hs-1">
# MAGIC     <div class="hs-label">1</div>
# MAGIC     <div class="hs-tooltip">
# MAGIC       <div class="hs-tooltip-title">Pipeline Settings</div>
# MAGIC       <div>General pipeline behavior and metadata.</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <!-- 2. Code assets -->
# MAGIC   <div class="hs-box hs-2">
# MAGIC     <div class="hs-label">2</div>
# MAGIC     <div class="hs-tooltip">
# MAGIC       <div class="hs-tooltip-title">Code Assets</div>
# MAGIC       <div>Managing your pipeline code files.</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <!-- 3. Default location for data assets -->
# MAGIC   <div class="hs-box hs-3">
# MAGIC     <div class="hs-label">3</div>
# MAGIC     <div class="hs-tooltip">
# MAGIC       <div class="hs-tooltip-title">Default Location for Data Assets</div>
# MAGIC       <div>Setting where data is stored by default.</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <!-- 4. Compute -->
# MAGIC   <div class="hs-box hs-4">
# MAGIC     <div class="hs-label">4</div>
# MAGIC     <div class="hs-tooltip">
# MAGIC       <div class="hs-tooltip-title">Compute</div>
# MAGIC       <div>Choosing your execution environment.</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <!-- 5. Configuration (+ environment dependencies) -->
# MAGIC   <div class="hs-box hs-5">
# MAGIC     <div class="hs-label">5</div>
# MAGIC     <div class="hs-tooltip">
# MAGIC       <div class="hs-tooltip-title">Configuration</div>
# MAGIC       <div>Parameterizing your pipeline code.</div>
# MAGIC       <div style="margin-top:4px;">
# MAGIC         <strong>Environment dependencies</strong> &mdash; A dependency is a line in a pip requirements file.
# MAGIC       </div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC   <!-- 6. Tags -->
# MAGIC   <div class="hs-box hs-6">
# MAGIC     <div class="hs-label">6</div>
# MAGIC     <div class="hs-tooltip">
# MAGIC       <div class="hs-tooltip-title">Tags</div>
# MAGIC       <div>Organizing and categorizing pipelines for easy management.</div>
# MAGIC     </div>
# MAGIC   </div>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC #####EXPAND FOR ADDITIONAL NOTES
# MAGIC <details>
# MAGIC
# MAGIC Other additional features include
# MAGIC - Budget - Defining cost controls and limits
# MAGIC - Advanced Settings - Additional options for fine-tuning your pipeline
# MAGIC
# MAGIC We’ll explore these in more detail with hands-on demonstrations to help you get the most out of Spark Declarative Pipelines.
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Conclusion
# MAGIC
# MAGIC In this lecture, you learned how dataset types, the multi-file editor, and common pipeline settings work together to support reliable Apache Spark™ Declarative Pipeline development:
# MAGIC
# MAGIC 1. **Streaming tables, materialized views, and views** support different data processing patterns, from incremental ingestion to refreshed aggregations and on-demand query execution.
# MAGIC 2. **Declarative SQL syntax** provides a consistent way to create pipeline datasets while Databricks automatically manages dependencies through the Declarative Pipeline Graph.
# MAGIC 3. **The multi-file editor** simplifies pipeline development by combining pipeline assets, modular code editing, DAG visualization, data previews, execution insights, and validation tools in one environment.
# MAGIC 4. **Pipeline settings** let you customize compute, code assets, configuration parameters, and additional options to make pipelines easier to manage, reuse, and scale.
# MAGIC

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