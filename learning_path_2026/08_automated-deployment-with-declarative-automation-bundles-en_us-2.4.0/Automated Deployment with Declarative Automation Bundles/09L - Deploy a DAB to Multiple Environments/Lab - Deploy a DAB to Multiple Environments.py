# Databricks notebook source
# MAGIC %md
# MAGIC ![DB Academy](https://files.training.databricks.com/binder/prod_main/automated-deployment-with-declarative-automation-bundles-en_us-2.4.0/images/20261005T093748Z/Automated Deployment with Declarative Automation Bundles/Includes/images/common/db-academy.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # 09L - Deploy a Declarative Automation Bundle (DAB) to Multiple Environments
# MAGIC
# MAGIC ### Estimated Duration: ~15 minutes
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC In this lab, you'll extend a single-environment bundle (the one you built in **02L - Deploy a Simple DAB**) so that it can deploy the same job to **both** development and production environments with different configurations per target. You'll move the job into its own resource YAML file, define **bundle variables**, and use **target-level overrides** to point each environment at the correct catalog.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lab, you will be able to:
# MAGIC
# MAGIC 1. **Modularize a bundle** by moving a job definition into a separate YAML file under `resources/` and pulling it in via the `include` mapping.
# MAGIC 2. **Define and reference bundle variables** so the same configuration adapts to dev vs prod.
# MAGIC 3. **Override job parameters at the target level** so dev and prod read from and write to different catalogs.
# MAGIC 4. **Validate, deploy, and run** the bundle against both `dev` and `prod` targets using the Databricks CLI.
# MAGIC 5. **Verify** the deployed job actually produced the expected tables in each environment.
# MAGIC
# MAGIC ## Reference Documentation
# MAGIC
# MAGIC - **What are bundles?** (intro): [AWS](https://docs.databricks.com/aws/en/dev-tools/bundles/) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/) | [GCP](https://docs.databricks.com/gcp/en/dev-tools/bundles/)
# MAGIC - **Bundle configuration reference (full YAML key list)**: [AWS](https://docs.databricks.com/aws/en/dev-tools/bundles/reference) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/reference) | [GCP](https://docs.databricks.com/gcp/en/dev-tools/bundles/reference)
# MAGIC - **Bundle settings (mappings, including `include` and `targets`)**: [AWS](https://docs.databricks.com/aws/en/dev-tools/bundles/settings) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/settings) | [GCP](https://docs.databricks.com/gcp/en/dev-tools/bundles/settings)
# MAGIC - **Variables and substitutions**: [AWS](https://docs.databricks.com/aws/en/dev-tools/bundles/variables) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/variables) | [GCP](https://docs.databricks.com/gcp/en/dev-tools/bundles/variables)
# MAGIC - **Deployment modes (`development` / `production`)**: [AWS](https://docs.databricks.com/aws/en/dev-tools/bundles/deployment-modes) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/deployment-modes) | [GCP](https://docs.databricks.com/gcp/en/dev-tools/bundles/deployment-modes)
# MAGIC - **`databricks bundle` CLI commands**: [AWS](https://docs.databricks.com/aws/en/dev-tools/cli/bundle-commands) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/cli/bundle-commands) | [GCP](https://docs.databricks.com/gcp/en/dev-tools/cli/bundle-commands)
# MAGIC - **Set a bundle run identity (`run_as`)**: [AWS](https://docs.databricks.com/aws/en/dev-tools/bundles/run-as) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/run-as) | [GCP](https://docs.databricks.com/gcp/en/dev-tools/bundles/run-as)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## REQUIRED - SELECT A COMPUTE ENVIRONMENT
# MAGIC <div style="
# MAGIC   border-left: 4px solid #f44336;
# MAGIC   background: #ffebee;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <strong style="display:block; color:#c62828; margin-bottom:6px; font-size: 1.1em;">Select All-Purpose Compute</strong>
# MAGIC   <div style="color:#333;">
# MAGIC
# MAGIC This notebook requires **all-purpose compute** (Dedicated). Serverless is not supported for this notebook.
# MAGIC
# MAGIC Follow these steps to attach an all-purpose compute cluster:
# MAGIC
# MAGIC 1. Navigate to the top-right of this notebook and click the drop-down menu to select your `labuser_USERNAME` cluster.
# MAGIC     - By default, the notebook might use **Serverless**.
# MAGIC
# MAGIC 2. If your cluster is available, select it and continue to the next cell. If the cluster is not shown:
# MAGIC
# MAGIC     - In the drop-down, select **More**.
# MAGIC
# MAGIC     - In the **Attach to an existing compute resource** pop-up, select the first drop-down. You will see a unique cluster name in that drop-down. Please select that cluster.
# MAGIC
# MAGIC ⚠️ **NOTE:** If the cluster shows a **terminated** state (red dot in the cluster picker), it needs to be started before you can attach. Click the cluster, then **Start**, and wait a few minutes until you see a green dot.
# MAGIC   </div>
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## REQUIRED - DATA SETUP
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #f44336;
# MAGIC   background: #ffebee;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <strong style="display:block; color:#c62828; margin-bottom:6px; font-size: 1.1em;">Data Setup</strong>
# MAGIC   <div style="color:#333;">
# MAGIC
# MAGIC Recall that your environment was set up using the **02 - REQUIRED - Course Setup and Authentication** notebook.
# MAGIC
# MAGIC If you end your lab or your lab session times out, your environment will be reset. You will need to rerun the **02 - REQUIRED - Course Setup and Authentication** notebook to recreate the catalogs and refresh your Databricks CLI credentials.
# MAGIC
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Classroom Setup
# MAGIC
# MAGIC Run the following cell to configure your working environment for this course.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-09L

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Lab Scenario
# MAGIC
# MAGIC You are responsible for deploying Databricks projects in your organization using **Declarative Automation Bundles (DABs)**. 
# MAGIC
# MAGIC You configured the project to deploy to a single development environment in **09L - Deploy a Simple DAB**. 
# MAGIC
# MAGIC Your next task is to extend the bundle so the **same** job can be deployed to **both** development and production environments with different configurations per target. You'll accomplish this with **bundle variables** and **target-level overrides**.
# MAGIC
# MAGIC ### Development target requirements
# MAGIC
# MAGIC - Read from and write to your **labuser_UNIQUE_ID_1_dev** catalog (development data, ~100 rows).
# MAGIC
# MAGIC ### Production target requirements
# MAGIC
# MAGIC - Read from and write to your **labuser_UNIQUE_ID_3_prod** catalog (production data, ~22,000 rows).

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Preview the Development and Production Data

# COMMAND ----------

# MAGIC %md
# MAGIC ### C1. View the Development Data

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Preview the **nyctaxi_raw** development data within your **labuser_UNIQUE_ID_1_dev** catalog. 
# MAGIC
# MAGIC     Notice that the dev data contains 100 rows.
# MAGIC

# COMMAND ----------

spark.sql(f'''
          SELECT * 
          FROM {catalog_dev}.default.nyctaxi_raw
          ''').display()

# COMMAND ----------

# MAGIC %md
# MAGIC 2. View the tables in your **labuser_UNIQUE_ID_1_dev** catalog. 
# MAGIC     
# MAGIC     Notice that the **nyctaxi_bronze** and **nyctaxi_silver** tables do not exist.
# MAGIC

# COMMAND ----------

spark.sql(f'SHOW TABLES IN {catalog_dev}.default').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### C2. View the Production Data
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Preview the **nyctaxi_raw** production data within your **labuser_UNIQUE_ID_3_prod** catalog. 
# MAGIC
# MAGIC     Notice that the production data contains about 22,000 rows.

# COMMAND ----------

spark.sql(f'''
          SELECT count(*) AS TotalRows 
          FROM {catalog_prod}.default.nyctaxi_raw
          ''').display()

# COMMAND ----------

spark.sql(f'''
          SELECT * 
          FROM {catalog_prod}.default.nyctaxi_raw
          ''').display()

# COMMAND ----------

# MAGIC %md
# MAGIC 2. View the tables in your **labuser_UNIQUE_ID_3_prod** catalog. Notice that the **nyctaxi_bronze** and **nyctaxi_silver** tables do not exist.

# COMMAND ----------

spark.sql(f'SHOW TABLES IN {catalog_prod}.default').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Pre-flight Checks
# MAGIC
# MAGIC Before starting the lab tasks, run a couple of quick checks to confirm the Databricks CLI is installed and authenticated against your workspace.

# COMMAND ----------

# MAGIC %md
# MAGIC ### D1. Check the Databricks CLI Version
# MAGIC
# MAGIC Run a CLI command to confirm the Databricks CLI version is **v0.298.0**.

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks -v

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="
# MAGIC   border-left: 4px solid #ff9800;
# MAGIC   background: #fff3e0;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <strong style="display:block; color:#e65100; margin-bottom:6px; font-size: 1.1em;">
# MAGIC     DATABRICKS CLI ERROR TROUBLESHOOTING:
# MAGIC   </strong>
# MAGIC   <div style="color:#333;">
# MAGIC
# MAGIC   - If you encounter a Databricks CLI authentication error, it means the authentication was not successful. Confirm you ran the notebook using your **all purpose compute**.
# MAGIC
# MAGIC   - If you encounter the error below, it means your **databricks.yml** file has syntax issues due to a modification. Even for non-DAB CLI commands, the **databricks.yml** file is still required, as it may contain important authentication details, such as the host and profile, which are utilized by the CLI commands.
# MAGIC
# MAGIC ![CLI Invalid YAML](https://files.training.databricks.com/binder/prod_main/automated-deployment-with-declarative-automation-bundles-en_us-2.4.0/images/20261005T093748Z/Automated Deployment with Declarative Automation Bundles/Includes/images/databricks_cli_error_invalid_yaml.png)
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Task 1 - Get Your Lab User Name
# MAGIC
# MAGIC You'll need your lab user name in the next task to populate a bundle variable. Run the cell below to print it.

# COMMAND ----------

print(my_catalog)

# COMMAND ----------

# MAGIC %md
# MAGIC ## F. Task 2 - Update the Resource YAML File
# MAGIC
# MAGIC In a new tab, open the **./resources/lab09_nyc.job.yml** file and complete the following:
# MAGIC
# MAGIC **Step 2.1** - Set the job `name` so it dynamically appends your user name: `name: lab09_dab_${workspace.current_user.userName}`
# MAGIC
# MAGIC **Step 2.2** - Under `parameters`, add the `${bundle.target}` substitution as the default for `display_target`
# MAGIC   - This is so the parameter automatically reflects which target the bundle was deployed to.
# MAGIC
# MAGIC **HINT:** Variables and substitutions documentation:
# MAGIC [AWS](https://docs.databricks.com/aws/en/dev-tools/bundles/variables) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/variables) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/dev-tools/bundles/variables)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### ANSWER
# MAGIC
# MAGIC <details>
# MAGIC   <summary>EXPAND FOR SOLUTION (Resource YAML)</summary>
# MAGIC
# MAGIC ```yaml
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     lab09_dab:
# MAGIC       name: lab09_dab_${workspace.current_user.userName}   # <--- append your user name
# MAGIC       tasks:
# MAGIC         - task_key: create_nyc_tables
# MAGIC           notebook_task:
# MAGIC             notebook_path: ../src/our_project_code.sql
# MAGIC             source: WORKSPACE
# MAGIC       parameters:
# MAGIC         - name: display_target
# MAGIC           default: ${bundle.target}                        # <--- bundle.target substitution
# MAGIC ```
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## G. Task 3 - Update **databricks.yml**
# MAGIC
# MAGIC In the same tab, open the **databricks.yml** file. First explore the bundle and then complete the four sub-steps below.
# MAGIC
# MAGIC **What to notice in the existing file:**
# MAGIC
# MAGIC - The bundle is named `demo09_lab_bundle`.
# MAGIC - The `include` mapping is **empty** (you'll fix that in 3.1).
# MAGIC - The `variables` mapping defines several variables (you'll set one in 3.2).
# MAGIC - The `targets` mapping has a `dev` and a `prod` target (you'll add a parameter override to each in 3.3 and 3.4).
# MAGIC
# MAGIC ### Step 3.1 - Add the resource file to `include`
# MAGIC
# MAGIC Add **./resources/lab09_nyc.job.yml** to the `include` mapping so the job you edited in Task 2 is pulled into the bundle.
# MAGIC   - **HINT:** `include` mapping documentation:
# MAGIC [AWS](https://docs.databricks.com/aws/en/dev-tools/bundles/settings#include) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/settings#include) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/dev-tools/bundles/settings#include)
# MAGIC
# MAGIC ### Step 3.2 - Set the `user_name` variable
# MAGIC
# MAGIC Set the `user_name` variable's value to your lab user name (from Task 1). 
# MAGIC   - The `user_name` variable feeds the `catalog_dev` and `catalog_prod` variables, so getting this right keeps every reference correct.
# MAGIC
# MAGIC ### Step 3.3 - Override `catalog_name` for the `dev` target
# MAGIC
# MAGIC   - Under the `dev` target, add a job parameter named `catalog_name` whose default is `${var.catalog_dev}`.
# MAGIC
# MAGIC ### Step 3.4 - Override `catalog_name` for the `prod` target
# MAGIC
# MAGIC   - Under the `prod` target, add a job parameter named `catalog_name` whose default is `${var.catalog_prod}`.
# MAGIC
# MAGIC **Why this works:** the same job runs against the dev catalog or the prod catalog depending on which target you deploy to, no duplication of the job definition required.
# MAGIC
# MAGIC **NOTE:** A complete example **databricks.yml** is in the **solutions** folder if you get stuck.

# COMMAND ----------

# MAGIC %md
# MAGIC ## H. Task 4 - Validate the Bundle
# MAGIC
# MAGIC Validate your **databricks.yml** bundle configuration file using the Databricks CLI. Run the cell and confirm validation succeeds. If there is an error, fix the **databricks.yml** file and re-run.
# MAGIC
# MAGIC **HINT:** `databricks bundle` CLI commands documentation:
# MAGIC [AWS](https://docs.databricks.com/aws/en/dev-tools/cli/bundle-commands) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/cli/bundle-commands) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/dev-tools/cli/bundle-commands)

# COMMAND ----------

# <FILL-IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="
# MAGIC   border-left: 4px solid #ff9800;
# MAGIC   background: #fff3e0;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <strong style="display:block; color:#e65100; margin-bottom:6px; font-size: 1.1em;">
# MAGIC      Troubleshooting
# MAGIC   </strong>
# MAGIC   <div style="color:#333;">
# MAGIC If you see the following error after validating your bundle, the format of your notebook could be incorrect.
# MAGIC
# MAGIC `Error: notebook src/xxx not found`. 
# MAGIC
# MAGIC Check the format of your notebook and adjust accordingly. 
# MAGIC
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### ANSWER
# MAGIC
# MAGIC <details>
# MAGIC   <summary>EXPAND FOR SOLUTION CODE</summary>
# MAGIC
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC <!-------------------ADD SOLUTION CODE BELOW------------------->
# MAGIC %sh
# MAGIC databricks bundle validate
# MAGIC <!-------------------END SOLUTION CODE------------------->
# MAGIC </code></pre>
# MAGIC
# MAGIC
# MAGIC <script>
# MAGIC function copyBlock() {
# MAGIC   const el = document.getElementById("copy-block");
# MAGIC   if (!el) return;
# MAGIC
# MAGIC   const text = el.innerText;
# MAGIC
# MAGIC   // Preferred modern API
# MAGIC   if (navigator.clipboard && navigator.clipboard.writeText) {
# MAGIC     navigator.clipboard.writeText(text)
# MAGIC       .then(() => alert("Copied to clipboard"))
# MAGIC       .catch(err => {
# MAGIC         console.error("Clipboard write failed:", err);
# MAGIC         fallbackCopy(text);
# MAGIC       });
# MAGIC   } else {
# MAGIC     fallbackCopy(text);
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC function fallbackCopy(text) {
# MAGIC   const textarea = document.createElement("textarea");
# MAGIC   textarea.value = text;
# MAGIC   textarea.style.position = "fixed";
# MAGIC   textarea.style.left = "-9999px";
# MAGIC   document.body.appendChild(textarea);
# MAGIC   textarea.select();
# MAGIC   try {
# MAGIC     document.execCommand("copy");
# MAGIC     alert("Copied to clipboard");
# MAGIC   } catch (err) {
# MAGIC     console.error("Fallback copy failed:", err);
# MAGIC     alert("Could not copy to clipboard. Please copy manually.");
# MAGIC   } finally {
# MAGIC     document.body.removeChild(textarea);
# MAGIC   }
# MAGIC }
# MAGIC </script>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## I. Task 5 - Deploy to the `dev` Target
# MAGIC
# MAGIC Deploy the bundle to the development environment using the Databricks CLI.
# MAGIC
# MAGIC After the cell completes:
# MAGIC
# MAGIC - Manually check that the job was created successfully. The job name will be **[dev <user>] lab09_dab_<userName>**.
# MAGIC - Check the **Job parameters** and confirm:
# MAGIC     - `catalog_name` references your `labuser_UNIQUE_ID_1_dev` catalog
# MAGIC     - `display_target` is `dev`
# MAGIC
# MAGIC **NOTE:** Deployment will take about a minute to complete.
# MAGIC
# MAGIC **HINT:** `databricks bundle` CLI commands documentation:
# MAGIC [AWS](https://docs.databricks.com/aws/en/dev-tools/cli/bundle-commands) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/cli/bundle-commands) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/dev-tools/cli/bundle-commands)

# COMMAND ----------

# <FILL-IN>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Checkpoint - Dev Deployment
# MAGIC ![Dev](https://files.training.databricks.com/binder/prod_main/automated-deployment-with-declarative-automation-bundles-en_us-2.4.0/images/20261005T093748Z/Automated Deployment with Declarative Automation Bundles/Includes/images/multiple-env-lab/dev-deployment.png)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### ANSWER
# MAGIC
# MAGIC <details>
# MAGIC   <summary>EXPAND FOR SOLUTION CODE</summary>
# MAGIC
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC <!-------------------ADD SOLUTION CODE BELOW------------------->
# MAGIC %sh
# MAGIC databricks bundle deploy -t dev
# MAGIC <!-------------------END SOLUTION CODE------------------->
# MAGIC </code></pre>
# MAGIC
# MAGIC
# MAGIC <script>
# MAGIC function copyBlock() {
# MAGIC   const el = document.getElementById("copy-block");
# MAGIC   if (!el) return;
# MAGIC
# MAGIC   const text = el.innerText;
# MAGIC
# MAGIC   // Preferred modern API
# MAGIC   if (navigator.clipboard && navigator.clipboard.writeText) {
# MAGIC     navigator.clipboard.writeText(text)
# MAGIC       .then(() => alert("Copied to clipboard"))
# MAGIC       .catch(err => {
# MAGIC         console.error("Clipboard write failed:", err);
# MAGIC         fallbackCopy(text);
# MAGIC       });
# MAGIC   } else {
# MAGIC     fallbackCopy(text);
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC function fallbackCopy(text) {
# MAGIC   const textarea = document.createElement("textarea");
# MAGIC   textarea.value = text;
# MAGIC   textarea.style.position = "fixed";
# MAGIC   textarea.style.left = "-9999px";
# MAGIC   document.body.appendChild(textarea);
# MAGIC   textarea.select();
# MAGIC   try {
# MAGIC     document.execCommand("copy");
# MAGIC     alert("Copied to clipboard");
# MAGIC   } catch (err) {
# MAGIC     console.error("Fallback copy failed:", err);
# MAGIC     alert("Could not copy to clipboard. Please copy manually.");
# MAGIC   } finally {
# MAGIC     document.body.removeChild(textarea);
# MAGIC   }
# MAGIC }
# MAGIC </script>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## J. Task 6 - Run the `dev` Job
# MAGIC
# MAGIC Run the deployed job against the `dev` target.
# MAGIC
# MAGIC **NOTE:** This will take 1-2 minutes to complete.
# MAGIC
# MAGIC **HINT:** Use the **job key** from the `resources` mapping (your name will differ):
# MAGIC
# MAGIC ```yaml
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     lab09_dab:    # <--- This is the job key
# MAGIC       name: lab09_dab_${workspace.current_user.userName}
# MAGIC ```

# COMMAND ----------

# <FILL-IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### ANSWER
# MAGIC
# MAGIC <details>
# MAGIC   <summary>EXPAND FOR SOLUTION CODE</summary>
# MAGIC
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC <!-------------------ADD SOLUTION CODE BELOW------------------->
# MAGIC %sh
# MAGIC databricks bundle run -t dev lab09_dab
# MAGIC <!-------------------END SOLUTION CODE------------------->
# MAGIC </code></pre>
# MAGIC
# MAGIC
# MAGIC <script>
# MAGIC function copyBlock() {
# MAGIC   const el = document.getElementById("copy-block");
# MAGIC   if (!el) return;
# MAGIC
# MAGIC   const text = el.innerText;
# MAGIC
# MAGIC   // Preferred modern API
# MAGIC   if (navigator.clipboard && navigator.clipboard.writeText) {
# MAGIC     navigator.clipboard.writeText(text)
# MAGIC       .then(() => alert("Copied to clipboard"))
# MAGIC       .catch(err => {
# MAGIC         console.error("Clipboard write failed:", err);
# MAGIC         fallbackCopy(text);
# MAGIC       });
# MAGIC   } else {
# MAGIC     fallbackCopy(text);
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC function fallbackCopy(text) {
# MAGIC   const textarea = document.createElement("textarea");
# MAGIC   textarea.value = text;
# MAGIC   textarea.style.position = "fixed";
# MAGIC   textarea.style.left = "-9999px";
# MAGIC   document.body.appendChild(textarea);
# MAGIC   textarea.select();
# MAGIC   try {
# MAGIC     document.execCommand("copy");
# MAGIC     alert("Copied to clipboard");
# MAGIC   } catch (err) {
# MAGIC     console.error("Fallback copy failed:", err);
# MAGIC     alert("Could not copy to clipboard. Please copy manually.");
# MAGIC   } finally {
# MAGIC     document.body.removeChild(textarea);
# MAGIC   }
# MAGIC }
# MAGIC </script>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## K. Task 7 - Verify the `dev` Tables
# MAGIC
# MAGIC After the job completes, run the following cells to confirm:
# MAGIC
# MAGIC - Both **nyctaxi_bronze** and **nyctaxi_silver** tables exist in your **labuser_UNIQUE_ID_1_dev** catalog.
# MAGIC - The **nyctaxi_bronze** table contains **100 rows**.

# COMMAND ----------

spark.sql(f'SHOW TABLES IN {catalog_dev}.default').display()

# COMMAND ----------

check_nyctaxi_bronze_table(user_catalog = catalog_dev, total_count=100)

# COMMAND ----------

# MAGIC %md
# MAGIC ## L. Task 8 - Deploy to the `prod` Target
# MAGIC
# MAGIC Deploy the bundle to the production environment using the Databricks CLI.
# MAGIC
# MAGIC After the cell completes:
# MAGIC
# MAGIC - Manually check that the job was created successfully. The production job name will be **lab09_dab_<userName>** (no `[dev …]` prefix in `production` mode).
# MAGIC - Check the **Job parameters** and confirm:
# MAGIC     - `catalog_name` references your `labuser_UNIQUE_ID_3_prod` catalog
# MAGIC     - `display_target` is `prod`
# MAGIC
# MAGIC **NOTE:** Deployment will take about a minute to complete.
# MAGIC
# MAGIC **HINT:** `databricks bundle` CLI commands documentation:
# MAGIC [AWS](https://docs.databricks.com/aws/en/dev-tools/cli/bundle-commands) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/cli/bundle-commands) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/dev-tools/cli/bundle-commands)
# MAGIC
# MAGIC **NOTE:** In real production, you typically run the job using a service principal. See the **Set a bundle run identity** documentation:
# MAGIC [AWS](https://docs.databricks.com/aws/en/dev-tools/bundles/run-as) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/run-as) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/dev-tools/bundles/run-as). For this lab, we run the production job as the user.

# COMMAND ----------

# <FILL-IN>

# COMMAND ----------

# MAGIC %md
# MAGIC #### Checkpoint - Prod Deployment
# MAGIC ![Dev](https://files.training.databricks.com/binder/prod_main/automated-deployment-with-declarative-automation-bundles-en_us-2.4.0/images/20261005T093748Z/Automated Deployment with Declarative Automation Bundles/Includes/images/multiple-env-lab/prod-deployment.png)
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### ANSWER
# MAGIC
# MAGIC <details>
# MAGIC   <summary>EXPAND FOR SOLUTION CODE</summary>
# MAGIC
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC <!-------------------ADD SOLUTION CODE BELOW------------------->
# MAGIC %sh
# MAGIC databricks bundle deploy -t prod
# MAGIC <!-------------------END SOLUTION CODE------------------->
# MAGIC </code></pre>
# MAGIC
# MAGIC
# MAGIC <script>
# MAGIC function copyBlock() {
# MAGIC   const el = document.getElementById("copy-block");
# MAGIC   if (!el) return;
# MAGIC
# MAGIC   const text = el.innerText;
# MAGIC
# MAGIC   // Preferred modern API
# MAGIC   if (navigator.clipboard && navigator.clipboard.writeText) {
# MAGIC     navigator.clipboard.writeText(text)
# MAGIC       .then(() => alert("Copied to clipboard"))
# MAGIC       .catch(err => {
# MAGIC         console.error("Clipboard write failed:", err);
# MAGIC         fallbackCopy(text);
# MAGIC       });
# MAGIC   } else {
# MAGIC     fallbackCopy(text);
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC function fallbackCopy(text) {
# MAGIC   const textarea = document.createElement("textarea");
# MAGIC   textarea.value = text;
# MAGIC   textarea.style.position = "fixed";
# MAGIC   textarea.style.left = "-9999px";
# MAGIC   document.body.appendChild(textarea);
# MAGIC   textarea.select();
# MAGIC   try {
# MAGIC     document.execCommand("copy");
# MAGIC     alert("Copied to clipboard");
# MAGIC   } catch (err) {
# MAGIC     console.error("Fallback copy failed:", err);
# MAGIC     alert("Could not copy to clipboard. Please copy manually.");
# MAGIC   } finally {
# MAGIC     document.body.removeChild(textarea);
# MAGIC   }
# MAGIC }
# MAGIC </script>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## M. Task 9 - Run the `prod` Job
# MAGIC
# MAGIC Run the deployed job against the `prod` target.
# MAGIC
# MAGIC **NOTE:** This will take 1-2 minutes to complete.

# COMMAND ----------

# <FILL-IN>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ##### ANSWER
# MAGIC
# MAGIC <details>
# MAGIC   <summary>EXPAND FOR SOLUTION CODE</summary>
# MAGIC
# MAGIC <button onclick="copyBlock()">Copy to clipboard</button>
# MAGIC
# MAGIC <pre id="copy-block" style="font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace; border:1px solid #e5e7eb; border-radius:10px; background:#f8fafc; padding:14px 16px; font-size:0.85rem; line-height:1.35; white-space:pre;">
# MAGIC <code>
# MAGIC <!-------------------ADD SOLUTION CODE BELOW------------------->
# MAGIC %sh
# MAGIC databricks bundle run -t prod lab09_dab
# MAGIC <!-------------------END SOLUTION CODE------------------->
# MAGIC </code></pre>
# MAGIC
# MAGIC
# MAGIC <script>
# MAGIC function copyBlock() {
# MAGIC   const el = document.getElementById("copy-block");
# MAGIC   if (!el) return;
# MAGIC
# MAGIC   const text = el.innerText;
# MAGIC
# MAGIC   // Preferred modern API
# MAGIC   if (navigator.clipboard && navigator.clipboard.writeText) {
# MAGIC     navigator.clipboard.writeText(text)
# MAGIC       .then(() => alert("Copied to clipboard"))
# MAGIC       .catch(err => {
# MAGIC         console.error("Clipboard write failed:", err);
# MAGIC         fallbackCopy(text);
# MAGIC       });
# MAGIC   } else {
# MAGIC     fallbackCopy(text);
# MAGIC   }
# MAGIC }
# MAGIC
# MAGIC function fallbackCopy(text) {
# MAGIC   const textarea = document.createElement("textarea");
# MAGIC   textarea.value = text;
# MAGIC   textarea.style.position = "fixed";
# MAGIC   textarea.style.left = "-9999px";
# MAGIC   document.body.appendChild(textarea);
# MAGIC   textarea.select();
# MAGIC   try {
# MAGIC     document.execCommand("copy");
# MAGIC     alert("Copied to clipboard");
# MAGIC   } catch (err) {
# MAGIC     console.error("Fallback copy failed:", err);
# MAGIC     alert("Could not copy to clipboard. Please copy manually.");
# MAGIC   } finally {
# MAGIC     document.body.removeChild(textarea);
# MAGIC   }
# MAGIC }
# MAGIC </script>
# MAGIC
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## N. Task 10 - Verify the `prod` Tables
# MAGIC
# MAGIC After the job completes, run the following cells to confirm:
# MAGIC
# MAGIC - Both **nyctaxi_bronze** and **nyctaxi_silver** tables exist in your **labuser_UNIQUE_ID_3_prod** catalog.
# MAGIC - The **nyctaxi_bronze** table contains **21,932 rows**.

# COMMAND ----------

spark.sql(f'SHOW TABLES IN {catalog_prod}.default').display()

# COMMAND ----------

check_nyctaxi_bronze_table(user_catalog = catalog_prod, total_count=21932)

# COMMAND ----------

# MAGIC %md
# MAGIC ## O. Further Reading
# MAGIC
# MAGIC This was a simple example of deploying a DAB to multiple environments. As you go further, two areas worth exploring:
# MAGIC
# MAGIC - **Other ways to set a variable's value.** 
# MAGIC   - In this lab, you set values inside **databricks.yml**. 
# MAGIC   - You can also pass values via the Databricks CLI, environment variables, or a `.databrickscfg` profile. 
# MAGIC   - **Set a variable's value** documentation:
# MAGIC [AWS](https://docs.databricks.com/aws/en/dev-tools/bundles/variables#set-a-variables-value) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/variables#set-a-variables-value) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/dev-tools/bundles/variables#set-a-variables-value)
# MAGIC
# MAGIC - **Override cluster settings per environment.** 
# MAGIC   - A common production pattern is to use small clusters in dev and larger clusters or Serverless in prod. 
# MAGIC   - **Override cluster settings in bundles** documentation:
# MAGIC [AWS](https://docs.databricks.com/aws/en/dev-tools/bundles/cluster-override) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/cluster-override) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/dev-tools/bundles/cluster-override)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC
# MAGIC Nice work. In this lab you took a single-environment bundle and extended it to deploy the same job to two environments without duplicating the job definition:
# MAGIC
# MAGIC 1. Moved the job into **./resources/lab09_nyc.job.yml** and pulled it in via the `include` mapping.
# MAGIC 2. Set the `user_name` bundle variable so per-target catalog variables resolve correctly.
# MAGIC 3. Added a `catalog_name` job parameter override under each of the `dev` and `prod` targets.
# MAGIC 4. Validated, deployed, and ran the bundle against both targets with `databricks bundle validate`, `databricks bundle deploy -t <target>`, and `databricks bundle run -t <target> lab09_dab`.
# MAGIC 5. Verified each environment's **nyctaxi_bronze** table had the expected row count (100 in dev, 21,932 in prod).

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>