# Databricks notebook source
# MAGIC %md
# MAGIC ![DB Academy](https://files.training.databricks.com/binder/prod_main/automated-deployment-with-declarative-automation-bundles-en_us-2.4.0/images/20261005T093748Z/Automated Deployment with Declarative Automation Bundles/Includes/images/common/db-academy.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # 06L - Deploy a Simple Declarative Automation Bundle (DAB)
# MAGIC
# MAGIC ### Estimated Duration: ~15 minutes
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC In this lab, you'll take a notebook project shared by a coworker and deploy it to a development environment using a **Declarative Automation Bundle (DAB)**. You'll build the source job in the UI, capture its YAML, finish the bundle's **databricks.yml** configuration, then validate, deploy, run, verify, and destroy the bundle using the Databricks CLI.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lab, you will be able to:
# MAGIC
# MAGIC 1. **Generate a YAML job configuration** by building a job in the UI and using **View as code**.
# MAGIC 2. **Update a `databricks.yml` file** with your job and a `dev` target that uses `mode: development`.
# MAGIC 3. **Validate, deploy, run, and destroy** a bundle with `databricks bundle` CLI commands.
# MAGIC 4. **Verify** that your deployed job actually produced the expected data.
# MAGIC
# MAGIC ## Reference Documentation
# MAGIC
# MAGIC - **What are bundles?** (intro): [AWS](https://docs.databricks.com/aws/en/dev-tools/bundles/) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/) | [GCP](https://docs.databricks.com/gcp/en/dev-tools/bundles/)
# MAGIC - **Bundle configuration reference (full YAML key list)**: [AWS](https://docs.databricks.com/aws/en/dev-tools/bundles/reference) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/reference) | [GCP](https://docs.databricks.com/gcp/en/dev-tools/bundles/reference)
# MAGIC - **Bundle settings (mappings)**: [AWS](https://docs.databricks.com/aws/en/dev-tools/bundles/settings) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/settings) | [GCP](https://docs.databricks.com/gcp/en/dev-tools/bundles/settings)
# MAGIC - **`databricks bundle` CLI commands**: [AWS](https://docs.databricks.com/aws/en/dev-tools/cli/bundle-commands) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/cli/bundle-commands) | [GCP](https://docs.databricks.com/gcp/en/dev-tools/cli/bundle-commands)
# MAGIC - **Variable substitution (`${workspace.…}`, `${bundle.…}`)**: [AWS](https://docs.databricks.com/aws/en/dev-tools/bundles/variables) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/variables) | [GCP](https://docs.databricks.com/gcp/en/dev-tools/bundles/variables)
# MAGIC - **Deployment modes**: [AWS](https://docs.databricks.com/aws/en/dev-tools/bundles/deployment-modes) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/deployment-modes) | [GCP](https://docs.databricks.com/gcp/en/dev-tools/bundles/deployment-modes)
# MAGIC - **Create and manage jobs**: [AWS](https://docs.databricks.com/aws/en/jobs/create-run-jobs) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/jobs/create-run-jobs) | [GCP](https://docs.databricks.com/gcp/en/jobs/create-run-jobs)

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
# MAGIC Recall that your environment was setup using the **02 - REQUIRED - Course Setup and Authentication**.
# MAGIC
# MAGIC If you end your lab or your lab session times out, your environment will be reset. You will need to rerun the **02 - REQUIRED - Course Setup and Authentication** notebook to recreate the catalogs and data for your environment.
# MAGIC
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Classroom Setup
# MAGIC
# MAGIC Run the following cell to configure your working environment for this course.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-06L

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Lab Scenario
# MAGIC
# MAGIC You are responsible for deploying Databricks projects through your organization's CI/CD process using **Declarative Automation Bundles (DABs)**.
# MAGIC
# MAGIC A coworker shared a notebook located in `./src/our_project_code`.
# MAGIC
# MAGIC Your task is to begin the deployment process by configuring and deploying the project to the **development** environment.
# MAGIC
# MAGIC **The provided notebook:**
# MAGIC
# MAGIC - Reads from the development dataset **nyctaxi_raw**
# MAGIC - Uses your **labuser_UNIQUE_ID_1_dev.default** catalog
# MAGIC - Creates a simple bronze and silver table pipeline
# MAGIC - Uses job parameters to define the development catalog
# MAGIC
# MAGIC **To complete this lab, you will:**
# MAGIC
# MAGIC - Retrieve the job's YAML configuration
# MAGIC - Update the **databricks.yml** bundle configuration file
# MAGIC - Deploy the bundle from the **06L - Deploy a Simple DAB** folder

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Preview the Development Data
# MAGIC 1. Preview the **nyctaxi_raw** data in your **labuser_UNIQUE_ID_1_dev** catalog.
# MAGIC
# MAGIC     Notice that the development data contains a small sample of the production data (100 rows).

# COMMAND ----------

spark.sql(f'''
SELECT *
FROM {catalog_dev}.default.nyctaxi_raw
''').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Pre-flight Checks
# MAGIC
# MAGIC Before starting the lab tasks, run a few quick checks to confirm the Databricks CLI is installed, authenticated, and pointed at the right working directory.

# COMMAND ----------

# MAGIC %md
# MAGIC ### D1. Check the Databricks CLI Version
# MAGIC
# MAGIC Run a CLI command to confirm the Databricks CLI version is **v0.298.0**.

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
# MAGIC databricks -v
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
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ### D2. Confirm CLI Authentication
# MAGIC
# MAGIC Run the cell below to confirm the Databricks CLI is authenticated against your workspace. If authentication is broken, the cell will return an error rather than a list of catalogs.

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks catalogs list

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

# COMMAND ----------

# MAGIC %md
# MAGIC ### D3. List the Working Directory
# MAGIC
# MAGIC Use the `ls` command to view the available files in the current directory. 
# MAGIC
# MAGIC   Confirm that you see the **databricks.yml** file, the **src** folder, and this lab notebook.

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
# MAGIC ls
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
# MAGIC </details>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Task 1 - Get the Job's YAML Configuration
# MAGIC
# MAGIC In this task, you'll build the source job in the UI, then export its YAML using **View as code**. 
# MAGIC
# MAGIC This is the same flow used in the previous demo.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1.1 - Get Your Cluster ID
# MAGIC
# MAGIC Run the following cell to obtain your cluster ID for the lab. You will see this in the YAML configuration file.
# MAGIC
# MAGIC **NOTE:** If you select your cluster when creating the job in the UI, the cluster ID will already be present in the generated YAML. The cell below is a fallback so you have it on hand.

# COMMAND ----------

spark.conf.get("spark.databricks.clusterUsageTags.clusterId")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1.2 - Build the Job in the UI
# MAGIC
# MAGIC Manually create the job that the bundle will deploy. The fastest way to get a correct YAML configuration is to build the job in the UI and then export it.
# MAGIC
# MAGIC **Job requirements:**
# MAGIC
# MAGIC a. Name the job `lab06_job_yourfirstname` (replace with your first name).
# MAGIC
# MAGIC b. Add a single notebook task with the following:
# MAGIC
# MAGIC | Configuration | Value |
# MAGIC |---|---|
# MAGIC | **Task Name** | `create_nyc_tables` |
# MAGIC | **Notebook** | `./06L - Deploy a Simple DAB/src/our_project_code` |
# MAGIC | **Compute** | Use your current lab cluster for the job's compute. Selecting the cluster automatically includes the cluster ID in the generated YAML. Using all-purpose compute for jobs is **not a best practice**. This is for training purposes only. |
# MAGIC
# MAGIC Then select **Create task**.
# MAGIC
# MAGIC c. Add the following **Job parameters**. Make sure these are added at the **job level**, not the task level.
# MAGIC
# MAGIC | Parameter | Value |
# MAGIC |---|---|
# MAGIC | `catalog_name` | Reference the catalog shown in **A. Classroom Setup** (your `labuser_UNIQUE_ID_1_dev` catalog) |
# MAGIC | `display_target` | `Development` |
# MAGIC
# MAGIC **Reference:** Create and manage jobs documentation:
# MAGIC [AWS](https://docs.databricks.com/aws/en/jobs/create-run-jobs) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/jobs/create-run-jobs) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/jobs/create-run-jobs)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1.3 - Copy the YAML via 'View as code'
# MAGIC
# MAGIC Once the job is created, click the kebab menu (three vertical dots) near **Run now** and select **View as code**. Choose the **YAML** format and click **Copy**.
# MAGIC
# MAGIC **NOTE:** You can also test the configuration by running the job from the UI before continuing.
# MAGIC
# MAGIC **Reference:** View jobs as code:
# MAGIC [AWS](https://docs.databricks.com/aws/en/jobs/automate#view-jobs-as-code) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/jobs/automate#view-jobs-as-code) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/jobs/automate#view-jobs-as-code)

# COMMAND ----------

# MAGIC %md
# MAGIC ## F. Task 2 - Update the **databricks.yml** File
# MAGIC
# MAGIC Modify the **databricks.yml** configuration file in the **06L - Deploy a Simple DAB** folder.
# MAGIC
# MAGIC ### Step 2.1 - Paste your job under `resources.jobs`
# MAGIC
# MAGIC Add the YAML configuration you copied in Step 1.3 under the `RESOURCES` comment in the **databricks.yml** file.
# MAGIC
# MAGIC ### Step 2.2 - Convert to a relative notebook path
# MAGIC
# MAGIC In the pasted job configuration, modify the `notebook_path` so it uses a relative path and includes the correct file extension.
# MAGIC
# MAGIC - The source file is `our_project_code.ADD_CORRECT_EXTENSION`, so the YAML must reference the file `./src/our_project_code`.
# MAGIC   - Make sure to specify the correct extension.
# MAGIC
# MAGIC ### Step 2.3 - Add the `dev` target
# MAGIC
# MAGIC In the `targets` mapping, add a target named `dev` with the following:
# MAGIC
# MAGIC - `default: true` (so `dev` is the default target if `-t` is omitted)
# MAGIC - `mode: development` (development mode prepends a `[dev <user>]` prefix, pauses schedules, and tags resources with a `dev` tag)
# MAGIC - A `workspace.root_path` that uses variable substitution:
# MAGIC
# MAGIC ```yaml
# MAGIC workspace:
# MAGIC   root_path: /Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}
# MAGIC ```
# MAGIC
# MAGIC **References:**
# MAGIC - Bundle settings (mappings):
# MAGIC [AWS](https://docs.databricks.com/aws/en/dev-tools/bundles/settings) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/settings) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/dev-tools/bundles/settings)
# MAGIC - Variable substitution:
# MAGIC [AWS](https://docs.databricks.com/aws/en/dev-tools/bundles/variables) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/variables) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/dev-tools/bundles/variables)
# MAGIC - Deployment modes:
# MAGIC [AWS](https://docs.databricks.com/aws/en/dev-tools/bundles/deployment-modes) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/deployment-modes) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/dev-tools/bundles/deployment-modes)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #1976d2;
# MAGIC   background: #e3f2fd;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <strong style="display:block; color:#0d47a1; margin-bottom:6px; font-size: 1.1em;">
# MAGIC     HINT
# MAGIC   </strong>
# MAGIC   <div style="color:#333;">
# MAGIC
# MAGIC If you get stuck, an example **databricks.yml** solution is in the accompanying **solution** folder.
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## G. Task 3 - Validate the Bundle
# MAGIC
# MAGIC Validate your **databricks.yml** bundle configuration file using the Databricks CLI. 
# MAGIC
# MAGIC   Run the cell and confirm validation succeeds. If there is an error, fix the **databricks.yml** file and re-run.
# MAGIC
# MAGIC **HINT:** See the `databricks bundle` CLI commands documentation:
# MAGIC [AWS](https://docs.databricks.com/aws/en/dev-tools/cli/bundle-commands) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/cli/bundle-commands) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/dev-tools/cli/bundle-commands)

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
# MAGIC </details>
# MAGIC
# MAGIC

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
# MAGIC     TROUBLESHOOTING Common Validation Issues:
# MAGIC   </strong>
# MAGIC   <div style="color:#333;">
# MAGIC
# MAGIC   - Path or extention of your notebook is incorrect.
# MAGIC   - Incorrect mappings under `target`. 
# MAGIC
# MAGIC   </div>
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## H. Task 4 - Deploy the Bundle to the `dev` Target
# MAGIC
# MAGIC Deploy the bundle to the development target using the Databricks CLI. After the cell completes, navigate to **Jobs & Pipelines** and confirm a job named **[dev <user>] lab06_job_<firstname>** was created.
# MAGIC
# MAGIC Specifically, verify:
# MAGIC
# MAGIC - The task references the correct notebook (`./src/our_project_code.ipynb`).
# MAGIC - The job parameters reference your `labuser_UNIQUE_ID_1_dev` catalog.
# MAGIC
# MAGIC **NOTE:** Deployment will take about a minute to complete.
# MAGIC
# MAGIC **HINT:** See the `databricks bundle` CLI commands documentation:
# MAGIC [AWS](https://docs.databricks.com/aws/en/dev-tools/cli/bundle-commands) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/cli/bundle-commands) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/dev-tools/cli/bundle-commands)

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
# MAGIC </details>
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## I. Task 5 - Run the Bundle
# MAGIC
# MAGIC Run the deployed job using the Databricks CLI.
# MAGIC
# MAGIC **NOTE:** This will take 1-2 minutes to complete.
# MAGIC
# MAGIC **HINT:** Use the **job key** from the `resources` mapping in your **databricks.yml** file (your name will differ):
# MAGIC
# MAGIC ```
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     lab06_job_yourfirstname:    # <--- This is the job key
# MAGIC       name: lab06_job_yourfirstname
# MAGIC ```
# MAGIC
# MAGIC **Reference:** `databricks bundle` CLI commands:
# MAGIC [AWS](https://docs.databricks.com/aws/en/dev-tools/cli/bundle-commands) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/cli/bundle-commands) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/dev-tools/cli/bundle-commands)

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
# MAGIC ## Instead of labuser123 use your own job key
# MAGIC databricks bundle run -t dev lab06_job_labuser123
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
# MAGIC </details>
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## J. Task 6 - Verify the Bronze Table
# MAGIC
# MAGIC After the job completes, run the cell below to confirm the bronze table was created correctly.
# MAGIC
# MAGIC **The job created two tables in your `labuser_UNIQUE_ID_1_dev` catalog:**
# MAGIC
# MAGIC - **nyctaxi_bronze**
# MAGIC - **nyctaxi_silver**
# MAGIC
# MAGIC The cell below checks the row count of the bronze table.

# COMMAND ----------

check_nyctaxi_bronze_table(user_catalog = catalog_dev, total_count=100)

# COMMAND ----------

# MAGIC %md
# MAGIC ## K. Task 7 - Destroy the Bundle
# MAGIC
# MAGIC You're done with this lab, so destroy the bundle to clean up the deployed job and artifacts.
# MAGIC
# MAGIC **HINT:** See the `databricks bundle` CLI commands documentation:
# MAGIC [AWS](https://docs.databricks.com/aws/en/dev-tools/cli/bundle-commands) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/cli/bundle-commands) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/dev-tools/cli/bundle-commands)

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
# MAGIC databricks bundle destroy --auto-approve
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
# MAGIC </details>
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC
# MAGIC Nice work. In this lab you took a notebook project shared by a coworker and went all the way through the bundle lifecycle:
# MAGIC
# MAGIC 1. Built the source job in the UI and exported its YAML using **View as code**.
# MAGIC 2. Updated the **databricks.yml** with your job under `resources.jobs` and added a `dev` target with `mode: development` and a templated `workspace.root_path`.
# MAGIC 3. Validated, deployed, and ran the bundle with `databricks bundle validate`, `databricks bundle deploy -t dev`, and `databricks bundle run -t dev <job_key>`.
# MAGIC 4. Verified the deployed job actually created the **nyctaxi_bronze** table with the expected row count.
# MAGIC 5. Cleaned up by destroying the bundle with `databricks bundle destroy --auto-approve`.

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>