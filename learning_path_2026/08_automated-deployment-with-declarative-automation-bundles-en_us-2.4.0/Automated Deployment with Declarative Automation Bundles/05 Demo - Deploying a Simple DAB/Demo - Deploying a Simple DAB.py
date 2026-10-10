# Databricks notebook source
# MAGIC %md
# MAGIC ![DB Academy](https://files.training.databricks.com/binder/prod_main/automated-deployment-with-declarative-automation-bundles-en_us-2.4.0/images/20261005T093748Z/Automated Deployment with Declarative Automation Bundles/Includes/images/common/db-academy.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # 05 - Deploying a Simple Declarative Automation Bundle (DAB)
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC In this demonstration, you'll create a simple Databricks job, examine its YAML configuration, and walk through the complete lifecycle of a **Declarative Automation Bundle (DAB)**: validate, deploy, run, modify, redeploy, and destroy. Everything runs from a Databricks notebook for training convenience. The Databricks CLI install and authentication were handled in **02 - REQUIRED - Course Setup and Authentication**.
# MAGIC
# MAGIC **Reference documentation:**
# MAGIC - **What are Databricks Asset Bundles / Declarative Automation Bundles?**: [AWS](https://docs.databricks.com/aws/en/dev-tools/bundles/) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/) | [GCP](https://docs.databricks.com/gcp/en/dev-tools/bundles/)
# MAGIC - **Bundle configuration reference (YAML)**: [AWS](https://docs.databricks.com/aws/en/dev-tools/bundles/reference) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/reference) | [GCP](https://docs.databricks.com/gcp/en/dev-tools/bundles/reference)
# MAGIC - **`databricks bundle` CLI commands**: [AWS](https://docs.databricks.com/aws/en/dev-tools/cli/bundle-commands) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/cli/bundle-commands) | [GCP](https://docs.databricks.com/gcp/en/dev-tools/cli/bundle-commands)
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this demonstration, you will be able to:
# MAGIC
# MAGIC 1. **Explain the purpose** of the **databricks.yml** configuration file in a DAB.
# MAGIC 2. **Identify the key sections** (`bundle`, `resources`, `targets`, `workspace`) in a bundle configuration.
# MAGIC 3. **Generate a YAML job configuration** from an existing job using **View as code**.
# MAGIC 4. **Validate, deploy, and run a job** using `databricks bundle validate`, `databricks bundle deploy`, and `databricks bundle run`.
# MAGIC 5. **Modify and redeploy** a bundle to update an existing job.
# MAGIC 6. **Destroy a bundle** with `databricks bundle destroy` to clean up deployed resources.

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

# MAGIC %run ../Includes/Classroom-Setup-05

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Create and Explore a Simple Job

# COMMAND ----------

# MAGIC %md
# MAGIC ### B1. Create a Lakeflow Job

# COMMAND ----------

# MAGIC %md
# MAGIC 1. During development, it's easier to manually create the job you want to automatically deploy with Declarative Automation Bundles in order to get the necessary YAML configuration for deployment.
# MAGIC
# MAGIC     Run the cell below and confirm that the job was created.
# MAGIC
# MAGIC     **NOTE:** To save time, we will use the Databricks Academy `DAJobConfig` class, which was created using the Databricks SDK to automatically create our job for this demonstration. In a typical development cycle, you would create the job manually.

# COMMAND ----------

# DBTITLE 1,Create Job
job_tasks = [
    {
        'task_name': 'create_bronze_table',
        'notebook_path': '/05 - Deploying a Simple DAB/src/create_bronze_table',
        'depends_on': None
    },
    {
        'task_name': 'create_silver_table',
        'notebook_path': '/05 - Deploying a Simple DAB/src/create_silver_table',
        'depends_on': [{'task_key': 'create_bronze_table'}]
    }
]

myjob = DAJobConfig(job_name=f'demo05_simple_dab_{my_catalog}',
                    job_tasks=job_tasks,
                    job_parameters=[
                      {'name':'display_target', 'default':'development'},
                      {'name':'catalog_name', 'default':catalog_dev}
                    ])

# COMMAND ----------

# MAGIC %md
# MAGIC ### B2. Explore the Job Configurations
# MAGIC Complete the following steps to explore the YAML configuration of the job:

# COMMAND ----------

# MAGIC %md
# MAGIC 1. In the left main navigation bar, right-click on **Jobs and Pipelines** and select **Open in a new tab**.
# MAGIC
# MAGIC 2. Locate your deployed job named **demo05_simple_dab_LABUSER_UNIQUE_ID**.
# MAGIC
# MAGIC 3. Select your job.
# MAGIC
# MAGIC 4. In the right **Job details** pane, scroll to the bottom and find **Job parameters**. Notice that two parameters have been set for this job:
# MAGIC    | Job parameters | Description |
# MAGIC    |---|---|
# MAGIC    | `catalog_name` | References your **labuser_UNIQUE_ID** catalog. |
# MAGIC    | `display_target` | Text value that specifies the environment where the job is running. In this example, we are using `development`. |
# MAGIC
# MAGIC 5. In the top navigation bar, select **Tasks**. Notice that this job has two tasks:
# MAGIC | Task | Description |
# MAGIC |---|---|
# MAGIC | **TASK 1** | - Runs the notebook **create_bronze_table**<br>- Reads from the development CSV file in the **labuser_UNIQUE_ID_dev** catalog<br>- Uses the job parameter `catalog_name`<br>- Creates the table **health_bronze_demo_05** |
# MAGIC | **TASK 2** | - Runs the notebook **create_silver_table**<br>- Reads from the bronze table in the **labuser_UNIQUE_ID_dev** catalog<br>- Uses the job parameter `catalog_name`<br>- Creates the table **health_silver_demo_05**<br>- Depends on **TASK 1** completing successfully |
# MAGIC     
# MAGIC     - Both tasks use **Serverless** compute.
# MAGIC
# MAGIC 6. Leave the job page open and move to the next task.
# MAGIC

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
# MAGIC     Job Validation
# MAGIC   </strong>
# MAGIC   <div style="color:#333;">
# MAGIC
# MAGIC During development it would be beneficial to run and confirm the job works. 
# MAGIC
# MAGIC For the purpose of this demonstration the job has been tested and validated.
# MAGIC
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### B3. View the Job YAML Configuration
# MAGIC
# MAGIC Complete the following steps to view the job configuration as code:
# MAGIC
# MAGIC 1. Go back to your job.
# MAGIC
# MAGIC 2. In the top-right corner of the job page, click the kebab menu (three vertical dots) near the **Run now** button.
# MAGIC
# MAGIC 3. Select **View as code**.
# MAGIC     - **View jobs as code** documentation:
# MAGIC     [AWS](https://docs.databricks.com/aws/en/jobs/automate#view-jobs-as-code) |
# MAGIC     [Azure](https://learn.microsoft.com/en-us/azure/databricks/jobs/automate#view-jobs-as-code) |
# MAGIC     [GCP](https://docs.databricks.com/gcp/en/jobs/automate#view-jobs-as-code)
# MAGIC 4. Notice that Databricks can generate the job configuration in multiple formats.
# MAGIC
# MAGIC | Format | What You Can Do |
# MAGIC |---|---|
# MAGIC | **YAML** | - View the job as YAML configuration<br>- Click **Copy** to paste the configuration directly into Declarative Automation Bundle `.yaml` files<br>- Click **Edit** to modify the job configuration in YAML instead of using the UI |
# MAGIC | **Python** | - Choose between **Databricks SDK** or **Declarative Automation Bundles** format<br>- Click **Copy** to generate reusable Python code<br>- Use the **Databricks SDK** version to create jobs in notebooks or local development environments<br>- Use the **Bundles** version to define jobs in Python-based bundle configurations |
# MAGIC | **JSON** | - Click **Copy** to retrieve the full job configuration in JSON format<br>- Use the JSON with the Databricks CLI, Databricks SDKs, or the Databricks REST API to create, update, or retrieve jobs |
# MAGIC
# MAGIC
# MAGIC 5. Copy the **YAML** configuration.
# MAGIC    
# MAGIC 6. Select **Close**.
# MAGIC
# MAGIC 7. Leave the tab with your job open. We use this copied YAML configuration in a later section.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Checkpoint - Example YAML Configuration (your values will differ slightly)
# MAGIC ```
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     demo05_simple_dab_labuser123:
# MAGIC       name: demo05_simple_dab_labuser123
# MAGIC       tasks:
# MAGIC         - task_key: create_bronze_table
# MAGIC           notebook_task:
# MAGIC             notebook_path: /Workspace/Users/your_user/automated-deployment-with-declarative-automation-bundles-source/labs/Source/en_us/notebooks/Automated
# MAGIC               Deployment with Declarative Automation Bundles/05 - Deploying a Simple DAB/src/create_bronze_table
# MAGIC             source: WORKSPACE
# MAGIC         - task_key: create_silver_table
# MAGIC           depends_on:
# MAGIC             - task_key: create_bronze_table
# MAGIC           notebook_task:
# MAGIC             notebook_path: /Workspace/Users/your_user/automated-deployment-with-declarative-automation-bundles-source/labs/Source/en_us/notebooks/Automated
# MAGIC               Deployment with Declarative Automation Bundles/05 - Deploying a Simple DAB/src/create_silver_table
# MAGIC             source: WORKSPACE
# MAGIC       parameters:
# MAGIC         - name: display_target
# MAGIC           default: development
# MAGIC         - name: catalog_name
# MAGIC           default: labuser123_1_dev
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Deploying your Job Using Declarative Automation Bundles (DABs)

# COMMAND ----------

# MAGIC %md
# MAGIC ### C1. Run Databricks CLI Commands
# MAGIC
# MAGIC 1. Run the `databricks -v` command to view the version of the Databricks CLI. 
# MAGIC
# MAGIC     Confirm that the cell returns version **v0.298.0**.

# COMMAND ----------

# DBTITLE 1,CLI Version
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

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Use the `pwd` command to view the current working directory. 
# MAGIC
# MAGIC     It should display that you are in the folder **05 - Deploying a Simple DAB**. 
# MAGIC       - The CLI is using the current directory of this notebook.

# COMMAND ----------

# DBTITLE 1,Current Path
# MAGIC %sh
# MAGIC pwd

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Use the `ls` command to view the available files in the current directory. 
# MAGIC
# MAGIC     Confirm that you see the **databricks.yml** file.

# COMMAND ----------

# DBTITLE 1,List Files
# MAGIC %sh
# MAGIC ls

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
# MAGIC     Notes
# MAGIC   </strong>
# MAGIC   <div style="color:#333;">
# MAGIC
# MAGIC A bundle must contain one (and only one) configuration file named **databricks.yml** at the root of the bundle project folder.
# MAGIC
# MAGIC The **databricks.yml** file is the main configuration file that defines a bundle, but it can reference other configuration files, such as resource configuration files, in the include mapping.
# MAGIC
# MAGIC A bundle configuration file must be in YAML format and must contain at least the top-level `bundle` mapping.
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### C2. Explore the Simple **databricks.yml** Bundle Configuration File
# MAGIC
# MAGIC Now that we have confirmed we are in the working directory of the **databricks.yml** file, let's open the bundle configuration file in a new tab and explore the bundle configuration.
# MAGIC
# MAGIC For the full list of bundle configuration keys, view the **Configuration reference** documentation:
# MAGIC [AWS](https://docs.databricks.com/aws/en/dev-tools/bundles/reference) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/reference) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/dev-tools/bundles/reference)

# COMMAND ----------

# MAGIC %md
# MAGIC 1. In the left workspace navigation, select the folder icon and confirm you are in the **05 - Deploying a Simple DAB** folder.
# MAGIC
# MAGIC 2. Right-click on the **databricks.yml** file and select *Open in a new tab*.
# MAGIC
# MAGIC 3. In the **databricks.yml** file, a configuration must contain only one top-level `bundle` mapping.
# MAGIC     - This `bundle` mapping must contain a `name` mapping that specifies a programmatic (or logical) name for the bundle.
# MAGIC         ```
# MAGIC         bundle:                   # Required
# MAGIC           name: demo05_bundle     # Required
# MAGIC         ```
# MAGIC
# MAGIC 4. The `resources` mapping (notice this is blank in the YAML) specifies:
# MAGIC     - Information about the Databricks resources used by the bundle.
# MAGIC     - This bundle configuration defines a job resource. We will add our specific job in the next section and review the configuration.
# MAGIC
# MAGIC 5. The `targets` mapping specifies:
# MAGIC     - One or more target environments in which to run a Databricks workflow.
# MAGIC     - Each target is a unique collection of artifacts, Databricks workspace settings, and Databricks job or pipeline details.
# MAGIC     - In this example, we have one target named `development` and it uses a simple configuration.
# MAGIC
# MAGIC 6. The `mode: development` mapping:
# MAGIC     - Defines this target as `development` mode.
# MAGIC     - Development mode implements a variety of behaviors. For example:
# MAGIC         - Prepends all resources that are not deployed as files or notebooks with the prefix **[dev ${workspace.current_user.short_name}]**
# MAGIC         - Tags each deployed job and pipeline with a `dev` Databricks tag.
# MAGIC     - For more behaviors, view the **Development mode** documentation:
# MAGIC     [AWS](https://docs.databricks.com/aws/en/dev-tools/bundles/deployment-modes#development-mode) |
# MAGIC     [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/deployment-modes#development-mode) |
# MAGIC     [GCP](https://docs.databricks.com/gcp/en/dev-tools/bundles/deployment-modes#development-mode)
# MAGIC
# MAGIC 7. The `default: true` mapping specifies that:
# MAGIC     - This is the default target environment if multiple targets are available.
# MAGIC     - Setting the default to the **development** target helps avoid accidentally deploying to a production environment.
# MAGIC
# MAGIC 8. In the `workspace` mapping the following are specified:
# MAGIC     - `host` specifies the workspace to run this in. By default it will use the current workspace. We will leave this commented out.
# MAGIC     - `root_path` specifies where the files will be deployed.

# COMMAND ----------

# MAGIC %md
# MAGIC ### C3. Add Our Job Configuration to the **databricks.yml** File
# MAGIC
# MAGIC After examining the bundle configuration in the **databricks.yml** file, let's go back to our job and copy the YAML configuration (if necessary).

# COMMAND ----------

# MAGIC %md
# MAGIC 1. In your **databricks.yml** file paste your job YAML configuration in the **resources** mapping with your specific job YAML configuration (under the RESOURCES comment).

# COMMAND ----------

# MAGIC %md
# MAGIC After pasting your specific job configuration to your **databricks.yml** file, let's modify some of the paths to make them relative paths, add the notebook extensions, and give it an easy job key name.
# MAGIC
# MAGIC 2. Under `resources` > `jobs` you will see a key named `demo05_simple_dab_username`.
# MAGIC       - Replace that key with `demo05_simple_dab`.
# MAGIC
# MAGIC    ```
# MAGIC    resources:
# MAGIC       jobs:
# MAGIC         demo05_simple_dab_labuser1234:    ## <--------MODIFY THIS VALUE HERE TO demo05_simple_dab
# MAGIC           name: demo05_simple_dab_labuser1234
# MAGIC    ```
# MAGIC
# MAGIC 3. For `task_key: create_bronze_table`:
# MAGIC       - Modify the `notebook_path` to: `./src/create_bronze_table.ipynb`.
# MAGIC
# MAGIC 4. For `task_key: create_silver_table`:
# MAGIC       - Modify the `notebook_path` to: `./src/create_silver_table.ipynb`.
# MAGIC
# MAGIC 5. Close the **databricks.yml** file.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Checkpoint - Example YAML Configuration (your values will differ slightly)
# MAGIC ```
# MAGIC ...
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     demo05_simple_dab:
# MAGIC       name: demo05_simple_dab_labuser1234
# MAGIC       tasks:
# MAGIC         - task_key: create_bronze_table
# MAGIC           notebook_task:
# MAGIC             notebook_path: ./src/create_bronze_table.ipynb
# MAGIC             source: WORKSPACE
# MAGIC         - task_key: create_silver_table
# MAGIC           depends_on:
# MAGIC             - task_key: create_bronze_table
# MAGIC           notebook_task:
# MAGIC             notebook_path: ./src/create_silver_table.ipynb
# MAGIC             source: WORKSPACE
# MAGIC       parameters:
# MAGIC         - name: display_target
# MAGIC           default: development
# MAGIC         - name: catalog_name
# MAGIC           default: labuser1234_1_dev
# MAGIC ...
# MAGIC ```

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
# MAGIC     Notebook Format Information
# MAGIC   </strong>
# MAGIC   <div style="color:#333;">
# MAGIC
# MAGIC Notebooks can be in a variety of extensions: `.ipynb`, `.sql`, `.py`. Always confirm the extension.
# MAGIC
# MAGIC - In the top navigation bar, below the notebook name, select **File**.
# MAGIC
# MAGIC - Scroll down and find the **Notebook format** option, then select it.
# MAGIC
# MAGIC - Here, you should see the notebook format listed as **Source (.ipynb, .py, .sql, etc)**.
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### C4. Validate Your Bundle
# MAGIC Let's validate our **databricks.yml** bundle configuration file using the Databricks CLI.

# COMMAND ----------

# MAGIC %md
# MAGIC 1.  Run the cell and confirm the validation of the bundle was successful.

# COMMAND ----------

# DBTITLE 1,Validate Bundle
# MAGIC %sh
# MAGIC databricks bundle validate

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
# MAGIC `Error: notebook src/create_bronze_table.ipynb not found`. 
# MAGIC
# MAGIC Check the format of your notebook and adjust accordingly. 
# MAGIC
# MAGIC   </div>
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Deploy the bundle using the Databricks CLI. Run the command below to deploy the bundle. 
# MAGIC
# MAGIC     - The command `databricks bundle deploy -t development` specifies to deploy the bundle to the `development` environment. 
# MAGIC     - By default if we did not specify the target environment it would use the default target we specified earlier (development).
# MAGIC
# MAGIC     **NOTE:** This will take about a minute to complete.

# COMMAND ----------

# DBTITLE 1,Deploy Bundle
# MAGIC %sh
# MAGIC databricks bundle deploy -t development

# COMMAND ----------

# MAGIC %md
# MAGIC ### C5. View the Deployed Job
# MAGIC
# MAGIC 1. Let's view where the Databricks assets were deployed.
# MAGIC
# MAGIC     a. In the main navigation bar, right-click on **Workspace** and select **Open in a New Tab**.
# MAGIC
# MAGIC     b. Navigate to **Workspace > Users > your user name** > **.bundle** folder.
# MAGIC
# MAGIC     c. Open **demo05_bundle** (the bundle name we specified in **databricks.yml**).
# MAGIC
# MAGIC     d. Here, we can see that we deployed the **development** target. 
# MAGIC
# MAGIC     - Within the **development** folder, there will be a variety of folders and files.
# MAGIC
# MAGIC     e. Close the Workspace tab.

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
# MAGIC     Information
# MAGIC   </strong>
# MAGIC   <div style="color:#333;">
# MAGIC
# MAGIC Because you deployed the bundle **from within the Databricks workspace**, the deployment used a **source-linked deployment** for the **development** target.
# MAGIC
# MAGIC In a source-linked deployment:
# MAGIC   - The source files are **not copied** into the target deployment folder
# MAGIC   - The deployment references the existing workspace files directly
# MAGIC
# MAGIC You can confirm this by checking the following folder. It will be empty:
# MAGIC
# MAGIC ```text
# MAGIC .bundle/demo05_bundle/development/files
# MAGIC ```
# MAGIC   </div>
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Complete the following steps to explore the job we deployed with a DAB.
# MAGIC
# MAGIC     a. In the left main navigation bar, right-click on **Jobs & Pipelines** and select *Open in a new tab*.
# MAGIC
# MAGIC     b. Find your deployed job named **[dev username] demo05_simple_dab**.
# MAGIC
# MAGIC       - By default, development mode prepends all resources that are not deployed as files or notebooks with the prefix `[dev ${workspace.current_user.short_name}]` and tags each deployed job and pipeline with a `dev` Databricks tag.
# MAGIC
# MAGIC       - For other **Development mode** behaviors, view the documentation:
# MAGIC       [AWS](https://docs.databricks.com/aws/en/dev-tools/bundles/deployment-modes#development-mode) |
# MAGIC       [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/deployment-modes#development-mode) |
# MAGIC       [GCP](https://docs.databricks.com/gcp/en/dev-tools/bundles/deployment-modes#development-mode)
# MAGIC
# MAGIC     c. Select the job.
# MAGIC
# MAGIC     d. Notice the note at the top of the job: **Connected to Declarative Automation Bundles**.
# MAGIC     
# MAGIC     e. Select the link **Learn more** and read the note.
# MAGIC
# MAGIC     f. In the right navigation pane, scroll down to **Job parameters**. Notice the values of the job parameters:
# MAGIC
# MAGIC       - `catalog_name` - your dev catalog
# MAGIC       - `display_target` - the value `development`
# MAGIC
# MAGIC     g. Leave the job tab open.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### C6. Run the Job Using the Databricks CLI
# MAGIC
# MAGIC 1. Run the cell below to run the job from the **databricks.yml** file using the CLI command and confirm the job runs successfully.
# MAGIC
# MAGIC       - `databricks bundle run -t development demo05_simple_dab` specifies to run this job in the development environment.
# MAGIC
# MAGIC       - This job key can be found under the **resources** mapping in the **databricks.yml** file.
# MAGIC
# MAGIC **Example (your actual job `name` will differ)**:
# MAGIC ```
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     demo05_simple_dab:    # <--- The job key. Your job key should be: demo05_simple_dab
# MAGIC       name: demo05_simple_dab_labuser1234   # <--- The job name (auto-generated, will differ)
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Run Job
# MAGIC %sh
# MAGIC databricks bundle run -t development demo05_simple_dab

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
# MAGIC     Troubleshooting
# MAGIC   </strong>
# MAGIC   <div style="color:#333;">
# MAGIC   
# MAGIC If the bundle run returns the following error: `Error: resource with key "demo05_simple_dab" not found`.
# MAGIC
# MAGIC That means you did not modify the job key correctly. View your `resources` mapping and confirm the job key is `demo05_simple_dab`.
# MAGIC
# MAGIC ```yaml
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     demo05_simple_dab:   # <--- This job key here
# MAGIC       name: demo05_simple_dab_labuser1234
# MAGIC       tasks:
# MAGIC ```
# MAGIC   </div>
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 2. After the job was successfully run, navigate back to the job tab. 
# MAGIC
# MAGIC     Notice that the cell above automatically ran the specified job using our development catalog that we specified within the job parameters in the **databricks.yml** file.
# MAGIC
# MAGIC ![Job Run 1](https://files.training.databricks.com/binder/prod_main/automated-deployment-with-declarative-automation-bundles-en_us-2.4.0/images/20261005T093748Z/Automated Deployment with Declarative Automation Bundles/Includes/images/simple-dab/job-run-1.png)

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Run the cell below and confirm the following tables were created from the job in our **labuser_UNIQUE_ID_1_dev** catalog:
# MAGIC     - **health_bronze_demo_05**
# MAGIC     - **health_silver_demo_05**

# COMMAND ----------

# DBTITLE 1,View New Tables
tables = spark.sql(f'''
SHOW TABLES IN {catalog_dev}.default
''')

tables.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### C7. Modify the **databricks.yml** and Redeploy the Job
# MAGIC
# MAGIC 1. Let's make a change to our bundle configuration in the **databricks.yml** file.
# MAGIC
# MAGIC     a. (If not already opened) Right click on the **databricks.yml** file and select *Open in a new tab*.
# MAGIC
# MAGIC     b. In the **resources** mapping modify the following:
# MAGIC
# MAGIC     - The default value of the job parameter `display_target` to `development_updating_the_value_test`.
# MAGIC
# MAGIC     c. Run the cell below to validate and deploy the new bundle.
# MAGIC
# MAGIC     - Wait until the cell completes (about 1 minute).

# COMMAND ----------

# DBTITLE 1,Validate and Deploy Bundle
# MAGIC %sh
# MAGIC databricks bundle validate
# MAGIC databricks bundle deploy -t development

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
# MAGIC     Information - Must Redeploy if you update the databricks.yml configuration file
# MAGIC   </strong>
# MAGIC   <div style="color:#333;">
# MAGIC
# MAGIC If you make a change to your configuration file you will have to redeploy the bundle. 
# MAGIC
# MAGIC After you modify the **databricks.yml** file wait about 30 seconds for the auto save to save the file before redeploying.
# MAGIC
# MAGIC   </div>
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 2. After the deployment completes, view the new deployed job by:
# MAGIC     - Navigating back to your job
# MAGIC     - Then view the **Job parameters** (if the page is already open, refresh the page). 
# MAGIC
# MAGIC     Notice that the default value for the **display_target** parameter has been updated based on the change we made in the **databricks.yml** file.
# MAGIC
# MAGIC ![Job Run 2](https://files.training.databricks.com/binder/prod_main/automated-deployment-with-declarative-automation-bundles-en_us-2.4.0/images/20261005T093748Z/Automated Deployment with Declarative Automation Bundles/Includes/images/simple-dab/job-run-2.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Destroy the Deployed Job
# MAGIC
# MAGIC 1. Lastly since we are finished with this bundle, let's delete it using the `databricks bundle destroy` command.
# MAGIC
# MAGIC     By default, you are prompted to confirm permanent deletion of the previously-deployed jobs, pipelines, and artifacts. To skip these prompts and perform automatic permanent deletion, add the `--auto-approve` option to the bundle destroy command.

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks bundle destroy --auto-approve

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="
# MAGIC   border-left: 4px solid #f44336;
# MAGIC   background: #ffebee;
# MAGIC   padding: 14px 18px;
# MAGIC   border-radius: 4px;
# MAGIC   margin: 16px 0;
# MAGIC ">
# MAGIC   <strong style="display:block; color:#c62828; margin-bottom:6px; font-size: 1.1em;">Destroy Warning</strong>
# MAGIC   <div style="color:#333;">
# MAGIC
# MAGIC Destroying a bundle permanently deletes a bundle's previously-deployed jobs, pipelines, and artifacts. This action cannot be undone.
# MAGIC
# MAGIC For more information, view the **Destroy the bundle** documentation:
# MAGIC [AWS](https://docs.databricks.com/aws/en/dev-tools/bundles/work-tasks#step-6-destroy-the-bundle) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/work-tasks#step-6-destroy-the-bundle) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/dev-tools/bundles/work-tasks#step-6-destroy-the-bundle)
# MAGIC
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC
# MAGIC In this demonstration, you walked through the complete lifecycle of a Declarative Automation Bundle (DAB) for a simple Databricks job:
# MAGIC
# MAGIC 1. Generated a YAML job configuration from an existing job using **View as code**.
# MAGIC 2. Pasted that configuration into the bundle's **databricks.yml** under the `resources.jobs` mapping.
# MAGIC 3. Validated the bundle with `databricks bundle validate`.
# MAGIC 4. Deployed the bundle to the `development` target with `databricks bundle deploy -t development`.
# MAGIC 5. Ran the deployed job with `databricks bundle run -t development demo05_simple_dab`.
# MAGIC 6. Modified a job parameter, redeployed, and verified the change in the UI.
# MAGIC 7. Cleaned up by destroying the bundle with `databricks bundle destroy --auto-approve`.

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>