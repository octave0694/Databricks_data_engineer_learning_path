# Databricks notebook source
# MAGIC %md
# MAGIC ![DB Academy](https://files.training.databricks.com/binder/prod_main/automated-deployment-with-declarative-automation-bundles-en_us-2.4.0/images/20261005T093748Z/Automated Deployment with Declarative Automation Bundles/Includes/images/common/db-academy.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # 08 - Deploying a Declarative Automation Bundle (DAB) to Multiple Environments
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC In this demonstration, you'll deploy the same job to two environments (`development` and `production`) from a single bundle. You'll work with **bundle variables**, **lookup variables**, the **`include`** mapping for modularizing resource files, and the **`targets`** mapping to override per-environment settings (catalog, compute, mode).
# MAGIC
# MAGIC The example builds on the simple bundle from the previous demonstration. The key new ideas here are: defining variables once and reusing them, splitting resources into their own YAML files, and overriding values at the target level so dev and prod can differ without duplicating job definitions.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this demonstration, you will be able to:
# MAGIC
# MAGIC 1. **Define and reference bundle variables** in `databricks.yml`, including a **lookup** variable that resolves a cluster name to a cluster ID.
# MAGIC 2. **Modularize resources** by moving a job definition into its own YAML file under `resources/` and referencing it via the `include` mapping.
# MAGIC 3. **Override values per target** by setting `mode`, `variables`, and task-level overrides differently for `development` vs `production`.
# MAGIC 4. **Inspect a fully-resolved bundle** with `databricks bundle validate --output json` to see what will actually be deployed.
# MAGIC 5. **Deploy, run, and destroy** the same bundle against two targets using `databricks bundle deploy -t <target>`, `databricks bundle run -t <target> <job_key>`, and `databricks bundle destroy --auto-approve`.

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
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Classroom Setup
# MAGIC
# MAGIC Run the following cell to configure your working environment for this course.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-08

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Run the Databricks CLI command below to confirm the Databricks CLI is authenticated.

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
# MAGIC 2. Run the `databricks -v` command to view the version of the Databricks CLI. 
# MAGIC
# MAGIC     Confirm that the cell returns version **v0.298.0**.

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks -v

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Explore the Development and Production Data

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Preview the development data in your **labuser_UNIQUE_ID_1_dev** catalog. Note the following:
# MAGIC    - It contains 7,500 rows (excluding the header column).
# MAGIC    - The PII data is masked.
# MAGIC
# MAGIC    **NOTE:** In this scenario, the sample data in the development environment is a subset of production data used for testing. We will test against this dataset later.

# COMMAND ----------

spark.sql(f'''
SELECT *
FROM text.`/Volumes/{catalog_dev}/default/health`
''').display()

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Preview the production data in your **labuser_UNIQUE_ID_3_prod** catalog. Note the following:
# MAGIC    - It contains 70,695 rows.
# MAGIC    - The PII data is available.
# MAGIC
# MAGIC    **NOTE:** In our scenario, a CSV file is added to the **health** volume in the prod catalog daily. If you inspect the **health** volume in the production catalog, you will find 3 days already populated.

# COMMAND ----------

spark.sql(f'''
SELECT count(*) AS Total
FROM text.`/Volumes/{catalog_prod}/default/health`
''').display()

# COMMAND ----------

spark.sql(f'''
SELECT *
FROM text.`/Volumes/{catalog_prod}/default/health`
''').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Deploy a DAB to Multiple Environments (Development and Production)
# MAGIC
# MAGIC In this example, we will be using the same job from the **05 - Deploying a Simple DAB** demonstration. Here are the desired configurations of each environment (catalog):
# MAGIC
# MAGIC #### Development target configuration requirements:
# MAGIC - Use the value **labuser_UNIQUE_ID_1_dev** for the development catalog to read and write to.
# MAGIC - Run the job using the **small lab cluster** since the development data is small and static.
# MAGIC - Make the development environment the **default** environment.
# MAGIC
# MAGIC #### Production target configuration requirements:
# MAGIC - Use the value **labuser_UNIQUE_ID_3_prod** for the production catalog to access the production data.
# MAGIC - Run the job using serverless compute since the data will continually grow, letting Databricks Serverless adjust to the compute needs.
# MAGIC
# MAGIC
# MAGIC **Deployment modes (`development` / `production`)**: [AWS](https://docs.databricks.com/aws/en/dev-tools/bundles/deployment-modes) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/deployment-modes) | [GCP](https://docs.databricks.com/gcp/en/dev-tools/bundles/deployment-modes)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### C1. Explore the `resources` Job YAML File
# MAGIC 1. Open the **./resources/demo_08_job.job.yml** file in a new tab and explore the job configuration.
# MAGIC
# MAGIC    a. The job key name is `demo_08_job`.
# MAGIC
# MAGIC    b. The job name template is `${bundle.target}_demo08_dab_${workspace.current_user.userName}`. Substitution at deploy time gives you a name that includes the target environment and user.
# MAGIC
# MAGIC    c. The job uses the `../src/create_bronze_table.ipynb` and `../src/create_silver_table.ipynb` notebooks.
# MAGIC    
# MAGIC    d. In the YAML file, scroll down and notice that the parameters for this job use variables:
# MAGIC    ```yaml
# MAGIC       parameters:
# MAGIC       - name: display_target
# MAGIC         default: ${bundle.target}
# MAGIC       - name: catalog_name
# MAGIC         default: ${var.target_catalog}
# MAGIC     ```
# MAGIC
# MAGIC    e. No cluster is specified, so this job runs on Serverless compute by default.
# MAGIC    
# MAGIC    f. Leave this tab open.

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Run the following code to obtain your lab user name. You will need this for the next section.

# COMMAND ----------

print(my_catalog)

# COMMAND ----------

# MAGIC %md
# MAGIC ### C2. Explore and Modify the `databricks.yml` File

# COMMAND ----------

# MAGIC %md
# MAGIC 1. In your other tab, navigate to the **./databricks.yml** file located in the main demonstration folder and explore the bundle. Notice the following:
# MAGIC
# MAGIC    a. This bundle is named `demo_08_bundle`.
# MAGIC
# MAGIC    b. This bundle contains an `include` top-level mapping:
# MAGIC     - This specifies the path to the **./resources/demo_08_job.job.yml** file.
# MAGIC     - That YAML file defines the job that will be merged into the `resources` mapping at deploy time, as you saw in the previous step.
# MAGIC     - **NOTE:** As your project grows, it's a best practice to modularize the resources of the DAB into per-resource YAML files.
# MAGIC
# MAGIC    c. This DAB also contains a `variables` top-level mapping. Let's review the defined variables:
# MAGIC
# MAGIC       - The `my_lab_user_name` variable uses the substitution `${workspace.current_user.short_name}`. This obtains your username for the lab and propagates the correct value to the remaining variables for each catalog.
# MAGIC
# MAGIC       - The `catalog_dev` variable uses the `my_lab_user_name` variable and appends `_1_dev` to your user name to reference your development catalog.
# MAGIC
# MAGIC       - The `catalog_prod` variable uses the `my_lab_user_name` variable and appends `_3_prod` to your user name to reference your production catalog.
# MAGIC
# MAGIC       - The `target_catalog` variable defaults to your dev catalog.
# MAGIC         - This variable is referenced in the job parameters defined in **./resources/demo_08_job.job.yml**:
# MAGIC
# MAGIC       </br>
# MAGIC     
# MAGIC     ```yaml
# MAGIC       parameters:
# MAGIC       - name: display_target
# MAGIC         default: ${bundle.target}
# MAGIC       - name: catalog_name
# MAGIC         default: ${var.target_catalog}
# MAGIC     ```
# MAGIC    </br>
# MAGIC    
# MAGIC       - The `raw_data_path` variable references the **health** volume using `target_catalog`, which by default points at the **health** volume in your dev catalog.
# MAGIC
# MAGIC **Variables and substitutions (`${var.…}`, `${bundle.…}`, lookups)**: [AWS](https://docs.databricks.com/aws/en/dev-tools/bundles/variables) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/variables) | [GCP](https://docs.databricks.com/gcp/en/dev-tools/bundles/variables)

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
# MAGIC     TO DO - Set the cluster lookup in <strong>databricks.yml</strong>
# MAGIC   </strong>
# MAGIC   <div style="color:#333;">
# MAGIC
# MAGIC The `cluster_id` variable uses a **lookup** to resolve a cluster name into a cluster ID at deploy time. Find the variable in **databricks.yml** and update the `cluster:` value with **your** lab cluster's name.
# MAGIC
# MAGIC ```yaml
# MAGIC variables:
# MAGIC   cluster_id:
# MAGIC     description: Look up your lab cluster's ID by name.
# MAGIC     lookup:
# MAGIC       cluster: <your-cluster-name>     # <-- paste your cluster name here
# MAGIC ```
# MAGIC
# MAGIC In the Databricks Academy lab, your cluster name matches the value printed by the cell above (your lab username).
# MAGIC
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC Leave the tab with your **databricks.yml** file open.

# COMMAND ----------

# MAGIC %md
# MAGIC 2. In the **databricks.yml** file, explore the `targets` first-level mapping. Notice the following:
# MAGIC
# MAGIC    a. When deploying to the `development` target:
# MAGIC    
# MAGIC       - It is set to `mode: development`.
# MAGIC       
# MAGIC       - It is the **default** target.
# MAGIC       
# MAGIC       - The `root_path` where files are placed ends with the target name, `development`.
# MAGIC       
# MAGIC       - Compute is overridden for each task in the `resources` mapping. The lookup variable `my_cluster_id` (defined earlier) supplies the small lab cluster's ID. We do this because development data is small and doesn't need large compute.
# MAGIC
# MAGIC    b. When deploying to the `production` target:
# MAGIC    
# MAGIC       - It is set to `mode: production`.
# MAGIC       
# MAGIC       - The `target_catalog` variable is overridden from the default `${var.catalog_dev}` to `${var.catalog_prod}`. This makes the deployed job read from and write to the production catalog.
# MAGIC       
# MAGIC       - The `root_path` where files are placed ends with the target name, `production`.
# MAGIC
# MAGIC       - The job runs on **Serverless** because we are not overriding the compute defined in the resource YAML.

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
# MAGIC - If available, you could specify the `host` and choose which Databricks workspace to deploy to. In this lab we have only one workspace, so we isolate environments by **catalog**.
# MAGIC   
# MAGIC - This example overrides only a few configurations for the `production` target. Many other settings can be overridden, see the **Bundle settings** documentation:
# MAGIC [AWS](https://docs.databricks.com/aws/en/dev-tools/bundles/settings) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/settings) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/dev-tools/bundles/settings)
# MAGIC
# MAGIC   </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Validate the bundle for this demonstration and confirm it validates correctly.
# MAGIC
# MAGIC     **NOTE:** If the bundle does not validate, read the error and fix the issue. Common causes:
# MAGIC
# MAGIC     - Missing file extensions on the notebook paths within the **demo_08_job.job.yml** file.
# MAGIC
# MAGIC     - Variables not defined or referenced correctly in the **databricks.yml** file.

# COMMAND ----------

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
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 4. You can run `databricks bundle validate --output json` to view the fully-resolved bundle configuration in JSON. This is useful for confirming variable substitutions resolved as expected.
# MAGIC
# MAGIC Some commonly used substitutions:
# MAGIC
# MAGIC - `${bundle.name}`
# MAGIC
# MAGIC - `${bundle.target}`  (preferred over the deprecated `${bundle.environment}`)
# MAGIC
# MAGIC - `${workspace.host}`
# MAGIC
# MAGIC - `${workspace.current_user.short_name}`
# MAGIC
# MAGIC - `${workspace.current_user.userName}`
# MAGIC
# MAGIC - `${workspace.file_path}`
# MAGIC
# MAGIC - `${workspace.root_path}`
# MAGIC
# MAGIC - `${resources.jobs.<job-name>.id}`
# MAGIC
# MAGIC - `${resources.models.<model-name>.name}`
# MAGIC
# MAGIC - `${resources.pipelines.<pipeline-name>.name}`
# MAGIC
# MAGIC For example, in the JSON output below:
# MAGIC
# MAGIC - `bundle.target` resolves to `development`. The YAML uses `${bundle.target}` to reference this.
# MAGIC - Look for `workspace` > `current_user` > `short_name`. This returns your lab user name.

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks bundle validate --output json

# COMMAND ----------

# MAGIC %md
# MAGIC ### C3. Deploy to the Development Environment

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Delete the tables **health_bronze_demo_08** and **health_silver_demo_08** if they exist in our development catalog so we can verify that our bundle creates them on deploy.
# MAGIC
# MAGIC     Run the code and confirm the tables are not in your **_1_dev** catalog. The output below will show any tables that exist in the **default** schema other than **health_bronze_demo_08** and **health_silver_demo_08**.

# COMMAND ----------

del_table(catalog_dev, 'default', 'health_bronze_demo_08')
del_table(catalog_dev, 'default', 'health_silver_demo_08')

spark.sql(f'''SHOW TABLES IN {catalog_dev}.default''').display()

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Let's deploy the bundle to the **development** environment using the specific configurations.
# MAGIC
# MAGIC     **NOTE:** If you do not specify `-t development`, it will deploy to this environment by default since the configuration `default: True` is used for the development target in the **databricks.yml** file. However, it's better to be explicit.

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks bundle deploy -t development

# COMMAND ----------

# MAGIC %md
# MAGIC 3. When the cell above completes (in about a minute), view the deployed job named `[dev username] development_demo08_dab_<username>`.
# MAGIC
# MAGIC     In the job, check the following:
# MAGIC
# MAGIC     - Select the job tasks and confirm each task uses the lab compute cluster specified in the configuration.
# MAGIC
# MAGIC     - Find the **Job parameters** section in the right details pane. Note the values:
# MAGIC
# MAGIC         **Job parameters**
# MAGIC         - `catalog_name` - your `labuser_UNIQUE_ID_1_dev` catalog
# MAGIC         - `display_target` - `development`
# MAGIC
# MAGIC     Recall that we deployed the job in `development` mode, and it uses the default variable values defined in **databricks.yml** to read from and write to your development catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Checkpoint
# MAGIC ![Dev](https://files.training.databricks.com/binder/prod_main/automated-deployment-with-declarative-automation-bundles-en_us-2.4.0/images/20261005T093748Z/Automated Deployment with Declarative Automation Bundles/Includes/images/multiple-env-demo/dev-deployment.png)

# COMMAND ----------

# MAGIC %md
# MAGIC 4. Run the job in the development environment.
# MAGIC
# MAGIC     **NOTE:** While the job is running, let's take a moment to address any specific questions.
# MAGIC

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks bundle run -t development demo_08_job

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
# MAGIC     Running Using the Job Key
# MAGIC   </strong>
# MAGIC   <div style="color:#333;">
# MAGIC
# MAGIC When running a job from the command line, you will need to pass the job key from the job YAML file. 
# MAGIC
# MAGIC For example, in our scenario, we have the following in our job YAML file:
# MAGIC
# MAGIC
# MAGIC ```YAML
# MAGIC   resources:
# MAGIC     jobs:
# MAGIC       demo_08_job:    #<---- Job key
# MAGIC         name: ${bundle.target}_demo08_dab_${workspace.current_user.userName}
# MAGIC         ...
# MAGIC ```
# MAGIC
# MAGIC So, we will run `databricks bundle run -t development demo_08_job`.
# MAGIC
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 5. Run the following cell to view the available tables in the development catalog after the job completes (in about 2 minutes).
# MAGIC
# MAGIC     Notice that the two new tables were created:
# MAGIC
# MAGIC     - **health_bronze_demo_08**
# MAGIC     - **health_silver_demo_08**

# COMMAND ----------

spark.sql(f'SHOW TABLES IN {catalog_dev}.default').display()

# COMMAND ----------

# MAGIC %md
# MAGIC 6. Count the number of rows in the **health_bronze_demo_08** table in the **user_name_1_dev** catalog. 
# MAGIC
# MAGIC     Notice that it contains 7,500 rows, as we are using the development data.
# MAGIC
# MAGIC     This confirms that our job correctly read from and wrote to the **development** catalog.

# COMMAND ----------

spark.sql(f'''
    SELECT count(*) 
    FROM {catalog_dev}.default.health_bronze_demo_08''').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### C4. Deploy to the Production Environment
# MAGIC Now that we've confirmed the job ran in the development environment, let's deploy the same job to the production environment.
# MAGIC
# MAGIC **NOTE:** In real production, you typically run the job using a service principal. 
# MAGIC   - See the **Set a bundle run identity** documentation:
# MAGIC [AWS](https://docs.databricks.com/aws/en/dev-tools/bundles/run-as) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/run-as) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/dev-tools/bundles/run-as)
# MAGIC
# MAGIC For demonstration purposes, we are simply running the production job as the user.

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Before we deploy to production let's check (and delete if necessary) the tables in the `<username>_3_prod` catalog. 
# MAGIC
# MAGIC     Notice that the following tables are not present in the production catalog:
# MAGIC       - **health_bronze_demo_08**
# MAGIC       - **health_silver_demo_08**

# COMMAND ----------

del_table(catalog_prod, 'default', 'health_bronze_demo_08')
del_table(catalog_prod, 'default', 'health_silver_demo_08')

spark.sql(f'SHOW TABLES IN {catalog_prod}.default').display()

# COMMAND ----------

# MAGIC %md
# MAGIC 2. Let's view the **production** configurations. 
# MAGIC
# MAGIC     Note the following:
# MAGIC
# MAGIC ```YAML
# MAGIC   production:
# MAGIC     mode: production
# MAGIC     workspace:
# MAGIC       # host: https://dbc-d9be2316-40bd.cloud.databricks.com/
# MAGIC       root_path: /Workspace/Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}
# MAGIC
# MAGIC     ## Change variable values when in the production environment to use the production catalog username_3_prod
# MAGIC     variables:
# MAGIC         target_catalog: ${var.catalog_prod}
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
# MAGIC     Information
# MAGIC   </strong>
# MAGIC   <div style="color:#333;">
# MAGIC
# MAGIC - Here, we are modifying the variable `target_catalog` to reference our variable `catalog_prod` that references our production catalog. This overrides the default job parameter in the **./resources/demo_08_job.job.yml** file.
# MAGIC
# MAGIC - We are not adding any overrides regarding the cluster to use for our job. Since we do not provide any overrides, it will use the default set in the **./resources/demo_08_job.job.yml** file, which is using Serverless compute.
# MAGIC
# MAGIC   </div>
# MAGIC </div>
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 3. Let's deploy the bundle to the **production** environment using the specified configurations.

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks bundle deploy -t production

# COMMAND ----------

# MAGIC %md
# MAGIC 4. When the cell above completes (in about a minute), view the deployed job named `production_demo08_dab_<username>`.
# MAGIC
# MAGIC     In the job, check the following:
# MAGIC
# MAGIC     - Select the job tasks and confirm each task uses Serverless compute.
# MAGIC
# MAGIC     - Find the **Job parameters** section in the right details pane. Note the values:
# MAGIC
# MAGIC         **Job parameters**
# MAGIC         - `catalog_name` - your `labuser_UNIQUE_ID_3_prod` catalog
# MAGIC         - `display_target` - `production`
# MAGIC
# MAGIC     Recall that we deployed the job in `production` mode, and it uses the configuration we specified in **databricks.yml** to read from and write to the production catalog and to use Serverless compute.

# COMMAND ----------

# MAGIC %md
# MAGIC #### Checkpoint
# MAGIC ![Dev](https://files.training.databricks.com/binder/prod_main/automated-deployment-with-declarative-automation-bundles-en_us-2.4.0/images/20261005T093748Z/Automated Deployment with Declarative Automation Bundles/Includes/images/multiple-env-demo/prod-deployment.png)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 5. Run the production job using the Databricks CLI.

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks bundle run -t production demo_08_job

# COMMAND ----------

# MAGIC %md
# MAGIC 6. While the job is running, let's view where the Databricks assets were bundled.
# MAGIC
# MAGIC     a. In the main navigation bar, right-click on **Workspace** and select *Open in a New Tab*.
# MAGIC
# MAGIC     b. Navigate to **Workspace > Users > your user name**.
# MAGIC
# MAGIC     c. Open the **.bundle** folder. Here, you should see the names of the bundles you have deployed (**demo05_bundle** and **demo_08_bundle**).
# MAGIC
# MAGIC     d. Open the deployed **demo_08_bundle** (the bundle name we specified in the **databricks.yml** file for this demonstration).
# MAGIC
# MAGIC     e. Here, we can see that we deployed to the **development** and **production** targets. 
# MAGIC
# MAGIC     f. Select the **production** folder.
# MAGIC     - You will see the **artifacts**, **files**, and **state** folders.
# MAGIC
# MAGIC     g. Select the **files** folder.
# MAGIC     - Notice that all of the files we deployed have been added to this location for the production mode deployment within the Workspace.
# MAGIC
# MAGIC     h. Close this tab.

# COMMAND ----------

# MAGIC %md
# MAGIC 7. By now, the **production** job should be completed. 
# MAGIC
# MAGIC     Navigate to the job and confirm it executed successfully.

# COMMAND ----------

# MAGIC %md
# MAGIC 8. Run the following code to view the tables in your `labuser_UNIQUE_ID_3_prod` catalog. 
# MAGIC
# MAGIC     Notice that the production job created the production tables:
# MAGIC       - **health_bronze_demo_08**
# MAGIC       - **health_silver_demo_08**

# COMMAND ----------

spark.sql(f'SHOW TABLES IN {catalog_prod}.default').display()

# COMMAND ----------

# MAGIC %md
# MAGIC 9. Count the number of rows in the **health_bronze_demo_08** table in the **labuser_UNIQUE_ID_3_prod** catalog. 
# MAGIC
# MAGIC   Notice that it contains over 70,692 rows because it's read from the production data and writing to the production catalog.

# COMMAND ----------

spark.sql(f'''
          SELECT count(*) 
          FROM {catalog_prod}.default.health_bronze_demo_08'''
          ).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Destroy the Bundles
# MAGIC Lastly, since we are finished with this bundle, let's delete it using the `databricks bundle destroy` command.
# MAGIC
# MAGIC
# MAGIC   By default, you are prompted to confirm permanent deletion of the previously-deployed jobs, pipelines, and artifacts. To skip these prompts and perform automatic permanent deletion, add the `--auto-approve` option to the bundle destroy command.

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Delete the bundles!

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks bundle destroy --auto-approve
# MAGIC databricks bundle destroy -t production --auto-approve

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
# MAGIC   <strong style="display:block; color:#c62828; margin-bottom:6px; font-size: 1.1em;">Warning!</strong>
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
# MAGIC In this demonstration, you deployed the same bundle to two targets and saw how to keep dev and prod from drifting apart:
# MAGIC
# MAGIC 1. Modularized the job by moving its definition into **./resources/demo_08_job.job.yml** and pulling it in via the `include` mapping.
# MAGIC 2. Defined reusable **variables** (`my_lab_user_name`, `catalog_dev`, `catalog_prod`, `target_catalog`, `raw_data_path`) and a **lookup** variable (`my_cluster_id`) that resolves a cluster name to a cluster ID.
# MAGIC 3. Used `databricks bundle validate --output json` to inspect the fully-resolved bundle and confirm substitutions.
# MAGIC 4. Deployed and ran the bundle against both `development` and `production` targets, with each target overriding the catalog and compute as needed.
# MAGIC 5. Cleaned up by destroying both targets with `databricks bundle destroy --auto-approve` and `databricks bundle destroy -t production --auto-approve`.

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>