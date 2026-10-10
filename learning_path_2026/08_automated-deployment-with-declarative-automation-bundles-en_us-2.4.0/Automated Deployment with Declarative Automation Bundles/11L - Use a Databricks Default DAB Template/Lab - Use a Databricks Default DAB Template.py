# Databricks notebook source
# MAGIC %md
# MAGIC ![DB Academy](https://files.training.databricks.com/binder/prod_main/automated-deployment-with-declarative-automation-bundles-en_us-2.4.0/images/20261005T093748Z/Automated Deployment with Declarative Automation Bundles/Includes/images/common/db-academy.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # 11L - Use a Databricks Default Declarative Automation Bundle (DAB) Template
# MAGIC
# MAGIC ### Estimated Duration: ~10 minutes
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC Up to this point you've authored bundles by hand. In this lab, you'll use the Databricks CLI's built-in **bundle templates** to scaffold a new project from scratch. Templates give you a working bundle with sensible defaults (job, pipeline, tests, README) that you can customize, instead of starting from a blank **databricks.yml**.
# MAGIC
# MAGIC You'll inspect the available templates, generate a `default-python` project with `databricks bundle init`, explore the generated structure, and validate it.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this lab, you will be able to:
# MAGIC
# MAGIC 1. **List the default bundle templates** Databricks ships with the CLI.
# MAGIC 2. **Initialize a new bundle** from a template using `databricks bundle init <template>`.
# MAGIC 3. **Navigate the generated project structure** (`resources/`, `src/`, `scratch/`, `tests/`, `databricks.yml`).
# MAGIC 4. **Validate the generated bundle** with `databricks bundle validate`, including the working-directory pitfall when the bundle lives in a subfolder.
# MAGIC
# MAGIC ## Reference Documentation
# MAGIC
# MAGIC - **What are bundles?** (intro): [AWS](https://docs.databricks.com/aws/en/dev-tools/bundles/) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/) | [GCP](https://docs.databricks.com/gcp/en/dev-tools/bundles/)
# MAGIC - **Bundle templates (default and custom)**: [AWS](https://docs.databricks.com/aws/en/dev-tools/bundles/templates) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/templates) | [GCP](https://docs.databricks.com/gcp/en/dev-tools/bundles/templates)
# MAGIC - **`databricks bundle` CLI commands** (includes `bundle init`): [AWS](https://docs.databricks.com/aws/en/dev-tools/cli/bundle-commands) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/cli/bundle-commands) | [GCP](https://docs.databricks.com/gcp/en/dev-tools/cli/bundle-commands)
# MAGIC - **Bundle configuration reference**: [AWS](https://docs.databricks.com/aws/en/dev-tools/bundles/reference) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/reference) | [GCP](https://docs.databricks.com/gcp/en/dev-tools/bundles/reference)
# MAGIC - **MLOps Stacks** (advanced template): [AWS](https://docs.databricks.com/aws/en/machine-learning/mlops/mlops-stacks) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/mlops/mlops-stacks) | [GCP](https://docs.databricks.com/gcp/en/machine-learning/mlops/mlops-stacks)

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

# MAGIC %md
# MAGIC ## A. Classroom Setup
# MAGIC
# MAGIC Run the following cell to configure your working environment for this course.
# MAGIC
# MAGIC **NOTE:** The `DA` object is only used in Databricks Academy courses and is not available outside of them. It dynamically references the information needed to run the course.

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-11L

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Lab Scenario
# MAGIC
# MAGIC You're starting a brand-new project and want to skip the boilerplate. Instead of writing **databricks.yml** by hand, you'll use the Databricks CLI's `bundle init` command with the `default-python` template to scaffold a working bundle, then validate it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Pre-flight Checks
# MAGIC
# MAGIC Before starting the lab tasks, run a couple of quick checks to confirm the Databricks CLI is installed and authenticated against your workspace.

# COMMAND ----------

# MAGIC %md
# MAGIC ### C1. Check the Databricks CLI Version
# MAGIC
# MAGIC Run a CLI command to confirm the Databricks CLI version is **v0.298.0**.

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks -v

# COMMAND ----------

# MAGIC %md
# MAGIC ### C2. Confirm CLI Authentication
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
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Task 1 - Explore Available Bundle Templates
# MAGIC
# MAGIC Databricks ships several default bundle templates with the CLI. Each one creates a working bundle for a specific use case so you don't have to start from a blank **databricks.yml**.
# MAGIC
# MAGIC | Template | Description |
# MAGIC |----------|-------------|
# MAGIC | `default-python` | A bundle with a Python job and a Apache Spark™ Declarative Pipeline. Good starting point for most ETL projects. |
# MAGIC | `default-sql` | A bundle that defines a job which runs SQL queries on a SQL warehouse. |
# MAGIC | `dbt-sql` | A bundle that uses dbt-core for local development and a bundle for deployment. Includes a job with a dbt task and dbt profile configuration. |
# MAGIC | `mlops-stacks` | An advanced full-stack template for MLOps projects (model training, serving, monitoring). |
# MAGIC
# MAGIC Templates evolve over time, so for the current list and what each one generates, see the **Bundle templates** documentation:
# MAGIC [AWS](https://docs.databricks.com/aws/en/dev-tools/bundles/templates) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/templates) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/dev-tools/bundles/templates)

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Task 2 - View `bundle init` Documentation
# MAGIC
# MAGIC Run the `--help` command to view documentation for the `databricks bundle init` command. Read the available flags and arguments before generating a project.

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks bundle init --help

# COMMAND ----------

# MAGIC %md
# MAGIC ## F. Task 3 - Initialize a Bundle from `default-python`
# MAGIC
# MAGIC Use `databricks bundle init <template-name>` to scaffold a new project. The cell below uses the `default-python` template.
# MAGIC
# MAGIC **NOTE:** If you don't specify a template name, `bundle init` enters **interactive** mode and prompts you to choose. Interactive mode is not available when running `%sh` cells in a notebook, so we always specify the template name in this lab.

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks bundle init default-python

# COMMAND ----------

# MAGIC %md
# MAGIC ## G. Task 4 - Explore the Generated Bundle Structure
# MAGIC
# MAGIC Navigate to the generated **my_project** folder in the workspace file browser and inspect what `bundle init` produced.
# MAGIC
# MAGIC ### Step 4.1
# MAGIC  - Top-level folders
# MAGIC
# MAGIC - **resources/** contains additional YAML files included by the bundle:
# MAGIC     - **my_project.job.yml**
# MAGIC     - **my_project.pipeline.yml**
# MAGIC - **scratch/** contains an exploration notebook for ad-hoc analysis.
# MAGIC - **src/** contains the production code (a Python notebook and a Spark Declarative Pipeline notebook).
# MAGIC - **tests/** contains unit and integration tests for the project.
# MAGIC - The project root also contains a **pytest.ini**, **README.md**, and a few config files.
# MAGIC
# MAGIC ### Step 4.2 - Open the generated **databricks.yml**
# MAGIC
# MAGIC Open the **databricks.yml** file at the root of **my_project** and notice the template provides two `targets` (typically `dev` and `prod`).

# COMMAND ----------

# MAGIC %md
# MAGIC ## H. Task 5 - Validate the Generated Bundle
# MAGIC
# MAGIC Now validate the generated bundle. There's a small wrinkle: `databricks bundle` commands always run against the **current working directory**, and the bundle was created in a subfolder (`my_project/`) of where the CLI was invoked. So you'll need to `cd` into `my_project/` first.
# MAGIC
# MAGIC ### Step 5.1 - Confirm your working directory
# MAGIC
# MAGIC Run the cell below to confirm you are **not** yet inside `my_project/`.

# COMMAND ----------

# MAGIC %sh
# MAGIC pwd
# MAGIC ls

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5.2 - Change directory and validate
# MAGIC
# MAGIC In a single `%sh` cell:
# MAGIC
# MAGIC 1. `cd` into `my_project`.
# MAGIC 2. Run `databricks bundle validate`.
# MAGIC
# MAGIC **Why both in one cell?** Each `%sh` cell starts a fresh shell, so a `cd` in one cell does not persist into the next.

# COMMAND ----------

# MAGIC %sh
# MAGIC cd 'my_project'
# MAGIC databricks bundle validate

# COMMAND ----------

# MAGIC %md
# MAGIC ## I. Wrap-Up
# MAGIC
# MAGIC **Note:** After validation, take a few minutes to skim the generated template, the **databricks.yml**, the resource YAMLs in `resources/`, and the source code in `src/`.
# MAGIC
# MAGIC **Why we stop at validate in this lab:** the Databricks Academy lab environment restricts your ability to create clusters, so the generated bundle's job and pipeline cannot actually be deployed and run here. The end-to-end deploy flow has been covered in earlier modules.
# MAGIC
# MAGIC
# MAGIC **Want to use templates locally?** This lab also provides a working VS Code environment. To deploy a template using VS Code, see **08 - Using VS Code with Databricks**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC
# MAGIC In this lab you used a Databricks default bundle template to scaffold a new project end-to-end without writing **databricks.yml** by hand:
# MAGIC
# MAGIC 1. Reviewed the four default templates (`default-python`, `default-sql`, `dbt-sql`, `mlops-stacks`).
# MAGIC 2. Read the `databricks bundle init --help` output to understand the available flags.
# MAGIC 3. Generated a working bundle with `databricks bundle init default-python`.
# MAGIC 4. Explored the resulting project structure (`resources/`, `src/`, `scratch/`, `tests/`, **databricks.yml**).
# MAGIC 5. Validated the generated bundle with `cd my_project && databricks bundle validate`.
# MAGIC
# MAGIC Templates are usually how you'll start a new project in real life. From here, the same `validate` / `deploy` / `run` / `destroy` workflow you've been practicing applies.

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>