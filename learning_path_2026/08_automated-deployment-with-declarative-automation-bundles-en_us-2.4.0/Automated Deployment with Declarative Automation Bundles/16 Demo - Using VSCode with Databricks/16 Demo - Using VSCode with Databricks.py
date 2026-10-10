# Databricks notebook source
# MAGIC %md
# MAGIC ![DB Academy](https://files.training.databricks.com/binder/prod_main/automated-deployment-with-declarative-automation-bundles-en_us-2.4.0/images/20261005T093748Z/Automated Deployment with Declarative Automation Bundles/Includes/images/common/db-academy.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # 16 Bonus - Using VS Code with Databricks
# MAGIC
# MAGIC ## Overview
# MAGIC
# MAGIC Up to this point you've built and deployed bundles entirely from inside the Databricks workspace. In real production work, most teams author bundles in **VS Code** with the **Databricks extension**, which gives you syntax highlighting, schema validation, autocomplete for **databricks.yml**, and one-click bundle deploy/run from the editor.
# MAGIC
# MAGIC This bonus walkthrough shows you how to authenticate VS Code against your Databricks workspace using the workspace URL and a Personal Access Token (PAT), so you can switch from notebook-based bundle work to a local editor.
# MAGIC
# MAGIC ## Learning Objectives
# MAGIC
# MAGIC By the end of this demonstration, you will be able to:
# MAGIC
# MAGIC 1. **Find the Databricks workspace URL** the VS Code extension needs for authentication.
# MAGIC 2. **Generate a Personal Access Token (PAT)** from the workspace **User Settings**.
# MAGIC 3. **Open the VS Code lab environment** and authenticate it with the workspace URL and PAT.
# MAGIC
# MAGIC ## Reference Documentation
# MAGIC
# MAGIC - **Databricks extension for VS Code**: [AWS](https://docs.databricks.com/aws/en/dev-tools/vscode-ext) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/vscode-ext) | [GCP](https://docs.databricks.com/gcp/en/dev-tools/vscode-ext)
# MAGIC - **DABs in the VS Code extension**: [AWS](https://docs.databricks.com/aws/en/dev-tools/vscode-ext/bundles) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/vscode-ext/bundles) | [GCP](https://docs.databricks.com/gcp/en/dev-tools/vscode-ext/bundles)
# MAGIC - **Personal access tokens (PATs)**: [AWS](https://docs.databricks.com/aws/en/dev-tools/auth/pat) | [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/auth/pat) | [GCP](https://docs.databricks.com/gcp/en/dev-tools/auth/pat)

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
# MAGIC You can select **Serverless** compute or your **all purpose cluster**.
# MAGIC
# MAGIC   </div>
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Open a Text Editor
# MAGIC
# MAGIC 1. Open a simple text editor on your computer.
# MAGIC
# MAGIC 2. Follow the steps below to paste the following information into the file so you can use it later when authenticating with VS Code:
# MAGIC
# MAGIC    - Your Databricks workspace URL
# MAGIC
# MAGIC    - Your Personal Access Token (PAT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Get Your Databricks Workspace URL
# MAGIC
# MAGIC 1. Run the cell below to print your workspace URL. Leave the output visible. You'll paste it into your text editor and again into VS Code.

# COMMAND ----------

lab_databricks_url = f'{spark.conf.get("spark.databricks.workspaceUrl")}/'
print(lab_databricks_url)

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Generate a Personal Access Token (PAT)
# MAGIC
# MAGIC Generate a PAT in the workspace so VS Code can authenticate.
# MAGIC
# MAGIC 1. Click on your username in the top bar, right-click on **User Settings** in the drop-down, and select **Open in a New Tab**.
# MAGIC
# MAGIC 2. In **Settings**, select **Developer**, then to the left of **Access tokens**, select **Manage**.
# MAGIC
# MAGIC 3. Click **Generate new token**.
# MAGIC    
# MAGIC 4. Set **Scope** to **Other APIs**.
# MAGIC
# MAGIC 5. Set **API Scope** and check **all APIs (not recommended)**.
# MAGIC    - For training purposes. Follow your organization's security and governance guidelines when defining token permissions.
# MAGIC
# MAGIC 6. Click **Generate**.
# MAGIC
# MAGIC 7. Copy the displayed token to the clipboard. **You will not be able to view the token again.** If you lose it, you'll need to delete it and create a new one.
# MAGIC
# MAGIC 8. Paste the PAT into your text editor below the workspace URL.
# MAGIC
# MAGIC **Reference:** Personal access tokens (PATs):
# MAGIC [AWS](https://docs.databricks.com/aws/en/dev-tools/auth/pat) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/auth/pat) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/dev-tools/auth/pat)

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
# MAGIC     Vocareum Lab Copy Issues
# MAGIC   </strong>
# MAGIC   <div style="color:#333;">
# MAGIC
# MAGIC - If the lab environment's clipboard button has issues, highlight the PAT and copy it manually. Confirm the copy succeeded before closing the dialog.
# MAGIC
# MAGIC - For training purposes, we are temporarily enabling the token to access all APIs. This is **not** typically recommended for production. Follow your organization's security and governance guidelines when defining token permissions.
# MAGIC
# MAGIC   </div>
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Open the VS Code Lab Environment
# MAGIC
# MAGIC 1. Open VS Code in your lab environment:
# MAGIC
# MAGIC    - In the Vocareum iframe, select **Lab > vscode**.
# MAGIC
# MAGIC    - Open the link in a new browser tab if needed.
# MAGIC    
# MAGIC    - A new tab should open with VS Code.
# MAGIC
# MAGIC 2. In VS Code, open the provided markdown file and follow the instructions.
# MAGIC
# MAGIC ![VSCode Access](https://files.training.databricks.com/binder/prod_main/automated-deployment-with-declarative-automation-bundles-en_us-2.4.0/images/20261005T093748Z/Automated Deployment with Declarative Automation Bundles/Includes/images/demo_vscode/selectvscode.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC
# MAGIC You've now connected VS Code to the Databricks workspace using your workspace URL and a personal access token. From here, the **Databricks extension for VS Code** lets you:
# MAGIC
# MAGIC - Edit **databricks.yml** with autocomplete and schema validation.
# MAGIC - Run `databricks bundle validate`, `deploy`, `run`, and `destroy` directly from the editor.
# MAGIC - Sync local code with the workspace for fast iteration.
# MAGIC
# MAGIC For the full feature tour, see the **Databricks extension for VS Code** documentation:
# MAGIC [AWS](https://docs.databricks.com/aws/en/dev-tools/vscode-ext) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/vscode-ext) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/dev-tools/vscode-ext).

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>