# Databricks notebook source
# MAGIC %md
# MAGIC ![DB Academy](https://files.training.databricks.com/binder/prod_main/automated-deployment-with-declarative-automation-bundles-en_us-2.4.0/images/20261005T093748Z/Automated Deployment with Declarative Automation Bundles/Includes/images/common/db-academy.png)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Summary and Next Steps

# COMMAND ----------

# MAGIC %md
# MAGIC ## What's New
# MAGIC
# MAGIC Three recent additions to the DABs ecosystem worth knowing about as you head into real-world projects.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="max-width: 1200px; margin: 0 auto; font-family: sans-serif;">
# MAGIC
# MAGIC <div style="background: #F9F7F4; border-radius: 10px; padding: 24px 28px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); border-top: 6px solid #FF5F46;">
# MAGIC
# MAGIC   <img src="https://files.training.databricks.com/binder/prod_main/automated-deployment-with-declarative-automation-bundles-en_us-2.4.0/images/20261005T093748Z/Automated Deployment with Declarative Automation Bundles/Includes/images/common/genie-code.png" style="height: 48px; margin-bottom: 10px;">
# MAGIC   <div style="font-size: 18pt; font-weight: 700; color: #0b2026; margin-bottom: 14px;">Move Faster With Genie Code</div>
# MAGIC
# MAGIC   <div style="font-size: 14pt; color: #0b2026; line-height: 1.7; margin-bottom: 16px;">
# MAGIC     You've authored <strong>databricks.yml</strong> by hand, set up variables, and wired up multi-target deployments. Use <strong>Genie Code</strong> as your AI pair programmer to <strong>scaffold YAML faster</strong>, <strong>draft target overrides</strong>, <strong>diagnose validation errors</strong>, and <strong>generate Python helpers</strong> for the new Python-based bundles below. The fundamentals stay yours, Genie Code just makes the typing faster.
# MAGIC   </div>
# MAGIC
# MAGIC   <a href="https://www.databricks.com/blog/introducing-genie-code" target="_blank" style="display: inline-block; background: #FF5F46; color: white; font-size: 14pt; font-weight: 700; padding: 10px 24px; border-radius: 8px; text-decoration: none;">
# MAGIC     Introducing Genie Code →
# MAGIC   </a>
# MAGIC   <div style="display: flex; gap: 10px; margin-top: 12px;">
# MAGIC     <a href="https://docs.databricks.com/aws/en/genie-code/" target="_blank" style="display: inline-block; background: #1B5162; color: white; font-size: 14pt; font-weight: 700; padding: 10px 24px; border-radius: 8px; text-decoration: none;">
# MAGIC       AWS →
# MAGIC     </a>
# MAGIC     <a href="https://learn.microsoft.com/en-us/azure/databricks/genie-code/" target="_blank" style="display: inline-block; background: #1B5162; color: white; font-size: 14pt; font-weight: 700; padding: 10px 24px; border-radius: 8px; text-decoration: none;">
# MAGIC       Azure →
# MAGIC     </a>
# MAGIC     <a href="https://docs.databricks.com/gcp/en/genie-code/" target="_blank" style="display: inline-block; background: #1B5162; color: white; font-size: 14pt; font-weight: 700; padding: 10px 24px; border-radius: 8px; text-decoration: none;">
# MAGIC       GCP →
# MAGIC     </a>
# MAGIC   </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="max-width: 1200px; margin: 0 auto; font-family: sans-serif;">
# MAGIC
# MAGIC <div style="background: #F9F7F4; border-radius: 10px; padding: 24px 28px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); border-top: 6px solid #FF5F46;">
# MAGIC
# MAGIC   <div style="font-size: 18pt; font-weight: 700; color: #0b2026; margin-bottom: 14px;">DABs Are Now First-Class in the Workspace UI</div>
# MAGIC
# MAGIC   <div style="font-size: 14pt; color: #0b2026; line-height: 1.7; margin-bottom: 16px;">
# MAGIC     You ran the entire course from notebooks because the CLI lives there. Bundles are now <strong>first-class citizens in the Databricks workspace</strong>: you can <strong>view, edit, and deploy</strong> a bundle directly from the workspace file browser, see which jobs and pipelines belong to a bundle, and toggle between source view and the rendered resource view. Especially useful for teams who don't want to leave the browser to manage their bundles.
# MAGIC   </div>
# MAGIC
# MAGIC   <a href="https://www.databricks.com/blog/announcing-databricks-asset-bundles-now-workspace" target="_blank" style="display: inline-block; background: #FF5F46; color: white; font-size: 14pt; font-weight: 700; padding: 10px 24px; border-radius: 8px; text-decoration: none;">
# MAGIC     Read the Announcement Blog →
# MAGIC   </a>
# MAGIC   <div style="display: flex; gap: 10px; margin-top: 12px;">
# MAGIC     <a href="https://docs.databricks.com/aws/en/dev-tools/bundles/workspace" target="_blank" style="display: inline-block; background: #1B5162; color: white; font-size: 14pt; font-weight: 700; padding: 10px 24px; border-radius: 8px; text-decoration: none;">
# MAGIC       AWS →
# MAGIC     </a>
# MAGIC     <a href="https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/workspace" target="_blank" style="display: inline-block; background: #1B5162; color: white; font-size: 14pt; font-weight: 700; padding: 10px 24px; border-radius: 8px; text-decoration: none;">
# MAGIC       Azure →
# MAGIC     </a>
# MAGIC     <a href="https://docs.databricks.com/gcp/en/dev-tools/bundles/workspace" target="_blank" style="display: inline-block; background: #1B5162; color: white; font-size: 14pt; font-weight: 700; padding: 10px 24px; border-radius: 8px; text-decoration: none;">
# MAGIC       GCP →
# MAGIC     </a>
# MAGIC   </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <div style="max-width: 1200px; margin: 0 auto; font-family: sans-serif;">
# MAGIC
# MAGIC <div style="background: #F9F7F4; border-radius: 10px; padding: 24px 28px; box-shadow: 0 2px 8px rgba(27,49,57,0.06); border-top: 6px solid #FF5F46;">
# MAGIC
# MAGIC   <div style="font-size: 18pt; font-weight: 700; color: #0b2026; margin-bottom: 14px;">Author Bundles in Python, Not Just YAML</div>
# MAGIC
# MAGIC   <div style="font-size: 14pt; color: #0b2026; line-height: 1.7; margin-bottom: 16px;">
# MAGIC     You worked with <strong>YAML</strong>-based bundles throughout this course. Databricks now supports authoring the same bundles in <strong>Python</strong>, so you can use <strong>loops, conditionals, and shared helper modules</strong> to generate <strong>databricks.yml</strong> programmatically. Great for projects with many similar resources or for teams that prefer Python over templating tricks.
# MAGIC   </div>
# MAGIC
# MAGIC   <a href="https://www.databricks.com/blog/announcing-python-support-databricks-asset-bundles-streamline-deployments" target="_blank" style="display: inline-block; background: #FF5F46; color: white; font-size: 14pt; font-weight: 700; padding: 10px 24px; border-radius: 8px; text-decoration: none;">
# MAGIC     Read the Announcement Blog →
# MAGIC   </a>
# MAGIC   <div style="display: flex; gap: 10px; margin-top: 12px;">
# MAGIC     <a href="https://docs.databricks.com/aws/en/dev-tools/bundles/python" target="_blank" style="display: inline-block; background: #1B5162; color: white; font-size: 14pt; font-weight: 700; padding: 10px 24px; border-radius: 8px; text-decoration: none;">
# MAGIC       AWS →
# MAGIC     </a>
# MAGIC     <a href="https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/python" target="_blank" style="display: inline-block; background: #1B5162; color: white; font-size: 14pt; font-weight: 700; padding: 10px 24px; border-radius: 8px; text-decoration: none;">
# MAGIC       Azure →
# MAGIC     </a>
# MAGIC     <a href="https://docs.databricks.com/gcp/en/dev-tools/bundles/python" target="_blank" style="display: inline-block; background: #1B5162; color: white; font-size: 14pt; font-weight: 700; padding: 10px 24px; border-radius: 8px; text-decoration: none;">
# MAGIC       GCP →
# MAGIC     </a>
# MAGIC   </div>
# MAGIC
# MAGIC </div>
# MAGIC
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Additional Resources
# MAGIC
# MAGIC Explore these resources to keep learning and stay up to date with Declarative Automation Bundles (DABs).
# MAGIC
# MAGIC #### Core documentation
# MAGIC
# MAGIC - **What are bundles?** (intro):
# MAGIC [AWS](https://docs.databricks.com/aws/en/dev-tools/bundles/) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/dev-tools/bundles/)
# MAGIC
# MAGIC - **Databricks platform release notes** (where new DABs features are announced):
# MAGIC [AWS](https://docs.databricks.com/aws/en/release-notes/product/) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/release-notes/product/) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/release-notes/product/)
# MAGIC
# MAGIC - **Databricks CLI version history** (DABs ships with the CLI): [GitHub releases](https://github.com/databricks/cli/releases)
# MAGIC
# MAGIC #### Authoring and configuration
# MAGIC
# MAGIC - **Bundle configuration reference** (full YAML key list):
# MAGIC [AWS](https://docs.databricks.com/aws/en/dev-tools/bundles/reference) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/reference) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/dev-tools/bundles/reference)
# MAGIC
# MAGIC - **Bundle settings** (all top-level mappings: `bundle`, `include`, `targets`, `workspace`, `resources`, `variables`, `presets`, `permissions`):
# MAGIC [AWS](https://docs.databricks.com/aws/en/dev-tools/bundles/settings) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/settings) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/dev-tools/bundles/settings)
# MAGIC
# MAGIC - **Variables and substitutions** (`${var.…}`, `${bundle.…}`, lookups):
# MAGIC [AWS](https://docs.databricks.com/aws/en/dev-tools/bundles/variables) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/variables) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/dev-tools/bundles/variables)
# MAGIC
# MAGIC - **Deployment modes** (`development` and `production`):
# MAGIC [AWS](https://docs.databricks.com/aws/en/dev-tools/bundles/deployment-modes) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/deployment-modes) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/dev-tools/bundles/deployment-modes)
# MAGIC
# MAGIC - **Bundle templates** (`bundle init`, default and custom):
# MAGIC [AWS](https://docs.databricks.com/aws/en/dev-tools/bundles/templates) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/templates) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/dev-tools/bundles/templates)
# MAGIC
# MAGIC - **Override cluster settings per environment**:
# MAGIC [AWS](https://docs.databricks.com/aws/en/dev-tools/bundles/cluster-override) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/cluster-override) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/dev-tools/bundles/cluster-override)
# MAGIC
# MAGIC #### CLI commands
# MAGIC
# MAGIC - **`databricks bundle` CLI commands** (`validate`, `summary`, `deploy`, `run`, `destroy`, `init`):
# MAGIC [AWS](https://docs.databricks.com/aws/en/dev-tools/cli/bundle-commands) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/cli/bundle-commands) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/dev-tools/cli/bundle-commands)
# MAGIC
# MAGIC - **Install or update the Databricks CLI**:
# MAGIC [AWS](https://docs.databricks.com/aws/en/dev-tools/cli/install) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/cli/install) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/dev-tools/cli/install)
# MAGIC
# MAGIC #### CI/CD and identity
# MAGIC
# MAGIC - **CI/CD on Databricks** (overview):
# MAGIC [AWS](https://docs.databricks.com/aws/en/dev-tools/index-ci-cd) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/index-ci-cd) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/dev-tools/index-ci-cd)
# MAGIC
# MAGIC - **Set a bundle run identity** (`run_as`, service principals):
# MAGIC [AWS](https://docs.databricks.com/aws/en/dev-tools/bundles/run-as) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/bundles/run-as) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/dev-tools/bundles/run-as)
# MAGIC
# MAGIC - **Authentication overview** (PAT, OAuth M2M, etc.):
# MAGIC [AWS](https://docs.databricks.com/aws/en/dev-tools/auth/) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/auth/) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/dev-tools/auth/)
# MAGIC
# MAGIC #### IDE integration
# MAGIC
# MAGIC - **Databricks extension for VS Code**:
# MAGIC [AWS](https://docs.databricks.com/aws/en/dev-tools/vscode-ext) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/vscode-ext) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/dev-tools/vscode-ext)
# MAGIC
# MAGIC - **DABs in the VS Code extension**:
# MAGIC [AWS](https://docs.databricks.com/aws/en/dev-tools/vscode-ext/bundles) |
# MAGIC [Azure](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/vscode-ext/bundles) |
# MAGIC [GCP](https://docs.databricks.com/gcp/en/dev-tools/vscode-ext/bundles)
# MAGIC
# MAGIC #### Databricks blog and announcements
# MAGIC
# MAGIC - [**Announcing Python support for Declarative Automation Bundles to streamline deployments**](https://www.databricks.com/blog/announcing-python-support-databricks-asset-bundles-streamline-deployments). Same announcement linked in the callout above.
# MAGIC
# MAGIC - [**Announcing Declarative Automation Bundles in the workspace**](https://www.databricks.com/blog/announcing-databricks-asset-bundles-now-workspace). Bundles now have first-class support inside the workspace UI.
# MAGIC
# MAGIC - [**Tutorial: How to ship AI/BI Dashboard changes safely at scale with Declarative Automation Bundles**](https://www.databricks.com/blog/tutorial-how-ship-aibi-dashboard-changes-safely-scale-databricks-asset-bundles). Apply the same DAB workflow to dashboards.
# MAGIC
# MAGIC #### Community blogs
# MAGIC
# MAGIC - [**Declarative Automation Bundles is now Declarative Automation Bundles**](https://community.databricks.com/t5/mvp-articles/databricks-asset-bundles-is-now-declarative-automation-bundles/td-p/151594). Context on the recent rename used throughout this course.
# MAGIC
# MAGIC - [**Customizing target deployments in Declarative Automation Bundles**](https://community.databricks.com/t5/technical-blog/customizing-target-deployments-in-databricks-asset-bundles/ba-p/124772). Goes deeper than this course on per-target overrides.
# MAGIC
# MAGIC - [**5 tricks to get the most out of Declarative Automation Bundles**](https://community.databricks.com/t5/technical-blog/5-tricks-to-get-the-most-out-of-databricks-asset-bundles/ba-p/96528). Practical tips after you've finished the basics.
# MAGIC
# MAGIC - [**CI/CD on Databricks with Asset Bundles (DABs) and GitHub Actions**](https://community.databricks.com/t5/community-articles/ci-cd-on-databricks-with-asset-bundles-dabs-and-github-actions/td-p/149565). End-to-end GitHub Actions workflow.
# MAGIC
# MAGIC - [**Azure DevOps with Databricks DABs: step-by-step integration guide**](https://community.databricks.com/t5/technical-blog/azure-devops-with-databricks-dabs-step-by-step-integration-guide/ba-p/141451). Same idea on Azure DevOps.
# MAGIC
# MAGIC #### Demo videos
# MAGIC
# MAGIC - [**Data engineering: Data to dashboards with DABs**](https://www.databricks.com/resources/demos/videos/data-engineering-data-to-dashboards-with-dabs). End-to-end demo from Databricks.
# MAGIC
# MAGIC - [**Declarative Automation Bundles demo**](https://www.databricks.com/resources/demos/videos/databricks-asset-bundles-demo). Short walkthrough.
# MAGIC
# MAGIC - [**Deploying Declarative Automation Bundles (DABs) at Scale**](https://www.youtube.com/watch?v=mMwprgB-sIU). Databricks YouTube.
# MAGIC
# MAGIC - [**CI/CD for Databricks: Advanced Asset Bundles and GitHub Actions**](https://www.youtube.com/watch?v=XumUXF1e6RI). Databricks YouTube.
# MAGIC
# MAGIC - [**Declarative Automation Bundles: Advanced Examples**](https://www.youtube.com/watch?v=ZuQzIbRoFC4). Dustin Vannoy on YouTube.

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Next Steps
# MAGIC
# MAGIC Continue building your Databricks skills with additional training and certification resources.
# MAGIC
# MAGIC ### B1. Continue Your Learning
# MAGIC
# MAGIC Expand your data and AI knowledge through Databricks self-paced and instructor-led training. These courses help you deepen your technical skills and gain hands-on experience with the platform.
# MAGIC
# MAGIC Visit [Databricks Training and Certification](https://www.databricks.com/learn/training/home).
# MAGIC
# MAGIC Recommended follow-on courses for someone who's just finished this DABs course:
# MAGIC
# MAGIC - [**DevOps Essentials for Data Engineering**](https://www.databricks.com/training/catalog/devops-essentials-for-data-engineering-3605). Code modularization, unit testing, and integration testing patterns referenced in the CI/CD demo.
# MAGIC
# MAGIC - [**Deploy Workloads with Lakeflow Jobs**](https://www.databricks.com/training/catalog/deploy-workloads-with-lakeflow-jobs-2978). Goes deeper on the job-orchestration side of what bundles deploy.
# MAGIC
# MAGIC - [**Build Data Pipelines with Apache Spark Declarative Pipelines**](https://www.databricks.com/training/catalog/build-data-pipelines-with-apache-spark-declarative-pipelines-1686)
# MAGIC
# MAGIC - [**Advanced Techniques with Apache Spark Declarative Pipelines**](https://www.databricks.com/training/catalog/advanced-techniques-with-apache-spark-declarative-pipelines-2972)
# MAGIC
# MAGIC
# MAGIC ### B2. Earn a Certification
# MAGIC
# MAGIC Validate your Databricks expertise by earning an official credential. Certifications demonstrate your ability to apply Databricks technologies in real-world data and AI workloads.
# MAGIC
# MAGIC Visit [Databricks Certification and Badging](https://www.databricks.com/learn/training/certification).
# MAGIC
# MAGIC DABs knowledge shows up directly on:
# MAGIC
# MAGIC - **Databricks Certified Data Engineer Associate**
# MAGIC - **Databricks Certified Data Engineer Professional**

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
# MAGIC