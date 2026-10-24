# Databricks notebook source
# MAGIC %md
# MAGIC ![DB Academy](https://files.training.databricks.com/binder/prod_main/automated-deployment-with-declarative-automation-bundles-en_us-2.4.0/images/20261005T093748Z/Automated Deployment with Declarative Automation Bundles/Includes/images/common/db-academy.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Automated Deployment with Declarative Automation Bundles 
# MAGIC
# MAGIC This course provides a comprehensive review of DevOps principles and their application to Databricks projects. It begins with an overview of core DevOps, DataOps, continuous integration (CI), continuous deployment (CD), and testing, and explores how these principles can be applied to data engineering pipelines.
# MAGIC
# MAGIC
# MAGIC The course then focuses on continuous deployment within the CI/CD process, examining tools like the Databricks REST API, SDK, and CLI for project deployment. You will learn about Declarative Automation Bundles (DABs) and how they fit into the CI/CD process. 
# MAGIC
# MAGIC You’ll dive into their key components, folder structure, and how they streamline deployment across various target environments in Databricks. You will also learn how to add variables, modify, validate, deploy, and execute Declarative Automation Bundles for multiple environments with different configurations using the Databricks CLI.
# MAGIC
# MAGIC
# MAGIC Finally, the course introduces Visual Studio Code as an Integrated Development Environment (IDE) for building, testing, and deploying Declarative Automation Bundles locally, optimizing your development process. The course concludes with an introduction to automating deployment pipelines using GitHub Actions to enhance the CI/CD workflow with Declarative Automation Bundles.
# MAGIC
# MAGIC By the end of this course, you will be equipped to automate Databricks project deployments with Declarative Automation Bundles, improving
# MAGIC efficiency through DevOps practices.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prerequisites
# MAGIC - Strong knowledge of the Databricks platform, including experience with Databricks Workspaces, Apache Spark, Delta Lake, the Medallion Architecture, Unity Catalog, Apache Spark™ Declarative Pipelines, and Lakeflow Jobs. In particular, knowledge of leveraging Expectations with Apache Spark™ Declarative Pipelines (SDP)
# MAGIC - Experience in data ingestion and transformation, with proficiency in PySpark for data processing and DataFrame manipulation. Candidates should also have experience writing intermediate-level SQL queries for data analysis and transformation
# MAGIC - Proficiency in Python programming, including the ability to design and implement functions and classes, and experience with creating, importing, and utilizing Python packages
# MAGIC - Familiarity with DevOps practices, particularly continuous integration and continuous delivery/deployment (CI/CD) principles
# MAGIC - A basic understanding of Git version control
# MAGIC - Prerequisite course: `DevOps Essentials for Data Engineering Course`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Course Agenda
# MAGIC The following modules are part of the **Data Engineer Learning** Path by Databricks Academy.
# MAGIC
# MAGIC | # | Module Name |
# MAGIC | --- | --- |
# MAGIC | 01 | [01 Lecture - DevOps and CI-CD Review]($./01 Lecture - DevOps and CI-CD Review) |
# MAGIC | 02 | [02 - REQUIRED - Course Setup and Authentication]($./02 - REQUIRED - Course Setup and Authentication) |
# MAGIC | 03 | [03 Lecture - Deploying Databricks Projects]($./03 Lecture - Deploying Databricks Projects) |
# MAGIC | 04 | [04 Lecture - Introduction to Declarative Automation Bundles]($./04 Lecture - Introduction to Declarative Automation Bundles) |
# MAGIC | 05 | [05 Demo - Deploying a Simple DAB]($./05 Demo - Deploying a Simple DAB) |
# MAGIC | 06L | [06L - Deploy a Simple DAB]($./06L - Deploy a Simple DAB) |
# MAGIC | 07 | [07 Lecture - Variable Substitutions in DABs]($./07 Lecture - Variable Substitutions in DABs) |
# MAGIC | 08 | [08 Demo - Deploying a DAB to Multiple Environments]($./08 Demo - Deploying a DAB to Multiple Environments) |
# MAGIC | 09L | [09L - Deploy a DAB to Multiple Environments]($./09L - Deploy a DAB to Multiple Environments) |
# MAGIC | 10 | [10 Lecture - DAB Project Templates Overview]($./10 Lecture - DAB Project Templates Overview) |
# MAGIC | 11L | [11L - Use a Databricks Default DAB Template]($./11L - Use a Databricks Default DAB Template) |
# MAGIC | 12 | [12 Lecture - CI-CD Project Overview with DABs]($./12 Lecture - CI-CD Project Overview with DABs) |
# MAGIC | 13 | [13 Demo - Continuous Integration and Continuous Deployment with DABs]($./13 Demo - Continuous Integration and Continuous Deployment with DABs) |
# MAGIC | 14L | [14L Bonus - Adding ML to Engineering Workflows with DABs]($./14L Bonus - Adding ML to Engineering Workflows with DABs) |
# MAGIC | 15 | [15 Lecture - Developing Locally with Visual Studio Code]($./15 Lecture - Developing Locally with Visual Studio Code) |
# MAGIC | 16 | [16 Demo - Using VSCode with Databricks]($./16 Demo - Using VSCode with Databricks) |
# MAGIC | 17 | [17 Lecture - CI-CD Best Practices and Next Steps with GitHub Actions]($./17 Lecture - CI-CD Best Practices and Next Steps with GitHub Actions) |
# MAGIC | 99 | [99 - Summary and Next Steps]($./99 - Summary and Next Steps) |
# MAGIC ---
# MAGIC ### Workspace Requirements
# MAGIC
# MAGIC Please review the following requirements before starting the lesson:
# MAGIC
# MAGIC * To run demo and lab notebooks, you need to use the following Databricks runtime: **`17.3.x-scala2.13`**
# MAGIC
# MAGIC * Create catalog permissions

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>