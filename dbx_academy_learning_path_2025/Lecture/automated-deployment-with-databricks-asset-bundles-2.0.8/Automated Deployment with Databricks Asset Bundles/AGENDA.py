# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Automated Deployment with Databricks Asset Bundles
# MAGIC This course provides a comprehensive review of DevOps principles and their application to Databricks projects. It begins with an
# MAGIC overview of core DevOps, DataOps, continuous integration (CI), continuous deployment (CD), and testing, and explores how these
# MAGIC principles can be applied to data engineering pipelines.
# MAGIC
# MAGIC
# MAGIC The course then focuses on continuous deployment within the CI/CD process, examining tools like the Databricks REST API, SDK,
# MAGIC and CLI for project deployment. You will learn about Databricks Asset Bundles (DABs) and how they fit into the CI/CD process. You’ll
# MAGIC dive into their key components, folder structure, and how they streamline deployment across various target environments in
# MAGIC Databricks. You will also learn how to add variables, modify, validate, deploy, and execute Databricks Asset Bundles for multiple
# MAGIC environments with different configurations using the Databricks CLI.
# MAGIC
# MAGIC
# MAGIC Finally, the course introduces Visual Studio Code as an Interactive Development Environment (IDE) for building, testing, and
# MAGIC deploying Databricks Asset Bundles locally, optimizing your development process. The course concludes with an introduction to
# MAGIC automating deployment pipelines using GitHub Actions to enhance the CI/CD workflow with Databricks Asset Bundles.
# MAGIC By the end of this course, you will be equipped to automate Databricks project deployments with Databricks Asset Bundles, improving
# MAGIC efficiency through DevOps practices.
# MAGIC ---
# MAGIC ### Prerequisites
# MAGIC - Strong knowledge of the Databricks platform, including experience with Databricks Workspaces, Apache Spark, Delta Lake, the
# MAGIC Medallion Architecture, Unity Catalog, Delta Live Tables, and Workflows. In particular, knowledge of leveraging Expectations with
# MAGIC DLTs
# MAGIC - Experience in data ingestion and transformation, with proficiency in PySpark for data processing and DataFrame manipulation.
# MAGIC Candidates should also have experience writing intermediate-level SQL queries for data analysis and transformation
# MAGIC - Proficiency in Python programming, including the ability to design and implement functions and classes, and experience with
# MAGIC creating, importing, and utilizing Python packages
# MAGIC - Familiarity with DevOps practices, particularly continuous integration and continuous delivery/deployment (CI/CD) principles
# MAGIC - A basic understanding of Git version control
# MAGIC - Prerequisite course: `DevOps Essentials for Data Engineering Course`
# MAGIC ---
# MAGIC ### Course Agenda
# MAGIC The following modules are part of the **Data Engineer Learning** Path by Databricks Academy.
# MAGIC | # | Module Name |
# MAGIC | --- | --- |
# MAGIC | 0 | [0 - REQUIRED - Course Setup and Authentication]($./0 - REQUIRED - Course Setup and Authentication) |
# MAGIC | 1 | [01 - Deploying a simple DAB]($./01 - Deploying a Simple DAB) |
# MAGIC | 2 | [02L - Deploy a Simple DAB]($./02L - Deploy a Simple DAB) |
# MAGIC | 3 | [03 - Deploying a DAB to Multiple Environments]($./03 - Deploying a DAB to Multiple Environments) |
# MAGIC | 4 | [04L - Deploy a DAB to Multiple Environments]($./04L - Deploy a DAB to Multiple Environments) |
# MAGIC | 5 | [05L - Use a Databricks Default DAB Template]($./05L - Use a Databricks Default DAB Template) |
# MAGIC | 6 | [06 - Continuous Integration and Continuous Deployment with DABs]($./06 - Continuous Integration and Continuous Deployment with DABs) |
# MAGIC | 7 | [07L Bonus - Adding ML to Engineering Workflows with DABs]($./07L Bonus - Adding ML to Engineering Workflows with DABs) |
# MAGIC | 8 | [08 - Using VSCode with Databricks]($./08 - Using VSCode with Databricks) |
# MAGIC ---
# MAGIC ### Requirements
# MAGIC
# MAGIC Please review the following requirements before starting the lesson:
# MAGIC
# MAGIC * To run demo and lab notebooks, you need to use the following Databricks runtime: **`16.4.x-scala2.12`**
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="blank">Apache Software Foundation</a>.<br/>
# MAGIC <br/><a href="https://databricks.com/privacy-policy" target="blank">Privacy Policy</a> | 
# MAGIC <a href="https://databricks.com/terms-of-use" target="blank">Terms of Use</a> | 
# MAGIC <a href="https://help.databricks.com/" target="blank">Support</a>
# MAGIC
