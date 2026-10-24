-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img
-- MAGIC     src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png"
-- MAGIC     alt="Databricks Learning"
-- MAGIC   >
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # 6 - Change Data Capture with AUTO CDC with Slowing Changing Dimensions (SCD) TYPE 1
-- MAGIC
-- MAGIC ##### NOTE: The AUTO CDC APIs replace the APPLY CHANGES APIs, and have the same syntax. The APPLY CHANGES APIs are still available, but Databricks recommends using the AUTO CDC APIs in their place.
-- MAGIC
-- MAGIC In this demonstration, we will continue to build our pipeline by ingesting **customer** data into our pipeline. The customer data includes new customers, customers who have deleted their accounts, and customers who have updated their information (such as address, email, etc.). We will need to build our customer pipeline by implementing change data capture (CDC) for customer data using SCD Type 1 (Type 2 is outside the scope of this course).
-- MAGIC
-- MAGIC The customer pipeline flow will:
-- MAGIC
-- MAGIC - The bronze table uses **Auto Loader** to ingest JSON data from cloud object storage with SQL (`FROM STREAM`).
-- MAGIC - A table is defined to enforce constraints before passing records to the silver layer.
-- MAGIC - `AUTO CDC` is used to automatically process CDC data into the silver layer as a Type 1.
-- MAGIC - A gold table is defined to create a materialized view of the current customers with updated information (dropped customers, new customers and updated customer information).
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ### Learning Objectives
-- MAGIC
-- MAGIC By the end of this lesson, students should feel comfortable:
-- MAGIC - Apply the `AUTO CDC` operation in Lakeflow Declarative Pipelines to process change data capture (CDC) by integrating and updating incoming data from a source stream into an existing Delta table, ensuring data accuracy and consistency.
-- MAGIC - Analyze Slowly Changing Dimensions (SCD Type 1) tables within Lakeflow Declarative Pipelines to effectively update, insert and drop customers in dimensional data, managing the state of records over time using appropriate keys, versioning, and timestamps.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## REQUIRED - SELECT CLASSIC COMPUTE (your cluster starts with **labuser**)
-- MAGIC
-- MAGIC Before executing cells in this notebook, please select your classic compute cluster in the lab. Be aware that **Serverless** is enabled by default.
-- MAGIC
-- MAGIC Follow these steps to select the classic compute cluster:
-- MAGIC
-- MAGIC 1. Navigate to the top-right of this notebook and click the drop-down menu to select your cluster. By default, the notebook will use **Serverless**.
-- MAGIC
-- MAGIC 1. If your cluster is available, select it and continue to the next cell. If the cluster is not shown:
-- MAGIC
-- MAGIC     - In the drop-down, select **More**.
-- MAGIC
-- MAGIC     - In the **Attach to an existing compute resource** pop-up, select the first drop-down. You will see a unique cluster name in that drop-down. Please select that cluster.
-- MAGIC
-- MAGIC **NOTE:** If your cluster has terminated, you might need to restart it in order to select it. To do this:
-- MAGIC
-- MAGIC 1. Right-click on **Compute** in the left navigation pane and select *Open in new tab*.
-- MAGIC
-- MAGIC 1. Find the triangle icon to the right of your compute cluster name and click it.
-- MAGIC
-- MAGIC 1. Wait a few minutes for the cluster to start.
-- MAGIC
-- MAGIC 1. Once the cluster is running, complete the steps above to select your cluster.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## A. Classroom Setup
-- MAGIC
-- MAGIC Run the following cell to configure your working environment for this course. This setup will reset your volume to one JSON file in each directory.
-- MAGIC
-- MAGIC **NOTE:** The `DA` object is only used in Databricks Academy courses and is not available outside of these courses. It will dynamically create and reference the information needed to run the course.

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-6

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## B. Explore the Customer Data Source Files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Run the cell below to programmatically view the files in your `/Volumes/dbacademy/ops/lab-user-name/customers` volume. Confirm you only see one **00.json** file for customers.

-- COMMAND ----------

-- DBTITLE 1,View files in the customers volume
-- MAGIC %python
-- MAGIC spark.sql(f'LIST "{DA.paths.working_dir}/customers"').display()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Run the query below to explore the customers **00.json** file located at `/Volumes/dbacademy/ops/lab-user-name/customers`. Note the following:
-- MAGIC
-- MAGIC    a. The file contains **939 customers** (remember this number).
-- MAGIC
-- MAGIC    b. It includes general customer information such as **email**, **name**, and **address**.
-- MAGIC
-- MAGIC    c. The **timestamp** column specifies the logical order of customer events in the source data.
-- MAGIC
-- MAGIC    d. The **operation** column indicates whether the entry is for a new customer, a deletion, or an update.
-- MAGIC       - **NOTE:** Since this is the first JSON file, all rows will be considered new customers.
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,Explore the customers raw JSON data
SELECT *
FROM read_files(
  DA.paths_working_dir || '/customers/00.json',
  format => "JSON"
)
ORDER BY operation;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Question: 
-- MAGIC How can we ingest new raw data source files (JSON) with customer updates into our pipeline to update the **customers_silver** table when inserts, updates, or deletes occur, without maintaining historical records (SCD Type 1)?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## C. Change Data Capture with AUTO CDC APIs in Lakeflow Declarative Pipelines

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Run the cell below to create your starter Lakeflow Declarative Pipeline for this demonstration. The pipeline will set the following for you:
-- MAGIC     - Your default catalog: `labuser`
-- MAGIC     - Your configuration parameter: `source` = `/Volumes/dbacademy/ops/your-labuser-name`
-- MAGIC
-- MAGIC     **NOTE:** If the pipeline already exists, an error will be returned. In that case, you'll need to delete the existing pipeline and rerun this cell.
-- MAGIC
-- MAGIC **NOTE:**  The `create_declarative_pipeline` function is a custom function built for this course to create the sample pipeline using the Databricks REST API. This avoids manually creating the pipeline and referencing the pipeline assets.

-- COMMAND ----------

-- DBTITLE 1,Create pipeline 6
-- MAGIC %python
-- MAGIC create_declarative_pipeline(pipeline_name=f'6 - Change Data Capture with AUTO CDC - {DA.catalog_name}', 
-- MAGIC                             root_path_folder_name='6 - Change Data Capture with AUTO CDC Project',
-- MAGIC                             catalog_name = DA.catalog_name,
-- MAGIC                             schema_name = 'default',
-- MAGIC                             source_folder_names=['orders', 'status', 'customers'],
-- MAGIC                             configuration = {'source':DA.paths.working_dir})

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Complete the following steps to open the starter Lakeflow Declarative Pipeline project for this demonstration:
-- MAGIC
-- MAGIC    a. In the main navigation bar right-click on **Jobs & Pipelines** and select **Open in Link in New Tab**.
-- MAGIC
-- MAGIC    b. In **Jobs & Pipelines** select your **6 - Change Data Capture with AUTO CDC - labuser** pipeline.
-- MAGIC
-- MAGIC    c. **REQUIRED:** At the top near your pipeline name, turn on **New pipeline monitoring**.
-- MAGIC
-- MAGIC    d. In the **Pipeline details** pane on the far right, select **Open in Editor** (field to the right of **Source code**) to open the pipeline in the **Lakeflow Pipeline Editor**.
-- MAGIC    
-- MAGIC    e. In the new tab you should see five folders: 
-- MAGIC       - **explorations**
-- MAGIC       - **orders**
-- MAGIC       - **status**
-- MAGIC       - **customers**
-- MAGIC       - Plus the extra **python_excluded** folder that contains the Python version. 
-- MAGIC
-- MAGIC    f. Open the **customers** folder and select the **customers_pipeline.sql** file.
-- MAGIC       - **NOTE:** The **status** and **orders** pipelines are the same as we saw in the previous demonstrations.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## D. Lakeflow Declarative Pipeline CDC SCD Type 1 Pipeline Steps
-- MAGIC Follow the steps below using the **customers_pipeline.sql** file in the Lakeflow Pipelines editor.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### PLEASE COMPLETE FIRST: Click the 'Run Pipeline' button to execute the Pipeline
-- MAGIC 1. To save some time, let's run the entire pipeline for **status**, **orders** and **customers**. While the pipeline is running explore the code in the **customers_pipeline.sql** for the new customers flow.
-- MAGIC
-- MAGIC ##### While the pipeline is running continue through the steps below to review the customer pipeline code.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### STEP 1: JSON -> Bronze Ingestion
-- MAGIC The code in **STEP 1** of the **customers_pipeline.sql** file:
-- MAGIC    - We define a bronze streaming table named **customers_bronze_raw_demo6** using a data source configured with Auto Loader (`FROM STREAM`).
-- MAGIC    - Adds the table property `pipelines.reset.allowed = false` to prevent deletion of all ingested bronze data if a full refresh is triggered.
-- MAGIC    - Creates columns to capture the time of data ingestion and the source file name for each row.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### STEP 2: Create the Bronze Clean Streaming Table with Data Quality Enforcement
-- MAGIC ##### **NOTE:** This displays how you can use advanced data quality techniques with expectations. Advanced expectations are outside the scope of this course.
-- MAGIC
-- MAGIC The code in **STEP 2** of the **customers_pipeline.sql** file:
-- MAGIC
-- MAGIC - Adds three violation constraint actions: **WARN**, **DROP**, and **FAIL**. Each defines how to handle constraint violations.
-- MAGIC - Applies multiple conditions to a single constraint.
-- MAGIC - Uses a built-in SQL function within a constraint.
-- MAGIC
-- MAGIC #### About the data source:
-- MAGIC
-- MAGIC - The data is a CDC feed that contains **`INSERT`**, **`UPDATE`**, and **`DELETE`** operations for customers.  
-- MAGIC - REQUIREMENT: **UPDATE** and **INSERT** operations should contain valid entries for all fields.  
-- MAGIC - REQUIREMENT: **DELETE** operations should contain **`NULL`** values for all fields except the **timestamp**, **customer_id**, and **operation** fields.
-- MAGIC
-- MAGIC **NOTE:** To ensure only valid data reaches our silver table, we'll write a series of quality enforcement rules that allow expected null values in **DELETE** operations while rejecting bad data elsewhere.
-- MAGIC
-- MAGIC
-- MAGIC ### We'll break down each of these constraints below:
-- MAGIC
-- MAGIC ##### 1. **`valid_id`**
-- MAGIC This constraint will cause our transaction to fail if a record contains a null value in the **`customer_id`** field.
-- MAGIC
-- MAGIC ##### 2. **`valid_operation`**
-- MAGIC This constraint will drop any records that contain a null value in the **`operation`** field.
-- MAGIC
-- MAGIC ##### 3. **`valid_name`**
-- MAGIC This constraint will track any records that contain a null value in the **`name`** field. Because there is no additional instruction for what to do with invalid records, violating rows will be recorded in metrics but not dropped.
-- MAGIC
-- MAGIC ##### 4. **`valid_address`**
-- MAGIC This constraint checks if the **`operation`** field is **`DELETE`**; if not, it checks for null values in any of the 4 fields comprising an address. Because there is no additional instruction for what to do with invalid records, violating rows will be recorded in metrics but not dropped.
-- MAGIC
-- MAGIC ##### 5. **`valid_email`**
-- MAGIC This constraint uses regex pattern matching to check that the value in the **`email`** field is a valid email address. It contains logic to not apply this to records if the **`operation`** field is **`DELETE`** (because these will have a null value for the **`email`** field). Violating records are dropped.
-- MAGIC
-- MAGIC **NOTE:** When a record is going to be dropped, all values except the **customer_id** will be `null`.
-- MAGIC | address                               | city         | customer_id | email                    | name           | operation | state |
-- MAGIC |---------------------------------------|--------------|-------------|--------------------------|----------------|-----------|-------|
-- MAGIC | null                                  | null         | 23617       | null                     | null           | DELETE    | null  |
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### STEP 3: Processing CDC Data with **`AUTO CDC INTO`**
-- MAGIC Lakeflow Declarative Pipelines introduces a new syntactic structure for simplifying CDC feed processing: `AUTO CDC INTO` (formerly `APPLY CHANGES INTO`).
-- MAGIC
-- MAGIC The code in **STEP 3** of the **customers_pipeline.sql** file uses `AUTO CDC INTO` to:
-- MAGIC - Create the **2_silver_db.scd_type_1_customers_silver_demo6** streaming table if it doesn't exist,
-- MAGIC - Updates the **2_silver_db.scd_type_1_customers_silver_demo6** streaming table with updates, inserts and deletes using records from the **1_bronze_db.customers_bronze_clean_demo6** streaming table.
-- MAGIC
-- MAGIC #### Additional Notes
-- MAGIC **`AUTO CDC INTO`** has the following guarantees and requirements:
-- MAGIC - Performs incremental/streaming ingestion of CDC data
-- MAGIC - Provides simple syntax to specify one or many fields as the primary key for a table
-- MAGIC - Default assumption is that rows will contain inserts and updates
-- MAGIC - Can optionally apply deletes
-- MAGIC - Automatically orders late-arriving records using user-provided sequencing key (order to process rows)
-- MAGIC - Uses a simple syntax for specifying columns to ignore with the **`EXCEPT`** keyword
-- MAGIC - The default to applying changes is SCD Type 1. You can also use SCD Type 2 if you would like. We will focus on SCD Type 1.
-- MAGIC
-- MAGIC
-- MAGIC #### Documentation
-- MAGIC [AUTO CDC INTO (Lakeflow Declarative Pipelines)](https://docs.databricks.com/aws/en/dlt-ref/dlt-sql-ref-apply-changes-into)
-- MAGIC
-- MAGIC [The AUTO CDC APIs: Simplify change data capture with Lakeflow Declarative Pipelines](https://docs.databricks.com/aws/en/dlt/cdc)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### STEP 4: Explore the Customers Pipeline Graph
-- MAGIC After running the pipeline and reviewing the code cells, take time to explore the pipeline results for the **customers** flow following the steps below.
-- MAGIC
-- MAGIC **Run with 1 JSON File**
-- MAGIC
-- MAGIC ![demo6_cdc_run01.png](./Includes/images/demo6_cdc_run_1.png)
-- MAGIC
-- MAGIC <br></br>
-- MAGIC Notice the following:
-- MAGIC 1. In the **customers** flow in the pipeline graph, notice that **939** rows were streamed into the three streaming tables. 
-- MAGIC     - This is because all records are new and valid entries, they were ingested throughout the flow.
-- MAGIC
-- MAGIC 2. In the table window below, find the **scd1_type_1_customers_silver_demo06** table and note the following:
-- MAGIC
-- MAGIC     - The **Upserted** column indicates that all **939** rows were upserted into the table, as all rows are new.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### STEP 5: Explore the Customers Pipeline Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Run the query below to view the **customers_silver_demo6** streaming table (the table with SCD Type 1 updates, inserts and deletes). 
-- MAGIC
-- MAGIC     Notice the following after the first run ingestion the **00.json** file:
-- MAGIC
-- MAGIC    - The streaming table contains all **939 rows** from the **00.json** file, since they are all new customers being added to the target table.
-- MAGIC
-- MAGIC    - Each record was inserted into the empty streaming table.

-- COMMAND ----------

-- DBTITLE 1,Explore the CDC SCD1 Streaming Table
SELECT *
FROM 2_silver_db.scd_type_1_customers_silver_demo6;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Query the **scd_type_1_customers_silver_demo6** streaming table for the following **customer_id** values (*23225*, *23617*). 
-- MAGIC
-- MAGIC    Notice the following:
-- MAGIC       - **customer_id** = *23225*
-- MAGIC          - **Address**: `76814 Jacqueline Mountains Suite 815`  
-- MAGIC          - **State**: `TX`  
-- MAGIC       - **customer_id** = *23617*
-- MAGIC          - This customer exists in the first execution (in file **00.json**)

-- COMMAND ----------

-- DBTITLE 1,View a customer in the CDC ST
SELECT *
FROM 2_silver_db.scd_type_1_customers_silver_demo6
WHERE customer_id IN (23225, 23617);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## E. Land New Data to Your Data Source Volume
-- MAGIC Complete the following after executing and reviewing the **customers** pipeline flow that consistent of ingesting one file (**00.json**) from cloud storage.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Run the cell below to land a new JSON file to each volume (**customers**, **status** and **orders**) to simulate new files being added to your cloud storage locations.

-- COMMAND ----------

-- DBTITLE 1,Land a new file in your volume
-- MAGIC %python
-- MAGIC copy_file_for_multiple_sources(copy_n_files = 2, 
-- MAGIC                                sleep_set = 1,
-- MAGIC                                copy_from_source='/Volumes/dbacademy_retail/v01/retail-pipeline',
-- MAGIC                                copy_to_target = DA.paths.working_dir)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 2. Run the cell below to programmatically view the files in your `/Volumes/dbacademy/ops/labuser-name/customers` volume. Confirm your volume now contains the original **00.json** file and the new **01.json** file.

-- COMMAND ----------

-- DBTITLE 1,View files in your customers volume
-- MAGIC %python
-- MAGIC spark.sql(f'LIST "{DA.paths.working_dir}/customers"').display()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Run the cell to explore the raw data in the new **01.json** file prior to ingesting it in your pipeline. 
-- MAGIC
-- MAGIC    Notice the following:
-- MAGIC
-- MAGIC    - This file contains **23** rows.
-- MAGIC
-- MAGIC    - The **operation** column specifies **UPDATE**, **DELETE**, and **NEW** operations for customers.
-- MAGIC       - **In the new 01.json file there are**:
-- MAGIC          - 12 customers with **UPDATE** values
-- MAGIC          - 1 customer with a **DELETE** value
-- MAGIC          - 10 new customers with a **NEW** value
-- MAGIC
-- MAGIC    - In the results below, find the row with **customer_id** *23225* and note the following:
-- MAGIC
-- MAGIC       - The original address for **Sandy Adams** (from the streaming table, file **00.json**) was: `76814 Jacqueline Mountains Suite 815`, `TX`
-- MAGIC       - The updated address for **Sandy Adams** (from the file below) is: `512 John Stravenue Suite 239`, `TN`
-- MAGIC
-- MAGIC    - In the results below, find the row with **customer_id** *23617* and note the following:
-- MAGIC       - The **operation** for this customer is **DELETE**.
-- MAGIC       - When the **operation** column is delete, all other column values are `null`.

-- COMMAND ----------

-- DBTITLE 1,Explore the new 01.json file
SELECT *
FROM read_files(
  DA.paths_working_dir || '/customers/01.json',
  format => "JSON"
)
ORDER BY customer_id;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### E1. Go back to your pipeline and click **'Run pipeline'** button to ingest the new JSON file (**01.json**) incrementally and perform CDC SCD Type 1 on the **scd_type1_customers_silver_demo06** table.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## F. Explore the Customers Pipeline
-- MAGIC
-- MAGIC After you have explored and landed 1 new JSON file into each of your cloud data sources, complete the following to explore the **customers** flow in the **Pipeline graph**:
-- MAGIC
-- MAGIC a. 23 rows were read into the: 
-- MAGIC
-- MAGIC   - **customers_bronze_raw_demo06** streaming table
-- MAGIC   - **customers_bronze_clean_demo06** streaming table (all data quality checks passed)
-- MAGIC   - The pipeline only ingested and processed the NEW **01.json** file 
-- MAGIC
-- MAGIC b. In the **scd_type_1_customers_silver_demo6** streaming table details (The CDC SCD Type 1 table) it contains:
-- MAGIC   - **Upserted = 22**:
-- MAGIC     - 12 customers with UPDATE values (previous customer were simply updated with the new values)
-- MAGIC     - 10 new customers with a NEW value (new customers were inserted into the table)
-- MAGIC   - **Deleted records = 1**:
-- MAGIC     - 1 customer was marked as DELETE and deleted from the table
-- MAGIC
-- MAGIC ![Run 2](./Includes/images/demo6_cdc_run_2.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## G. Explore the CDC SCD Type 1 on the scd_type_1_customers_silver_demo6 Streaming Table

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. View the data in the **scd_type_1_customers_silver_demo6** streaming table with SCD Type 1 and observe the following:
-- MAGIC
-- MAGIC    a. The table contains **948 rows**:
-- MAGIC       - **initial 939 customers** 
-- MAGIC       - \+ **10** new customers
-- MAGIC       - \- **1** deleted customer
-- MAGIC       - **NOTES:** 
-- MAGIC          - The **12** updates to original customers were made in place and updated the original record (SCD Type 1 does not keep historical records).
-- MAGIC          - The **1** record marked for deletion was deleted from the table.

-- COMMAND ----------

-- DBTITLE 1,View the updated CDC ST
SELECT customer_id, address, name
FROM 2_silver_db.scd_type_1_customers_silver_demo6;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 3. Query the **2_silver_db.scd_type_1_customers_silver_demo6** table for the following **customer_id** values: *23225* and *23617*. These were the values we reviewed earlier.  
-- MAGIC
-- MAGIC     Notice the following:  
-- MAGIC
-- MAGIC     - **customer_id** *23225* has been updated to the new address. The historical address was not retained because we used SCD Type 1.  
-- MAGIC     - **customer_id** *23617* has been deleted from the table. It no longer exists because we used SCD Type 1.  
-- MAGIC

-- COMMAND ----------

-- DBTITLE 1,View the updated customer 23225
SELECT *
FROM 2_silver_db.scd_type_1_customers_silver_demo6
WHERE customer_id IN (23225, 23617);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Additional Resources
-- MAGIC
-- MAGIC - [What is change data capture (CDC)?](https://docs.databricks.com/aws/en/dlt/what-is-change-data-capture)
-- MAGIC
-- MAGIC - [AUTO CDC INTO (Lakeflow Declarative Pipelines)](https://docs.databricks.com/gcp/en/dlt-ref/dlt-sql-ref-apply-changes-into) documentation
-- MAGIC
-- MAGIC - [The AUTO CDC APIs: Simplify change data capture with Lakeflow Declarative Pipelines](https://docs.databricks.com/aws/en/dlt/cdc) documentation
-- MAGIC
-- MAGIC - [How to implement Slowly Changing Dimensions when you have duplicates - Part 1: What to look out for?](https://community.databricks.com/t5/technical-blog/how-to-implement-slowly-changing-dimensions-when-you-have/ba-p/40568)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC &copy; 2025 Databricks, Inc. All rights reserved. Apache, Apache Spark, Spark, the Spark Logo, Apache Iceberg, Iceberg, and the Apache Iceberg logo are trademarks of the <a href="https://www.apache.org/" target="_blank">Apache Software Foundation</a>.<br/><br/><a href="https://databricks.com/privacy-policy" target="_blank">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use" target="_blank">Terms of Use</a> | <a href="https://help.databricks.com/" target="_blank">Support</a>
