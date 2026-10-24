# Databricks notebook source
# MAGIC %run ./Classroom-Setup-Common

# COMMAND ----------

DA.display_config_values([('Course Catalog',DA.catalog_name),('Your Schema',DA.schema_name)])

# COMMAND ----------

import os

my_query = f'''
CREATE OR REPLACE TABLE {DA.catalog_name}.{DA.schema_name}.sales_bronze 
AS
SELECT * FROM dbacademy_retail.v01.sales'''

folder_path = os.path.join(os.getcwd(), "Task Files", "Lesson 1 Files")
new_filename = "1.2 - Creating sales table - SQL Query"

create_sql_editor_file(sql_query = my_query, 
                       folder_path = folder_path, 
                       filename = new_filename,
                       default_catalog=f'{DA.catalog_name}',
                       default_schema=f'{DA.schema_name}')
