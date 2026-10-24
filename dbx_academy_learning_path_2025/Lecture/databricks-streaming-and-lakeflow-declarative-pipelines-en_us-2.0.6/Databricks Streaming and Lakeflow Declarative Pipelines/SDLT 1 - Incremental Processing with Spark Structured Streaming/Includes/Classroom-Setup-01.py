# Databricks notebook source
# MAGIC %run ./Classroom-Setup-Common

# COMMAND ----------

DA = DBAcademyHelper()
DA.init()

DA.display_config_values([('Course Catalog',DA.catalog_name),('Your Schema',DA.schema_name)])
# setattr(DA, 'paths.storage_location', f'{DA.paths.working_dir}/storage_location')

# COMMAND ----------

DA.paths.datasets.ecommerce = "/Volumes/dbacademy_ecommerce/v01"

# COMMAND ----------

DA.paths.sales = f"{DA.paths.datasets.ecommerce}/delta/sales_hist"
DA.paths.users = f"{DA.paths.datasets.ecommerce}/delta/users_hist"
DA.paths.events = f"{DA.paths.datasets.ecommerce}/delta/events_hist"
DA.paths.products = f"{DA.paths.datasets.ecommerce}/delta/item_lookup"

# COMMAND ----------

# DA.display_config_values([('Course Catalog',DA.catalog_name),('Your Schema',DA.schema_name)])
