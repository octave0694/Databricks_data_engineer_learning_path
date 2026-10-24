# Databricks notebook source
# MAGIC %run ./Classroom-Setup-Common

# COMMAND ----------

# MAGIC %run ./Classroom-Setup-Common-Install-CLI

# COMMAND ----------

check_if_catalogs_are_created(check_catalogs=['dev', 'stage', 'prod'])

## Create the DA keys for the user's catalogs
DA.create_DA_keys()

## Display the course catalog and schema name for the user.
DA.display_config_values(
  [
    ('DEV catalog reference: DA.catalog_dev', DA.catalog_dev)
   ]
)

# COMMAND ----------

create_taxi_dev_data()
