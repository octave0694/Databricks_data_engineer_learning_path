# Databricks notebook source
# MAGIC %run ./Classroom-Setup-Common-Python

# COMMAND ----------

# MAGIC %run ./Classroom-Setup-Common-Install-CLI

# COMMAND ----------

check_if_catalogs_are_created(check_catalogs=[catalog_dev, catalog_stage, catalog_prod])

create_taxi_dev_data()

## Display the course catalog and schema name for the user.
display_config_values(
  [
    ('DEV catalog reference: catalog_dev', catalog_dev),
    ('STAGE catalog reference: catalog_stage', catalog_stage),
    ('PROD catalog reference: catalog_prod', catalog_prod)
   ]
)

compute_validation(recommend_dbr_classic_version = 17.3)
