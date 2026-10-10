# Databricks notebook source
# MAGIC %run ./Classroom-Setup-Common-Python

# COMMAND ----------

# MAGIC %run ./Classroom-Setup-Common-Install-CLI

# COMMAND ----------

check_if_catalogs_are_created(check_catalogs=[catalog_dev, catalog_stage, catalog_prod])


## Display the course catalog and schema name for the user.
display_config_values(
  [
    ('DEV catalog variable: "catalog_dev"', catalog_dev),
    ('STAGE catalog variable: "catalog_stage"', catalog_stage),
    ('PROD catalog variable: "catalog_prod"', catalog_prod)
   ]
)

compute_validation(recommend_dbr_classic_version = 17.3)
