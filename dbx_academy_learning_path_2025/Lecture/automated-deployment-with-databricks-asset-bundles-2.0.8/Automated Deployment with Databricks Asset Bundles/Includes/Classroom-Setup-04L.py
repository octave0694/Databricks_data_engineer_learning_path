# Databricks notebook source
# MAGIC %run ./Classroom-Setup-Common

# COMMAND ----------

# MAGIC %run ./Classroom-Setup-Common-Install-CLI

# COMMAND ----------

check_if_catalogs_are_created(check_catalogs=['dev', 'stage', 'prod'])

## Create the DA keys for the user's catalogs
DA.create_DA_keys()


## Delete NYC tables to start with nothing
del_table(catalog=DA.catalog_dev, schema='default', table='nyctaxi_bronze')
del_table(catalog=DA.catalog_dev, schema='default', table='nyctaxi_silver')
del_table(catalog=DA.catalog_prod, schema='default', table='nyctaxi_bronze')
del_table(catalog=DA.catalog_prod, schema='default', table='nyctaxi_silver')


## Display the course catalog and schema name for the user.
DA.display_config_values(
  [
    ('DEV catalog reference: DA.catalog_dev', DA.catalog_dev),
    ('STAGE catalog reference: DA.catalog_stage', DA.catalog_stage),
    ('PROD catalog reference: DA.catalog_prod', DA.catalog_prod)
   ]
)

# COMMAND ----------


create_taxi_dev_data()
create_taxi_prod_data()
