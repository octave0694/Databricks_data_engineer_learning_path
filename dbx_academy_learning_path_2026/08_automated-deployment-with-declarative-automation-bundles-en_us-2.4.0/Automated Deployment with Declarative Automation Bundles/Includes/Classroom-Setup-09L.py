# Databricks notebook source
# MAGIC %run ./Classroom-Setup-Common-Python

# COMMAND ----------

# MAGIC %run ./Classroom-Setup-Common-Install-CLI

# COMMAND ----------

check_if_catalogs_are_created(check_catalogs=[catalog_dev, catalog_stage, catalog_prod])


## Delete NYC tables to start with nothing
del_table(catalog=catalog_dev, schema='default', table='nyctaxi_bronze')
del_table(catalog=catalog_dev, schema='default', table='nyctaxi_silver')
del_table(catalog=catalog_prod, schema='default', table='nyctaxi_bronze')
del_table(catalog=catalog_prod, schema='default', table='nyctaxi_silver')


create_taxi_dev_data()
create_taxi_prod_data()


## Display the course catalog and schema name for the user.
display_config_values(
  [
    ('DEV catalog reference: catalog_dev', catalog_dev),
    ('STAGE catalog reference: catalog_stage', catalog_stage),
    ('PROD catalog reference: catalog_prod', catalog_prod)
   ]
)


compute_validation(recommend_dbr_classic_version = 17.3)
