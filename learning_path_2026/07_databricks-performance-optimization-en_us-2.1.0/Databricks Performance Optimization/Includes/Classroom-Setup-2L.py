# Databricks notebook source
# MAGIC %run ./Classroom-Setup-Common

# COMMAND ----------

## Set user catalog
my_catalog = build_user_catalog(catalog_forced = None) ## <-- Forces the usage of a catalog if you can't create one. Catalog is assumed to exist. Reference as 'string'.

# COMMAND ----------

## Creating Schema 
schema = 'perf_opt_lab'
print(f'Creating schema {my_catalog}.{schema}...')
spark.sql(f'CREATE SCHEMA IF NOT EXISTS {my_catalog}.{schema}')

## Using my_catalog
spark.sql(f"USE CATALOG {my_catalog}")

## Using newly created schema
spark.sql(f"USE SCHEMA {schema}")

## Display the course catalog and schema name for the user.
display_config_values(
  [('Course Catalog', my_catalog),
   ('Your Schema', schema),
   ('Airline Data Catalog', 'dbacademy_flightdata'),
   ('Airline Schema', 'flightdata')
   ]
)

# COMMAND ----------

setup_complete_msg()