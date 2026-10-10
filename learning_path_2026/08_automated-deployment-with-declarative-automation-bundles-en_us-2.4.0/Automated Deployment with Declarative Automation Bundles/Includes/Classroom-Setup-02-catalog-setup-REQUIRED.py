# Databricks notebook source
# MAGIC %run ./Classroom-Setup-Common-Python

# COMMAND ----------

# DBTITLE 1,Create Catalogs
def create_catalogs(catalog_names: list):
    """
    Ensure each catalog in `catalog_names` exists in the workspace.

    For each catalog:
        - If it already exists: prints a note that it exists.
        - If it does not exist and was created: prints a note that it was created.
        - If creation fails (typically because the user lacks the CREATE CATALOG
          privilege in this workspace): prints an ERROR note and continues with
          the next catalog.

    Args:
    -------
        catalog_names (list): A list of full catalog names to ensure exist.
                              The strings are used as-is, no prefix or suffix
                              is appended.

    Returns:
    -------
        None. Prints one log line per catalog.

    Example:
    -------
        catalogs = ['my_catalog_dev', 'my_catalog_prod', 'my_catalog_test']
        create_catalogs(catalogs)
    """
    list_of_curr_catalogs = {c.name for c in spark.catalog.listCatalogs()}

    for catalog_name in catalog_names:
        if catalog_name in list_of_curr_catalogs:
            print(f"Catalog {catalog_name!r} already exists. Skipping creation.")
            continue

        try:
            spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
            print(f"Created catalog: {catalog_name!r}.")
        except Exception as e:
            msg = str(e)
            if (
                "PERMISSION_DENIED" in msg
                or "INSUFFICIENT_PERMISSIONS" in msg
                or "does not have permission" in msg.lower()
                or "not authorized" in msg.lower()
            ):
                print(
                    f"ERROR: Could not create catalog {catalog_name!r}. "
                    f"You do not have permission to create catalogs in this workspace. "
                    f"Ask an admin to grant CREATE CATALOG on the metastore, "
                    f"or use a workspace where you have that privilege."
                )
            else:
                print(f"ERROR: Could not create catalog {catalog_name!r}. {e}")

# COMMAND ----------

# DBTITLE 1,Create Volumes
def create_volumes(in_catalog: str, in_schema: str, vol_names: list):
    """
    Ensure each volume in `vol_names` exists in `in_catalog.in_schema`.

    For each volume:
        - If it already exists: prints a note.
        - If it does not exist and was created: prints a note.
        - If creation fails (commonly because the user lacks the CREATE VOLUME
          privilege on the schema): prints an ERROR note and continues with
          the next volume.

    Args:
    -------
        in_catalog (str): The catalog the schema lives in.
        in_schema (str):  The schema to create the volumes in.
        vol_names (list): A list of volume names to ensure exist. Names are
                          used as-is.

    Returns:
    -------
        None. Prints one log line per volume.

    Example:
    -------
        create_volumes(
            in_catalog=catalog_dev_1,
            in_schema='default',
            vol_names=['health']
        )
    """
    fq_schema = f"{in_catalog}.{in_schema}"

    current_volumes = {
        row["volume_name"]
        for row in spark.sql(f"SHOW VOLUMES IN {fq_schema}").collect()
    }

    for vol in vol_names:
        fq_volume = f"{fq_schema}.{vol}"

        if vol in current_volumes:
            print(f"Volume {fq_volume!r} already exists. Skipping creation.")
            continue

        try:
            spark.sql(f"CREATE VOLUME IF NOT EXISTS {fq_volume}")
            print(f"Created volume: {fq_volume!r}.")
        except Exception as e:
            msg = str(e)
            if (
                "PERMISSION_DENIED" in msg
                or "INSUFFICIENT_PERMISSIONS" in msg
                or "does not have permission" in msg.lower()
                or "not authorized" in msg.lower()
            ):
                print(
                    f"ERROR: Could not create volume {fq_volume!r}. "
                    f"You do not have permission to create volumes in {fq_schema!r}. "
                    f"Ask an admin to grant CREATE VOLUME on the schema."
                )
            else:
                print(f"ERROR: Could not create volume {fq_volume!r}. {e}")


# COMMAND ----------

# DBTITLE 1,Create SDF
def create_spark_data_frame_from_cdc(cdc_csv_file_path):
    '''
    Create the DataFrame used to create the CSV files for the course.
    '''
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import rand, when, lit, to_date, monotonically_increasing_id, col, coalesce
    from pyspark.sql.types import StringType
    import uuid

    ##
    ## Generate a column with a unique id (using it as a 'fake' PII column)
    ##

    # Define a UDF to generate deterministic UUID based on the row index
    def generate_deterministic_uuid(index):
        # Use UUID5 or UUID3 based on a namespace and the row index as the name
        return str(uuid.uuid5(uuid.NAMESPACE_DNS, str(index)))  # You can use uuid3 as well

    # Register UDF
    generate_deterministic_uuid_udf = udf(generate_deterministic_uuid, StringType())

    ##
    ## Create spark dataframe with required columns and values to save CSV files.
    ##
    sdf = (spark
        .read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(cdc_csv_file_path)
        .repartition(1)
        .withColumn("ID", monotonically_increasing_id())
        .select("ID",
                generate_deterministic_uuid_udf(monotonically_increasing_id()).alias('PII'),
                when(col("ID") < 15121, "2025-01-01")
                    .when(col("ID") < 40351, "2025-01-02")
                    .otherwise("2025-01-03")
                    .alias("date"),
                when(col("HighChol") < .2, 0)  # when value is less than .2
                    .when((col("HighChol").cast('float') >= .2) & (col("HighChol") < 1.03), 1) # when value is between .2 and 1.03 
                    .otherwise(2)              # else when value is greater than or equal to 0.9
                    .alias("HighCholest"),
                "HighBP", 
                "BMI",
                "Age",
                "Education",
                "income",
                "Diabetes_binary" # Included for ML model prediction       
        )
    )

    return sdf

# COMMAND ----------

# DBTITLE 1,Create Course CSVs
def create_course_csv_files(dataframe, 
                            prod_volume_write_path: str, 
                            stage_volume_write_path: str,
                            dev_volume_write_path: str,
                            data_filter_conditions: list, 
                            data_name_append: str, 
                            del_files_first: bool = True):
    '''
    This method will first delete all files (by default) in the specified prod, stage and dev volume paths.

    Then it will create the CSV files for the course in the following volumes:
        - user dev/volume/health/ 1 csv file with 15% of the rows
        - user stage volume/volume/health/ 1 csv file with 40% of the rows
        - user prod volume/volume/health/ 3 csv files all files
    '''
    from pyspark.sql.functions import rand, when, lit, to_date, monotonically_increasing_id, col, coalesce

    print('\n----Creating the CSV files in the necessary volumes for the course----\n')

    ##
    ## Delete the files in the volume prior to creating the CSV files.
    ##
    if del_files_first == True:
        print('---DELETE FILES THE VOLUMES IN THE DEV, STAGE AND PROD CATALOGS----')
        delete_source_files(source_files = prod_volume_write_path)
        delete_source_files(source_files = stage_volume_write_path)
        delete_source_files(source_files = dev_volume_write_path)


    ## Utility method to delete files that begin with an underscore when creating files in a volume from a spark data frame.
    def util_del_files_with_underscore_in_volume(volume_path: str):
        """
        Deletes files in the specified volume (directory) whose names start with an underscore ('_').

        This method lists all files in the given volume directory, filters out the ones that start 
        with an underscore, and deletes them using the `dbutils.fs.rm()` function cleaning up a 
        volume when creating files from a spark data frame.

        Parameters:
        ----------
            volume_path (str): The path to the volume (directory) where the files are located.

        Returns:
        -------
            Output information in the log.
        
        Example:
        --------
        >>> del_files_with_underscore_in_volume("/Volumes/inquisitive_layer_stage_2/default/health")
        """ 
        
        ## List files in specified volume
        files = dbutils.fs.ls(volume_path)

        # Filter files that start with an underscore '_'
        files_to_remove = [file.path for file in files if file.name.startswith('_')]
        
        ## If no files found, pass. Otherwise delete files.
        if len(files_to_remove) == 0:
            pass
        else: 
            print(f'Deleting files with underscores in: {volume_path}:')
            for file_path in files_to_remove:
                dbutils.fs.rm(file_path)


    ##
    ## Utility method to renameCSV files based on filter conditions in the PROD catalog volume
    ##
    def util_rename_csv_file(volume_path: str, file_prefix: str, file_suffix: str):
        """
        Renames CSV files in a specified volume path that start with 'part-' by appending 
        a date filter condition and a custom name suffix.

        `<date_filter_condition>_<data_name_append>.csv`. The renaming is performed using the `os.rename` method.

        Parameters:
        ----------
        volume_path (str): The directory path where the CSV files are located.
        date_filter_condition (str): A date or condition to be added at the beginning of the new file name.
        data_name_append (str): A suffix to append to the new file name before the `.csv` extension.

         Returns:
        -------
            Output information in the log.

        Example:
        --------
        >>> rename_csv_file('/Volumes/inquisitive_layer_stage_2/default/health', '2025-01-01', 'sales_data')
        """
        import os

        output_files = os.listdir(volume_path)

        for file in output_files:
            if file.startswith('part-'):
                file_to_rename = f'{volume_path}/{file}'
                new_csv_file_name = f'{file_prefix}_{file_suffix}.csv'
                rename_file_to = f'{volume_path}/{new_csv_file_name}'  

                print(f'Renaming CSV file: {file_to_rename}')
                print(f'New CSV file name: {rename_file_to}')
                os.rename(file_to_rename, rename_file_to)
                print(f'Created CSV file: {new_csv_file_name}')
        print('\n')


    ###
    ### Create the prod csv files based on filter
    ###
    def create_prod_csv_files():
        """
        Creates and writes CSV files for each filter condition in the provided list of filter conditions. All arguments
        are obtained from the main outer method.

        This function takes a DataFrame, filters it by each date condition in the `data_filter_conditions` list, 
        writes each filtered DataFrame as a single CSV file to the specified directory (`prod_volume_write_path`).
        Each CSV file is written with headers and appended in case the directory already contains data.

        Parameters (obtained from outer/main method):
        ----------
            - data_filter_conditions (list): A list of date filter conditions to filter the DataFrame by.
            - dataframe (DataFrame): The Spark DataFrame to be filtered and written to CSV.
            - prod_volume_write_path (str): The destination path where the CSV files will be written.

        Returns:
        -------
            Prints output in the log.

        Example:
        >>> create_prod_csv_files()
        """
        print('-------------------------------------------------------')
        print(f'--- Creating CSV files in PROD: {prod_volume_write_path}')
        print('-------------------------------------------------------')
        for filter in data_filter_conditions:
            (dataframe
             .filter(col("date") == filter)
             .repartition(1)   ## One CSV file per date
             .write
             .option("header", "true")
             .mode('append')
             .csv(prod_volume_write_path)
            )

            ## Del files with underscores after writing the csv files
            util_del_files_with_underscore_in_volume(prod_volume_write_path)

            ## Rename file with the filter condition + data_name_append
            util_rename_csv_file(prod_volume_write_path, file_prefix=filter, file_suffix=data_name_append)
            

        print(f'Finished creating CSV files in PROD: {prod_volume_write_path} \n')



    
    ###
    ### Create the dev csv file
    ###
    def create_dev_csv_files():
        """
        Creates and writes a CSV file that is a subset of the orginal with the dev catalog

        Parameters (obtained from outer/main method):
        ----------
            - dataframe (DataFrame): The Spark DataFrame to be filtered and written to CSV.

        Returns:
        -------
            Prints output in the log.

        Example:
        >>> create_dev_csv_files(dataframe)
        """
        print('-------------------------------------------------------')
        print(f'--- Creating CSV files in DEV: {dev_volume_write_path}')
        print('-------------------------------------------------------')
        (dataframe
         .filter(col("ID") <= 7499)
         .repartition(1)   ## One CSV file per date
         .withColumn("PII", lit("********"))
         .write
         .option("header", "true")
         .mode('append')
         .csv(dev_volume_write_path)
        )

        ## Del files with underscores after writing the csv files
        util_del_files_with_underscore_in_volume(dev_volume_write_path)

        ## Rename file with the filter condition + data_name_append
        ## Rename file with the filter condition + data_name_append
        util_rename_csv_file(dev_volume_write_path, file_prefix="dev", file_suffix=data_name_append)

        print(f'Finished creating CSV file in DEV: {dev_volume_write_path} \n')



    def create_stage_csv_files():
        """
        Creates and writes a CSV file that is a subset of the orginal with the stage catalog

        Parameters (obtained from outer/main method):
        ----------
            - dataframe (DataFrame): The Spark DataFrame to be filtered and written to CSV.

        Returns:
        -------
            Prints output in the log.

        Example:
        >>> create_dev_csv_files(dataframe)
        """
        print('-------------------------------------------------------')
        print(f'--- Creating CSV files in STAGE: {stage_volume_write_path}')
        print('-------------------------------------------------------')
        (dataframe
         .filter(col("ID") <= 34999)
         .repartition(1)   ## One CSV file per date
         .write
         .option("header", "true")
         .mode('append')
         .csv(stage_volume_write_path)
        )

        ## Del files with underscores after writing the csv files
        util_del_files_with_underscore_in_volume(stage_volume_write_path)

        ## Rename file with the filter condition + data_name_append
        ## Rename file with the filter condition + data_name_append
        util_rename_csv_file(stage_volume_write_path, file_prefix="stage", file_suffix=data_name_append)

        print(f'Finished creating CSV file in STAGE: {stage_volume_write_path} \n')



    ## CREATE PROD CSV FILES FOR PROD DEV STAGE
    create_prod_csv_files()
    create_stage_csv_files()
    create_dev_csv_files()

# COMMAND ----------

# DBTITLE 1,Run Setup
################################
## Setup course environment
################################

################################
## Create user specific catalogs
################################
list_of_catalogs = [catalog_dev, catalog_stage, catalog_prod]
create_catalogs(list_of_catalogs)


# ######################################
# ## Create the volumes in each catalog
# ######################################
create_volumes(in_catalog=catalog_dev, in_schema='default', vol_names=['health'])
create_volumes(in_catalog=catalog_stage, in_schema='default', vol_names=['health'])
create_volumes(in_catalog=catalog_prod, in_schema='default', vol_names=['health'])


# ######################################
# ## Move and create the CSV files
# ######################################

## Set Data Volume Path
prod_vol_path = f'/Volumes/{catalog_prod}/default/health'
stage_vol_path = f'/Volumes/{catalog_stage}/default/health'
dev_vol_path = f'/Volumes/{catalog_dev}/default/health'

## Staging area for the main file
vol_path = f'/Volumes/{catalog_dev}/default/csv_file_staging'

## FIND DEMO DATA IN WORKSPACE FOLDER
try:
    data_path = find_folder('Includes/data/')
except:
    data_path = find_folder('data/')

copy_workspace_files_to_volume(
    src_workspace_folder=f'{data_path}',
    target_volume_path=vol_path,
    n=1
)

# COMMAND ----------


## Create the Spark dataframe to use
sdf = create_spark_data_frame_from_cdc(f"{vol_path}/diabetes_binary_5050_raw.csv")


## Create the CSV files for the course.
create_course_csv_files(dataframe=sdf, 
                        prod_volume_write_path = prod_vol_path, 
                        stage_volume_write_path = stage_vol_path,
                        dev_volume_write_path = dev_vol_path,
                        data_filter_conditions = ["2025-01-01", "2025-01-02","2025-01-03"], 
                        data_name_append = "health", 
                        del_files_first = True)

# COMMAND ----------

## Display the course catalog and schema name for the user.
display_config_values(
  [
    ('DEV catalog variable: "catalog_dev"', catalog_dev),
    ('STAGE catalog variable: "catalog_stage"', catalog_stage),
    ('PROD catalog variable: "catalog_prod"', catalog_prod)
   ]
)
