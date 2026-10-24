# Databricks notebook source
# MAGIC %run ../../Includes/_common

# COMMAND ----------

# DBTITLE 1,Create DA Keys
@DBAcademyHelper.add_method
def create_DA_keys(self): 
    '''
    Create the DA references to the dev, prod and stage catalogs for the user.
    '''
    print('Set DA dynamic references to the dev, stage and prod catalogs.\n')
    setattr(DA, f'catalog_dev', f'{self.catalog_name}_1_dev')
    setattr(DA, f'catalog_stage', f'{self.catalog_name}_2_stage')
    setattr(DA, f'catalog_prod', f'{self.catalog_name}_3_prod')

# COMMAND ----------

# DBTITLE 1,Check Catalogs
def check_if_catalogs_are_created(check_catalogs: list[str]):
    '''
    Search for the dev, stage, prod catalogs by default. Return error if those don't exist.
    '''

    list_of_catalogs = spark.sql('SHOW CATALOGS')
    end_of_catalog_names = set(list_of_catalogs.toPandas().catalog.str.split('_').str[-1].to_list())
    
    # Convert check_catalogs to a set
    check_catalogs_set = set(check_catalogs)
    
    # Check if all items are in the predefined items set
    missing_items = check_catalogs_set - end_of_catalog_names
    
    if missing_items:
        # If there are any missing items, raise an error
        raise ValueError(f"Necessary catalogs do not exist. Please run the 0 - REQUIRED - Course Setup and Authentication notebook to setup your environment.")
    
    # If all items are found, return True
    print('Catalog check for the labs passed.')

# COMMAND ----------

# DBTITLE 1,Delete Files
@DBAcademyHelper.add_method
def delete_source_files(self, source_files):
        """
        Deletes all files in the specified source volume.

        This function iterates through all the files in the given volume,
        deletes them, and prints the name of each file being deleted.

        Parameters:
        ----------
        source_files : str, optional
            The path to the volume containing the files to delete. 
            Use the {DA.paths.working_dir} to dynamically navigate to the user's volume location in dbacademy/ops/vocareumlab@name:
                Example: DA.paths.working_dir = /Volumes/dbacademy/ops/vocareumlab@name

        Returns:
        -------
        None
            This function does not return any value. It performs file deletion as a side effect and prints all files that it deletes.

        Example:
        --------
        delete_source_files(f'{DA.paths.working_dir}/pii/stream_source/user_reg')
        """
        
        import os
        
        print(f'\nSearching for files in {source_files} volume to delete prior to creating files...')
        if os.path.exists(source_files):
            list_of_files = sorted(os.listdir(source_files))
        else:
            list_of_files = None

        if not list_of_files:  # Checks if the list is empty.
            print(f"No files found in {source_files}.\n")
        else:
            for file in list_of_files:
                file_to_delete = source_files + '/' + file
                print(f'Deleting file: {file_to_delete}')
                dbutils.fs.rm(file_to_delete)

# COMMAND ----------

import os

class DeclarativePipelineCreator:
    """
    A class to create a Lakeflow Declarative DLT pipeline using the Databricks REST API.

    Attributes:
    -----------
    pipeline_name : str
        Name of the pipeline to be created.
    root_path_folder_name : str
        The folder containing the pipeline code relative to current working directory.
    source_folder_names : list
        List of subfolders inside the root path containing source notebooks or scripts.
    catalog_name : str
        The catalog where the pipeline tables will be stored.
    schema_name : str
        The schema (aka database) under the catalog.
    serverless : bool
        Whether to use serverless compute.
    configuration : dict
        Optional key-value configurations passed to the pipeline.
    continuous : bool
        If True, enables continuous mode (streaming).
    photon : bool
        Whether to use Photon execution engine.
    channel : str
        The DLT release channel to use (e.g., "PREVIEW", "CURRENT").
    development : bool
        Whether to run the pipeline in development mode.
    pipeline_type : str
        Type of pipeline (e.g., 'WORKSPACE').
    """

    def __init__(self,
                 pipeline_name: str,
                 root_path_folder_name: str,
                 catalog_name: str,
                 schema_name: str,
                 source_folder_names: list = None,
                 serverless: bool = True,
                 configuration: dict = None,
                 continuous: bool = False,
                 photon: bool = True,
                 channel: str = 'PREVIEW',
                 development: bool = True,
                 pipeline_type: str = 'WORKSPACE'):

        # Assign all input arguments to instance attributes
        self.pipeline_name = pipeline_name
        self.root_path_folder_name = root_path_folder_name
        self.source_folder_names = source_folder_names or []
        self.catalog_name = catalog_name
        self.schema_name = schema_name
        self.serverless = serverless
        self.configuration = configuration or {}
        self.continuous = continuous
        self.photon = photon
        self.channel = channel
        self.development = development
        self.pipeline_type = pipeline_type

        # Instantiate the WorkspaceClient to communicate with Databricks REST API
        self.workspace = WorkspaceClient()
        self.pipeline_body = {}

    def _check_pipeline_exists(self):
        """
        Checks if a pipeline with the same name already exists.
        Raises:
            ValueError if the pipeline already exists.
        """
        for pipeline in self.workspace.pipelines.list_pipelines():
            if pipeline.name == self.pipeline_name:
                raise ValueError(
                    f"Lakeflow Declarative Pipeline name '{self.pipeline_name}' already exists. "
                    "Please delete the pipeline using the UI and rerun to recreate."
                )

    def _build_pipeline_body(self):
        """
        Constructs the body of the pipeline creation request based on class attributes.
        """
        # Get current working directory
        cwd = os.getcwd()

        # Source folder is not in current working directory, so go up two level (Only for this course)
        main_course_folder = os.path.dirname(os.path.dirname(cwd))

        # Create full path to root folder
        root_path_folder = os.path.join('/', main_course_folder, self.root_path_folder_name)

        # Convert source folder names into glob pattern paths for the DLT pipeline
        source_paths = [os.path.join(main_course_folder, folder) for folder in self.source_folder_names]
        libraries = [{'glob': {'include': path}} for path in source_paths]

        # Build dictionary to be sent in the API request
        self.pipeline_body = {
            'name': self.pipeline_name,
            'pipeline_type': self.pipeline_type,
            'root_path': root_path_folder,
            'libraries': libraries,
            'catalog': self.catalog_name,
            'schema': self.schema_name,
            'serverless': self.serverless,
            'configuration': self.configuration,
            'continuous': self.continuous,
            'photon': self.photon,
            'channel': self.channel,
            'development': self.development
        }

    def create_pipeline(self):
        """
        Creates the pipeline on Databricks using the defined attributes.

        Returns:
            dict: The response from the Databricks API after creating the pipeline.
        """
        # Check for name conflicts
        self._check_pipeline_exists()

        # Build the body of the API request and creates self.pipeline_body variable
        self._build_pipeline_body()

        # Display information to user
        print(f"Creating the Lakeflow Declarative Pipeline '{self.pipeline_name}'...")
        print(f"Root folder path: {self.pipeline_body['root_path']}")
        print(f"Source folder path(s): {self.pipeline_body['libraries']}")

        # Make the API call
        self.response = self.workspace.api_client.do('POST', '/api/2.0/pipelines', body=self.pipeline_body)

        # Notify of completion
        print(f"\nLakeflow Declarative Pipeline Creation '{self.pipeline_name}' Complete!")

        return self.response

    def get_pipeline_id(self):
        """
        Returns the ID of the created pipeline.
        """
        if not hasattr(self, 'response'):
            raise RuntimeError("Pipeline has not been created yet. Call create_pipeline() first.")

        return self.response.get("pipeline_id")

    def start_pipeline(self):
        '''
        Starts the pipeline using the attribute set from the generate_pipeline() method.
        '''
        print('Started the pipeline run. Navigate to Jobs and Pipelines to view the pipeline.')
        self.workspace.pipelines.start_update(self.get_pipeline_id())

# COMMAND ----------

# DBTITLE 1,DAJobConfig
import os
from databricks.sdk.service import jobs, pipelines
from databricks.sdk import WorkspaceClient  

class DAJobConfig:
    '''
    Example
    ------------
    job_tasks = [
        {
            'task_name': 'create_table',
            'notebook_path': '/01 - Simple DAB/create_table',
            'depends_on': None
        },
        {
            'task_name': 'create_table1',
            'notebook_path': '/01 - Simple DAB/other_table2',
            'depends_on': [{'task_key': 'create_table'}]
        },
        {
            'task_name': 'create_table3',
            'notebook_path': '/01 - Simple DAB/other_table2',
            'depends_on': [{'task_key': 'create_table'},{'task_key': 'create_table1'}]
        }
    ]


    myjob = DAJobConfig(job_name='test3',
                        job_tasks=job_tasks,
                        job_parameters=[
                            {'name':'target', 'default':'dev'},
                            {'name':'catalog_name', 'default':'test'}
                        ])
    '''
    def __init__(self, 
                 job_name: str,
                 job_tasks: list[dict],
                 job_parameters: list[dict]):
    
        self.job_name = job_name
        self.job_tasks = job_tasks
        self.job_parameters = job_parameters
        
        ## Connect the Workspace
        self.w = self.get_workspace_client()

        ## Execute methods
        self.check_for_duplicate_job_name(check_job_name=self.job_name)
        print(f'Job name is unique. Creating the job {self.job_name}...')

        self.course_path = self.get_path_one_folder_back()
        self.list_job_tasks = self.create_job_tasks()

        self.create_job(job_tasks = self.list_job_tasks)


    ## Get Workspace client
    def get_workspace_client(self):
        """
        Establishes and returns a WorkspaceClient instance for interacting with the Databricks API.
        This is set when the object is created within self.w

        Returns:
            WorkspaceClient: A client instance to interact with the Databricks workspace.
        """
        w = WorkspaceClient()
        return w


    # Check if the job name already exists, return error if it does.
    def check_for_duplicate_job_name(self, check_job_name: str):
        for job in self.w.jobs.list():
            if job.settings.name == check_job_name:
                test_job_name = False
                assert test_job_name, f'You already have a job with the same name. Please manually delete the job {self.job_name}'                


    ## Store the path of one folder one folder back
    def get_path_one_folder_back(self):
        current_path = os.path.dirname(os.getcwd())
        print(f'Using the following path to reference the notebooks: {current_path}/.')
        return current_path


    ## Create the job tasks
    def create_job_tasks(self):
        all_job_tasks = []
        for task in job_tasks:
            if task.get('notebook_path', False) != False:

                ## Create a list of jobs.TaskDependencies
                task_dependencies = [jobs.TaskDependency(task_key=depend_task['task_key']) for depend_task in task['depends_on']] if task['depends_on'] else None

                ## Create the task
                job_task_notebook = jobs.Task(task_key=task['task_name'],
                                              notebook_task=jobs.NotebookTask(notebook_path=self.course_path+task['notebook_path']),
                                              depends_on=task_dependencies,
                                              timeout_seconds=0)
                all_job_tasks.append(job_task_notebook)

            elif task.get('pipeline_task', False) != False:
                job_task_dlt = jobs.Task(task_key=task['task_name'],
                                         pipeline_task=jobs.PipelineTask(pipeline_id=task['pipeline_id'], full_refresh=True),
                                         timeout_seconds=0)
                all_job_tasks.append(job_task_info)

        return all_job_tasks
    

    def set_job_parameters(self, parameters: dict):

        job_params_list = []
        for param in self.job_parameters:
            job_parameter = jobs.JobParameterDefinition(name=param['name'], default=param['default'])
            job_params_list.append(job_parameter)

        return job_params_list
    

    ## Create final job
    def create_job(self, job_tasks: list[jobs.Task]):
        created_job = self.w.jobs.create(
                name=self.job_name,
                tasks=job_tasks,
                parameters = self.set_job_parameters(self.job_parameters)
            )

# COMMAND ----------

# DBTITLE 1,Load Credentials
import os

@DBAcademyHelper.add_method
def load_credentials(self):
    from pathlib import Path
    import configparser
    c = configparser.ConfigParser()

    folder_name = f'var_{DA.catalog_name}'

    # Get the current directory and one directory back to search for the credentials.cfg file.
    current_path = Path.cwd()
    go_back_path = current_path.parents[0]

    find_current_path_cfg_file  = current_path / f'{folder_name}/credentials.cfg'
    find_back_path_cfg_file  = go_back_path / f'{folder_name}/credentials.cfg'

    ## Search for the credentials.cfg file. If not found it does not exist.
    if os.path.exists(find_current_path_cfg_file):
        print(f'Found credentials.cfg in {find_current_path_cfg_file}.')
        c.read(filenames=find_current_path_cfg_file)
    elif os.path.exists(find_back_path_cfg_file):
        print(f'Found credentials.cfg in {find_back_path_cfg_file}.')
        c.read(filenames=find_back_path_cfg_file)
    else:
        pass

    try:
        token = c.get('DEFAULT', 'db_token')
        host = c.get('DEFAULT', 'db_instance')
        os.environ["DATABRICKS_HOST"] = host
        os.environ["DATABRICKS_TOKEN"] = token
    except:
        token = ''
        host = ''

    return token, host    

@DBAcademyHelper.add_method
def get_credentials(self):

    ## Get Databricks Lab URL value to use in the step below.
    lab_databricks_url = f'https://{spark.conf.get("spark.databricks.workspaceUrl")}/'
    
    import ipywidgets as widgets

    (current_token, current_host) = self.load_credentials()
    current_host = lab_databricks_url
    
    @widgets.interact(host=widgets.Text(description='Host:',
                                        placeholder='Paste workspace URL here',
                                        value = current_host,
                                        continuous_update=False),
                      token=widgets.Password(description='Token:',
                                             placeholder='Paste PAT here',
                                             value = current_token,
                                             continuous_update=False)
    )
    def _f(host='', token=''):
        from urllib.parse import urlparse,urlunsplit

        u = urlparse(host)
        host = urlunsplit((u.scheme, u.netloc, '', '', ''))

        if host and token:
            os.environ["DATABRICKS_HOST"] = host
            os.environ["DATABRICKS_TOKEN"] = token

            contents = f"""
[DEFAULT]
db_token = {token}
db_instance = {host}
            """
            make_folder = f'var_{DA.catalog_name}'
            os.makedirs(make_folder, exist_ok=True)
            with open(f"{make_folder}/credentials.cfg", "w") as f:
                print(f"Credentials stored ({f.write(contents)} bytes written).")

None

# COMMAND ----------

# DBTITLE 1,create_nyc_trips_data
def create_taxi_dev_data():
    spark.sql(f'''
        CREATE OR REPLACE TABLE {DA.catalog_dev}.default.nyctaxi_raw AS
        SELECT *
        FROM samples.nyctaxi.trips
        LIMIT 100
    ''')
    print(f'Created the nyctaxi_dev table in your dev catalog: {DA.catalog_dev}!')
        
def create_taxi_prod_data():
    spark.sql(f'''
        CREATE OR REPLACE TABLE {DA.catalog_prod}.default.nyctaxi_raw AS
        SELECT *
        FROM samples.nyctaxi.trips
    ''')
    print(f'Created the nyctaxi_prod table in your dev catalog: {DA.catalog_prod}!')


def check_nyctaxi_bronze_table(user_catalog: str, total_count: int):
    total_rows_in_table = spark.sql(f'''
        SELECT count(*)
        FROM {user_catalog}.default.nyctaxi_bronze
    ''').collect()

    assert total_rows_in_table[0][0] == total_count, 'The bronze table was not created successfully'
    print(f'The nyctaxi_bronze table has was created successfully from your DAB deployment!')

# COMMAND ----------

# DBTITLE 1,delete_tables
def del_table(catalog, schema, table):
    print(f'Deleting the table {catalog}.{schema}.{table} if it exists.')
    spark.sql(f'DROP TABLE IF EXISTS {catalog}.{schema}.{table}')

# COMMAND ----------

# DBTITLE 1,Initialize DA Object
DA = DBAcademyHelper()
DA.init()

# COMMAND ----------

# # Initialize the next lesson and load credentials
# LESSON = "using_cli"
# # Load Credentials
# (db_token, db_instance) = DA.load_credentials()
