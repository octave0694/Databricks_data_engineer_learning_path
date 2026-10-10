# Databricks notebook source
# DBTITLE 1,Build Catalog
## Determines if in Vocareum or Other Workspace and sets up the catalog
## Usage: my_catalog = build_user_catalog() within your demo/lab setup.

import re
from typing import Optional

def _safe_uc_name(value: str) -> str:
    # UC identifiers are generally safest with letters, numbers, underscores
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9_]", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value or "user"


def _current_user_email() -> str:
    """
    Get the user's name and email address.
    """
    return spark.sql("SELECT current_user()").first()[0]


def _get_workspace_catalogs():
    """
    Returns a set of Catalogs visible to that user.
    """
    list_of_catalogs_in_workspace = [row["catalog"].strip().lower() for row in spark.sql("SHOW CATALOGS").collect()]
    return list_of_catalogs_in_workspace


def _catalog_exists(name: str, catalogs: set[str]) -> bool:
    """
    Catalog checker to see if the catalog already exists for that user.
    """
    catalog_exists = name.lower() in catalogs
    return catalog_exists


def build_user_catalog(prefix: str = "labuser", catalog_forced = None) -> str:
    """
    Returns a UC catalog name for the current user.

    Parameters
    ----------
    prefix: str
        Prefix for the catalog name. Default is 'labuser'.
    catalog_forced: str
        Uses this catalog name if specified. Otherwise uses the prefix and user's name.

    Vocareum behavior:
      - If a catalog equals the user's 'labuserxxx' name and already exists,
        assume you are in Vocareum and use it.
      - Assumes users have a catalog by default in Vocareum.

    Other workspaces:
      - Use <prefix>_<user> and create it if possible for that user.

    Example:
        my_catalog = build_user_catalog(catalog_forced=None)  ## <-- Force the usage of a catalog if you can't create one.
    """

    # Obtain user's email and user name name
    user_email = _current_user_email()
    user_name = user_email.split("@")[0]

    # Make the user name safe if it's not in Vocareum
    safe_user_name = _safe_uc_name(user_name)


    # VOCAREUM CHECKER: Catalog is just the username (already provisioned)
    # and starts with 'labuser'
    vocareum_catalog_name = safe_user_name

    if user_email.lower().endswith("@vocareum.com"):
        print("✅ Vocareum workspace detected.")

        if _catalog_exists(
            name=vocareum_catalog_name,
            catalogs=_get_workspace_catalogs()
        ):
            print(f"✅ Using existing Vocareum catalog: '{vocareum_catalog_name}'.")
            return vocareum_catalog_name
        else:
            raise ValueError(
                f"❌ Catalog '{vocareum_catalog_name}' does not exist in this Vocareum workspace. "
                "Please create the catalog or verify the catalog name before continuing."
            )

    # OTHER WORKSPACE SETUP
    else:
        print("ℹ️ Non-Vocareum workspace detected. Setting up catalog.")

        # Setting catalog for workspaces outside of Vocareum using the provided prefix and user name
        # Limit the user's name to 19 characters. THis is done because there is a limit to the catalog.schema.object name (64 characters). For someone with a long name this could cause issus. Using 19 because that is the general size of the vocareum user name
        safe_user_name_char_restrict = safe_user_name[:19]

        # If catalog_forced is set, will use that by default.
        if catalog_forced is None:
            catalog_name = f"{prefix}_{safe_user_name_char_restrict}"
            print(f"ℹ️ Using default catalog name: '{catalog_name}'.")
        else:
            catalog_name = catalog_forced
            print(f"ℹ️ Using specified catalog name: '{catalog_name}'.")


        # Check if the user already has this catalog with the prefix_safeusername
        if _catalog_exists(name=catalog_name, catalogs=_get_workspace_catalogs()) == True:
            print(f"✅ Catalog '{catalog_name}' already exists. Using this catalog.")
            return catalog_name
        elif _catalog_exists(name=catalog_name, catalogs=_get_workspace_catalogs()) == False and catalog_forced is not None:
            raise RuntimeError(
                f"❌ Catalog '{catalog_name}' does not exist in this workspace. "
                "A forced catalog name must reference an existing catalog. "
                "Create the catalog or reference an existing one, then rerun the notebook."
            )
        else:
            try:
                print(f"ℹ️ Catalog '{catalog_name}' not found. Creating it now...")
                spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
                print(f"✅ Catalog '{catalog_name}' created successfully.")
                return catalog_name
            except Exception as e:
                print(
                    f"⚠️ Could not create catalog '{catalog_name}'. "
                    "You may not have privileges to create catalogs in this workspace.\n"
                    f"Error: {e}"
                )


# COMMAND ----------

# DBTITLE 1,Create Schemas
def create_schemas(in_catalog: str, schemas_to_create: list):
    """
    Create one or more schemas in a Unity Catalog catalog.

    Parameters
    ----------
    in_catalog : str
        The catalog where schemas will be created.

    schemas_to_create : list
        A list of schema names to create (e.g., ["bronze", "silver", "gold"]).

    Example
    -------
    >>> create_schemas("my_catalog", ["bronze", "silver", "gold"])
    """
    # Step 1: Verify the catalog exists
    print(f"\n{'='*60}")
    print(f"  STEP 1: Verifying catalog exists: {in_catalog}")
    print(f"{'='*60}")
    try:
        catalogs = [row.catalog for row in spark.sql("SHOW CATALOGS").collect()]  # noqa: F821
        if in_catalog not in catalogs:
            raise ValueError(
                f"Catalog '{in_catalog}' does not exist.\n"
                f"  Available catalogs: {', '.join(catalogs)}\n"
                f"  Make sure the catalog has been created before running this function."
            )
        print(f"  Catalog '{in_catalog}' exists.")
    except ValueError:
        raise
    except Exception as e:
        raise RuntimeError(f"  Failed to verify catalog: {e}")

    # Step 2: Create schemas
    print(f"\n{'='*60}")
    print(f"  STEP 2: Setting up {len(schemas_to_create)} schema(s) in catalog: {in_catalog}")
    print(f"{'='*60}")

    # Get existing schemas to report accurately
    existing_schemas = set(
        row.databaseName
        for row in spark.sql(f"SHOW SCHEMAS IN `{in_catalog}`").collect()  # noqa: F821
    )

    created = 0
    for i, schema_name in enumerate(schemas_to_create, start=1):
        full_name = f"`{in_catalog}`.`{schema_name}`"
        print(f"  [{i}/{len(schemas_to_create)}] Checking: {full_name}...", end=" ")

        if schema_name in existing_schemas:
            print(f"ALREADY EXISTS")
        else:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {full_name}")  # noqa: F821
            created += 1
            print(f"CREATED")

    print(f"\n{'='*60}")
    print(f"  COMPLETE: {created} schema(s) created, {len(schemas_to_create) - created} already existed.")
    print(f"{'='*60}\n")



# COMMAND ----------

# DBTITLE 1,Check Compute
# -----------------------------------------------
# CHECK COMPUTE FUNCTION
#
# The function `compute_validation(recommend_dbr_classic_version=17.3, recommended_serverless_version=1)`
# checks the current Databricks compute type (All-Purpose or Serverless)
# and returns WARNINGS if the user's environment does not meet the specified requirements.
#
# Example uses:
# 1. Allow BOTH All-Purpose and Serverless:
#    compute_validation(recommend_dbr_classic_version=17.3, recommended_serverless_version=1)
#
# 2. Require ONLY All-Purpose (minimum DBR version 16.4):
#    compute_validation(recommend_dbr_classic_version=16.4, recommended_serverless_version=None)
#
# 3. Require ONLY Serverless (minimum version 3):
#    compute_validation(recommend_dbr_classic_version=None, recommended_serverless_version=3)
# -----------------------------------------------


import os


def _get_env():
    """
    Read the Databricks compute environment and extract both All Purpose or Serverless versions.

    Behavior assumptions:
      - Serverless runtime values look like 'client.X.Y'. The middle token (X) is used as the Serverless version.
      - All Purpose runtime values look like '17.3'. The full string is converted to float.
      - IS_SERVERLESS may appear as 'TRUE', 'true', or be absent. The value is uppercased.

    Returns
    -------
    dict containing:
      - is_serverless: 'TRUE' or 'FALSE'
      - current_serverless_version: int or None
      - current_dbr_version_all_purpose: float or None
    """

    # Note: IS_SERVERLESS may be 'TRUE' or 'true' in some envs, or absent. Uppercase the string
    is_serverless = os.environ.get("IS_SERVERLESS", "FALSE").upper()
    runtime_version = os.environ.get("DATABRICKS_RUNTIME_VERSION", "")

    # Serverless: require IS_SERVERLESS == 'TRUE', then extract the second token from 'client.X.Y'
    if is_serverless == "TRUE":
        current_serverless_version = int(runtime_version.split(".")[1])
    else:
        current_serverless_version = None

    # All Purpose: Serverless is set to FALSE in the env variable
    if is_serverless == "FALSE":
        current_dbr_version_all_purpose = float(runtime_version)
    else:
        current_dbr_version_all_purpose = None

    return {
        "is_serverless": is_serverless,
        "current_serverless_version": current_serverless_version,
        "current_dbr_version_all_purpose": current_dbr_version_all_purpose,
    }


def _render_result(compute_type, recommended, current, match):
    """Build one row of the results table."""
    if match:
        badge = '<span style="color:#2e7d32;font-weight:600">&#10003; Match</span>'
        detail = f"Version {current}"
        row_style = ""
    else:
        badge = '<span style="color:#c62828;font-weight:700">&#9888; Mismatch</span>'
        detail = f"Found {current} &mdash; recommended <strong>{recommended}</strong>"
        row_style = 'background:#FDE0DC;'

    return f"""
    <tr style="{row_style}">
      <td style="padding:8px 12px;border-bottom:1px solid #e0e0e0">{compute_type}</td>
      <td style="padding:8px 12px;border-bottom:1px solid #e0e0e0">{badge}</td>
      <td style="padding:8px 12px;border-bottom:1px solid #e0e0e0">{detail}</td>
    </tr>"""


def _render_wrong_compute(expected_type, recommended):
    """Build a single-row notice when the user is on the wrong compute type entirely."""
    return f"""
    <tr style="background:#FDE0DC;">
      <td style="padding:8px 12px;border-bottom:1px solid #e0e0e0">{expected_type}</td>
      <td style="padding:8px 12px;border-bottom:1px solid #e0e0e0"><span style="color:#c62828;font-weight:700">&#9888; Wrong compute type</span></td>
      <td style="padding:8px 12px;border-bottom:1px solid #e0e0e0">This notebook expects <strong>{expected_type}</strong> (version {recommended})</td>
    </tr>"""


def _has_mismatch(rows_html):
    """Check if any rendered row contains the mismatch background color."""
    return "#FDE0DC" in rows_html


def _display(title, rows):
    """Render the full HTML card with all result rows."""
    joined = "".join(rows)
    mismatch = _has_mismatch(joined)

    header_bg = "#FF5F46" if mismatch else "#1b3a4b"

    html = f"""
    <div style="font-family:system-ui,-apple-system,sans-serif;max-width:1100px;margin:12px 0;border:1px solid #e0e0e0;border-radius:8px;overflow:hidden">
      <div style="background:{header_bg};color:#fff;padding:10px 16px;font-size:16px;font-weight:600">{title}</div>
      <table style="width:100%;border-collapse:collapse;font-size:15px">
        <tr style="background:#f5f5f5">
          <th style="padding:8px 12px;text-align:left;border-bottom:1px solid #e0e0e0">Compute</th>
          <th style="padding:8px 12px;text-align:left;border-bottom:1px solid #e0e0e0">Status</th>
          <th style="padding:8px 12px;text-align:left;border-bottom:1px solid #e0e0e0">Details</th>
        </tr>
        {joined}
      </table>
    </div>"""
    displayHTML(html)


def _check_serverless_only(current_serverless_version, recommended_serverless_version):
    rows = []
    if current_serverless_version is None:
        rows.append(_render_wrong_compute("Serverless", recommended_serverless_version))
    else:
        match = current_serverless_version == recommended_serverless_version
        rows.append(_render_result("Serverless", recommended_serverless_version, current_serverless_version, match))
    _display(f"Compute Check &mdash; Tested on Serverless v{recommended_serverless_version}", rows)


def _check_all_purpose_only(current_dbr_version_all_purpose, recommend_dbr_classic_version):
    rows = []
    if current_dbr_version_all_purpose is None:
        rows.append(_render_wrong_compute("All-Purpose", recommend_dbr_classic_version))
    else:
        match = current_dbr_version_all_purpose == recommend_dbr_classic_version
        rows.append(_render_result("All-Purpose", recommend_dbr_classic_version, current_dbr_version_all_purpose, match))
    _display(f"Compute Check &mdash; Tested on All-Purpose DBR {recommend_dbr_classic_version}", rows)


def _check_both(recommended_serverless_version, recommend_dbr_classic_version, current_serverless_version, current_dbr_version_all_purpose):
    rows = []
    if current_dbr_version_all_purpose is not None:
        match = current_dbr_version_all_purpose == recommend_dbr_classic_version
        rows.append(_render_result("All-Purpose", recommend_dbr_classic_version, current_dbr_version_all_purpose, match))
    if current_serverless_version is not None:
        match = current_serverless_version == recommended_serverless_version
        rows.append(_render_result("Serverless", recommended_serverless_version, current_serverless_version, match))
    _display(f"Compute Check &mdash; Tested on All-Purpose DBR {recommend_dbr_classic_version} / Serverless v{recommended_serverless_version}", rows)


def compute_validation(
    recommended_serverless_version: int = None,
    recommend_dbr_classic_version: float = None,
):
    """
    Check the Databricks compute environment and warn users when they are not running on the
    compute type or version this notebook was tested on.

    The function supports three cases:
      - Serverless only: provide `recommended_serverless_version`.
      - All Purpose only: provide `recommend_dbr_classic_version`.
      - Either compute type: provide both.

    The check compares the exact versions detected in the environment with the versions provided
    and prints warnings if they do not match. It does not raise errors for mismatches, only for
    missing inputs.

    Parameters
    ----------
    recommended_serverless_version : int or None
        Expected Serverless version this notebook was validated on.
    recommend_dbr_classic_version : float or None
        Expected All Purpose DBR version this notebook was validated on.

    Returns
    -------
    None
        Displays a styled HTML card in the notebook output.
    """
    if recommended_serverless_version is None and recommend_dbr_classic_version is None:
        raise ValueError(
            "Serverless version or DBR version was not specified in the function. Please specify a compute type to check."
        )

    env_values = _get_env()
    current_serverless_version = env_values["current_serverless_version"]
    current_dbr_version_all_purpose = env_values["current_dbr_version_all_purpose"]

    if recommended_serverless_version is not None and recommend_dbr_classic_version is None:
        _check_serverless_only(current_serverless_version, recommended_serverless_version)

    if recommended_serverless_version is None and recommend_dbr_classic_version is not None:
        _check_all_purpose_only(current_dbr_version_all_purpose, recommend_dbr_classic_version)

    if recommended_serverless_version is not None and recommend_dbr_classic_version is not None:
        _check_both(
            recommended_serverless_version=recommended_serverless_version,
            recommend_dbr_classic_version=recommend_dbr_classic_version,
            current_serverless_version=current_serverless_version,
            current_dbr_version_all_purpose=current_dbr_version_all_purpose,
        )


# COMMAND ----------

# DBTITLE 1,Check Catalogs
def check_if_catalogs_are_created(check_catalogs: list[str]):
    """
    Verify that the named catalogs exist in the workspace.
    Raise an error if any are missing, indicating the user probably
    skipped course setup.

    Args:
    -------
        check_catalogs (list[str]): Full catalog names that must exist.

    Raises:
    -------
        ValueError: if one or more catalogs are missing. The message lists
                    exactly which catalogs were not found.

    Example:
    -------
        check_if_catalogs_are_created([
            'labuser_jdoe_dev',
            'labuser_jdoe_stage',
            'labuser_jdoe_prod',
        ])
    """
    existing = {c.name for c in spark.catalog.listCatalogs()}

    missing = [name for name in check_catalogs if name not in existing]

    if missing:
        missing_str = ", ".join(repr(m) for m in missing)
        raise ValueError(
            f"Required catalog(s) not found: {missing_str}. "
            f"Please run the '0 - REQUIRED - Course Setup and Authentication' "
            f"notebook to set up your environment."
        )

    print("Catalog check for the labs passed.")


def check_if_catalogs_are_created_by_suffix(check_suffixes: list[str]):
    """
    Verify that catalogs with the given suffixes exist in the workspace.

    A catalog "matches" a suffix if its name ends with `_<suffix>` (or is
    exactly equal to the suffix). Useful when the per-user catalog prefix
    is unknown and you only care that, say, a `_dev`, `_stage`, and
    `_prod` catalog exist.

    Args:
    -------
        check_suffixes (list[str]): Suffixes that must be present on at
                                    least one catalog each.

    Raises:
    -------
        ValueError: if one or more suffixes have no matching catalog.

    Example:
    -------
        check_if_catalogs_are_created_by_suffix(['dev', 'stage', 'prod'])
    """
    existing_names = [c.name for c in spark.catalog.listCatalogs()]

    missing = []
    for suffix in check_suffixes:
        suffix_match = f"_{suffix}"
        if not any(name == suffix or name.endswith(suffix_match) for name in existing_names):
            missing.append(suffix)

    if missing:
        missing_str = ", ".join(repr(m) for m in missing)
        raise ValueError(
            f"No catalog found for suffix(es): {missing_str}. "
            f"Please run the '0 - REQUIRED - Course Setup and Authentication' "
            f"notebook to set up your environment."
        )

    print("Catalog suffix check for the labs passed.")


# COMMAND ----------

# DBTITLE 1,Delete Files
def delete_source_files(source_files: str):
    """
    Delete every entry inside `source_files`. Files are removed with
    `os.remove`; directories are removed recursively with `shutil.rmtree`,
    so nested folders are handled.

    Parameters:
    ----------
        source_files (str): Path to the volume or directory whose contents
                            you want to wipe. For example,
                            f'{DA.paths.working_dir}/pii/stream_source/user_reg'.

    Returns:
    -------
        None. Prints one line per deletion plus a summary.
    """
    import os
    import shutil

    print(f"\nSearching for files in {source_files!r} to delete...")

    if not os.path.exists(source_files):
        print(f"Path does not exist. Nothing to delete.\n")
        return

    list_of_entries = sorted(os.listdir(source_files))

    if not list_of_entries:
        print(f"No files found in {source_files!r}.\n")
        return

    deleted = 0
    failed = 0
    for entry in list_of_entries:
        target = os.path.join(source_files, entry)
        try:
            if os.path.isdir(target):
                shutil.rmtree(target)
                print(f"Deleted directory: {target}")
            else:
                os.remove(target)
                print(f"Deleted: {target}")
            deleted += 1
        except Exception as e:
            print(f"ERROR: Could not delete {target}. {e}")
            failed += 1

    summary = f"\nDeleted {deleted} item(s)."
    if failed:
        summary += f" {failed} failure(s)."
    print(summary)


# COMMAND ----------

# DBTITLE 1,SDP Pipeline Creator
import os

class DeclarativePipelineCreator:
    """
    A class to create a Spark Declarative pipeline using the Databricks REST API.

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
                    f"Spark Declarative Pipeline name '{self.pipeline_name}' already exists. "
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
        print(f"Creating the Spark Declarative Pipeline '{self.pipeline_name}'...")
        print(f"Root folder path: {self.pipeline_body['root_path']}")
        print(f"Source folder path(s): {self.pipeline_body['libraries']}")

        # Make the API call
        self.response = self.workspace.api_client.do('POST', '/api/2.0/pipelines', body=self.pipeline_body)

        # Notify of completion
        print(f"\nSpark Declarative Pipeline Creation '{self.pipeline_name}' Complete!")

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

import os


def load_credentials():
    """
    Load the current notebook's API token and workspace host into the
    DATABRICKS_HOST and DATABRICKS_TOKEN environment variables.

    Use this in a training context to bootstrap the Databricks CLI without
    setting up profile-based auth. The token is the short-lived one
    Databricks automatically issues for the running notebook session.

    Returns:
    -------
        tuple[str, str]: (host, token_preview). `token_preview` is the
                         first 4 characters of the token followed by "..."
                         so the full secret never lands in notebook output.

    Raises:
    -------
        RuntimeError: if the notebook context cannot be reached, no API
                      token is available, or the workspace URL is missing.
    """
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        token = ctx.apiToken().getOrElse(None)
    except Exception as e:
        raise RuntimeError(
            f"Could not read the notebook API token. Are you running this "
            f"inside a Databricks notebook attached to a cluster or warehouse? "
            f"Underlying error: {e}"
        )

    if not token:
        raise RuntimeError(
            "No notebook API token found. The current session does not expose "
            "a token, so the Databricks CLI auth cannot be set automatically. "
            "Try detaching/reattaching the notebook, or fall back to a manual "
            "PAT (databricks configure --token)."
        )

    workspace_url = spark.conf.get("spark.databricks.workspaceUrl", None)
    if not workspace_url:
        raise RuntimeError(
            "Could not read the workspace URL from spark.conf. Is this "
            "notebook attached to a cluster or SQL warehouse?"
        )

    host = f"https://{workspace_url}"

    os.environ["DATABRICKS_HOST"] = host
    os.environ["DATABRICKS_TOKEN"] = token

    token_preview = (token[:4] + "...") if len(token) > 4 else "..."
    print(f"DATABRICKS_HOST set to:  {host}")
    print(f"DATABRICKS_TOKEN set.")

# COMMAND ----------

# DBTITLE 1,Load Credentials
# import os


# def load_credentials():
#     from pathlib import Path
#     import configparser

#     c = configparser.ConfigParser()

#     folder_name = f"var_{my_catalog}"

#     # Get the current directory and one directory back to search for the credentials.cfg file.
#     current_path = Path.cwd()
#     go_back_path = current_path.parents[0]

#     find_current_path_cfg_file = current_path / f"{folder_name}/credentials.cfg"
#     find_back_path_cfg_file = go_back_path / f"{folder_name}/credentials.cfg"

#     ## Search for the credentials.cfg file. If not found it does not exist.
#     if os.path.exists(find_current_path_cfg_file):
#         print(f"Found credentials.cfg in {find_current_path_cfg_file}.")
#         c.read(filenames=find_current_path_cfg_file)
#     elif os.path.exists(find_back_path_cfg_file):
#         print(f"Found credentials.cfg in {find_back_path_cfg_file}.")
#         c.read(filenames=find_back_path_cfg_file)
#     else:
#         pass

#     try:
#         token = c.get("DEFAULT", "db_token")
#         host = c.get("DEFAULT", "db_instance")
#         os.environ["DATABRICKS_HOST"] = host
#         os.environ["DATABRICKS_TOKEN"] = token
#     except:
#         token = ""
#         host = ""

#     return token, host


# def get_credentials():

#     ## Get Databricks Lab URL value to use in the step below.
#     lab_databricks_url = f'https://{spark.conf.get("spark.databricks.workspaceUrl")}/'

#     import ipywidgets as widgets

#     (current_token, current_host) = load_credentials()
#     current_host = lab_databricks_url

#     @widgets.interact(
#         host=widgets.Text(
#             description="Host:",
#             placeholder="Paste workspace URL here",
#             value=current_host,
#             continuous_update=False,
#         ),
#         token=widgets.Password(
#             description="Token:",
#             placeholder="Paste PAT here",
#             value=current_token,
#             continuous_update=False,
#         ),
#     )
#     def _f(host="", token=""):
#         from urllib.parse import urlparse, urlunsplit

#         u = urlparse(host)
#         host = urlunsplit((u.scheme, u.netloc, "", "", ""))

#         if host and token:
#             os.environ["DATABRICKS_HOST"] = host
#             os.environ["DATABRICKS_TOKEN"] = token

#             contents = f"""
# [DEFAULT]
# db_token = {token}
# db_instance = {host}
#             """
#             make_folder = f"var_test"
#             os.makedirs(make_folder, exist_ok=True)
#             with open(f"{make_folder}/credentials.cfg", "w") as f:
#                 print(f"Credentials stored ({f.write(contents)} bytes written).")


# None

# COMMAND ----------

# DBTITLE 1,create_nyc_trips_data
def create_taxi_dev_data():
    spark.sql(f'''
        CREATE OR REPLACE TABLE {catalog_dev}.default.nyctaxi_raw AS
        SELECT *
        FROM samples.nyctaxi.trips
        LIMIT 100
    ''')
    print(f'Created the nyctaxi_dev table in your dev catalog: {catalog_dev}!')
        
def create_taxi_prod_data():
    spark.sql(f'''
        CREATE OR REPLACE TABLE {catalog_prod}.default.nyctaxi_raw AS
        SELECT *
        FROM samples.nyctaxi.trips
    ''')
    print(f'Created the nyctaxi_prod table in your dev catalog: {catalog_prod}!')


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

# DBTITLE 1,Find Data Folder
def find_folder(folder_name: str) -> str:
    """
    Locate a folder in the current working directory.

    Parameters
    ----------
    folder_name : str
        Name of the folder to find (e.g., "data", "config", "scripts").

    Returns
    -------
    str
        The full path to the folder.

    Raises
    ------
    FileNotFoundError
        If the folder cannot be found.
    """
    cwd = os.getcwd()
    folder_path = os.path.join(cwd, folder_name)

    print(f"\n{'='*60}")
    print(f"  Searching for '{folder_name}' folder...")
    print(f"{'='*60}")
    print(f"  Current directory: {cwd}")

    print(f"  Checking: {folder_path}...", end=" ")
    if os.path.isdir(folder_path):
        print(f"FOUND")
        print(f"{'='*60}\n")
        return folder_path
    else:
        print(f"NOT FOUND")

    print(f"{'='*60}\n")
    raise FileNotFoundError(
        f"Could not find '{folder_name}' folder in: {cwd}\n"
        f"  Make sure the folder exists in your current working directory."
    )


# COMMAND ----------

# DBTITLE 1,Copy File to Volume
import os
import shutil
import time

def copy_workspace_files_to_volume(
    src_workspace_folder: str,
    target_volume_path: str,
    n: int,
    overwrite: bool = False,
    sleep: int = 2,
):
    """
    Copy n files from a Databricks workspace folder to a Unity Catalog volume,
    simulating incremental weekly data drops for Auto Loader.

    Files are sorted alphabetically before copying to ensure consistent ordering
    across runs (e.g., week_1 before week_2). Files already present at the
    destination are skipped unless overwrite is True. A configurable pause
    between copies gives Auto Loader time to detect each new file.

    Parameters
    ----------
    src_workspace_folder : str
        Full workspace folder path, for example
        /Workspace/Users/user@databricks.com/data/

    target_volume_path : str
        Full volume folder path, for example
        /Volumes/catalog/schema/volume/

    n : int
        Number of files to copy. Must be <= total files in source.

    overwrite : bool, default False
        Whether to overwrite files that already exist at the target.

    sleep : int, default 2
        Seconds to pause after each file copy. Gives Auto Loader time
        to pick up new files between copies.

    Raises
    ------
    FileNotFoundError
        If the source workspace folder does not exist.
    ValueError
        If n is greater than the number of available files in the source.

    Example
    -------
    >>> copy_workspace_files_to_volume(
    ...     src_workspace_folder='/Workspace/Users/user@databricks.com/how_data/meetings',
    ...     target_volume_path='/Volumes/workshop_catalog/how_analytics/raw_landing',
    ...     n=3
    ... )
    """
    # Step 1: Validate the source workspace folder exists
    print(f"\n{'='*60}")
    print(f"  STEP 1: Validating source workspace folder...")
    print(f"{'='*60}")
    if not os.path.isdir(src_workspace_folder):
        raise FileNotFoundError(
            f"Source folder does not exist: {src_workspace_folder}\n"
            f"  Make sure you have the correct workspace path to your data files."
        )
    print(f"  Source folder found: {src_workspace_folder}")

    # Step 2: Ensure the target volume and any subdirectories exist
    #         Path format: /Volumes/<catalog>/<schema>/<volume>[/optional/subdirs]
    print(f"\n{'='*60}")
    print(f"  STEP 2: Checking target volume path...")
    print(f"{'='*60}")
    if not os.path.isdir(target_volume_path):
        # Parse the volume path to extract catalog, schema, and volume name
        parts = target_volume_path.strip("/").split("/")
        # parts[0] = "Volumes", parts[1] = catalog, parts[2] = schema, parts[3] = volume
        if len(parts) < 4:
            raise ValueError(
                f"  Invalid volume path: {target_volume_path}\n"
                f"  Expected format: /Volumes/<catalog>/<schema>/<volume>"
            )
        catalog = parts[1]
        schema = parts[2]
        volume_name = parts[3]

        # Check if the volume itself exists (the /Volumes/catalog/schema/volume root)
        volume_root = f"/Volumes/{catalog}/{schema}/{volume_name}"
        if not os.path.isdir(volume_root):
            print(f"  Volume does not exist. Creating volume: {catalog}.{schema}.{volume_name}")
            spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`{volume_name}`")  # noqa: F821
            print(f"  Volume created: {catalog}.{schema}.{volume_name}")

        # If the target path has subdirectories beyond the volume root, create them
        if target_volume_path.rstrip("/") != volume_root:
            print(f"  Creating subdirectory within volume...")
            dbutils.fs.mkdirs(target_volume_path)  # noqa: F821
            print(f"  Created: {target_volume_path}")
        else:
            print(f"  Volume is ready: {target_volume_path}")
    else:
        print(f"  Target volume path already exists: {target_volume_path}")

    # Step 3: Read and sort source files so weeks copy in order (week_1, week_2, etc.)
    print(f"\n{'='*60}")
    print(f"  STEP 3: Reading source files...")
    print(f"{'='*60}")
    source_files = sorted(
        f for f in os.listdir(src_workspace_folder)
        if os.path.isfile(os.path.join(src_workspace_folder, f))
    )
    print(f"  Found {len(source_files)} file(s) in source folder.")

    if n > len(source_files):
        raise ValueError(
            f"  You requested {n} files but source only contains {len(source_files)}.\n"
            f"  Reduce n to <= {len(source_files)}."
        )

    # Step 4: Copy files from workspace to volume
    existing_files = set(os.listdir(target_volume_path))
    print(f"\n{'='*60}")
    print(f"  STEP 4: Copying {n} file(s) to target volume...")
    print(f"  Source:      {src_workspace_folder}")
    print(f"  Destination: {target_volume_path}")
    print(f"{'='*60}")

    copied = 0
    for i, filename in enumerate(source_files[:n], start=1):
        src_path = os.path.join(src_workspace_folder, filename)
        dest_path = os.path.join(target_volume_path, filename)

        print(f"  [{i}/{n}] Checking: {filename}...", end=" ")
        if filename in existing_files and not overwrite:
            print(f"EXISTS at destination. Skipping.")
        else:
            shutil.copy(src_path, dest_path)
            copied += 1
            print(f"NOT found at destination. Copied successfully.")

            if sleep > 0 and i < n:
                print(f"           Sleeping {sleep}s before next file...")
                time.sleep(sleep)

    # Summary
    print(f"\n{'='*60}")
    print(f"  COMPLETE: Copied {copied} new file(s), skipped {n - copied}.")
    print(f"{'='*60}\n")


# COMMAND ----------

def display_config_values(config_values, copy_values=False):
    """
    Displays list of key-value pairs as rows of HTML text.

    Parameters
    ----------
    config_values : list of (key, value[, copy_value]) tuples
        key: text to display in "information" column
        value: text to display in "value" column (HTML safe; can include links)
        copy_value: per-row override for copy button. Can be specified as True or as a string value
                    if you want to copy something different than what's displayed. For example,
                    value might display a folder name while copy_value includes the entire path
    copy_values : bool, optional
        If True, a copy button appears next to each value. Default is False.

    Returns
    ----------
    HTML output displaying the config values

    Example
    --------
    display_config_values([('catalog', 'your catalog'), ('schema', 'your schema')])
    display_config_values([('catalog', 'your catalog')], copy_values=True)
    """
    rows = ""
    for name, value, *rest in config_values:
        copy_value = rest[0] if rest else (copy_values or "")
        copy_btn = ""
        if copy_value:
            if isinstance(copy_value, str):
                copy_value = f'data-copy="{copy_value}"'
            else:
                copy_value = ""
                
            copy_btn = """
                <button type="button" onclick="copyFromSibling(this)"
                    style="margin-left:auto;padding:2px 10px;border:1px solid #ccc;border-radius:4px;background:#f5f5f5;cursor:pointer;font-size:13px;flex:0 0 auto;min-width:68px; text-align:center; box-sizing:border-box;">
                    Copy
                </button>"""

        rows += f"""
        <tr>
          <td style="padding:6px 12px;white-space:nowrap;border-bottom:1px solid #e0e0e0;font-weight:600">{name}:</td>
          <td style="padding:6px 12px;border-bottom:1px solid #e0e0e0">
            <div style="display:flex; align-items:center; gap:12px;">
                <span class="copy-source" {copy_value} style="display:inline-block;padding:4px 8px;font-size:15px;min-width:0;">{value}</span>
                {copy_btn}
            </div>
          </td>
        </tr>"""

    html = """
    <div style="font-family:system-ui,-apple-system,sans-serif;max-width:1100px;margin:12px 0;border:1px solid #e0e0e0;border-radius:8px;overflow:hidden">
      <div style="background:#1b3a4b;color:#fff;padding:10px 16px;font-size:16px;font-weight:600">Configuration Values</div>
      <table style="width:100%;border-collapse:collapse;font-size:15px">
        <tr style="background:#f5f5f5">
          <th style="padding:6px 12px;text-align:left;border-bottom:1px solid #e0e0e0">Information</th>
          <th style="padding:6px 12px;text-align:left;border-bottom:1px solid #e0e0e0">Value</th>
        </tr>""" + rows + """
      </table>
    </div>
    <script>
        if (!window.copyFromSibling) {
            window.copyFromSibling = function(btn) {
            var source = btn.parentElement.querySelector('.copy-source');
            if (!source) return;

            var text = source.dataset.copy || source.innerText;

            var t = document.createElement('textarea');
            t.value = text;
            t.style.position = 'fixed';
            t.style.opacity = '0';
            t.style.pointerEvents = 'none';
            document.body.appendChild(t);
            t.select();
            document.execCommand('copy');
            document.body.removeChild(t);

            var original = btn.textContent;
            btn.textContent = 'Copied!';
            setTimeout(function() {
                btn.textContent = original;
            }, 1500);
            };
        }
    </script>"""
    displayHTML(html)

# COMMAND ----------

##############################
## Create base catalog
##############################

my_catalog = build_user_catalog(catalog_forced = 'dev_samarth')

##############################
## Create base catalog names in variables
##############################
catalog_dev = f'{my_catalog}_1_dev'
catalog_stage = f'{my_catalog}_2_stage'
catalog_prod = f'{my_catalog}_3_prod'