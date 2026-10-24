# Databricks notebook source
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
    if is_serverless == 'TRUE':
        current_serverless_version = int(runtime_version.split(".")[1])
    else:
        current_serverless_version = None

    # All Purpose: Serverless is set to FALSE in the env variable
    if is_serverless == 'FALSE':
        current_dbr_version_all_purpose = float(runtime_version)
    else:
        current_dbr_version_all_purpose = None

    return {
        'is_serverless': is_serverless,
        'current_serverless_version':current_serverless_version, 
        'current_dbr_version_all_purpose':current_dbr_version_all_purpose
    }


def _check_serverless_only(current_serverless_version, recommended_serverless_version: int):
    """
    Validate only the Serverless requirement path and report status.
    """

    print(f"NOTE: This notebook was tested on Serverless compute version {recommended_serverless_version}. Checking compute...")

    if current_serverless_version is None:
        print(f"⚠️ WARNING: This notebook was not tested on All Purpose compute. Unexpected results or errors might occur. Please select the recommended Serverless compute version '{recommended_serverless_version}'.")
    elif current_serverless_version == recommended_serverless_version:
        print(f"✅ Serverless environment check passed: Serverless version '{current_serverless_version}' matches the recommended version.")
    else:
        print(f"⚠️ WARNING: This notebook was not tested on Serverless version '{current_serverless_version}'. Unexpected results or errors might occur. Please use Serverless version '{recommended_serverless_version}'.")


def _check_all_purpose_only(current_dbr_version_all_purpose, recommend_dbr_classic_version: float):
    """
    Validate only the All Purpose requirement path and report status.
    """

    print(f"NOTE: This notebook was tested on All Purpose compute DBR version {recommend_dbr_classic_version}. Checking compute...")

    if current_dbr_version_all_purpose is None:
        print(f"⚠️ WARNING: This notebook was not tested on Serverless compute. Unexpected results or errors might occur. Please select the recommended All Purpose compute DBR version '{recommend_dbr_classic_version}'.")
    elif current_dbr_version_all_purpose == recommend_dbr_classic_version:
        print(f"✅ All Purpose compute DBR version check passed: DBR version '{current_dbr_version_all_purpose}' matches the recommended version.")
    else:
        print(f"⚠️ WARNING: This notebook was not tested on All Purpose compute DBR version '{current_dbr_version_all_purpose}'. Unexpected results or errors might occur. Please use the recommended All Purpose compute DBR version '{recommend_dbr_classic_version}'.")


def _check_both(
    recommended_serverless_version: int, 
    recommend_dbr_classic_version: float,
    current_serverless_version: int, 
    current_dbr_version_all_purpose: float
):
    """
    Validate when either All Purpose or Serverless is acceptable and report both paths.
    """

    print(f"NOTE: This notebook can run on either All Purpose compute (recommended DBR {recommend_dbr_classic_version}) or Serverless compute (recommended version {recommended_serverless_version}).")
    print("Checking compute...")

    # All Purpose path
    if current_dbr_version_all_purpose is None:
        pass
    elif current_dbr_version_all_purpose == recommend_dbr_classic_version:
        print(f"✅ All Purpose compute DBR version check passed: DBR '{recommend_dbr_classic_version}' is the recommended version.")
    else:
        print(f"⚠️ WARNING: This notebook was not tested on All Purpose compute DBR version '{current_dbr_version_all_purpose}'. Unexpected results or errors might occur. Please use the recommended All Purpose DBR version '{recommend_dbr_classic_version}'.")

    # Serverless path
    if current_serverless_version is None:
        pass
    elif current_serverless_version == recommended_serverless_version:
        print(f"✅ Serverless environment check passed: Serverless version '{recommended_serverless_version}' is the recommended version.")
    else:
        print(f"⚠️ WARNING: This notebook was not tested on Serverless version '{current_serverless_version}'. Unexpected results or errors might occur. Please use the recommended Serverless version '{recommended_serverless_version}'.")


##
## Full function
##
def compute_validation(
    recommended_serverless_version: int = None, 
    recommend_dbr_classic_version: float = None
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
        Prints validation messages directly to the notebook output.
    """
    ## Add line break

    print('\n')
    # Require at least one target compute (can use both)
    if recommended_serverless_version is None and recommend_dbr_classic_version is None:
        raise ValueError(
            "Serverless version or DBR version was not specified in the function. Please specify a compute type to check."
        )

    # Get environment variable values
    env_values = _get_env()
    is_serverless = env_values["is_serverless"]   ## If it's running serverless or not
    current_serverless_version = env_values["current_serverless_version"] 
    current_dbr_version_all_purpose = env_values["current_dbr_version_all_purpose"] 


    # Perform checks: serverless only
    if recommended_serverless_version is not None and recommend_dbr_classic_version is None:
        _check_serverless_only(current_serverless_version, recommended_serverless_version)

    # Perform checks: all purpose only
    if recommended_serverless_version is None and recommend_dbr_classic_version is not None:
        _check_all_purpose_only(current_dbr_version_all_purpose, recommend_dbr_classic_version)

    # Perform checks: either path acceptable
    if recommended_serverless_version is not None and recommend_dbr_classic_version is not None:
        _check_both(
            recommended_serverless_version=recommended_serverless_version, 
            recommend_dbr_classic_version=recommend_dbr_classic_version,
            current_serverless_version=current_serverless_version,
            current_dbr_version_all_purpose=current_dbr_version_all_purpose
        )

    ## Add line break for clarity in the output
    print('\n')

# COMMAND ----------

def display_config_values(config_values):
    """
    Displays list of key-value pairs as rows of HTML text and textboxes
    
    param config_values: 
        list of (key, value) tuples
        
    Returns
    ----------
    HTML output displaying the config values
    Example
    --------
    display_config_values([('catalog', 'your catalog'),('schema','your schema')])
    """
    html = """<table style="width:100%">"""
    for name, value in config_values:
        html += f"""
        <tr>
            <td style="white-space:nowrap; width:1em">{name}:</td>
            <td><input type="text" value="{value}" style="width: 100%"></td></tr>"""
    html += "</table>"
    displayHTML(html)

# COMMAND ----------

# -----------------------------------------------
# CHECK REQUIRED VARIABLES FUNCTION (STRICT VERSION)
# -----------------------------------------------

def check_required_vars(*var_names: str) -> bool:
    """
    Validate that specified variables exist in the global scope and contain non-empty values.

    This function checks whether each provided variable name exists as a global variable.
    If any variable is missing or empty (None or an empty string), it raises a ValueError
    and stops execution with a clear message listing the problematic variables.

    Parameters
    ----------
    *var_names : str
        One or more variable names (as strings) to verify.

    Returns
    -------
    bool
        True if all required variables exist and have valid, non-empty values.

    Raises
    ------
    ValueError
        If one or more variables are missing or empty.

    Examples
    --------
    >>> your_marketplace_share_catalog_name = "retail_share"
    >>> my_catalog = "peter_catalog"
    >>> check_required_vars("your_marketplace_share_catalog_name", "my_catalog")
    ✅ All required variables are defined and have valid values:
      - your_marketplace_share_catalog_name = retail_share
      - my_catalog = peter_catalog

    >>> check_required_vars("missing_var")
    ValueError: Variable check failed:
    ❌ Missing variables: missing_var
    """
    missing = []
    empty = []
    globals_ = globals()
    valid = {}

    for name in var_names:
        if name not in globals_:
            missing.append(name)
        else:
            val = globals_[name]
            if val is None or (isinstance(val, str) and val.strip() == ""):
                empty.append(name)
            else:
                valid[name] = val

    if missing or empty:
        msg = []
        if missing:
            msg.append(f"❌ The following python variables were not set: {', '.join(missing)}. \n Please review the classroom setup and set the variables following the instructions")
        if empty:
            msg.append(f"⚠️ Empty variables: {', '.join(empty)}")
        full_msg = "\n".join(msg)
        raise ValueError(f"Variable check failed:\n{full_msg}")

    print("✅ All required variables are defined and have the following values:")
    for name, val in valid.items():
        print(f"  - {name} = {val}")
    
    ## Add line breaks for clarity in the output
    print('\n')
    return True


# COMMAND ----------

def setup_complete_msg():
  '''
  Prints a note in the output that the setup was complete.
  '''
  print('\n------------------------------------------------------------------------------')
  print('✅ SETUP COMPLETE!')
  print('------------------------------------------------------------------------------')