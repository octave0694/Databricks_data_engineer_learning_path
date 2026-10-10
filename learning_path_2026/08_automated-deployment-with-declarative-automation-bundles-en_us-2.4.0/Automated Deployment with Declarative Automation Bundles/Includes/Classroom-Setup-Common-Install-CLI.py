# Databricks notebook source
# DBTITLE 1,Set Notebook Token
## Load the current notebook's API token and workspace host into the following environment variables:
## - DATABRICKS_HOST
## - DATABRICKS_TOKEN

## Use this in a training context to bootstrap the Databricks CLI without
## setting up profile-based auth. The token is the short-lived one
load_credentials()

# COMMAND ----------

# DBTITLE 1,Install CLI
# MAGIC %sh
# MAGIC ############################################################
# MAGIC ## Only works on a Dedicated (formerly: Single user) cluster with policy Unrestricted
# MAGIC ## Has not been tested elsewhere.
# MAGIC ############################################################
# MAGIC
# MAGIC # Remove any existing Databricks CLI binary from the root user's local bin directory
# MAGIC # -f forces removal and avoids errors if the file does not exist
# MAGIC sudo rm -f /root/bin/databricks
# MAGIC
# MAGIC # Remove any existing Databricks CLI binary from the system-wide bin directory
# MAGIC # This ensures no old or conflicting versions remain
# MAGIC sudo rm -f /usr/local/bin/databricks
# MAGIC
# MAGIC # Download the official Databricks CLI install script from GitHub
# MAGIC # - `curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/v0.240.0/install.sh | sh`: 
# MAGIC #   - Downloads the `install.sh` script from a specific GitHub URL using `curl`.
# MAGIC #   - The options used:
# MAGIC #     - `-f`: Fail silently on server errors.
# MAGIC #     - `-s`: Silent mode (no progress or error messages).
# MAGIC #     - `-S`: Show errors if they occur.
# MAGIC #     - `-L`: Follow redirects if any.
# MAGIC #   - Pipes (`|`) the downloaded script directly to the shell (`sh`), which executes it.
# MAGIC # Pipe the script directly into the shell to execute the installation
# MAGIC curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/v0.298.0/install.sh | sh
# MAGIC
# MAGIC # Move the installed CLI binary from the root bin directory to a system-wide location
# MAGIC # This makes the `databricks` command available globally on the cluster
# MAGIC # Moves the `databricks` executable from `/root/bin/` to `/usr/local/bin/`, making it available to users without having to specify the location during each cell execution in the notebook.
# MAGIC sudo mv /root/bin/databricks /usr/local/bin/databricks
# MAGIC
# MAGIC # The cell will return the note: *Installed Databricks CLI v0.2XX.0 at /root/bin/databricks.*

# COMMAND ----------

# %sh
# # Download Terraform binary to bypass expired GPG key issue
# curl -fsSL https://releases.hashicorp.com/terraform/1.5.7/terraform_1.5.7_linux_amd64.zip -o /tmp/terraform.zip
# unzip -o /tmp/terraform.zip -d /tmp/ > /dev/null
# chmod +x /tmp/terraform

# import os
# os.environ["DATABRICKS_TF_EXEC_PATH"] = "/tmp/terraform"
# os.environ["DATABRICKS_TF_VERSION"] = "1.5.7"