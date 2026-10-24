# Databricks notebook source
creds = DA.load_credentials()

# COMMAND ----------

# Run the following cell to install the Databricks CLI. The code completes the following:
# - `%sh`: Executes a shell command in a Databricks notebook.
  
# - `sudo rm -f /root/bin/databricks`: Removes any existing `databricks` file from the `/root/bin/` directory (forces removal without confirmation).

# - `sudo rm -f /usr/local/bin/databricks`: Similarly, removes the `databricks` file from the `/usr/local/bin/` directory.

# - `curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/v0.240.0/install.sh | sh`: 
#   - Downloads the `install.sh` script from a specific GitHub URL using `curl`.
#   - The options used:
#     - `-f`: Fail silently on server errors.
#     - `-s`: Silent mode (no progress or error messages).
#     - `-S`: Show errors if they occur.
#     - `-L`: Follow redirects if any.
#   - Pipes (`|`) the downloaded script directly to the shell (`sh`), which executes it.

# - `sudo mv /root/bin/databricks /usr/local/bin/databricks`: Moves the `databricks` executable from `/root/bin/` to `/usr/local/bin/`, making it available to users without having to specify the location during each cell execution in the notebook.

# The cell will return the note: *Installed Databricks CLI v0.240.0 at /root/bin/databricks.*

# COMMAND ----------

# MAGIC %sh
# MAGIC sudo rm -f /root/bin/databricks
# MAGIC sudo rm -f /usr/local/bin/databricks
# MAGIC curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/v0.257.0/install.sh | sh
# MAGIC sudo mv /root/bin/databricks /usr/local/bin/databricks
