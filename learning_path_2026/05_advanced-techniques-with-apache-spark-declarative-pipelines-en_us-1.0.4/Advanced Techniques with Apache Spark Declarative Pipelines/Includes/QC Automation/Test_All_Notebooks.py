# Databricks notebook source
# MAGIC %md
# MAGIC # Automated Test Runner — Single Course
# MAGIC
# MAGIC Creates a **single multi-task Lakeflow Job** for one course, with optional QA checks,
# MAGIC so the full run is visible in one timeline.
# MAGIC
# MAGIC **Run structure:**
# MAGIC - Course notebooks run in the dependency order you define in `COURSE_TASKS`.
# MAGIC - **QA Content Checker** can run independently (no dependencies) alongside the course tasks.
# MAGIC - The separate **Course-specific inputs** cell makes it easy to reuse this notebook for another course.
# MAGIC
# MAGIC ```
# MAGIC  QA Check      :  qa_content_checker  (optional, parallel)
# MAGIC  Course Tasks  :  task_01  ──►  task_02  ──►  task_03
# MAGIC ```
# MAGIC
# MAGIC **What it does**
# MAGIC 1. Reads course-specific settings from a dedicated configuration cell (`COURSE_NAME`, `LAB_NOTEBOOKS`, `COURSE_TASKS`).
# MAGIC 2. Auto-fills all `<FILL_IN>` placeholders in the configured lab notebooks using the inline solution blocks.
# MAGIC 3. Resolves all notebook paths relative to this notebook's location.
# MAGIC 4. Creates (or re-creates) a single persistent Lakeflow Job for the configured course.
# MAGIC 5. Optionally adds `qa_content_checker` as an **independent task** (no dependencies).
# MAGIC 6. Triggers the job with `jobs.run_now` and polls until completion.
# MAGIC 7. Appends one result row per task to a generic results table.
# MAGIC 8. Displays a combined summary table with clickable `run_page_url` links.
# MAGIC 9. Creates and publishes a **Lakeview dashboard** from the run results and QA findings.
# MAGIC 10. Raises if any task failed — triggering Lakeflow Job failure email.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebooks with UI Instructions (Auto-Scanned)
# MAGIC
# MAGIC The following cell **dynamically scans** all course notebooks (demos and labs) to detect manual UI steps that cannot be fully automated by the test runner.
# MAGIC
# MAGIC It looks for markdown cells containing UI action patterns (e.g., "Select...", "Click...", "Navigate to...", icon references, etc.) and reports which notebooks have them.
# MAGIC
# MAGIC **This runs fresh on every execution** — no hardcoded values — so it always reflects the latest course content.

# COMMAND ----------

# MAGIC %run ./notebook_path

# COMMAND ----------

# ── Auto-Scan: Notebooks with UI Instructions ───────────────────────────────
# Dynamically scans ALL course notebooks (demos and labs) to detect cells
# containing manual UI steps that cannot be automated by the test runner.
#
# NOT HARDCODED — runs fresh every time so it reflects the latest course content.
# ─────────────────────────────────────────────────────────────────────────────

import base64
import json
import re
import os
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ExportFormat

w = WorkspaceClient()

# Derive course_root from this notebook's own path
_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
course_root = "/".join(_nb_path.split("/")[:-1])

# ── UI Detection Patterns ─────────────────────────────────────────────────────
# These regex patterns identify markdown cells that contain UI interaction steps.
# They match common instructional language used in Databricks Academy notebooks.

UI_ACTION_PATTERNS = [
    # Direct UI actions
    re.compile(r'\bSelect\s+(the\s+)?\*\*', re.IGNORECASE),          # "Select the **Catalog** icon"
    re.compile(r'\bClick\s+(the\s+|on\s+)?\*\*', re.IGNORECASE),    # "Click the **Run pipeline** button"
    re.compile(r'\bright-click\b', re.IGNORECASE),                    # "right-click on Jobs & Pipelines"
    re.compile(r'\bOpen\s+in\s+(a\s+)?(New|new)\s+Tab\b'),           # "Open in New Tab"
    re.compile(r'\bOpen\s+(Link\s+)?in\s+New\s+(Browser\s+)?Tab\b', re.IGNORECASE),
    # Navigation instructions
    re.compile(r'\b(left|top|main|far-left)\s+navigation\s+(bar|pane)\b', re.IGNORECASE),
    re.compile(r'\bNavigate\s+to\b', re.IGNORECASE),
    re.compile(r'\bExpand\s+(your|the)\s+\*\*', re.IGNORECASE),       # "Expand the **sdp_1_bronze** schema"
    # Icon references (images in markdown = UI screenshot)
    re.compile(r'!\[.*?Icon.*?\]\('),                                 # ![Catalog Icon](./path)
    re.compile(r'!\[.*?(icon|button|select|settings).*?\]\(', re.IGNORECASE),
    # Pipeline editor / Jobs & Pipelines UI
    re.compile(r'\bJobs\s*(&|and)\s*Pipelines\b', re.IGNORECASE),
    re.compile(r'\bLakeflow\s+(Pipelines?\s+)?Editor\b', re.IGNORECASE),
    re.compile(r'\bPipeline\s+(graph|details|settings)\b', re.IGNORECASE),
    re.compile(r'\bOpen\s+in\s+Editor\b', re.IGNORECASE),
    re.compile(r'\bRun\s+pipeline\b', re.IGNORECASE),
    re.compile(r'\bDry\s+Run\b', re.IGNORECASE),
    # Settings and configuration via UI
    re.compile(r'\bgear\s+icon\b', re.IGNORECASE),
    re.compile(r'\bellipsis\s+icon\b', re.IGNORECASE),
    re.compile(r'\bthree-dot\s+menu\b', re.IGNORECASE),
    re.compile(r'\bSelect\s+\*\*Create\*\*', re.IGNORECASE),
    re.compile(r'\bSelect\s+\*\*Settings\*\*', re.IGNORECASE),
    re.compile(r'\bAdd\s+configuration\b', re.IGNORECASE),
    re.compile(r'\bSelect\s+\*\*Save\*\*', re.IGNORECASE),
]

# Minimum number of distinct pattern matches in a cell to qualify as a UI step
MIN_PATTERN_MATCHES = 2


def extract_section_header(content: str) -> str:
    """Extract the markdown section header (##, ###) from a cell's content."""
    # Look for markdown headers
    match = re.search(r'^#{1,4}\s+(.+?)$', content, re.MULTILINE)
    if match:
        # Clean markdown formatting
        header = match.group(1).strip()
        header = re.sub(r'\*\*(.+?)\*\*', r'\1', header)  # Remove bold
        header = re.sub(r'!\[.*?\]\(.*?\)', '', header)    # Remove images
        return header.strip()
    return ""


def get_ui_step_summary(content: str) -> str:
    """Generate a short summary of what UI actions are described in the cell."""
    actions = []
    if re.search(r'\bSelect\s+(the\s+)?\*\*Catalog\*\*', content, re.IGNORECASE):
        actions.append("Navigate Catalog")
    if re.search(r'\bJobs\s*(&|and)\s*Pipelines\b', content, re.IGNORECASE):
        actions.append("Jobs & Pipelines")
    if re.search(r'\bRun\s+pipeline\b', content, re.IGNORECASE):
        actions.append("Run pipeline")
    if re.search(r'\bDry\s+Run\b', content, re.IGNORECASE):
        actions.append("Dry run")
    if re.search(r'\bOpen\s+in\s+Editor\b', content, re.IGNORECASE):
        actions.append("Open in Editor")
    if re.search(r'\bSchedule\b', content, re.IGNORECASE) and re.search(r'\bpipeline\b', content, re.IGNORECASE):
        actions.append("Schedule pipeline")
    if re.search(r'\bSettings\b', content) and re.search(r'\b(Select|gear|configure)\b', content, re.IGNORECASE):
        actions.append("Configure settings")
    if re.search(r'\bExpand\b', content, re.IGNORECASE) and re.search(r'\b(schema|catalog|volume|folder)\b', content, re.IGNORECASE):
        actions.append("Explore catalog/schema")
    if re.search(r'\bCreate\b.*\b(ETL|Pipeline)\b', content, re.IGNORECASE):
        actions.append("Create pipeline")
    if re.search(r'\bright-click\b', content, re.IGNORECASE):
        actions.append("Right-click menu")
    if re.search(r'\bPipeline\s+graph\b', content, re.IGNORECASE):
        actions.append("Pipeline graph")
    if re.search(r'\bevent.*log\b', content, re.IGNORECASE):
        actions.append("Event log")
    if re.search(r'\bEnable\b.*\b(Lakeflow|Editor|feature)\b', content, re.IGNORECASE):
        actions.append("Enable feature")
    return ", ".join(actions) if actions else "UI interaction"


def scan_notebook_for_ui_steps(notebook_path: str) -> list:
    """Scan a notebook for markdown cells containing UI instructions.
    
    Returns a list of dicts with section header and summary for each UI step found.
    """
    ui_steps = []
    
    try:
        export_resp = w.workspace.export(path=notebook_path, format=ExportFormat.JUPYTER)
        nb_json = json.loads(base64.b64decode(export_resp.content))
    except Exception as e:
        return [{"section": "ERROR", "summary": str(e)}]
    
    cells = nb_json.get("cells", [])
    
    for cell in cells:
        source = ''.join(cell.get("source", []))
        
        # Only check markdown cells and code cells with %md magic (used in Databricks)
        is_markdown = cell.get("cell_type") == "markdown"
        is_md_magic = cell.get("cell_type") == "code" and bool(re.match(r'\s*%md', source))
        
        if not (is_markdown or is_md_magic):
            continue
        
        # Count how many distinct UI patterns match in this cell
        matched_patterns = sum(1 for p in UI_ACTION_PATTERNS if p.search(source))
        
        if matched_patterns >= MIN_PATTERN_MATCHES:
            section = extract_section_header(source)
            summary = get_ui_step_summary(source)
            # Avoid duplicate entries for the same section
            if not any(s["section"] == section and section for s in ui_steps):
                ui_steps.append({
                    "section": section or "(unlabeled section)",
                    "summary": summary,
                    "pattern_matches": matched_patterns,
                })
    
    return ui_steps


# ── Scan All Course Notebooks ─────────────────────────────────────────────────
# Only scan Demos and Labs (lectures are presentation-only, no UI steps expected)

print("═" * 70)
print("AUTO-SCAN: NOTEBOOKS WITH UI INSTRUCTIONS")
print("═" * 70)
print("\nScanning all Demo and Lab notebooks for UI instruction patterns...\n")

UI_INSTRUCTION_NOTEBOOKS = {}
total_scanned = 0

for task in COURSE_TASKS:
    notebook_name = task["name"]
    # Scan Demos and Labs (skip Lectures and overview/summary notebooks)
    if not any(keyword in notebook_name for keyword in ["Demo", "Lab"]):
        continue
    
    notebook_path = os.path.normpath(f"{course_root}/{task['relative_path']}")
    total_scanned += 1
    
    ui_steps = scan_notebook_for_ui_steps(notebook_path)
    
    if ui_steps:
        UI_INSTRUCTION_NOTEBOOKS[notebook_name] = ui_steps

# ── Display Results ───────────────────────────────────────────────────────────
print(f"{len(UI_INSTRUCTION_NOTEBOOKS)} of {total_scanned} Demo/Lab notebooks contain UI steps:\n")

for notebook_name, steps in UI_INSTRUCTION_NOTEBOOKS.items():
    print(f"📋 {notebook_name}  ({len(steps)} UI sections)")
    for step in steps:
        section = step['section']
        summary = step['summary']
        print(f"     • {section} — {summary}")
    print()

total_steps = sum(len(s) for s in UI_INSTRUCTION_NOTEBOOKS.values())
print("═" * 70)
print(f"SCANNED: {total_scanned} notebooks  |  WITH UI STEPS: {len(UI_INSTRUCTION_NOTEBOOKS)}  |  TOTAL UI SECTIONS: {total_steps}")
print("═" * 70)

# COMMAND ----------

# Catalog / schema / table names
RESULTS_CATALOG   = "dbacademy"
RESULTS_SCHEMA    = spark.sql("SELECT current_user()").collect()[0][0].split("@")[0].replace(".", "_").replace("-", "_")
RESULTS_TABLE     = "notebook_run_results"
QA_FINDINGS_TABLE = "qa_content_findings"

# How long to wait for the entire job run (seconds)
TOTAL_RUN_TIMEOUT_SECONDS = 60 * 60 * 2   # 2 hour ceiling
POLL_INTERVAL_SECONDS     = 15

# Job and dashboard names
JOB_NAME       = f"[Course Validation] {COURSE_NAME}"
DASHBOARD_NAME = f"[Course Validation] {COURSE_NAME} Results Dashboard"

# Build the task list from the reusable course configuration above.
TASKS = []

if RUN_QA_CHECKER:
    TASKS.append({
        "task_key": "qa_content_checker",
        "course": "QA",
        "name": QA_TASK_NAME,
        "relative_path": QA_TASK_RELATIVE_PATH,
        "depends_on": [],
        "env_key": "qa_env",
    })

for task in COURSE_TASKS:
    TASKS.append({
        "task_key": task["task_key"],
        "course": COURSE_NAME,
        "name": task["name"],
        "relative_path": task["relative_path"],
        "depends_on": task.get("depends_on", []),
        "env_key": task["env_key"],
        "use_classic": task.get("use_classic", False),
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports and workspace client

# COMMAND ----------

import json
import os
import time
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import Environment
from databricks.sdk.service.jobs import (
    JobEmailNotifications,
    JobEnvironment,
    NotebookTask,
    RunLifeCycleState,
    RunResultState,
    Task,
    TaskDependency,
)

from pyspark.sql import Row
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

w = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resolve workspace paths

# COMMAND ----------

this_notebook_path = (
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)
course_root = "/".join(this_notebook_path.split("/")[:-1])

for t in TASKS:
    # os.path.normpath resolves '../' so Databricks gets a clean absolute path
    t["notebook_path"] = os.path.normpath(f"{course_root}/{t['relative_path']}")

print(f"Course root: {course_root}\n")
for t in TASKS:
    print(f"  [{t['task_key']}]  {t['name']}\n      {t['notebook_path']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ensure results table exists

# COMMAND ----------

results_schema_name = RESULTS_SCHEMA if RESULTS_SCHEMA != "information_schema" else "default"
results_fqn = f"{RESULTS_CATALOG}.{results_schema_name}.{RESULTS_TABLE}"

results_schema = StructType([
    StructField("run_timestamp",    TimestampType(), nullable=False),
    StructField("job_id",           LongType(),      nullable=True),
    StructField("job_run_id",       LongType(),      nullable=True),
    StructField("course",           StringType(),    nullable=True),
    StructField("task_key",         StringType(),    nullable=False),
    StructField("demo_name",        StringType(),    nullable=False),
    StructField("notebook_path",    StringType(),    nullable=False),
    StructField("status",           StringType(),    nullable=False),  # PASS / FAIL / TIMEOUT
    StructField("result_state",     StringType(),    nullable=True),
    StructField("life_cycle_state", StringType(),    nullable=True),
    StructField("duration_seconds", DoubleType(),    nullable=True),
    StructField("run_id",           LongType(),      nullable=True),
    StructField("run_page_url",     StringType(),    nullable=True),
    StructField("error_message",    StringType(),    nullable=True),
])

try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {RESULTS_CATALOG}.{results_schema_name}")
except Exception as e:
    if "UNAUTHORIZED_ACCESS" in str(e) or "PERMISSION_DENIED" in str(e):
        # Fall back to user's own labuser catalog (derived from username)
        user_catalog = spark.sql("SELECT current_user()").collect()[0][0].split("@")[0].replace(".", "_").replace("-", "_")
        print(f"⚠️  No permission to create schema in '{RESULTS_CATALOG}'. Falling back to catalog: {user_catalog}")
        RESULTS_CATALOG = user_catalog
        results_fqn = f"{RESULTS_CATALOG}.{results_schema_name}.{RESULTS_TABLE}"
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {RESULTS_CATALOG}.{results_schema_name}")
    else:
        raise

if not spark.catalog.tableExists(results_fqn):
    spark.createDataFrame([], results_schema).write.format("delta").saveAsTable(results_fqn)
    print(f"Created results table: {results_fqn}")
else:
    print(f"Results table exists: {results_fqn}")

# COMMAND ----------

# ── Skip interactive cells that use input() — they fail in job context ────────
# Cell 104 in "2 Demo - Multi Flow SDP..." calls delete_schemas() which uses
# input() for confirmation. This raises StdinNotImplementedError in job runs.
# Patch those cells before the job executes the notebooks.
#
# Also skip <FILL_IN> cells — these contain pipeline-only SQL that is already
# handled by the pipeline source files created in notebook_path. Running them
# directly in the notebook causes SyntaxError.

import base64
import json
from databricks.sdk.service.workspace import ExportFormat, ImportFormat

SKIP_PATTERNS = ["delete_schemas(", "input(", "raw_input("]
FILL_IN_PATTERN = "<FILL_IN>"

for t in TASKS:
    nb_path = t["notebook_path"]
    try:
        exported = w.workspace.export(path=nb_path, format=ExportFormat.JUPYTER)
        nb_json = json.loads(base64.b64decode(exported.content))

        patched = False
        for idx, cell in enumerate(nb_json.get("cells", [])):
            if cell.get("cell_type") != "code":
                continue
            source_lines = cell.get("source", [])
            source_text = "".join(source_lines)

            # ── Handle <FILL_IN> cells (pipeline-only SQL) ────────────────────
            # These may start with comments so we check BEFORE the startswith("#") guard
            if FILL_IN_PATTERN in source_text:
                cell["source"] = [
                    "# Skipped for automated job runs — pipeline-only SQL (solution is in pipeline source files)\n"
                ] + ["# " + line for line in source_lines]
                patched = True
                print(f"    cell {idx} will be skipped (<FILL_IN> placeholder)")
                continue

            # Skip if already commented out
            if source_text.strip().startswith("#"):
                continue
            if any(pat in source_text for pat in SKIP_PATTERNS):
                cell["source"] = [
                    "# Skipped for automated job runs — interactive input() not supported\n"
                ] + ["# " + line for line in source_lines]
                patched = True

        if patched:
            content_b64 = base64.b64encode(json.dumps(nb_json).encode()).decode()
            w.workspace.import_(
                path=nb_path,
                format=ImportFormat.JUPYTER,
                content=content_b64,
                overwrite=True,
            )
            print(f"  ✅ Patched cells in: {nb_path}")
        else:
            print(f"  ℹ️  No cells to patch: {nb_path}")
    except Exception as e:
        print(f"  ⚠️  Could not patch {nb_path}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create (or re-create) the course Lakeflow Job

# COMMAND ----------

# Delete any existing job with the same name so we always get a clean definition.
ENVIRONMENT_VERSION = "5"

existing = [j for j in w.jobs.list(name=JOB_NAME)]
for j in existing:
    w.jobs.delete(job_id=j.job_id)
    print(f"Deleted existing job: {j.job_id} ({j.settings.name})")

# Build Task objects from TASKS config.
import re

def _safe_key(k):
    return re.sub(r'[^a-zA-Z0-9_-]', '_', k)

# All tasks share one environment (identical spec — consolidates to stay within the 10-env API limit).
SHARED_ENV_KEY = "shared_env"

job_tasks = []
for t in TASKS:
    job_tasks.append(
        Task(
            task_key=_safe_key(t["task_key"]),
            description=t["name"],
            notebook_task=NotebookTask(notebook_path=t["notebook_path"]),
            environment_key=SHARED_ENV_KEY,
            depends_on=[TaskDependency(task_key=_safe_key(dep['task_key'])) for dep in t["depends_on"]],
        )
    )

# ── Route classic-compute tasks to the existing cluster ────────────────────────
# Dynamically fetch the user's classic cluster
current_user = spark.sql("SELECT current_user()").collect()[0][0]
user_prefix = current_user.split("@")[0]

CLASSIC_CLUSTER_ID = None
for c in w.clusters.list():
    if c.creator_user_name == current_user or c.cluster_name == user_prefix:
        CLASSIC_CLUSTER_ID = c.cluster_id
        print(f"Found classic cluster: {c.cluster_name} (ID: {c.cluster_id})")
        break

if not CLASSIC_CLUSTER_ID:
    print("⚠️  No classic cluster found for user. Classic-compute tasks will fall back to serverless.")

if CLASSIC_CLUSTER_ID:
    for i, t in enumerate(TASKS):
        if t.get("use_classic"):
            job_tasks[i].environment_key = None
            job_tasks[i].existing_cluster_id = CLASSIC_CLUSTER_ID

job_environments = [
    JobEnvironment(
        environment_key=SHARED_ENV_KEY,
        spec=Environment(environment_version=ENVIRONMENT_VERSION),
    )
]

created_job = w.jobs.create(
    name=JOB_NAME,
    tasks=job_tasks,
    environments=job_environments,
    email_notifications=JobEmailNotifications(
        on_failure=TESTER_EMAILS,
        on_success=TESTER_EMAILS,
    ),
)
job_id = created_job.job_id
print(f"Created Lakeflow Job: {job_id}  ({JOB_NAME})")
print(f"\nTask DAG:")
for t in TASKS:
    deps = " → depends on: " + ", ".join(dep["task_key"] for dep in t["depends_on"]) if t["depends_on"] else " (starts immediately)"
    compute_label = "[CLASSIC]" if t.get("use_classic") else "[SERVERLESS]"
    print(f"  {compute_label} {t['task_key']}{deps}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Pipelines and Patch Demo Notebooks for Automated Execution
# MAGIC
# MAGIC This course uses the **Lakeflow Pipelines Editor** UI to create and run pipelines.
# MAGIC For automated testing, we must:
# MAGIC
# MAGIC 1. **Extract SQL** from each demo notebook's copy-to-clipboard blocks
# MAGIC 2. **Create pipelines programmatically** using the Databricks SDK
# MAGIC 3. **Patch notebooks** with trigger cells at every "Run pipeline" UI instruction point
# MAGIC
# MAGIC This ensures the full demo flow executes end-to-end without manual UI interaction.

# COMMAND ----------

# ── Helper Functions: Pipeline Creation + Notebook Patching ──────────────────
# These utilities handle:
#   1. Extracting SQL from notebook copy-to-clipboard blocks
#   2. Creating SDP pipelines via the Databricks SDK
#   3. Injecting pipeline trigger cells into demo notebooks
# ─────────────────────────────────────────────────────────────────────────────

import base64
import json
import re
import os
from databricks.sdk.service.workspace import ExportFormat, ImportFormat, Language, ObjectType
from databricks.sdk.service.pipelines import PipelineLibrary, PipelineCluster, FileLibrary

# Derive user catalog (same logic as Classroom-Setup-Common)
user_catalog = spark.sql("SELECT current_user()").collect()[0][0].split("@")[0].replace(".", "_").replace("-", "_")
print(f"User catalog: {user_catalog}")

# Course content root (parent of Includes/)
_this_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_parts = _this_path.split("/")
_includes_idx = next(i for i, p in enumerate(_parts) if p == "Includes")
course_content_root = "/".join(_parts[:_includes_idx])
print(f"Course content root: {course_content_root}")


def extract_sql_from_copy_blocks(notebook_path: str) -> list:
    """Extract all code from copy-to-clipboard <code> blocks in a notebook.
    
    Handles two patterns:
      1. <button onclick="copyBlock()">Copy to clipboard</button> ... <code>...</code>
      2. Falls back to <!---ADD SOLUTION CODE BELOW---> if no copy blocks found
    
    Returns a list of code strings in the order they appear.
    """
    export_resp = w.workspace.export(path=notebook_path, format=ExportFormat.JUPYTER)
    nb_json = json.loads(base64.b64decode(export_resp.content))
    
    # Primary pattern: extract from <code>...</code> in copy-to-clipboard cells
    code_pattern = re.compile(r'<code>\s*\n(.*?)</code>', re.DOTALL)
    # Pattern to strip nested ADD SOLUTION markers
    solution_marker = re.compile(r'<!---+(?:ADD SOLUTION CODE BELOW|END SOLUTION CODE)---+>\s*\n?')
    
    sql_blocks = []
    for cell in nb_json.get("cells", []):
        source = "".join(cell.get("source", []))
        if "Copy to clipboard" in source or "copyBlock()" in source:
            matches = code_pattern.findall(source)
            for match in matches:
                sql = match.strip()
                # Strip any nested solution markers
                sql = solution_marker.sub('', sql).strip()
                # Clean HTML entities
                sql = sql.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
                if sql:
                    sql_blocks.append(sql)
    
    # Fallback: use ADD SOLUTION pattern if no copy blocks found
    if not sql_blocks:
        fallback_pattern = re.compile(
            r'<!---+ADD SOLUTION CODE BELOW---+>\s*\n(.*?)<!---+END SOLUTION CODE---+>',
            re.DOTALL
        )
        for cell in nb_json.get("cells", []):
            source = "".join(cell.get("source", []))
            matches = fallback_pattern.findall(source)
            for match in matches:
                sql = match.strip()
                sql = sql.replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">")
                if sql:
                    sql_blocks.append(sql)
    
    return sql_blocks


def create_pipeline_with_sql(
    pipeline_name: str,
    catalog: str,
    target_schemas: list,
    sql_files: dict,
    configuration: dict = None,
    folder_name: str = None,
) -> str:
    """Create a pipeline with SQL source files written to a workspace folder.
    
    Args:
        pipeline_name: Name of the pipeline to create
        catalog: Target catalog for the pipeline
        target_schemas: List of target schemas (first is used as default)
        sql_files: Dict of {filename: sql_content} to write as source files
        configuration: Pipeline configuration parameters dict
        folder_name: Workspace folder name for pipeline source (defaults to pipeline_name)
    
    Returns:
        pipeline_id of the created pipeline
    """
    if folder_name is None:
        folder_name = pipeline_name.replace(" ", "_")
    
    # Create workspace folder for pipeline source
    pipeline_folder = f"{course_content_root}/{folder_name}"
    try:
        w.workspace.mkdirs(pipeline_folder)
    except Exception:
        pass  # folder may already exist
    
    # Write source files (detect language from extension)
    for filename, sql_content in sql_files.items():
        file_path = f"{pipeline_folder}/{filename}"
        content_b64 = base64.b64encode(sql_content.encode()).decode()
        file_language = Language.PYTHON if filename.endswith(".py") else Language.SQL
        # Delete existing file first to ensure language metadata is set correctly on re-import
        try:
            w.workspace.delete(file_path)
        except Exception:
            pass
        w.workspace.import_(
            path=file_path,
            content=content_b64,
            format=ImportFormat.SOURCE,
            language=file_language,
            overwrite=True,
        )
        print(f"  Wrote: {file_path}")
    
    # Delete existing pipeline with same name
    existing = list(w.pipelines.list_pipelines(filter=f"name LIKE '{pipeline_name}'"))
    for p in existing:
        if p.name == pipeline_name:
            w.pipelines.delete(pipeline_id=p.pipeline_id)
            print(f"  Deleted existing pipeline: {p.pipeline_id}")
    
    # Create pipeline — each SQL file must be listed individually
    pipeline_libraries = [
        PipelineLibrary(file=FileLibrary(path=f"/Workspace{pipeline_folder}/{fname}"))
        for fname in sql_files.keys()
    ]
    
    create_resp = w.pipelines.create(
        name=pipeline_name,
        catalog=catalog,
        target=target_schemas[0] if target_schemas else None,
        libraries=pipeline_libraries,
        configuration=configuration or {},
        serverless=True,
        channel="CURRENT",
    )
    
    pipeline_id = create_resp.pipeline_id
    print(f"  Created pipeline: {pipeline_name} (ID: {pipeline_id})")
    return pipeline_id


def make_pipeline_trigger_cell(pipeline_name: str, comment: str) -> dict:
    """Create a Jupyter notebook cell that triggers and waits for a pipeline run."""
    code = f'''%python
# {comment}
import time
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

PIPELINE_NAME = "{pipeline_name}"
pipelines_list = list(w.pipelines.list_pipelines(filter=f"name LIKE '{{PIPELINE_NAME}}'"))
pipeline_id = next(p.pipeline_id for p in pipelines_list if p.name == PIPELINE_NAME)

def _run_pipeline(pipeline_id, full_refresh=False):
    """Trigger a pipeline update and poll until terminal state."""
    # Check for any active update before starting a new one (handles retries)
    active_states = ("QUEUED", "CREATED", "WAITING_FOR_RESOURCES", "INITIALIZING", "RUNNING", "SETTING_UP_TABLES")
    active_updates = [u for u in (w.pipelines.list_updates(pipeline_id=pipeline_id).updates or [])
                      if u.state and u.state.value in active_states]
    if active_updates:
        update_id = active_updates[0].update_id
        print(f"Found active update {{update_id}} — waiting for it to finish...")
    else:
        refresh_label = " (full_refresh)" if full_refresh else ""
        print(f"Triggering pipeline update{{refresh_label}} for: {{PIPELINE_NAME}} ({{pipeline_id}})")
        update_response = w.pipelines.start_update(pipeline_id=pipeline_id, full_refresh=full_refresh)
        update_id = update_response.update_id
        print(f"Update ID: {{update_id}} - waiting for completion...")

    while True:
        status = w.pipelines.get_update(pipeline_id=pipeline_id, update_id=update_id)
        state = status.update.state.value
        if state in ("COMPLETED", "FAILED", "CANCELED"):
            break
        time.sleep(15)
    return state, update_id

def _get_pipeline_error(pipeline_id, update_id):
    """Retrieve the most recent error events for a failed pipeline update."""
    try:
        events = list(w.pipelines.list_pipeline_events(
            pipeline_id=pipeline_id,
            filter=f"update_id = \'{{update_id}}\'  AND level = \'ERROR\'",
            max_results=5,
        ))
        if events:
            msgs = [e.message for e in events if e.message]
            return "; ".join(msgs[:3])
    except Exception:
        pass
    return "(no error details available)"

# First attempt
state, update_id = _run_pipeline(pipeline_id, full_refresh=True)

# If first attempt fails, retry once
if state != "COMPLETED":
    error_detail = _get_pipeline_error(pipeline_id, update_id)
    print(f"⚠️ First attempt failed ({{state}}): {{error_detail}}")
    print("Retrying with full_refresh...")
    time.sleep(10)
    state, update_id = _run_pipeline(pipeline_id, full_refresh=True)

if state != "COMPLETED":
    error_detail = _get_pipeline_error(pipeline_id, update_id)
    assert False, f"Pipeline update failed with state: {{state}}. Errors: {{error_detail}}"

print(f"✅ Pipeline update {{update_id}} completed successfully")
'''
    return {
        "cell_type": "code",
        "source": [line + "\n" for line in code.split("\n")],
        "metadata": {},
        "outputs": [],
        "execution_count": None,
    }


def patch_notebook_with_triggers(notebook_path: str, pipeline_name: str, markers: list):
    """Patch a notebook by inserting pipeline trigger cells at specified markers.
    
    Args:
        notebook_path: Workspace path to the notebook
        pipeline_name: Name of the pipeline to trigger
        markers: List of dicts with:
            - 'marker': text to search for in cell content
            - 'position': 'before' or 'after' the found cell
            - 'comment': descriptive comment for the trigger cell
    """
    print(f"\nPatching: {notebook_path}")
    export_resp = w.workspace.export(path=notebook_path, format=ExportFormat.JUPYTER)
    nb_json = json.loads(base64.b64decode(export_resp.content))
    cells = nb_json["cells"]
    
    inserted = 0
    for marker_cfg in markers:
        marker = marker_cfg["marker"]
        position = marker_cfg.get("position", "before")
        comment = marker_cfg.get("comment", f"AUTO-INSERTED: Run pipeline ({pipeline_name})")
        
        # Find the cell containing the marker
        idx = None
        for i, cell in enumerate(cells):
            if marker in "".join(cell.get("source", [])):
                idx = i
                break
        
        if idx is not None:
            trigger_cell = make_pipeline_trigger_cell(pipeline_name, comment)
            insert_idx = idx if position == "before" else idx + 1
            # Adjust for previously inserted cells
            insert_idx += inserted
            cells.insert(insert_idx, trigger_cell)
            inserted += 1
            print(f"  Inserted trigger {position} marker '{marker[:50]}...' (index {insert_idx})")
        else:
            print(f"  ⚠️ Marker not found: '{marker[:60]}...'")
    
    if inserted > 0:
        nb_json["cells"] = cells
        modified_content = base64.b64encode(json.dumps(nb_json).encode()).decode()
        w.workspace.import_(
            path=notebook_path,
            content=modified_content,
            format=ImportFormat.JUPYTER,
            language=Language.PYTHON,
            overwrite=True,
        )
        print(f"  ✅ Patched with {inserted} trigger cell(s)")
    else:
        print(f"  ⚠️ No triggers inserted")
    
    return inserted


print("\n✅ Helper functions loaded.")

# COMMAND ----------

# ── Create Pipelines & Patch Notebooks for All Demos ──────────────────────────
# For each demo:
#   1. Extract SQL from the notebook's copy-to-clipboard solution blocks
#   2. Create the pipeline with the extracted SQL source files
#   3. Patch the notebook with trigger cells at "Run pipeline" UI markers
# ─────────────────────────────────────────────────────────────────────────────

print("=" * 70)
print("PIPELINE AUTOMATION: Creating Pipelines & Patching Notebooks")
print("=" * 70)

# Volume paths (derived from user catalog)
multi_flow_vol = f"/Volumes/{user_catalog}/multi_flow_1_bronze"
multiplex_vol = f"/Volumes/{user_catalog}/multiplex_1_bronze"
auto_cdc_vol = f"/Volumes/{user_catalog}/sdp_cdc_1_bronze"
dq_vol = f"/Volumes/{user_catalog}/dq_1_bronze"

# ─────────────────────────────────────────────────────────────────────────────
# DEMO 2: Multi Flow SDP with Liquid Clustering and Data Quality
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "━" * 70)
print("DEMO 2: Multi Flow SDP with Liquid Clustering and Data Quality")
print("━" * 70)

demo_02_path = f"{course_content_root}/2 Demo - Multi Flow SDP with Liquid Clustering and Data Quality"
PIPELINE_NAME_02 = "Multi Flow SDP Demo - Automated Test"

# Extract SQL from Demo 2 notebook
sql_blocks_02 = extract_sql_from_copy_blocks(demo_02_path)
print(f"  Extracted {len(sql_blocks_02)} SQL blocks from Demo 2")

# Build SQL files from extracted blocks
# Demo 2 has: bronze table + 3 flows (block 0-1), silver (block 2), gold MVs (block 3)
# We combine all SQL into logical files
if len(sql_blocks_02) >= 3:
    sql_files_02 = {
        "flow_ingestion.sql": "\n\n".join(sql_blocks_02[:-2]),
        "silver_transformation.sql": sql_blocks_02[-2],
        "gold_mvs.sql": sql_blocks_02[-1],
    }
else:
    # Fallback: put all in one file
    sql_files_02 = {"pipeline.sql": "\n\n".join(sql_blocks_02)}

# Configuration parameters for Demo 2
config_02 = {
    "bright_home_orders_source": f"{multi_flow_vol}/bright_home_orders",
    "lumina_sports_orders_source": f"{multi_flow_vol}/lumina_sports_orders",
    "northstar_outfitters_orders_source": f"{multi_flow_vol}/northstar_outfitters_orders",
}

pipeline_id_02 = create_pipeline_with_sql(
    pipeline_name=PIPELINE_NAME_02,
    catalog=user_catalog,
    target_schemas=["multi_flow_1_bronze"],
    sql_files=sql_files_02,
    configuration=config_02,
    folder_name="ingest_multiple_flows",
)

# Patch Demo 2 with trigger cells at each "Run pipeline" point
patch_notebook_with_triggers(
    notebook_path=demo_02_path,
    pipeline_name=PIPELINE_NAME_02,
    markers=[
        {"marker": "### D6. Run and Explore the Pipeline", "position": "after", "comment": "AUTO-INSERTED: Run pipeline (Bronze - 3 flows, 449 rows)"},
        {"marker": "### E3. Run and Explore the Pipeline", "position": "after", "comment": "AUTO-INSERTED: Run pipeline (Silver - transformations + DQ)"},
        {"marker": "### F3. Run and Explore the Pipeline", "position": "after", "comment": "AUTO-INSERTED: Run pipeline (Gold - materialized views)"},
    ]
)

# ─────────────────────────────────────────────────────────────────────────────
# DEMO 4: Multiplex Streaming SDP with Delta Sinks and Iceberg Reads
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "━" * 70)
print("DEMO 4: Multiplex Streaming SDP with Delta Sinks and Iceberg Reads")
print("━" * 70)

demo_04_path = f"{course_content_root}/4 Demo - Multiplex Streaming SDP with Delta Sinks and Iceberg Reads"
PIPELINE_NAME_04 = "Multiplex Streaming SDP Demo - Automated Test"

# Extract SQL from Demo 4 notebook
sql_blocks_04 = extract_sql_from_copy_blocks(demo_04_path)
print(f"  Extracted {len(sql_blocks_04)} SQL blocks from Demo 4")

# Build SQL files - Demo 4 pattern: bronze(0), intermediate(1-3), silver(4-6), gold(7), python sink(8)
# Separate Python blocks (Delta Sink) from SQL blocks
sql_only_04 = [b for b in sql_blocks_04 if not b.lstrip().startswith('#') and 'from pyspark' not in b.lower()]
python_blocks_04 = [b for b in sql_blocks_04 if b.lstrip().startswith('#') or 'from pyspark' in b.lower()]

if len(sql_only_04) >= 4:
    sql_files_04 = {
        "bronze_multiplex.sql": "\n\n".join(sql_only_04[:4]),
        "silver_transforms.sql": "\n\n".join(sql_only_04[4:-1]) if len(sql_only_04) > 5 else sql_only_04[4] if len(sql_only_04) > 4 else "",
        "gold_views.sql": sql_only_04[-1],
    }
    # Remove empty files
    sql_files_04 = {k: v for k, v in sql_files_04.items() if v}
elif len(sql_only_04) >= 2:
    sql_files_04 = {
        "bronze_multiplex.sql": sql_only_04[0],
        "silver_gold.sql": "\n\n".join(sql_only_04[1:]),
    }
else:
    sql_files_04 = {"pipeline.sql": "\n\n".join(sql_only_04)}

# Add Python Delta Sink file if present
if python_blocks_04:
    sql_files_04["delta_sink.py"] = python_blocks_04[0]

# Configuration parameters for Demo 4
config_04 = {
    "business_events_source": f"{multiplex_vol}/business_events",
    "my_catalog": user_catalog,
}

pipeline_id_04 = create_pipeline_with_sql(
    pipeline_name=PIPELINE_NAME_04,
    catalog=user_catalog,
    target_schemas=["multiplex_1_bronze"],
    sql_files=sql_files_04,
    configuration=config_04,
    folder_name="multiplex_pipeline",
)

# Patch Demo 4 with trigger cells
patch_notebook_with_triggers(
    notebook_path=demo_04_path,
    pipeline_name=PIPELINE_NAME_04,
    markers=[
        {"marker": "Run pipeline", "position": "before", "comment": "AUTO-INSERTED: Run pipeline (Bronze multiplex ingestion)"},
        {"marker": "Run and Explore the Demultiplexed", "position": "after", "comment": "AUTO-INSERTED: Run pipeline (Silver demultiplex + transforms)"},
        {"marker": "Run and Explore the Gold", "position": "after", "comment": "AUTO-INSERTED: Run pipeline (Gold aggregation views)"},
    ]
)

# ─────────────────────────────────────────────────────────────────────────────
# DEMO 6: Automating SCD Type 2 with AUTO CDC
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "━" * 70)
print("DEMO 6: Automating SCD Type 2 with AUTO CDC")
print("━" * 70)

demo_06_path = f"{course_content_root}/6 Demo - Automating SCD Type 2 with AUTO CDC in Apache Spark Declarative Pipelines"
PIPELINE_NAME_06 = "Auto CDC SCD Type 2 Demo - Automated Test"

# Extract SQL from Demo 6
sql_blocks_06 = extract_sql_from_copy_blocks(demo_06_path)
print(f"  Extracted {len(sql_blocks_06)} SQL blocks from Demo 6")

# Build SQL files for Demo 6 (AUTO CDC pattern)
if len(sql_blocks_06) >= 2:
    sql_files_06 = {
        "bronze_cdc.sql": sql_blocks_06[0],
        "silver_auto_cdc.sql": "\n\n".join(sql_blocks_06[1:]),
    }
else:
    sql_files_06 = {"pipeline.sql": "\n\n".join(sql_blocks_06)}

# Configuration parameters for Demo 6
config_06 = {
    "source": f"{auto_cdc_vol}/customer_source_files",
}

pipeline_id_06 = create_pipeline_with_sql(
    pipeline_name=PIPELINE_NAME_06,
    catalog=user_catalog,
    target_schemas=["sdp_cdc_1_bronze"],
    sql_files=sql_files_06,
    configuration=config_06,
    folder_name="auto_cdc_pipeline",
)

# Patch Demo 6 with trigger cells
patch_notebook_with_triggers(
    notebook_path=demo_06_path,
    pipeline_name=PIPELINE_NAME_06,
    markers=[
        {"marker": "D1. Run the Spark Declarative Pipeline", "position": "after", "comment": "AUTO-INSERTED: Run pipeline (Initial CDC load)"},
        {"marker": "Run pipeline with full table refresh", "position": "before", "comment": "AUTO-INSERTED: Run pipeline (Process new CDC events)"},
    ]
)

# ─────────────────────────────────────────────────────────────────────────────
# DEMO 8: Advanced Data Quality Checks and Expectations in SDP
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "━" * 70)
print("DEMO 8: Advanced Data Quality Checks and Expectations in SDP")
print("━" * 70)

demo_08_path = f"{course_content_root}/8 Demo - Advanced Data Quality Checks and Expectations in SDP"
PIPELINE_NAME_08 = "Advanced DQ Expectations Demo - Automated Test"

# Extract SQL from Demo 8
sql_blocks_08 = extract_sql_from_copy_blocks(demo_08_path)
print(f"  Extracted {len(sql_blocks_08)} SQL blocks from Demo 8")

# Build SQL files for Demo 8 (DQ expectations pattern)
if len(sql_blocks_08) >= 2:
    sql_files_08 = {
        "bronze_ingestion.sql": sql_blocks_08[0],
        "silver_dq_expectations.sql": "\n\n".join(sql_blocks_08[1:]),
    }
else:
    sql_files_08 = {"pipeline.sql": "\n\n".join(sql_blocks_08)}

# Configuration parameters for Demo 8
config_08 = {
    "source": f"{dq_vol}/sales",
}

pipeline_id_08 = create_pipeline_with_sql(
    pipeline_name=PIPELINE_NAME_08,
    catalog=user_catalog,
    target_schemas=["dq_1_bronze"],
    sql_files=sql_files_08,
    configuration=config_08,
    folder_name="dq_expectations_pipeline",
)

# Patch Demo 8 with trigger cells
patch_notebook_with_triggers(
    notebook_path=demo_08_path,
    pipeline_name=PIPELINE_NAME_08,
    markers=[
        {"marker": "D3. Run and Validate the Bronze Layer", "position": "after", "comment": "AUTO-INSERTED: Run pipeline (Bronze layer initial run)"},
        {"marker": "G1. Execute the Complete Pipeline", "position": "after", "comment": "AUTO-INSERTED: Run pipeline (Bronze + DQ expectations)"},
        {"marker": "H3. Execute Pipeline Run 2", "position": "after", "comment": "AUTO-INSERTED: Run pipeline (Updated DQ rules)"},
    ]
)

# ─────────────────────────────────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("AUTOMATION COMPLETE")
print("=" * 70)
print(f"\nPipelines created:")
print(f"  • Demo 2: {PIPELINE_NAME_02} ({pipeline_id_02})")
print(f"  • Demo 4: {PIPELINE_NAME_04} ({pipeline_id_04})")
print(f"  • Demo 6: {PIPELINE_NAME_06} ({pipeline_id_06})")
print(f"  • Demo 8: {PIPELINE_NAME_08} ({pipeline_id_08})")
print(f"\nNotebooks patched with pipeline trigger cells.")
print(f"The Lakeflow Job can now run all demos end-to-end without UI interaction.")

# COMMAND ----------

# ── LAB 9: Building Multi-Source Ecommerce Pipeline with SDP ─────────────
# Same pattern as Demo notebooks:
#   1. Extract SQL from the notebook's copy-to-clipboard solution blocks
#   2. Create the pipeline with the extracted SQL source files
#   3. Patch the notebook with trigger cells at "Run pipeline" UI markers
# NOTE: <FILL_IN> cells and pipeline-only cells (e.g. cell 38) are LEFT
#       UNTOUCHED in the notebook — the solutions go ONLY into the pipeline.
# ─────────────────────────────────────────────────────────────────────────────
print("\n" + "━" * 70)
print("LAB 9: Building Multi-Source Ecommerce Pipeline with SDP")
print("━" * 70)

lab_09_path = f"{course_content_root}/9 Lab - Building Multi-Source Ecommerce Pipeline with SDP"
PIPELINE_NAME_09 = "Multi-Source Ecommerce Lab - Automated Test"

# ── Step 1: Extract SQL from solution blocks and create pipeline ──────────
print("\n  Step 1: Extracting SQL from solution blocks...")

sql_blocks_lab = extract_sql_from_copy_blocks(lab_09_path)
print(f"  Extracted {len(sql_blocks_lab)} SQL blocks from Lab 9")

if sql_blocks_lab:
    sql_files_lab = {"lab_pipeline.sql": "\n\n".join(sql_blocks_lab)}

    # Configuration for lab — parameters must match ${...} references in the SQL
    config_lab = {
        "app_orders_source": f"/Volumes/{user_catalog}/lab_1_bronze/app_orders",
        "web_orders_source": f"/Volumes/{user_catalog}/lab_1_bronze/web_orders",
        "product_catalog_source": f"/Volumes/{user_catalog}/lab_1_bronze/ops",
    }

    pipeline_id_09 = create_pipeline_with_sql(
        pipeline_name=PIPELINE_NAME_09,
        catalog=user_catalog,
        target_schemas=["lab_1_bronze", "lab_2_silver", "lab_3_gold"],
        sql_files=sql_files_lab,
        configuration=config_lab,
        folder_name="lab_ecommerce_pipeline",
    )
else:
    print("  ⚠️ No SQL blocks found in Lab 9 - pipeline may need manual configuration")
    pipeline_id_09 = None

# ── Step 2: Patch lab notebook with trigger cells ─────────────────────────
# Same approach as Demos 2, 4, 6, 8: insert trigger cells at "Run pipeline"
# markers. The notebook content (including <FILL_IN> and pipeline-only cells)
# is left as-is — solutions live only in the pipeline source files.
if pipeline_id_09:
    print("\n  Step 2: Patching lab notebook with trigger cells...")
    patch_notebook_with_triggers(
        notebook_path=lab_09_path,
        pipeline_name=PIPELINE_NAME_09,
        markers=[
            {"marker": "D4. Run and Explore the Pipeline", "position": "after", "comment": "AUTO-INSERTED: Run pipeline (Bronze layer initial load)"},
            {"marker": "E6. Run and Explore the Pipeline", "position": "after", "comment": "AUTO-INSERTED: Run pipeline (Silver layer with expectations)"},
            {"marker": "F3. Run the Full Pipeline and Validate Gold Layer", "position": "after", "comment": "AUTO-INSERTED: Run pipeline (Gold layer analytics)"},
            {"marker": "G3. Run and Explore the Pipeline", "position": "after", "comment": "AUTO-INSERTED: Run pipeline (Incremental processing)"},
        ]
    )

print(f"\n  ✅ Pipeline: {PIPELINE_NAME_09} ({pipeline_id_09})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trigger the job run and poll until all tasks complete

# COMMAND ----------

run_response  = w.jobs.run_now(job_id=job_id)
job_run_id    = run_response.run_id
run_timestamp = datetime.now(timezone.utc)

print(f"Triggered job run: {job_run_id}")
print(f"Polling every {POLL_INTERVAL_SECONDS}s (timeout={TOTAL_RUN_TIMEOUT_SECONDS}s)...\n")

deadline        = time.time() + TOTAL_RUN_TIMEOUT_SECONDS
final_run       = None
terminal_states = {RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED, RunLifeCycleState.INTERNAL_ERROR}

while time.time() < deadline:
    run = w.jobs.get_run(run_id=job_run_id)
    lc  = run.state.life_cycle_state if run.state else None

    task_states = {
        tk.task_key: (
            tk.state.life_cycle_state.value if tk.state and tk.state.life_cycle_state else "PENDING"
        )
        for tk in (run.tasks or [])
    }
    print(f"  [{datetime.now(timezone.utc).strftime('%H:%M:%S')}] run={lc}  tasks={task_states}")

    if lc in terminal_states:
        final_run = run
        break

    time.sleep(POLL_INTERVAL_SECONDS)
else:
    print("TIMEOUT — cancelling the run.")
    try:
        w.jobs.cancel_run(run_id=job_run_id)
    except Exception:
        pass
    final_run = w.jobs.get_run(run_id=job_run_id)

print(f"\nFinal run state: {final_run.state.life_cycle_state if final_run.state else 'UNKNOWN'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Collect per-task results

# COMMAND ----------

task_cfg = {t["task_key"]: t for t in TASKS}

results = []
for task_run in (final_run.tasks or []):
    cfg   = task_cfg.get(task_run.task_key, {})
    state = task_run.state

    result_state = state.result_state.value     if state and state.result_state     else None
    life_cycle   = state.life_cycle_state.value  if state and state.life_cycle_state  else None
    error_msg    = state.state_message           if state                             else None

    duration = None
    if task_run.start_time and task_run.end_time:
        duration = float((task_run.end_time - task_run.start_time) / 1000.0)

    if life_cycle == RunLifeCycleState.TERMINATED.value and result_state == RunResultState.SUCCESS.value:
        status = "PASS"
    elif life_cycle in (RunLifeCycleState.SKIPPED.value, RunLifeCycleState.INTERNAL_ERROR.value):
        status = "FAIL"
    elif life_cycle == RunLifeCycleState.TERMINATED.value:
        status = "FAIL"
    else:
        status = "TIMEOUT"

    results.append({
        "job_id":           job_id,
        "job_run_id":       job_run_id,
        "course":           cfg.get("course", ""),
        "task_key":         task_run.task_key,
        "demo_name":        cfg.get("name", task_run.task_key),
        "notebook_path":    cfg.get("notebook_path", ""),
        "status":           status,
        "result_state":     result_state,
        "life_cycle_state": life_cycle,
        "duration_seconds": duration,
        "run_id":           task_run.run_id,
        "run_page_url":     task_run.run_page_url,
        "error_message":    error_msg,
    })

# Sort back into configured TASKS order
order = {t["task_key"]: i for i, t in enumerate(TASKS)}
results.sort(key=lambda r: order.get(r["task_key"], 999))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Append results to Delta + display summary

# COMMAND ----------

rows       = [Row(run_timestamp=run_timestamp, **r) for r in results]
results_df = spark.createDataFrame(rows, schema=results_schema)

results_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(results_fqn)

print(f"Appended {results_df.count()} rows to {results_fqn}\n")

# Summary for the configured single-course run
summary_groups = [COURSE_NAME]
if RUN_QA_CHECKER:
    summary_groups.append("QA")

for group_name in summary_groups:
    group_results = [r for r in results if r["course"] == group_name]
    if not group_results:
        continue

    passed = sum(1 for r in group_results if r["status"] == "PASS")
    failed = sum(1 for r in group_results if r["status"] != "PASS")
    icon = "✅" if failed == 0 else "❌"
    label = "QA Checks" if group_name == "QA" else COURSE_NAME
    print(f"  {icon} {label}:  {passed}/{len(group_results)} passed")

print(f"\nOverall: {sum(1 for r in results if r['status'] == 'PASS')}/{len(results)} tasks passed")

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Lakeview Dashboard

# COMMAND ----------

import json
from databricks.sdk.service.dashboards import Dashboard

# ── Dashboard Configuration ────────────────────────────────────────────────────
DASHBOARD_FOLDER = "/".join(this_notebook_path.split("/")[:-2])  # Up from CourseRunner/
qa_findings_fqn = f"{RESULTS_CATALOG}.{RESULTS_SCHEMA}.{QA_FINDINGS_TABLE}"
# The QA checker may write to the 'default' schema — check both locations
if not spark.catalog.tableExists(qa_findings_fqn):
    qa_findings_alt = f"{RESULTS_CATALOG}.default.{QA_FINDINGS_TABLE}"
    if spark.catalog.tableExists(qa_findings_alt):
        qa_findings_fqn = qa_findings_alt
course_label_sql = COURSE_NAME.replace("'", "''")

# ── Dataset SQL (always filters to the latest run) ────────────────────────────
run_filter = f"job_run_id = (SELECT MAX(job_run_id) FROM {results_fqn})"
qa_filter  = f"run_timestamp = (SELECT MAX(run_timestamp) FROM {qa_findings_fqn})"

status_summary_sql = (
    f"SELECT status, COUNT(*) AS task_count "
    f"FROM {results_fqn} WHERE {run_filter} "
    f"GROUP BY status ORDER BY status"
)

task_detail_sql = (
    f"SELECT CASE WHEN course = 'QA' THEN 'QA Checks' ELSE '{course_label_sql}' END AS run_group, "
    f"task_key, demo_name, status, ROUND(duration_seconds, 1) AS duration_seconds, "
    f"run_page_url, error_message "
    f"FROM {results_fqn} WHERE {run_filter} "
    f"ORDER BY CASE WHEN course = 'QA' THEN 0 ELSE 1 END, task_key"
)

qa_sev_sql = (
    f"SELECT severity, COUNT(*) AS issue_count "
    f"FROM {qa_findings_fqn} WHERE {qa_filter} "
    f"GROUP BY severity ORDER BY issue_count DESC"
) if spark.catalog.tableExists(qa_findings_fqn) else (
    "SELECT 'No Data' AS severity, 0 AS issue_count WHERE 1=0"
)

qa_detail_sql = (
    f"SELECT notebook_name, cell_index, issue_type, severity, "
    f"offending_text, suggested_fix, check_source "
    f"FROM {qa_findings_fqn} WHERE {qa_filter} "
    f"ORDER BY severity, notebook_name, cell_index"
) if spark.catalog.tableExists(qa_findings_fqn) else (
    "SELECT '' AS notebook_name, 0 AS cell_index, '' AS issue_type, "
    "'' AS severity, '' AS offending_text, '' AS suggested_fix, '' AS check_source WHERE 1=0"
)

datasets = [
    {
        "name": "ds_status_summary",
        "displayName": "Task Status Summary",
        "query": status_summary_sql,
    },
    {
        "name": "ds_task_detail",
        "displayName": "Task Results Detail",
        "query": task_detail_sql,
    },
]

pages = [
    {
        "name": "pg_runs",
        "displayName": f"{COURSE_NAME} Run Results",
        "layout": [
            {
                "widget": {
                    "name": "w_bar",
                    "title": "Task Status — Latest Run",
                    "description": "",
                    "queries": [{
                        "name": "main",
                        "query": {
                            "datasetName": "ds_status_summary",
                            "fields": [
                                {"name": "status", "expression": "`status`"},
                                {"name": "task_count", "expression": "`task_count`"}
                            ],
                            "disaggregated": True
                        }
                    }],
                    "spec": {
                        "version": 3,
                        "widgetType": "bar",
                        "encodings": {
                            "x": {"fieldName": "status", "scale": {"type": "categorical"}},
                            "y": {"fieldName": "task_count", "scale": {"type": "quantitative"}}
                        }
                    }
                },
                "position": {"x": 0, "y": 0, "width": 6, "height": 6}
            },
            {
                "widget": {
                    "name": "w_task_table",
                    "title": "Task Results — Latest Run",
                    "description": "",
                    "queries": [{
                        "name": "main",
                        "query": {
                            "datasetName": "ds_task_detail",
                            "fields": [
                                {"name": "run_group", "expression": "`run_group`"},
                                {"name": "task_key", "expression": "`task_key`"},
                                {"name": "demo_name", "expression": "`demo_name`"},
                                {"name": "status", "expression": "`status`"},
                                {"name": "duration_seconds", "expression": "`duration_seconds`"},
                                {"name": "run_page_url", "expression": "`run_page_url`"},
                                {"name": "error_message", "expression": "`error_message`"}
                            ],
                            "disaggregated": True
                        }
                    }],
                    "spec": {
                        "version": 2,
                        "widgetType": "table",
                        "encodings": {
                            "columns": [
                                {"fieldName": "run_group"},
                                {"fieldName": "task_key"},
                                {"fieldName": "demo_name"},
                                {"fieldName": "status"},
                                {"fieldName": "duration_seconds"},
                                {"fieldName": "run_page_url"},
                                {"fieldName": "error_message"}
                            ]
                        }
                    }
                },
                "position": {"x": 0, "y": 6, "width": 12, "height": 8}
            }
        ]
    }
]

if RUN_QA_CHECKER:
    datasets.extend([
        {
            "name": "ds_qa_severity",
            "displayName": "QA Issues by Severity",
            "query": qa_sev_sql,
        },
        {
            "name": "ds_qa_detail",
            "displayName": "QA Findings Detail",
            "query": qa_detail_sql,
        },
    ])

    pages.append({
        "name": "pg_qa",
        "displayName": "QA Findings",
        "layout": [
            {
                "widget": {
                    "name": "w_qa_bar",
                    "title": "QA Issues by Severity — Latest Run",
                    "description": "",
                    "queries": [{
                        "name": "main",
                        "query": {
                            "datasetName": "ds_qa_severity",
                            "fields": [
                                {"name": "severity", "expression": "`severity`"},
                                {"name": "issue_count", "expression": "`issue_count`"}
                            ],
                            "disaggregated": True
                        }
                    }],
                    "spec": {
                        "version": 3,
                        "widgetType": "bar",
                        "encodings": {
                            "x": {"fieldName": "severity", "scale": {"type": "categorical"}},
                            "y": {"fieldName": "issue_count", "scale": {"type": "quantitative"}}
                        }
                    }
                },
                "position": {"x": 0, "y": 0, "width": 6, "height": 6}
            },
            {
                "widget": {
                    "name": "w_qa_table",
                    "title": "QA Findings Detail — Latest Run",
                    "description": "",
                    "queries": [{
                        "name": "main",
                        "query": {
                            "datasetName": "ds_qa_detail",
                            "fields": [
                                {"name": "notebook_name", "expression": "`notebook_name`"},
                                {"name": "cell_index", "expression": "`cell_index`"},
                                {"name": "issue_type", "expression": "`issue_type`"},
                                {"name": "severity", "expression": "`severity`"},
                                {"name": "offending_text", "expression": "`offending_text`"},
                                {"name": "suggested_fix", "expression": "`suggested_fix`"},
                                {"name": "check_source", "expression": "`check_source`"}
                            ],
                            "disaggregated": True
                        }
                    }],
                    "spec": {
                        "version": 2,
                        "widgetType": "table",
                        "encodings": {
                            "columns": [
                                {"fieldName": "notebook_name"},
                                {"fieldName": "cell_index"},
                                {"fieldName": "issue_type"},
                                {"fieldName": "severity"},
                                {"fieldName": "offending_text"},
                                {"fieldName": "suggested_fix"},
                                {"fieldName": "check_source"}
                            ]
                        }
                    }
                },
                "position": {"x": 0, "y": 6, "width": 12, "height": 8}
            }
        ]
    })

spec = {
    "datasets": datasets,
    "pages": pages,
}

# ── Auto-discover a SQL warehouse for the dashboard ───────────────────────────
from databricks.sdk.service.sql import State as WarehouseState

warehouse_id = None
try:
    for wh in w.warehouses.list():
        if wh.state in (WarehouseState.RUNNING, WarehouseState.STOPPED):
            warehouse_id = wh.id
            if wh.state == WarehouseState.RUNNING:
                break  # Prefer a running warehouse
except Exception as e:
    print(f"⚠️  Could not list warehouses: {e}")

if warehouse_id:
    print(f"Using SQL warehouse: {warehouse_id}")
else:
    print("⚠️  No SQL warehouse found — dashboard will have no data until one is assigned.")

# ── Create / re-create the Lakeview Dashboard ─────────────────────────────────
try:
    for d in w.lakeview.list():
        if d.display_name == DASHBOARD_NAME:
            w.lakeview.trash(dashboard_id=d.dashboard_id)
            print(f"Replaced existing dashboard: {d.dashboard_id}")
            break
except Exception:
    pass  # No existing dashboard — proceed to create

# ── Create / re-create the Lakeview Dashboard ─────────────────────────────────
# Clean up: remove any existing dashboard (API-created or file-based)
try:
    for d in w.lakeview.list():
        if d.display_name == DASHBOARD_NAME:
            state = str(d.lifecycle_state).upper() if d.lifecycle_state else ""
            if "TRASH" not in state:
                w.lakeview.trash(dashboard_id=d.dashboard_id)
                print(f"Replaced existing dashboard: {d.dashboard_id}")
except Exception:
    pass

# Also remove any .lvdash.json file with the same name
try:
    w.workspace.delete(path=f"{DASHBOARD_FOLDER}/{DASHBOARD_NAME}.lvdash.json")
except Exception:
    pass

dashboard = w.lakeview.create(Dashboard(
    display_name=DASHBOARD_NAME,
    serialized_dashboard=json.dumps(spec),
    parent_path=DASHBOARD_FOLDER,
    warehouse_id=warehouse_id,
))
dashboard_id = dashboard.dashboard_id
w.lakeview.publish(dashboard_id=dashboard_id, warehouse_id=warehouse_id, embed_credentials=True)

workspace_host = spark.conf.get("spark.databricks.workspaceUrl")
dashboard_url  = f"https://{workspace_host}/dashboardsv3/{dashboard_id}"

print(f"✅  Lakeview Dashboard created & published!")
print(f"    Name : {DASHBOARD_NAME}")
print(f"    ID   : {dashboard_id}")
print(f"    URL  : {dashboard_url}")
print(f"\n    ⚠️  NOTE: Lakeview API limitation — widget visualizations require one-time")
print(f"    activation via the dashboard editor. Open the dashboard and re-save widgets.")

# ── Inline Visualization (always works, regardless of dashboard rendering) ────
print("\n" + "═" * 70)
print("INLINE RESULTS VISUALIZATION")
print("═" * 70)

# Status summary chart
status_df = spark.sql(status_summary_sql)
display(status_df)

# QA severity chart (if table exists)
if spark.catalog.tableExists(qa_findings_fqn):
    qa_df = spark.sql(qa_sev_sql)
    display(qa_df)

# COMMAND ----------

# Display summary of failed tasks (deduplicated) with detailed error description
failed_rows = []
for r in results:
    if r["status"] != "PASS":
        name = r["demo_name"]
        if not any(row["Task"] == name for row in failed_rows):
            # Fetch detailed error from run output
            error_desc = ""
            try:
                run_output = w.jobs.get_run_output(run_id=r["run_id"])
                error_desc = (run_output.error or "").strip()
                if not error_desc and run_output.error_trace:
                    error_desc = run_output.error_trace.strip().split("\n")[-1]
            except Exception:
                pass
            failed_rows.append({
                "Task": name,
                "Error": r["error_message"] or "Workload failed",
                "Error Description": error_desc or "See run output for details",
            })

if failed_rows:
    failed_df = spark.createDataFrame(failed_rows)
    display(failed_df.select("Task", "Error", "Error Description"))
else:
    print("All tasks passed — no failures to report.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fail the notebook if any task failed

# COMMAND ----------

failed = [r for r in results if r["status"] != "PASS"]
if failed:
    summary = "\n".join(
        f"  - [{r['task_key']}] {r['demo_name']}: {r['status']} ({r['result_state']}) — {r['run_page_url']}"
        for r in failed
    )
    raise RuntimeError(
        f"{len(failed)} of {len(results)} task(s) failed on Serverless v{ENVIRONMENT_VERSION}:\n{summary}"
    )

print(f"All {len(results)} tasks passed on Serverless v{ENVIRONMENT_VERSION}.")
dbutils.notebook.exit(json.dumps({
    "total":  len(results),
    "passed": len(results),
    "failed": 0,
}))