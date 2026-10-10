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

# MAGIC %run ./notebook_path

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

for task in COURSE_TASKS:
    TASKS.append({
        "task_key": task["task_key"],
        "course": COURSE_NAME,
        "name": task["name"],
        "relative_path": task["relative_path"],
        "depends_on": task.get("depends_on", []),
        "use_classic": task.get("use_classic", False),
    })

if RUN_QA_CHECKER:
    TASKS.append({
        "task_key": "qa_content_checker",
        "course": "QA",
        "name": QA_TASK_NAME,
        "relative_path": QA_TASK_RELATIVE_PATH,
        "depends_on": [],
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

# Patch DP 1.2: add pipeline completion wait so downstream cells don't hit TABLE_OR_VIEW_NOT_FOUND
dp12_tasks = [t for t in TASKS if "1.2" in t["task_key"]]
if dp12_tasks:
    dp12_path = dp12_tasks[0]["notebook_path"]
    export_resp = w.workspace.export(path=dp12_path, format=ExportFormat.SOURCE)
    source = base64.b64decode(export_resp.content).decode("utf-8")

    old_code = "demo_pipeline.start_pipeline()"
    new_code = (
        "demo_pipeline.start_pipeline()\n"
        "\n"
        "# Wait for the pipeline to complete before querying its tables\n"
        "import time\n"
        "_pipeline_id = demo_pipeline.get_pipeline_id()\n"
        "_ws = demo_pipeline.workspace\n"
        'print("Waiting for pipeline to complete...")\n'
        "while True:\n"
        "    updates = _ws.pipelines.list_updates(_pipeline_id)\n"
        "    if updates.updates:\n"
        "        latest = updates.updates[0]\n"
        '        state = latest.state.value if latest.state else "UNKNOWN"\n'
        '        if state in ("COMPLETED",):\n'
        '            print("Pipeline completed successfully!")\n'
        "            break\n"
        '        elif state in ("FAILED", "CANCELED"):\n'
        '            raise Exception(f"Pipeline {state}. Check the pipeline UI for details.")\n'
        "        else:\n"
        '            print(f"Pipeline state: {state}. Waiting...")\n'
        "    time.sleep(15)"
    )

    if old_code in source:
        patched = source.replace(old_code, new_code, 1)
        w.workspace.import_(
            path=dp12_path,
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            content=base64.b64encode(patched.encode("utf-8")).decode("utf-8"),
            overwrite=True,
        )
        print("Patched DP 1.2: added pipeline completion wait after start_pipeline()")
    else:
        print("DP 1.2 start_pipeline() already patched or not found - skipping")
else:
    print("DP 1.2 not in TASKS - no patch needed")

# COMMAND ----------

# Patch DP 1.3: wait for actual table versions instead of relying on fixed sleeps
# so CDF reads do not fail with "Start version cannot be greater than the latest version"
dp13_tasks = [t for t in TASKS if "1.3" in t["task_key"]]
if dp13_tasks:
    dp13_path = dp13_tasks[0]["notebook_path"]
    export_resp = w.workspace.export(path=dp13_path, format=ExportFormat.SOURCE)
    source = base64.b64decode(export_resp.content).decode("utf-8")

    patched = source
    patches_applied = 0

    # Patch 1: After Auto Loader stream + first DA.load (batch 1)
    old1 = "DA.load(copy_from=DA.paths.stream_source.cdf_demo, \n        copy_to=DA.paths.cdc_stream, \n        n=1)"
    new1 = (
        old1 + "\n\n"
        "import time\n"
        'print("Waiting 60 seconds for the Auto Loader stream to process...")\n'
        "time.sleep(60)"
    )
    if old1 in patched and "Waiting 60 seconds for the Auto Loader" not in patched:
        patched = patched.replace(old1, new1, 1)
        patches_applied += 1

    # Patch 2: After silver merge stream start
    old2 = (
        "silver_users_stream = (\n"
        "              spark\n"
        "              .readStream\n"
        '                .table("bronze_users")\n'
        "              .writeStream\n"
        "                .foreachBatch(upsert_to_delta)  # Upsert micro-batch data into the silver table\n"
        "                .trigger(processingTime='3 seconds')  # Trigger the stream processing every 3 seconds\n"
        "                .start()\n"
        "        )"
    )
    new2 = (
        old2 + "\n\n"
        "import time\n"
        'print("Waiting 60 seconds for the streaming merge into silver_users...")\n'
        "time.sleep(60)"
    )
    if old2 in patched and "Waiting 60 seconds for the streaming merge" not in patched:
        patched = patched.replace(old2, new2, 1)
        patches_applied += 1

    # Patch 3: Make the CDF read wait for silver_users version 2 to exist
    old3 = (
        'cdf_df = (spark.read\n'
        '               .format("delta")\n'
        '               .option("readChangeData", True)   # Read the change data\n'
        '               .option("startingVersion", 2)     # Since we want to start reading changes from version 2 (when we merged the data)\n'
        '               .table("silver_users"))\n\n'
        '## Display the changed data\n'
        'display(cdf_df)'
    )
    new3 = (
        'import time\n\n'
        'def wait_for_table_version(table_name, min_version, timeout_seconds=300, poll_seconds=10):\n'
        '    deadline = time.time() + timeout_seconds\n'
        '    latest_version = None\n'
        '    while time.time() < deadline:\n'
        '        latest_version = spark.sql(f"DESCRIBE HISTORY {table_name}").selectExpr("max(version) as max_version").collect()[0]["max_version"]\n'
        '        if latest_version is not None and latest_version >= min_version:\n'
        '            print(f"{table_name} is now at version {latest_version}; continuing with CDF read from version {min_version}.")\n'
        '            return latest_version\n'
        '        print(f"Waiting for {table_name} to reach version {min_version}. Current latest version: {latest_version}")\n'
        '        time.sleep(poll_seconds)\n'
        '    raise Exception(f"Timed out waiting for {table_name} to reach version {min_version}. Latest version: {latest_version}")\n\n'
        'wait_for_table_version("silver_users", 2)\n\n'
        'cdf_df = (spark.read\n'
        '               .format("delta")\n'
        '               .option("readChangeData", True)   # Read the change data\n'
        '               .option("startingVersion", 2)     # Since we want to start reading changes from version 2 (when we merged the data)\n'
        '               .table("silver_users"))\n\n'
        '## Display the changed data\n'
        'display(cdf_df)'
    )
    if old3 in patched and "wait_for_table_version(\"silver_users\", 2)" not in patched:
        patched = patched.replace(old3, new3, 1)
        patches_applied += 1

    # Patch 4: After second DA.load (batch 2)
    old4 = "DA.load(copy_from=DA.paths.stream_source.cdf_demo, \n        copy_to=DA.paths.cdc_stream, \n        n=2)"
    new4 = (
        old4 + "\n\n"
        "import time\n"
        'print("Waiting 60 seconds for the stream to process batch 2...")\n'
        "time.sleep(60)"
    )
    if old4 in patched and "Waiting 60 seconds for the stream to process batch 2" not in patched:
        patched = patched.replace(old4, new4, 1)
        patches_applied += 1

    # ── Regex fallbacks for non-English notebooks (Korean, Japanese, etc.) ────
    # The exact-match patches above only work for English comments.
    # These regex patterns match the CODE structure while ignoring inline comments,
    # so they work for any translated version of the notebook.
    import re as _re

    # Fallback Patch 2: silver_users_stream block (regex ignores inline comments)
    if "Waiting 60 seconds for the streaming merge" not in patched:
        _p2 = _re.compile(
            r'(silver_users_stream\s*=\s*\(\s*\n'
            r'\s*spark\s*\n'
            r'\s*\.readStream\s*\n'
            r'\s*\.table\("bronze_users"\)\s*\n'
            r'\s*\.writeStream\s*\n'
            r'\s*\.foreachBatch\(upsert_to_delta\)[^\n]*\n'
            r'\s*\.trigger\(processingTime=.3 seconds.\)[^\n]*\n'
            r'\s*\.start\(\)\s*\n'
            r'\s*\))'
        )
        _m2 = _p2.search(patched)
        if _m2:
            patched = patched[:_m2.end()] + (
                "\n\nimport time\n"
                'print("Waiting 60 seconds for the streaming merge into silver_users...")\n'
                "time.sleep(60)"
            ) + patched[_m2.end():]
            patches_applied += 1
            print("  Applied regex fallback for Patch 2 (non-English comments)")

    # Fallback Patch 3: CDF read block (regex ignores inline comments)
    if 'wait_for_table_version("silver_users", 2)' not in patched:
        _p3 = _re.compile(
            r'cdf_df\s*=\s*\(spark\.read\s*\n'
            r'\s*\.format\("delta"\)\s*\n'
            r'\s*\.option\("readChangeData",\s*True\)[^\n]*\n'
            r'\s*\.option\("startingVersion",\s*2\)[^\n]*\n'
            r'\s*\.table\("silver_users"\)\)\s*\n'
            r'\s*\n'
            r'[^\n]*\n'
            r'display\(cdf_df\)'
        )
        _m3 = _p3.search(patched)
        if _m3:
            _new3 = (
                'import time\n\n'
                'def wait_for_table_version(table_name, min_version, timeout_seconds=300, poll_seconds=10):\n'
                '    deadline = time.time() + timeout_seconds\n'
                '    latest_version = None\n'
                '    while time.time() < deadline:\n'
                '        latest_version = spark.sql(f"DESCRIBE HISTORY {table_name}").selectExpr("max(version) as max_version").collect()[0]["max_version"]\n'
                '        if latest_version is not None and latest_version >= min_version:\n'
                '            print(f"{table_name} is now at version {latest_version}; continuing with CDF read from version {min_version}.")\n'
                '            return latest_version\n'
                '        print(f"Waiting for {table_name} to reach version {min_version}. Current latest version: {latest_version}")\n'
                '        time.sleep(poll_seconds)\n'
                '    raise Exception(f"Timed out waiting for {table_name} to reach version {min_version}. Latest version: {latest_version}")\n\n'
                'wait_for_table_version("silver_users", 2)\n\n'
                'cdf_df = (spark.read\n'
                '               .format("delta")\n'
                '               .option("readChangeData", True)\n'
                '               .option("startingVersion", 2)\n'
                '               .table("silver_users"))\n\n'
                'display(cdf_df)'
            )
            patched = patched[:_m3.start()] + _new3 + patched[_m3.end():]
            patches_applied += 1
            print("  Applied regex fallback for Patch 3 (non-English comments)")

    if patched != source:
        w.workspace.import_(
            path=dp13_path,
            format=ImportFormat.SOURCE,
            language=Language.PYTHON,
            content=base64.b64encode(patched.encode("utf-8")).decode("utf-8"),
            overwrite=True,
        )
        print(f"Patched DP 1.3: applied {patches_applied} fix(es)")
    else:
        print("DP 1.3 already patched or patterns not found - skipping")
else:
    print("DP 1.3 not in TASKS - no patch needed")

# COMMAND ----------

# Patch DP 1.4L: remove HTML comment markers that cause PARSE_SYNTAX_ERROR
# The lab notebook contains <!---...--> markers around solution code blocks
# that are not valid SQL and cause [PARSE_SYNTAX_ERROR] at or near '<'
import re

dp14_tasks = [t for t in TASKS if "1.4" in t["task_key"]]
if dp14_tasks:
    dp14_path = dp14_tasks[0]["notebook_path"]
    export_resp = w.workspace.export(path=dp14_path, format=ExportFormat.SOURCE)
    source = base64.b64decode(export_resp.content).decode("utf-8")

    original = source
    # Remove lines containing the HTML comment markers
    source = re.sub(r'^.*<!---+.*ADD SOLUTION CODE BELOW.*---+>.*\n?', '', source, flags=re.MULTILINE)
    source = re.sub(r'^.*<!---+.*END SOLUTION CODE.*---+>.*\n?', '', source, flags=re.MULTILINE)

    if source != original:
        w.workspace.import_(
            path=dp14_path,
            format=ImportFormat.SOURCE,
            language=Language.SQL,
            content=base64.b64encode(source.encode("utf-8")).decode("utf-8"),
            overwrite=True,
        )
        print("Patched DP 1.4L: removed HTML comment markers from SQL cells")
    else:
        print("DP 1.4L HTML comment markers already removed or not found - skipping")
else:
    print("DP 1.4L not in TASKS - no patch needed")

# COMMAND ----------

# Patch DP 1.1: comment out the GRANT code in cell 26.
# Uses JUPYTER format so magic commands (%python) stay intact.
import json as _json

dp11_tasks = [t for t in TASKS if "1.1" in t["task_key"]]
if dp11_tasks:
    dp11_path = dp11_tasks[0]["notebook_path"]
    export_resp = w.workspace.export(path=dp11_path, format=ExportFormat.JUPYTER)
    nb = _json.loads(base64.b64decode(export_resp.content).decode("utf-8"))

    patched = False
    for cell in nb.get("cells", []):
        if cell.get("cell_type") != "code":
            continue
        src = "".join(cell.get("source", []))
        if "GRANT USE CATALOG, USE SCHEMA, SELECT ON CATALOG" in src and "account users" in src:
            # Comment out code lines but keep %python magic and blank lines
            new_source = []
            for line in cell["source"]:
                stripped = line.strip()
                if stripped.startswith("%") or stripped == "":
                    new_source.append(line)
                else:
                    new_source.append("# " + line)
            cell["source"] = new_source
            patched = True
            break

    if patched:
        w.workspace.import_(
            path=dp11_path,
            format=ImportFormat.JUPYTER,
            content=base64.b64encode(_json.dumps(nb).encode("utf-8")).decode("utf-8"),
            overwrite=True,
        )
        print("Patched DP 1.1: commented out GRANT code in cell 26")
    else:
        print("DP 1.1 GRANT cell already commented or not found — skipping")
else:
    print("DP 1.1 not in TASKS — no patch needed")

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
    # Check if any task actually needs classic compute
    classic_needed = any(t.get("use_classic") for t in TASKS)
    if classic_needed:
        raise RuntimeError("❌ No classic cluster found for user. Some tasks require classic compute — cannot proceed.")
    else:
        print("⚠️  No classic cluster found, but all tasks use serverless — continuing.")

# Route only classic-compute tasks to the cluster; leave serverless tasks on shared env
for i, t in enumerate(TASKS):
    if t.get("use_classic") and CLASSIC_CLUSTER_ID:
        job_tasks[i].environment_key = None
        job_tasks[i].existing_cluster_id = CLASSIC_CLUSTER_ID

created_job = w.jobs.create(
    name=JOB_NAME,
    tasks=job_tasks,
    environments=[
        JobEnvironment(
            environment_key=SHARED_ENV_KEY,
            spec=Environment(client=ENVIRONMENT_VERSION),
        )
    ],
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
passed = [r for r in results if r["status"] == "PASS"]

if failed:
    summary = "\n".join(
        f"  - [{r['task_key']}] {r['demo_name']}: {r['status']} ({r['result_state']}) — {r['run_page_url']}"
        for r in failed
    )
    print(f"⚠️  {len(failed)} of {len(results)} task(s) failed on Serverless v{ENVIRONMENT_VERSION}:\n{summary}")
else:
    print(f"✅ All {len(results)} tasks passed on Serverless v{ENVIRONMENT_VERSION}.")

dbutils.notebook.exit(json.dumps({
    "total":  len(results),
    "passed": len(passed),
    "failed": len(failed),
}))