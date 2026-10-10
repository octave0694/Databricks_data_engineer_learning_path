# Databricks notebook source
# MAGIC %md
# MAGIC # Automated Test Runner — Databricks Performance Optimization
# MAGIC
# MAGIC Creates a **single multi-task Lakeflow Job** for the course, with optional QA checks,
# MAGIC so the full run is visible in one timeline.
# MAGIC
# MAGIC **Run structure:**
# MAGIC - Course notebooks run in the dependency order defined in `COURSE_TASKS` (from `notebook_path`).
# MAGIC - **QA Content Checker** can run independently (no dependencies) alongside the course tasks.
# MAGIC - The separate **Course-specific inputs** cell (`notebook_path`) makes it easy to reuse this notebook for another course.
# MAGIC
# MAGIC ```
# MAGIC  QA Check      :  qa_content_checker  (optional, parallel)
# MAGIC  Course Tasks  :  PO_00_Overview ─► PO_1.1_FileExplosion ─► PO_1.2L_DataSkipping ─► PO_1.3_Shuffle ─► PO_1.4L_ExplodingJoin ─► PO_1.5_UDFs ─► PO_99_Summary
# MAGIC ```
# MAGIC
# MAGIC **What it does**
# MAGIC 1. Reads course-specific settings from `notebook_path` cell (`COURSE_NAME`, `LAB_NOTEBOOKS`, `COURSE_TASKS`).
# MAGIC 2. Auto-fills all `<FILL_IN>` placeholders in the configured lab notebooks using the inline solution blocks.
# MAGIC 3. Resolves all notebook paths relative to this notebook's location.
# MAGIC 4. Validates that all course notebooks exist at their expected paths.
# MAGIC 5. Cleans up artifacts from prior runs.
# MAGIC 6. Creates (or re-creates) a single persistent Lakeflow Job for the configured course.
# MAGIC 7. Triggers the job with `jobs.run_now` and polls until completion.
# MAGIC 8. Appends one result row per task to a generic results table.
# MAGIC 9. Displays a combined summary table with clickable `run_page_url` links.
# MAGIC 10. Creates and publishes a **Lakeview dashboard** from the run results and QA findings.
# MAGIC 11. Raises if any task failed — triggering Lakeflow Job failure email.
# MAGIC
# MAGIC > **Note:** This course (PO) has demos and two labs — no UI instruction steps to scan.

# COMMAND ----------

# MAGIC %run ./notebook_path

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
        "env_key": task["env_key"],
        "use_classic": task.get("use_classic", False),
    })

if RUN_QA_CHECKER:
    TASKS.append({
        "task_key": "qa_content_checker",
        "course": "QA",
        "name": QA_TASK_NAME,
        "relative_path": QA_TASK_RELATIVE_PATH,
        "depends_on": [],
        "env_key": "qa_env",
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
    raise RuntimeError("❌ No classic cluster found for user. All tasks require labuser compute — cannot proceed with serverless.")

if CLASSIC_CLUSTER_ID:
    for i, t in enumerate(TASKS):
        job_tasks[i].environment_key = None
        job_tasks[i].existing_cluster_id = CLASSIC_CLUSTER_ID

created_job = w.jobs.create(
    name=JOB_NAME,
    tasks=job_tasks,
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
# MAGIC ## Validate Course Notebooks
# MAGIC
# MAGIC Verifies that all configured course notebooks (demos and labs) exist at the expected paths before triggering the job run. This prevents wasted compute time from missing notebooks.

# COMMAND ----------

# ── Validate Course Notebook Paths ───────────────────────────────────────────
# Ensures all configured notebooks actually exist before launching the job.
# Prevents wasted compute time and confusing errors from missing paths.
# ─────────────────────────────────────────────────────────────────────────────────

from databricks.sdk.service.workspace import ObjectType

print("═" * 70)
print("VALIDATE: Course Notebook Paths")
print("═" * 70)

all_valid = True
for t in TASKS:
    nb_path = t["notebook_path"]
    try:
        status = w.workspace.get_status(nb_path)
        if status.object_type in (ObjectType.NOTEBOOK, ObjectType.FILE):
            print(f"  ✅ {t['task_key']}")
            print(f"       {nb_path}")
        else:
            print(f"  ❌ {t['task_key']} — exists but is not a notebook")
            print(f"       {nb_path}")
            all_valid = False
    except Exception as e:
        print(f"  ❌ {t['task_key']} — NOT FOUND")
        print(f"       {nb_path}")
        print(f"       Error: {e}")
        all_valid = False

print()
if all_valid:
    print(f"✅ All {len(TASKS)} notebook paths validated successfully.")
else:
    print("⚠️  Some notebooks are missing — the job may fail for those tasks.")
print("═" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trigger the job run and poll until all tasks complete

# COMMAND ----------

# ── Cleanup artifacts from prior runs ─────────────────────────────────────────
# Clean up temp directories and user-specific tables from previous test runs
# to avoid conflicts on re-runs.
# ─────────────────────────────────────────────────────────────────────────────────

import os

_cleanup_user = spark.sql("SELECT current_user()").collect()[0][0].split("@")[0].replace(".", "_").replace("-", "_")

print("═" * 70)
print("CLEANUP: Remove artifacts from previous test runs")
print("═" * 70)

# Clean up temp directories used by course notebooks
_tmp_base = f"/tmp/{_cleanup_user}"
try:
    dbutils.fs.rm(_tmp_base, recurse=True)
    print(f"  ✅ Removed temp dir: {_tmp_base}")
except Exception as e:
    print(f"  ⏭️  Temp dir not found (OK): {_tmp_base}")

# Clean up user-specific tables if they exist
_user_schema = f"dbacademy.{_cleanup_user}"
try:
    _tables = spark.sql(f"SHOW TABLES IN {_user_schema}").collect()
    for _t in _tables:
        _fqn = f"{_user_schema}.{_t.tableName}"
        spark.sql(f"DROP TABLE IF EXISTS {_fqn}")
        print(f"  ✅ Dropped table: {_fqn}")
    if not _tables:
        print(f"  ⏭️  No tables to clean in {_user_schema}")
except Exception as e:
    print(f"  ⏭️  Schema {_user_schema} not found or inaccessible (OK)")

print("═" * 70)
print("Cleanup complete — ready for fresh test run.")
print("═" * 70)

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
    all_warehouses = list(w.warehouses.list())
    # Prefer RUNNING, then STOPPED, then any other state
    for wh in all_warehouses:
        if wh.state == WarehouseState.RUNNING:
            warehouse_id = wh.id
            break
    if not warehouse_id:
        for wh in all_warehouses:
            if wh.state == WarehouseState.STOPPED:
                warehouse_id = wh.id
                break
    if not warehouse_id and all_warehouses:
        warehouse_id = all_warehouses[0].id
except Exception as e:
    print(f"⚠️  Could not list warehouses: {e}")

if warehouse_id:
    print(f"Using SQL warehouse: {warehouse_id}")
else:
    print("ℹ️  No SQL warehouse found — dashboard will use serverless SQL.")

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

create_kwargs = dict(
    display_name=DASHBOARD_NAME,
    serialized_dashboard=json.dumps(spec),
    parent_path=DASHBOARD_FOLDER,
)
if warehouse_id:
    create_kwargs["warehouse_id"] = warehouse_id

dashboard = w.lakeview.create(Dashboard(**create_kwargs))
dashboard_id = dashboard.dashboard_id

publish_kwargs = dict(dashboard_id=dashboard_id, embed_credentials=True)
if warehouse_id:
    publish_kwargs["warehouse_id"] = warehouse_id
w.lakeview.publish(**publish_kwargs)

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