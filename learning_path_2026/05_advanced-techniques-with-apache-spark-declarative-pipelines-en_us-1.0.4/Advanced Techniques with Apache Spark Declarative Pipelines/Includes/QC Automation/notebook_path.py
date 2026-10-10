# Databricks notebook source
# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Course-specific inputs

# COMMAND ----------

# Change the course name as per your course name
COURSE_NAME = "Advanced Techniques with Apache Spark Declarative Pipelines"
RUN_QA_CHECKER = True
QA_TASK_RELATIVE_PATH = "./qa_content_checker"
QA_TASK_NAME = "QA Content Checker — Grammar, Deprecated, UI Steps"

# Tester emails — notified on job failure and success. Add as many as you like with comma separated emails
TESTER_EMAILS = [
    "nitesh.ojha@databricks.com",
    "shivam.pandey@databricks.com",
    "s.kumar@databricks.com"
]

# Add your lab notebook paths relative from the CourseRunner folder as per your course
LAB_NOTEBOOKS = [
    "../../9 Lab - Building Multi-Source Ecommerce Pipeline with SDP",
]

# Define the course notebook DAG here so this can be used for job scheduling as per your course
COURSE_TASKS = [
    {
        "task_key": "2 Demo - Multi Flow SDP with Liquid Clustering and Data Quality",
        "name": "2 Demo - Multi Flow SDP with Liquid Clustering and Data Quality",
        "relative_path": "../../2 Demo - Multi Flow SDP with Liquid Clustering and Data Quality",
        "env_key": "course_02",
    },
    {
        "task_key": "4 Demo - Multiplex Streaming SDP with Delta Sinks and Iceberg Reads",
        "depends_on": [
            {"task_key": "2 Demo - Multi Flow SDP with Liquid Clustering and Data Quality"},
        ],
        "name": "4 Demo - Multiplex Streaming SDP with Delta Sinks and Iceberg Reads",
        "relative_path": "../../4 Demo - Multiplex Streaming SDP with Delta Sinks and Iceberg Reads",
        "env_key": "course_04",
    },
    {
        "task_key": "6 Demo - Automating SCD Type 2 with AUTO CDC in Apache Spark Declarative Pipelines",
        "depends_on": [
            {"task_key": "4 Demo - Multiplex Streaming SDP with Delta Sinks and Iceberg Reads"},
        ],
        "name": "6 Demo - Automating SCD Type 2 with AUTO CDC in Apache Spark Declarative Pipelines",
        "relative_path": "../../6 Demo - Automating SCD Type 2 with AUTO CDC in Apache Spark Declarative Pipelines",
        "env_key": "course_06",
    },
    {
        "task_key": "8 Demo - Advanced Data Quality Checks and Expectations in SDP",
        "depends_on": [
            {"task_key": "6 Demo - Automating SCD Type 2 with AUTO CDC in Apache Spark Declarative Pipelines"},
        ],
        "name": "8 Demo - Advanced Data Quality Checks and Expectations in SDP",
        "relative_path": "../../8 Demo - Advanced Data Quality Checks and Expectations in SDP",
        "env_key": "course_08",
    },
    {
        "task_key": "9 Lab - Building Multi-Source Ecommerce Pipeline with SDP",
        "depends_on": [
            {"task_key": "8 Demo - Advanced Data Quality Checks and Expectations in SDP"},
        ],
        "name": "9 Lab - Building Multi-Source Ecommerce Pipeline with SDP",
        "relative_path": "../../9 Lab - Building Multi-Source Ecommerce Pipeline with SDP",
        "env_key": "course_09",
    },
]

# COMMAND ----------

# ── Create Pipeline & Patch Lab Notebooks ────────────────────────────────────
# Same pattern as the Demo notebooks in Test_All_Notebooks:
#   1. Extract SQL from copy-to-clipboard solution blocks in the lab notebook
#   2. Create the SDP pipeline with the extracted SQL source files
#   3. Patch the notebook with trigger cells at "Run pipeline" markers
#
# NOTE: <FILL_IN> cells and pipeline-only cells (e.g. cell 38) are LEFT
#       UNTOUCHED in the notebook — solutions go ONLY into the pipeline.
# ──────────────────────────────────────────────────────────────────────────────

import base64
import json
import os
import re
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ExportFormat, ImportFormat, Language
from databricks.sdk.service.pipelines import PipelineLibrary, PipelineCluster, FileLibrary

w = WorkspaceClient()

# Derive course_root from this notebook's own path
_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
course_root = "/".join(_nb_path.split("/")[:-1])

# Derive user catalog (same logic as Classroom-Setup-Common)
user_catalog = spark.sql("SELECT current_user()").collect()[0][0].split("@")[0].replace(".", "_").replace("-", "_")
print(f"User catalog: {user_catalog}")

# Course content root (parent of Includes/)
_parts = _nb_path.split("/")
_includes_idx = next(i for i, p in enumerate(_parts) if p == "Includes")
course_content_root = "/".join(_parts[:_includes_idx])
print(f"Course content root: {course_content_root}")


# ── Helper Functions ──────────────────────────────────────────────────────────

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
    """Create a pipeline with SQL source files written to a workspace folder."""
    if folder_name is None:
        folder_name = pipeline_name.replace(" ", "_")

    # Create workspace folder for pipeline source
    pipeline_folder = f"{course_content_root}/{folder_name}"
    try:
        w.workspace.mkdirs(pipeline_folder)
    except Exception:
        pass  # folder may already exist

    # Write source files
    for filename, sql_content in sql_files.items():
        file_path = f"{pipeline_folder}/{filename}"
        content_b64 = base64.b64encode(sql_content.encode()).decode()
        file_language = Language.PYTHON if filename.endswith(".py") else Language.SQL
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

    # Create pipeline
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
    """Patch a notebook by inserting pipeline trigger cells at specified markers."""
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


# ── DEMO 2: Multi Flow SDP with Liquid Clustering and Data Quality ───────────
print("═" * 70)
print("DEMO 2: Multi Flow SDP with Liquid Clustering and Data Quality")
print("═" * 70)

demo_02_path = f"{course_content_root}/2 Demo - Multi Flow SDP with Liquid Clustering and Data Quality"
PIPELINE_NAME_02 = "Multi Flow SDP Demo - Automated Test"

# ── Step 1: Create (or re-create) Demo 2 pipeline with dynamic catalog ────────
print("\n  Step 1: Creating Demo 2 pipeline...")

# The pipeline source files already exist in ingest_multiple_flows/
pipeline_folder_02 = f"{course_content_root}/ingest_multiple_flows"
sql_files_02 = ["flow_ingestion.sql", "silver_transformation.sql", "gold_mvs.sql"]

# Configuration — volume paths use the dynamic user catalog
config_02 = {
    "bright_home_orders_source": f"/Volumes/{user_catalog}/multi_flow_1_bronze/bright_home_orders",
    "lumina_sports_orders_source": f"/Volumes/{user_catalog}/multi_flow_1_bronze/lumina_sports_orders",
    "northstar_outfitters_orders_source": f"/Volumes/{user_catalog}/multi_flow_1_bronze/northstar_outfitters_orders",
}

# Delete existing pipeline with same name
existing_02 = list(w.pipelines.list_pipelines(filter=f"name LIKE '{PIPELINE_NAME_02}'"))
for p in existing_02:
    if p.name == PIPELINE_NAME_02:
        w.pipelines.delete(pipeline_id=p.pipeline_id)
        print(f"  Deleted existing pipeline: {p.pipeline_id}")

# Create pipeline with dynamic catalog
from databricks.sdk.service.pipelines import PipelineLibrary, FileLibrary

pipeline_libraries_02 = [
    PipelineLibrary(file=FileLibrary(path=f"/Workspace{pipeline_folder_02}/{fname}"))
    for fname in sql_files_02
]

create_resp_02 = w.pipelines.create(
    name=PIPELINE_NAME_02,
    catalog=user_catalog,
    target="multi_flow_1_bronze",
    libraries=pipeline_libraries_02,
    configuration=config_02,
    serverless=True,
    channel="CURRENT",
)
pipeline_id_02 = create_resp_02.pipeline_id
print(f"  Created pipeline: {PIPELINE_NAME_02} (ID: {pipeline_id_02})")
print(f"  Catalog: {user_catalog}, Target: multi_flow_1_bronze")

# ── Step 2: Patch Demo 2 notebook with trigger cells ──────────────────────────
print("\n  Step 2: Patching Demo 2 notebook with trigger cells...")
patch_notebook_with_triggers(
    notebook_path=demo_02_path,
    pipeline_name=PIPELINE_NAME_02,
    markers=[
        {"marker": "D6.", "position": "after", "comment": "AUTO-INSERTED: Run pipeline (Bronze layer - multi flow ingestion)"},
        {"marker": "E3.", "position": "after", "comment": "AUTO-INSERTED: Run pipeline (Silver layer with data quality)"},
        {"marker": "F3.", "position": "after", "comment": "AUTO-INSERTED: Run pipeline (Gold layer materialized views)"},
        {"marker": "G3.", "position": "after", "comment": "AUTO-INSERTED: Run pipeline (Incremental processing with new files)"},
    ]
)

print(f"\n  ✅ Pipeline: {PIPELINE_NAME_02} ({pipeline_id_02})")
print("═" * 70)


# ── DEMO 4: Multiplex Streaming SDP with Delta Sinks and Iceberg Reads ─────
print("═" * 70)
print("DEMO 4: Multiplex Streaming SDP with Delta Sinks and Iceberg Reads")
print("═" * 70)

demo_04_path = f"{course_content_root}/4 Demo - Multiplex Streaming SDP with Delta Sinks and Iceberg Reads"
PIPELINE_NAME_04 = "Multiplex Streaming SDP Demo - Automated Test"

print("\n  Step 1: Creating Demo 4 pipeline...")
pipeline_folder_04 = f"{course_content_root}/multiplex_pipeline"
sql_files_04 = ["bronze_multiplex.sql", "silver_transforms.sql", "gold_views.sql", "delta_sink.py"]

config_04 = {
    "business_events_source": f"/Volumes/{user_catalog}/multiplex_1_bronze/business_events",
    "my_catalog": user_catalog,
}

existing_04 = list(w.pipelines.list_pipelines(filter=f"name LIKE '{PIPELINE_NAME_04}'"))
for p in existing_04:
    if p.name == PIPELINE_NAME_04:
        w.pipelines.delete(pipeline_id=p.pipeline_id)
        print(f"  Deleted existing pipeline: {p.pipeline_id}")

pipeline_libraries_04 = [
    PipelineLibrary(file=FileLibrary(path=f"/Workspace{pipeline_folder_04}/{fname}"))
    for fname in sql_files_04
]

create_resp_04 = w.pipelines.create(
    name=PIPELINE_NAME_04,
    catalog=user_catalog,
    target="multiplex_1_bronze",
    libraries=pipeline_libraries_04,
    configuration=config_04,
    serverless=True,
    channel="CURRENT",
)
pipeline_id_04 = create_resp_04.pipeline_id
print(f"  Created pipeline: {PIPELINE_NAME_04} (ID: {pipeline_id_04})")
print(f"  Catalog: {user_catalog}, Target: multiplex_1_bronze")

print("\n  Step 2: Patching Demo 4 notebook with trigger cells...")
patch_notebook_with_triggers(
    notebook_path=demo_04_path,
    pipeline_name=PIPELINE_NAME_04,
    markers=[
        {"marker": "E5.", "position": "after", "comment": "AUTO-INSERTED: Run pipeline (Bronze + intermediate tables)"},
        {"marker": "F4.", "position": "after", "comment": "AUTO-INSERTED: Run pipeline (Silver layer transforms)"},
        {"marker": "G3.", "position": "after", "comment": "AUTO-INSERTED: Run pipeline (Gold materialized views)"},
        {"marker": "H2.", "position": "after", "comment": "AUTO-INSERTED: Run pipeline (Delta sink + Iceberg)"},
    ]
)
print(f"\n  ✅ Pipeline: {PIPELINE_NAME_04} ({pipeline_id_04})")
print("═" * 70)


# ── DEMO 6: Automating SCD Type 2 with AUTO CDC ─────────────────────────────
print("═" * 70)
print("DEMO 6: Automating SCD Type 2 with AUTO CDC")
print("═" * 70)

demo_06_path = f"{course_content_root}/6 Demo - Automating SCD Type 2 with AUTO CDC in Apache Spark Declarative Pipelines"
PIPELINE_NAME_06 = "Auto CDC SCD Type 2 Demo - Automated Test"

print("\n  Step 1: Creating Demo 6 pipeline...")
pipeline_folder_06 = f"{course_content_root}/auto_cdc_pipeline"
sql_files_06 = ["bronze_cdc.sql", "silver_auto_cdc.sql"]

config_06 = {
    "source": f"/Volumes/{user_catalog}/sdp_cdc_1_bronze/customer_source_files",
}

existing_06 = list(w.pipelines.list_pipelines(filter=f"name LIKE '{PIPELINE_NAME_06}'"))
for p in existing_06:
    if p.name == PIPELINE_NAME_06:
        w.pipelines.delete(pipeline_id=p.pipeline_id)
        print(f"  Deleted existing pipeline: {p.pipeline_id}")

pipeline_libraries_06 = [
    PipelineLibrary(file=FileLibrary(path=f"/Workspace{pipeline_folder_06}/{fname}"))
    for fname in sql_files_06
]

create_resp_06 = w.pipelines.create(
    name=PIPELINE_NAME_06,
    catalog=user_catalog,
    target="sdp_cdc_1_bronze",
    libraries=pipeline_libraries_06,
    configuration=config_06,
    serverless=True,
    channel="CURRENT",
)
pipeline_id_06 = create_resp_06.pipeline_id
print(f"  Created pipeline: {PIPELINE_NAME_06} (ID: {pipeline_id_06})")
print(f"  Catalog: {user_catalog}, Target: sdp_cdc_1_bronze")

print("\n  Step 2: Patching Demo 6 notebook with trigger cells...")
patch_notebook_with_triggers(
    notebook_path=demo_06_path,
    pipeline_name=PIPELINE_NAME_06,
    markers=[
        {"marker": "D1.", "position": "after", "comment": "AUTO-INSERTED: Run pipeline (Initial CDC pipeline run)"},
    ]
)
print(f"\n  ✅ Pipeline: {PIPELINE_NAME_06} ({pipeline_id_06})")
print("═" * 70)


# ── DEMO 8: Advanced Data Quality Checks and Expectations in SDP ─────────────
print("═" * 70)
print("DEMO 8: Advanced Data Quality Checks and Expectations in SDP")
print("═" * 70)

demo_08_path = f"{course_content_root}/8 Demo - Advanced Data Quality Checks and Expectations in SDP"
PIPELINE_NAME_08 = "Advanced DQ Expectations Demo - Automated Test"

print("\n  Step 1: Creating Demo 8 pipeline...")
pipeline_folder_08 = f"{course_content_root}/dq_expectations_pipeline"
sql_files_08 = ["bronze_ingestion.sql", "silver_dq_expectations.sql"]

config_08 = {
    "source": f"/Volumes/{user_catalog}/dq_1_bronze/sales",
}

existing_08 = list(w.pipelines.list_pipelines(filter=f"name LIKE '{PIPELINE_NAME_08}'"))
for p in existing_08:
    if p.name == PIPELINE_NAME_08:
        w.pipelines.delete(pipeline_id=p.pipeline_id)
        print(f"  Deleted existing pipeline: {p.pipeline_id}")

pipeline_libraries_08 = [
    PipelineLibrary(file=FileLibrary(path=f"/Workspace{pipeline_folder_08}/{fname}"))
    for fname in sql_files_08
]

create_resp_08 = w.pipelines.create(
    name=PIPELINE_NAME_08,
    catalog=user_catalog,
    target="dq_1_bronze",
    libraries=pipeline_libraries_08,
    configuration=config_08,
    serverless=True,
    channel="CURRENT",
)
pipeline_id_08 = create_resp_08.pipeline_id
print(f"  Created pipeline: {PIPELINE_NAME_08} (ID: {pipeline_id_08})")
print(f"  Catalog: {user_catalog}, Target: dq_1_bronze")

print("\n  Step 2: Patching Demo 8 notebook with trigger cells...")
patch_notebook_with_triggers(
    notebook_path=demo_08_path,
    pipeline_name=PIPELINE_NAME_08,
    markers=[
        {"marker": "D3.", "position": "after", "comment": "AUTO-INSERTED: Run pipeline (Bronze + Silver with DQ expectations)"},
        {"marker": "E4.", "position": "after", "comment": "AUTO-INSERTED: Run pipeline (After adding DQ rules)"},
    ]
)
print(f"\n  ✅ Pipeline: {PIPELINE_NAME_08} ({pipeline_id_08})")
print("═" * 70)


# ── LAB 9: Building Multi-Source Ecommerce Pipeline with SDP ─────────────────
print("═" * 70)
print("LAB 9: Building Multi-Source Ecommerce Pipeline with SDP")
print("═" * 70)

lab_09_path = f"{course_content_root}/9 Lab - Building Multi-Source Ecommerce Pipeline with SDP"
PIPELINE_NAME_09 = "Multi-Source Ecommerce Lab - Automated Test"

# ── Step 1: Extract SQL from solution blocks and create pipeline ──────────────
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

# ── Step 2: Patch lab notebook with trigger cells ─────────────────────────────
# Same approach as Demos 2, 4, 6, 8: insert trigger cells at "Run pipeline"
# markers. The notebook content (including <FILL_IN> and pipeline-only cells)
# is left as-is — solutions live only in the pipeline source files.
if pipeline_id_09:
    print("\n  Step 2: Patching lab notebook with trigger cells...")
    patch_notebook_with_triggers(
        notebook_path=lab_09_path,
        pipeline_name=PIPELINE_NAME_09,
        markers=[
            {"marker": "### D4.", "position": "after", "comment": "AUTO-INSERTED: Run pipeline (Bronze layer initial load)"},
            {"marker": "### E6.", "position": "after", "comment": "AUTO-INSERTED: Run pipeline (Silver layer with expectations)"},
            {"marker": "### F3.", "position": "after", "comment": "AUTO-INSERTED: Run pipeline (Gold layer analytics)"},
            {"marker": "### G3.", "position": "after", "comment": "AUTO-INSERTED: Run pipeline (Incremental processing)"},
        ]
    )

print(f"\n  ✅ Pipeline: {PIPELINE_NAME_09} ({pipeline_id_09})")
print("═" * 70)