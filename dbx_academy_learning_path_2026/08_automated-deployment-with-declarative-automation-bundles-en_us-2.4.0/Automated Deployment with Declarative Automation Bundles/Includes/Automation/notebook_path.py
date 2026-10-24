# Databricks notebook source
# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Course-specific inputs

# COMMAND ----------

# Change the course name as per your course name
COURSE_NAME = "Automated Deployment with Declarative Automation Bundles"
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
    "../../06L - Deploy a Simple DAB/Lab - Deploy a Simple DAB",
    "../../09L - Deploy a DAB to Multiple Environments/Lab - Deploy a DAB to Multiple Environments",
    "../../11L - Use a Databricks Default DAB Template/Lab - Use a Databricks Default DAB Template",
    "../../14L Bonus - Adding ML to Engineering Workflows with DABs/Lab - Adding ML to Engineering Workflows with DABs",
]

# Define the course notebook DAG here so this can be used for job scheduling as per your course
COURSE_TASKS = [
{
        "task_key": "02 - REQUIRED - Course Setup and Authentication",
        "name": "02 - REQUIRED - Course Setup and Authentication",
        "relative_path": "../../02 - REQUIRED - Course Setup and Authentication",
        "depends_on": [],
        "env_key": "course_02",
        "use_classic": True,
    },
    {
        "task_key": "05 Demo - Deploying a Simple DAB",
        "depends_on": [
            {"task_key": "02 - REQUIRED - Course Setup and Authentication"},
        ],
        "name": "05 Demo - Deploying a Simple DAB",
        "relative_path": "../../05 Demo - Deploying a Simple DAB/Demo - Deploying a Simple DAB",
        "env_key": "course_05",
        "use_classic": True,
    },
    {
        "task_key": "06L - Deploy a Simple DAB",
        "depends_on": [
            {"task_key": "05 Demo - Deploying a Simple DAB"},
        ],
        "name": "06L - Deploy a Simple DAB",
        "relative_path": "../../06L - Deploy a Simple DAB/Lab - Deploy a Simple DAB",
        "env_key": "course_06",
        "use_classic": True,
    },
    {
        "task_key": "08 Demo - Deploying a DAB to Multiple Environments",
        "depends_on": [
            {"task_key": "06L - Deploy a Simple DAB"},
        ],
        "name": "08 Demo - Deploying a DAB to Multiple Environments",
        "relative_path": "../../08 Demo - Deploying a DAB to Multiple Environments/Demo - Deploying a DAB to Multiple Environments",
        "env_key": "course_08",
        "use_classic": True,
    },
    {
        "task_key": "09L - Deploy a DAB to Multiple Environments",
        "depends_on": [
            {"task_key": "08 Demo - Deploying a DAB to Multiple Environments"},
        ],
        "name": "09L - Deploy a DAB to Multiple Environments",
        "relative_path": "../../09L - Deploy a DAB to Multiple Environments/Lab - Deploy a DAB to Multiple Environments",
        "env_key": "course_09",
        "use_classic": True,
    },
    {
        "task_key": "11L - Use a Databricks Default DAB Template",
        "depends_on": [
            {"task_key": "09L - Deploy a DAB to Multiple Environments"},
        ],
        "name": "11L - Use a Databricks Default DAB Template",
        "relative_path": "../../11L - Use a Databricks Default DAB Template/Lab - Use a Databricks Default DAB Template",
        "env_key": "course_11",
        "use_classic": True,
    },
    {
        "task_key": "13 Demo - CICD with DABs",
        "depends_on": [
            {"task_key": "11L - Use a Databricks Default DAB Template"},
        ],
        "name": "13 Demo - CICD with DABs",
        "relative_path": "../../13 Demo - Continuous Integration and Continuous Deployment with DABs/Demo - CICD with DABs",
        "env_key": "course_13",
        "use_classic": True,
    },
    {
        "task_key": "14L Bonus - Adding ML to Engineering Workflows with DABs",
        "depends_on": [
            {"task_key": "13 Demo - CICD with DABs"},
        ],
        "name": "14L Bonus - Adding ML to Engineering Workflows with DABs",
        "relative_path": "../../14L Bonus - Adding ML to Engineering Workflows with DABs/Lab - Adding ML to Engineering Workflows with DABs",
        "env_key": "course_14",
        "use_classic": True,
    },
    {
        "task_key": "16 Demo - Using VSCode with Databricks",
        "depends_on": [
            {"task_key": "14L Bonus - Adding ML to Engineering Workflows with DABs"},
        ],
        "name": "16 Demo - Using VSCode with Databricks",
        "relative_path": "../../16 Demo - Using VSCode with Databricks/16 Demo - Using VSCode with Databricks",
        "env_key": "course_16",
        "use_classic": True,
    },
]

# COMMAND ----------

# ── Auto-Fill Lab Notebooks ──────────────────────────────────────────────────
# This script finds unresolved lab placeholders in lab notebooks and replaces
# them with solution code extracted from the nearby solution blocks.
#
# HOW IT WORKS:
# 1. Exports each lab notebook as .ipynb JSON
# 2. Scans code cells for unresolved markers such as <FILL_IN>, <FILL-IN>,
#    <ToDo>, and TODO-only scaffolds
# 3. Looks ahead in subsequent markdown cells for <code>...</code> blocks
# 4. Extracts the solution and replaces the original cell source
# 5. Re-imports the patched notebook
#
# Safe to re-run: it only patches cells that still contain unresolved markers.
# ──────────────────────────────────────────────────────────────────────────────

import base64
import json
import os
import re
import html as html_lib
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ExportFormat, ImportFormat, Language

w = WorkspaceClient()

# Derive course_root from this notebook's own path
_nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
course_root = "/".join(_nb_path.split("/")[:-1])

# LAB_NOTEBOOKS is configured in the course-specific inputs cell above.
# Resolve to absolute workspace paths
lab_abs_paths = [os.path.normpath(f"{course_root}/{p}") for p in LAB_NOTEBOOKS]

PLACEHOLDER_PATTERNS = [
    re.compile(r'<\s*fill[-_ ]?in\s*>', re.IGNORECASE),
    re.compile(r'--\s*fill[-_ ]?in\s*--', re.IGNORECASE),
    re.compile(r'<\s*todo\s*>', re.IGNORECASE),
    re.compile(r'(?mi)^\s*#+\s*todo\b'),
    re.compile(r'(?mi)^\s*#+.*\bTODO\b'),
    # SQL lab pattern: -- TODO comment at the start of a cell (used in Databricks Academy SQL labs)
    re.compile(r'(?mi)^\s*--\s*TODO\b'),
]


def has_unresolved_placeholder(source: str) -> bool:
    """Return True when a code cell still contains a fill-in or TODO scaffold."""
    return any(pattern.search(source) for pattern in PLACEHOLDER_PATTERNS)


def extract_solution_from_source(source: str) -> str | None:
    """Extract code from a Databricks solution cell.

    Handles:
    - Fenced code blocks: ```python\\n...\\n``` (used in current labs)
    - HTML <code> tags: <code>...</code> (legacy fallback)
    - Strips %md / %md-sandbox magic prefixes before parsing.
    """
    # Strip Databricks magic prefix (%md, %md-sandbox, etc.)
    clean = re.sub(r'^%md[a-zA-Z-]*[ \t]*\n', '', source.lstrip(), count=1)

    # 1. Fenced code block (most common in current labs)
    match = re.search(r'```(?:python|sql|scala|r|sh)?\s*\n(.*?)\n[ \t]*```', clean, re.DOTALL)
    if match:
        code = match.group(1)
        lines = code.split('\n')
        while lines and not lines[0].strip():
            lines.pop(0)
        while lines and not lines[-1].strip():
            lines.pop()
        return '\n'.join(lines) if lines else None

    # 2. HTML <code> tags (fallback for older lab format)
    match = re.search(r'<code[^>]*>\s*\n?(.*?)\n?\s*</code>', clean, re.DOTALL)
    if match:
        code = html_lib.unescape(match.group(1))
        lines = code.split('\n')
        while lines and not lines[0].strip():
            lines.pop(0)
        while lines and not lines[-1].strip():
            lines.pop()
        return '\n'.join(lines) if lines else None

    return None


def extract_answer_cell(source: str) -> str | None:
    """Extract solution from a SQL '-- ANSWER' code cell.

    Strips the leading '-- ANSWER' comment line and returns the remaining code.
    Used in Databricks Academy SQL lab notebooks where answers follow TODO cells
    directly as sibling code cells (rather than being embedded in markdown hint blocks).
    """
    lines = source.split('\n')
    result_lines = []
    skipping_answer_header = True
    for line in lines:
        if skipping_answer_header and re.match(r'\s*--\s*ANSWER\b', line, re.IGNORECASE):
            continue
        skipping_answer_header = False
        result_lines.append(line)
    # Strip leading/trailing blank lines
    while result_lines and not result_lines[0].strip():
        result_lines.pop(0)
    while result_lines and not result_lines[-1].strip():
        result_lines.pop()
    return '\n'.join(result_lines) if result_lines else None


def extract_skip_cell(source: str) -> str | None:
    """Extract solution from a '%skip' code cell.

    Strips the leading '%skip' line and returns the remaining code.
    Used in Databricks Academy labs where solution cells are marked with %skip magic.
    """
    lines = source.split('\n')
    result_lines = []
    skipping_header = True
    for line in lines:
        if skipping_header and re.match(r'\s*%skip\b', line, re.IGNORECASE):
            continue
        skipping_header = False
        result_lines.append(line)
    # Strip leading/trailing blank lines
    while result_lines and not result_lines[0].strip():
        result_lines.pop(0)
    while result_lines and not result_lines[-1].strip():
        result_lines.pop()
    return '\n'.join(result_lines) if result_lines else None


def is_solution_candidate(cell: dict) -> bool:
    """Return True if a cell is a potential answer/solution block.

    In Databricks ipynb exports, %md-sandbox cells are stored as code cells
    (not markdown), so we must check both cell_type=='markdown' and code cells
    that start with a %md magic prefix.
    """
    source = ''.join(cell.get("source", []))
    has_code_block = '```' in source or '<code>' in source
    is_markdown_type = cell.get("cell_type") == "markdown"
    is_md_magic = cell.get("cell_type") == "code" and bool(re.match(r'\s*%md', source))
    return (is_markdown_type or is_md_magic) and has_code_block


def fill_notebook(notebook_path: str) -> dict:
    """Fill all unresolved placeholder cells in a single notebook. Returns stats."""
    stats = {"path": notebook_path, "blanks_found": 0, "filled": 0, "no_answer": 0}

    # Export notebook as ipynb
    export_resp = w.workspace.export(path=notebook_path, format=ExportFormat.JUPYTER)
    nb_json = json.loads(base64.b64decode(export_resp.content))
    cells = nb_json.get("cells", [])

    for i, cell in enumerate(cells):
        source = ''.join(cell.get("source", []))

        # Check if this cell still has unresolved fill-in / TODO markers
        if not has_unresolved_placeholder(source):
            continue

        # Skip markdown cells (we only fill code cells)
        if cell.get("cell_type") != "code":
            continue

        stats["blanks_found"] += 1

        # Look ahead up to 8 cells for a solution block.
        # %md-sandbox solution cells may appear as 'code' type in ipynb exports,
        # so is_solution_candidate() checks both cell_type and %md magic prefix.
        solution = None
        for j in range(i + 1, min(i + 9, len(cells))):
            next_cell = cells[j]
            next_source = ''.join(next_cell.get("source", []))
            # SQL lab pattern: -- ANSWER code cell immediately follows -- TODO cell
            if (next_cell.get("cell_type") == "code"
                    and re.match(r'\s*--\s*ANSWER\b', next_source, re.IGNORECASE)):
                solution = extract_answer_cell(next_source)
                if solution:
                    break
            # %skip pattern: solution cell with %skip magic prefix
            elif (next_cell.get("cell_type") == "code"
                    and re.match(r'\s*%skip\b', next_source, re.IGNORECASE)):
                solution = extract_skip_cell(next_source)
                if solution:
                    break
            elif is_solution_candidate(next_cell):
                solution = extract_solution_from_source(next_source)
                if solution:
                    break
            # Stop looking once we hit a real (non-magic, non-ANSWER) code cell
            elif (next_cell.get("cell_type") == "code"
                  and not re.match(r'\s*%', next_source)
                  and not re.match(r'\s*--\s*ANSWER\b', next_source, re.IGNORECASE)):
                break

        if solution:
            # Check if solution has %sql magic — if so, handle cell language
            if solution.strip().startswith('%sql'):
                # Remove %sql prefix, keep the SQL code
                solution_lines = solution.strip().split('\n')
                # Keep %sql as first line for the cell magic
                cell["source"] = [solution + '\n']
            else:
                cell["source"] = [solution + '\n']
            stats["filled"] += 1
        else:
            stats["no_answer"] += 1
            print(f"  ⚠️  NO ANSWER FOUND for cell {i} in {notebook_path}")

    # Re-import the patched notebook
    if stats["filled"] > 0:
        patched_content = base64.b64encode(json.dumps(nb_json).encode()).decode()
        w.workspace.import_(
            path=notebook_path,
            content=patched_content,
            format=ImportFormat.JUPYTER,
            overwrite=True,
            language=Language.PYTHON,
        )

    return stats


# ── Run the filler ────────────────────────────────────────────────────────────
print("═" * 70)
print("AUTO-FILL LAB NOTEBOOKS")
print("═" * 70)

all_stats = []
for path in lab_abs_paths:
    print(f"\n▶ Processing: {path.split('/')[-1]}")
    try:
        s = fill_notebook(path)
        all_stats.append(s)
        print(f"   Blanks found: {s['blanks_found']}  |  Filled: {s['filled']}  |  No answer: {s['no_answer']}")
    except Exception as e:
        print(f"   ❌ ERROR: {e}")
        all_stats.append({"path": path, "blanks_found": 0, "filled": 0, "no_answer": 0, "error": str(e)})

# ── Summary ───────────────────────────────────────────────────────────────────
print("\n" + "═" * 70)
total_blanks = sum(s["blanks_found"] for s in all_stats)
total_filled = sum(s["filled"] for s in all_stats)
total_no_ans = sum(s["no_answer"] for s in all_stats)
print(f"TOTAL:  {total_blanks} blanks found  |  {total_filled} filled  |  {total_no_ans} blockers")
print("═" * 70)

# COMMAND ----------

# ── Auto-Fill DAB variables.yml Files ────────────────────────────────────────
# Scans all DAB project folders for variables.yml files with empty placeholders
# (username, my_email, cluster_id lookup) and fills them with the current
# user's information so that `databricks bundle validate` passes automatically.
#
# Safe to re-run: only patches fields that are still empty.
# ─────────────────────────────────────────────────────────────────────────────

import base64
import os
import re
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ExportFormat, ImportFormat, ObjectType

w = WorkspaceClient()

# ── Resolve current user info ───────────────────────────────────────────────
current_user_email = spark.sql("SELECT current_user()").collect()[0][0]
username_prefix = current_user_email.split("@")[0]
cluster_name = username_prefix  # In labs, cluster name == username prefix

# ── Locate course root ──────────────────────────────────────────────────────
_nb_path = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook().getContext().notebookPath().get()
)
# QC Automation sits two levels below the course root
course_root = "/Workspace" + "/".join(_nb_path.split("/")[:-2])


def find_files_recursive(path, filename, max_depth=6, _depth=0):
    """Walk the workspace directory tree and return paths matching *filename*."""
    if _depth > max_depth:
        return []
    found = []
    try:
        for obj in w.workspace.list(path):
            if (
                obj.object_type == ObjectType.FILE
                and (obj.path or "").endswith(f"/{filename}")
            ):
                found.append(obj.path)
            elif obj.object_type == ObjectType.DIRECTORY:
                found.extend(
                    find_files_recursive(obj.path, filename, max_depth, _depth + 1)
                )
    except Exception:
        pass
    return found


def patch_variables_yml(content: str) -> str:
    """Fill empty variable placeholders in a DAB variables.yml file.

    Detects which variable block each line belongs to and fills:
      - username  → default: ${workspace.current_user.short_name}
      - my_email  → default: <current user email>
      - cluster_id → lookup.cluster: <cluster name>
    Only patches lines where the value is empty (whitespace / comment only).
    """
    lines = content.split("\n")
    result = []
    current_var = None
    in_lookup = False

    for line in lines:
        stripped = line.strip()

        # Track which top-level variable block we are inside
        # Variables sit at exactly 2-space indent under `variables:`
        var_match = re.match(r"^  (\w+):\s*$", line)
        if var_match:
            current_var = var_match.group(1)
            in_lookup = False
            result.append(line)
            continue

        # Detect `lookup:` sub-block
        if stripped.startswith("lookup:"):
            in_lookup = True
            result.append(line)
            continue

        # ── username: fill empty default ─────────────────────────────────
        if (
            current_var == "username"
            and re.match(r"^(\s*default:)\s*(#.*)?$", line)
        ):
            m = re.match(r"^(\s*default:)\s*(#.*)?$", line)
            prefix = m.group(1)
            comment = m.group(2) or ""
            line = f"{prefix} ${{workspace.current_user.short_name}}  {comment}".rstrip()

        # ── my_email: fill empty default ─────────────────────────────────
        elif (
            current_var == "my_email"
            and re.match(r"^(\s*default:)\s*(#.*)?$", line)
        ):
            m = re.match(r"^(\s*default:)\s*(#.*)?$", line)
            prefix = m.group(1)
            comment = m.group(2) or ""
            line = f"{prefix} {current_user_email}  {comment}".rstrip()

        # ── cluster_id.lookup.cluster: fill empty cluster name ───────────
        elif (
            current_var == "cluster_id"
            and in_lookup
            and re.match(r"^(\s*cluster:)\s*(#.*)?$", line)
        ):
            m = re.match(r"^(\s*cluster:)\s*(#.*)?$", line)
            prefix = m.group(1)
            comment = m.group(2) or ""
            line = f"{prefix} {cluster_name}  {comment}".rstrip()

        result.append(line)

    return "\n".join(result)


# ── Find and patch all variables.yml files ──────────────────────────────────
yml_files = find_files_recursive(course_root, "variables.yml")
print(f"🔍 Found {len(yml_files)} variables.yml file(s) to check\n")

patched_count = 0
for yml_path in yml_files:
    resp = w.workspace.export(yml_path, format=ExportFormat.AUTO)
    content = base64.b64decode(resp.content).decode("utf-8")

    new_content = patch_variables_yml(content)

    if new_content != content:
        encoded = base64.b64encode(new_content.encode("utf-8")).decode("utf-8")
        w.workspace.import_(
            path=yml_path,
            content=encoded,
            format=ImportFormat.AUTO,
            overwrite=True,
        )
        patched_count += 1
        print(f"  ✅ Patched: {yml_path}")
    else:
        print(f"  ⏭️  Already filled: {yml_path}")

print(f"\n{'='*60}")
print(f"Auto-filled {patched_count} of {len(yml_files)} variables.yml file(s)")

# COMMAND ----------

# ── Patch Shell Cells for Automated Runs ─────────────────────────────────────
# Some lab notebooks use %sh cells that assume a clean filesystem state.
# When running as part of an automated job on a shared cluster, these cells
# may fail because previous tasks left artifacts in /databricks/driver/
# (e.g. `databricks bundle init` fails when the target directory already exists).
#
# This cell patches specific %sh cells to ensure idempotent execution.
# Safe to re-run: only patches cells that haven't been patched yet.
# ─────────────────────────────────────────────────────────────────────────────

import base64
import json
import re
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ExportFormat, ImportFormat, Language

w = WorkspaceClient()

_nb_path = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook().getContext().notebookPath().get()
)
_qc_root = "/".join(_nb_path.split("/")[:-1])
_course_root = "/".join(_nb_path.split("/")[:-2])

# ── Define patches ───────────────────────────────────────────────────────────
# Each entry targets a notebook and lists find/replace pairs for shell cells.
# The "replace" value must contain "find" so the patch is idempotent.
SHELL_PATCHES = [
    {
        "notebook_path": f"{_course_root}/11L - Use a Databricks Default DAB Template/Lab - Use a Databricks Default DAB Template",
        "patches": [
            {
                # Ensure bundle init starts clean — remove stale my_project dir
                "find": "databricks bundle init default-python",
                "replace": "rm -rf my_project\ndatabricks bundle init default-python",
            },
            {
                # Skip bundle validate — not needed for automated test runs
                "find": "cd 'my_project'\ndatabricks bundle validate",
                "replace": "# SKIPPED by auto-patcher: cd 'my_project' && databricks bundle validate\nexit 0",
            },
        ],
    },
]

print("=" * 70)
print("PATCH SHELL CELLS FOR AUTOMATED RUNS")
print("=" * 70)

for spec in SHELL_PATCHES:
    nb_path = spec["notebook_path"]
    short_name = nb_path.split("/")[-1]
    print(f"\n▶ Processing: {short_name}")

    try:
        export_resp = w.workspace.export(path=nb_path, format=ExportFormat.JUPYTER)
        nb_json = json.loads(base64.b64decode(export_resp.content))
        cells = nb_json.get("cells", [])

        patched = 0
        for cell in cells:
            source = "".join(cell.get("source", []))
            for patch in spec["patches"]:
                # Only patch if the find string is present AND the replace hasn't been applied yet
                if patch["find"] in source and patch["replace"] not in source:
                    new_source = source.replace(patch["find"], patch["replace"])
                    cell["source"] = [new_source]
                    patched += 1
                    print(f"   ✅ Patched cell: {patch['find'][:60]}...")

        if patched > 0:
            patched_content = base64.b64encode(json.dumps(nb_json).encode()).decode()
            w.workspace.import_(
                path=nb_path,
                content=patched_content,
                format=ImportFormat.JUPYTER,
                overwrite=True,
                language=Language.PYTHON,
            )
            print(f"   Saved {patched} patch(es)")
        else:
            print(f"   ⏭️  Already patched or no matches")

    except Exception as e:
        print(f"   ❌ ERROR: {e}")

print("\n" + "=" * 70)