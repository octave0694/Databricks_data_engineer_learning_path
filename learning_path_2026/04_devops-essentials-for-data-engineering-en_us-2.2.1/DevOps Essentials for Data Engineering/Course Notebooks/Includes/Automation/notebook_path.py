# Databricks notebook source
# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# MAGIC %md
# MAGIC ## Course-specific inputs

# COMMAND ----------

# Change the course name as per your course name
COURSE_NAME = "DevOps Essentials for Data Engineering"
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
    "../../M02 - CI/04 Lab - Modularize PySpark Code",
    "../../M02 - CI/11 Lab - Create and Execute Unit Tests",
]

# Define the course notebook DAG here so this can be used for job scheduling as per your course
COURSE_TASKS = [
    # ── M02 - CI ──────────────────────────────────────────────────────────────
{
        "task_key": "03 Demo - Modularizing PySpark Code - REQUIRED",
        "name": "03 Demo - Modularizing PySpark Code - REQUIRED",
        "relative_path": "../../M02 - CI/03 Demo - Modularizing PySpark Code - REQUIRED",
        "depends_on": [],
        "env_key": "course_03",
        "use_classic": True,
    },
    {
        "task_key": "04 Lab - Modularize PySpark Code",
        "name": "04 Lab - Modularize PySpark Code",
        "relative_path": "../../M02 - CI/04 Lab - Modularize PySpark Code",
        "depends_on": ["03 Demo - Modularizing PySpark Code - REQUIRED"],
        "env_key": "course_04",
        "use_classic": True,
    },
    {
        "task_key": "08 Demo - Project Setup Exploration",
        "name": "08 Demo - Project Setup Exploration",
        "relative_path": "../../M02 - CI/08 Demo - Project Setup Exploration",
        "depends_on": ["03 Demo - Modularizing PySpark Code - REQUIRED"],
        "env_key": "course_08",
        "use_classic": True,
    },
    {
        "task_key": "10 Demo - Creating and Executing Unit Tests",
        "name": "10 Demo - Creating and Executing Unit Tests",
        "relative_path": "../../M02 - CI/10 Demo - Creating and Executing Unit Tests",
        "depends_on": ["08 Demo - Project Setup Exploration"],
        "env_key": "course_10",
        "use_classic": True,
    },
    {
        "task_key": "11 Lab - Create and Execute Unit Tests",
        "name": "11 Lab - Create and Execute Unit Tests",
        "relative_path": "../../M02 - CI/11 Lab - Create and Execute Unit Tests",
        "depends_on": ["10 Demo - Creating and Executing Unit Tests"],
        "env_key": "course_11",
        "use_classic": True,
    },
    {
        "task_key": "13 Demo - Performing Integration Tests",
        "name": "13 Demo - Performing Integration Tests",
        "relative_path": "../../M02 - CI/13 Demo - Performing Integration Tests",
        "depends_on": ["11 Lab - Create and Execute Unit Tests"],
        "env_key": "course_13",
        "use_classic": True,
    },
# ── M03 - CD ──────────────────────────────────────────────────────────────
    {
        "task_key": "02 Demo - Deploying the Databricks Assets",
        "name": "02 Demo - Deploying the Databricks Assets",
        "relative_path": "../../M03 - CD/02 Demo - Deploying the Databricks Assets",
        "depends_on": ["13 Demo - Performing Integration Tests"],
        "env_key": "course_17",
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
    """Extract solution from an answer/solution code cell in any language.

    Strips the leading answer-label comment line (e.g., # ANSWER, # RESPOSTA,
    -- RÉPONSE, # ANTWORT, etc.) and returns the remaining code.
    Works with any single-word comment label regardless of language.
    """
    lines = source.split('\n')
    # Strip the first line if it is an answer label (a comment with a single word)
    if lines and re.match(r'\s*(?:#|--)\s+\w+\s*$', lines[0]):
        lines = lines[1:]
    # Strip leading/trailing blank lines
    while lines and not lines[0].strip():
        lines.pop(0)
    while lines and not lines[-1].strip():
        lines.pop()
    return '\n'.join(lines) if lines else None


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
    stats = {"path": notebook_path, "blanks_found": 0, "filled": 0, "no_answer": 0, "skip_removed": 0}

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
            # Answer/solution cell in any language (# ANSWER, # RESPOSTA, -- RÉPONSE, etc.)
            # Matches code cells whose first line is a single-word comment label
            if (next_cell.get("cell_type") == "code"
                    and re.match(r'\s*(?:#|--)\s+\w+\s*$', next_source.split('\n', 1)[0])):
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
                  and not re.match(r'\s*(?:#|--)\s+\w+\s*$', next_source.split('\n', 1)[0])):
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

    # Remove all %skip cells — they contain solution code and would fail or
    # duplicate results when the notebook runs in an automated job.
    original_cell_count = len(cells)
    cells[:] = [c for c in cells if not (
        c.get("cell_type") == "code"
        and re.match(r'\s*%skip\b', ''.join(c.get("source", [])), re.IGNORECASE)
    )]
    stats["skip_removed"] = original_cell_count - len(cells)

    # Re-import the patched notebook
    if stats["filled"] > 0 or stats["skip_removed"] > 0:
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
        print(f"   Blanks found: {s['blanks_found']}  |  Filled: {s['filled']}  |  No answer: {s['no_answer']}  |  Skip cells removed: {s['skip_removed']}")
    except Exception as e:
        print(f"   ❌ ERROR: {e}")
        all_stats.append({"path": path, "blanks_found": 0, "filled": 0, "no_answer": 0, "skip_removed": 0, "error": str(e)})

# ── Summary ───────────────────────────────────────────────────────────────────
print("\n" + "═" * 70)
total_blanks = sum(s["blanks_found"] for s in all_stats)
total_filled = sum(s["filled"] for s in all_stats)
total_no_ans = sum(s["no_answer"] for s in all_stats)
total_skip   = sum(s["skip_removed"] for s in all_stats)
print(f"TOTAL:  {total_blanks} blanks found  |  {total_filled} filled  |  {total_no_ans} blockers  |  {total_skip} skip cells removed")
print("═" * 70)