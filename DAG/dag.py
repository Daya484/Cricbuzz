from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.state import State

# ─── Git Repo Config ──────────────────────────────────────────────────────────
GIT_REPO_URL  = "https://github.com/Daya484/Cricbuzz.git"  # ← your repo URL
GIT_BRANCH    = "main"                                      # ← your branch
CLONE_DIR     = "/home/airflow/gcs/data/cricbuzz_repo"      # persistent GCS-backed path

# ─── Derived paths (all come from the cloned repo) ────────────────────────────
EXTRACT_SCRIPT = f"{CLONE_DIR}/data_extraction/data_extraction.py"
DBT_PROJECT    = f"{CLONE_DIR}/DBT"
DBT_PROFILES   = f"{CLONE_DIR}/DBT"

# ─── GCP / Notification Config ────────────────────────────────────────────────
NOTIFY_EMAIL   = "dayasagarreddy484@gmail.com"
GCP_PROJECT_ID = "project-75b24ad0-af2b-4b2b-863"
BQ_DATASET     = "dayasagar"
GCP_LOCATION   = "US"

# ─── Extraction task IDs for failure checking ─────────────────────────────────
EXTRACTION_TASK_IDS = [
    "extract_icc_allrounders",
    "extract_icc_batsmen",
    "extract_icc_bowlers",
    "extract_icc_teams",
    "extract_photo_gallery",
    "extract_photo_photos",
    "extract_team_international",
    "extract_team_players",
    "extract_team_results",
]

default_args = {
    "owner": "airflow",
    "start_date": datetime(2026, 2, 10),
    "depends_on_past": False,
    "email": [NOTIFY_EMAIL],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ─── Callable: write profiles.yml from config ─────────────────────────────────
def write_dbt_profiles():
    profiles_content = f"""daya:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: oauth
      project: {GCP_PROJECT_ID}
      dataset: {BQ_DATASET}
      threads: 4
      timeout_seconds: 3000
      location: {GCP_LOCATION}
"""
    os.makedirs(DBT_PROFILES, exist_ok=True)
    profiles_path = os.path.join(DBT_PROFILES, "profiles.yml")
    with open(profiles_path, "w") as f:
        f.write(profiles_content)
    print(f"✅ profiles.yml written to: {profiles_path}")


# ─── Callable: check which extractions failed ─────────────────────────────────
def check_extraction_results(**context):
    dag_run = context["dag_run"]
    failed = []
    for task_id in EXTRACTION_TASK_IDS:
        ti = dag_run.get_task_instance(task_id)
        if ti and ti.state != State.SUCCESS:
            failed.append(task_id)
    context["ti"].xcom_push(key="failed_tasks", value=failed)
    return "email_failure" if failed else "email_success"


# ─── DAG ──────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="cricbuzz_pipeline",
    default_args=default_args,
    description="Git pull → Extract → DBT → Archive → Email",
    schedule_interval="30 3 1 * *",   # 1st of every month at 09:00 AM IST (03:30 UTC)
    catchup=False,
) as dag:

    # ── Step 1: Clone or pull latest code from GitHub ─────────────────────────
    # Deletes old clone and does a fresh clone every run to guarantee latest code.
    # Uses pip-installed gitpython as fallback if system git is unavailable.
    git_clone = BashOperator(
        task_id="git_clone_repo",
        bash_command=(
            f"echo '🔄 Fetching latest code from GitHub...' && "
            f"rm -rf {CLONE_DIR} && "
            f"pip install gitpython --quiet && "
            f"python -c \""
            f"import git; "
            f"git.Repo.clone_from("
            f"'{GIT_REPO_URL}', "
            f"'{CLONE_DIR}', "
            f"branch='{GIT_BRANCH}'"
            f"); "
            f"print('✅ Repo cloned successfully to {CLONE_DIR}')\" "
        ),
    )

    # ── Step 2a: Install dbt-core + dbt-bigquery ─────────────────────────────
    setup_dbt = BashOperator(
        task_id="setup_dbt",
        bash_command=(
            "pip install dbt-core>=1.7.0 dbt-bigquery>=1.7.0 --quiet "
            "&& echo '✅ dbt installed'"
        ),
    )

    # ── Step 2b: Install Python dependencies ─────────────────────────────────
    install_deps = BashOperator(
        task_id="install_python_deps",
        bash_command=(
            "pip install requests google-cloud-storage --quiet "
            "&& echo '✅ Python deps installed'"
        ),
    )

    # ── Step 2c: Write profiles.yml ──────────────────────────────────────────
    create_dbt_profiles = PythonOperator(
        task_id="create_dbt_profiles",
        python_callable=write_dbt_profiles,
    )

    # ── Step 3: 9 Parallel extraction tasks (all use cloned script) ──────────
    extract_icc_allrounders = BashOperator(
        task_id="extract_icc_allrounders",
        bash_command=f"python {EXTRACT_SCRIPT} extract_icc_rankings_allrounders",
    )
    extract_icc_batsmen = BashOperator(
        task_id="extract_icc_batsmen",
        bash_command=f"python {EXTRACT_SCRIPT} extract_icc_rankings_batsmen",
    )
    extract_icc_bowlers = BashOperator(
        task_id="extract_icc_bowlers",
        bash_command=f"python {EXTRACT_SCRIPT} extract_icc_rankings_bowlers",
    )
    extract_icc_teams = BashOperator(
        task_id="extract_icc_teams",
        bash_command=f"python {EXTRACT_SCRIPT} extract_icc_rankings_teams",
    )
    extract_photo_gallery = BashOperator(
        task_id="extract_photo_gallery",
        bash_command=f"python {EXTRACT_SCRIPT} extract_photo_gallery",
    )
    extract_photo_photos = BashOperator(
        task_id="extract_photo_photos",
        bash_command=f"python {EXTRACT_SCRIPT} extract_photo_photos",
    )
    extract_team_international = BashOperator(
        task_id="extract_team_international",
        bash_command=f"python {EXTRACT_SCRIPT} extract_team_international",
    )
    extract_team_players = BashOperator(
        task_id="extract_team_players",
        bash_command=f"python {EXTRACT_SCRIPT} extract_team_players",
    )
    extract_team_results = BashOperator(
        task_id="extract_team_results",
        bash_command=f"python {EXTRACT_SCRIPT} extract_team_results",
    )

    all_extractions = [
        extract_icc_allrounders, extract_icc_batsmen, extract_icc_bowlers,
        extract_icc_teams, extract_photo_gallery, extract_photo_photos,
        extract_team_international, extract_team_players, extract_team_results,
    ]

    # ── Step 4: dbt run ──────────────────────────────────────────────────────
    run_dbt = BashOperator(
        task_id="run_dbt",
        bash_command=(
            f"dbt run"
            f" --project-dir {DBT_PROJECT}"
            f" --profiles-dir {DBT_PROFILES}"
            f" --select +models/Gold/final_v2.sql+"
        ),
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ── Step 5: Archive ──────────────────────────────────────────────────────
    archive = BashOperator(
        task_id="archive_to_gcs",
        bash_command=f"python {EXTRACT_SCRIPT} archive_to_gcs",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ── Step 6: Check results (always runs) ──────────────────────────────────
    check_results = BranchPythonOperator(
        task_id="check_extraction_results",
        python_callable=check_extraction_results,
        trigger_rule=TriggerRule.ALL_DONE,
        provide_context=True,
    )

    # ── Step 7a: Success email ───────────────────────────────────────────────
    email_success = EmailOperator(
        task_id="email_success",
        to=NOTIFY_EMAIL,
        subject="✅ Cricbuzz Pipeline — All DAGs Executed Successfully",
        html_content="""
        <h3>✅ Cricbuzz Daily Pipeline — Success</h3>
        <p>All tasks completed successfully.</p>
        <ul>
            <li>Git pull: <b>DONE</b></li>
            <li>All 9 extraction tasks: <b>PASSED</b></li>
            <li>DBT run (<code>dbt run --select +final+</code>): <b>PASSED</b></li>
            <li>Archive to GCS: <b>PASSED</b></li>
        </ul>
        <p><b>All DAGs are executed successfully.</b></p>
        """,
    )

    # ── Step 7b: Failure email ───────────────────────────────────────────────
    email_failure = EmailOperator(
        task_id="email_failure",
        to=NOTIFY_EMAIL,
        subject="⚠️ Cricbuzz Pipeline — Some DAG Tasks Failed",
        html_content="""
        <h3>⚠️ Cricbuzz Daily Pipeline — Partial Failure</h3>
        <p>Most DAGs passed, but the following source task(s) failed:</p>
        <p><b>{{ ti.xcom_pull(task_ids='check_extraction_results', key='failed_tasks') }}</b></p>
        <p>All other DAGs executed successfully.</p>
        """,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ── Task Dependency Graph ─────────────────────────────────────────────────
    #
    #            git_clone_repo  (always pulls latest from GitHub)
    #           /       |       \
    #      setup_dbt  install   create_dbt_profiles
    #           \     _deps    /       |
    #            \            /        |
    #         [9 extract_* tasks] ─────┘
    #              (parallel)
    #                  │
    #            (ALL_SUCCESS)
    #                  │
    #               run_dbt
    #                  │
    #            (ALL_SUCCESS)
    #                  │
    #           archive_to_gcs
    #                  │
    #             (ALL_DONE)
    #           check_results
    #            /           \
    #     email_success   email_failure

    # git clone runs first — everything depends on it
    git_clone >> [setup_dbt, install_deps, create_dbt_profiles]
    git_clone >> all_extractions

    # run_dbt waits for: dbt installed + profiles ready + all extractions done
    [setup_dbt, create_dbt_profiles] + all_extractions >> run_dbt

    # final chain
    run_dbt >> archive >> check_results
    check_results >> [email_success, email_failure]