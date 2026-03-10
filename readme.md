# 🏏 Cricbuzz Real-Time Data Pipeline

An end-to-end automated data pipeline that extracts cricket data from the **Cricbuzz API**, transforms it using **dbt Core**, archives it in **Google Cloud Storage**, and notifies via **email** — all orchestrated by **Apache Airflow (Cloud Composer)**.

---

## 📁 Project Structure

```
Cricbuzz/
├── data_extraction/
│   ├── data_extraction.py     ← All 9 API sources + GCS archive in one file (CLI-dispatchable)
│   └── requirements.json      ← API key, endpoint paths, GCS bucket names
│
├── DBT/
│   ├── dbt_project.yml        ← dbt project config (profile: cricbuzz, Bronze/Silver/Gold layers)
│   ├── source.yml             ← All 9 BigQuery source tables registered
│   └── models/
│       ├── Bronze/            ← Raw ingestion (materialized: incremental)
│       │   ├── ranking_allrounder_bronze_ingest.sql
│       │   ├── ranking_batsmens_bronze_ingest.sql
│       │   ├── ranking_bowlers_bronze_ingest.sql
│       │   ├── ranking_teams_bronze_ingest.sql
│       │   ├── photo_bronze_ingest.sql
│       │   ├── photo_galary_bronze_ingest.sql
│       │   ├── team_international_bronze_ingest.sql
│       │   ├── team_players_bronze_ingest.sql
│       │   └── team_result_bronze_ingest.sql
│       ├── Silver/            ← Deduplication layer (materialized: incremental)
│       │   ├── ranking_allrounders_silver_distinct_ingest.sql
│       │   ├── ranking_batsmens_silver_distinct_ingest.sql
│       │   ├── ranking_bowlers_silver_distinct_ingest.sql
│       │   ├── ranking_teams_silver_distinct_ingest.sql
│       │   ├── photo_silver_distinct_ingest.sql
│       │   ├── photo_galary_silver_distinct_ingest.sql
│       │   ├── team_international_silver_distinct_ingest.sql
│       │   ├── team_players_silver_distinct_ingest.sql
│       │   └── team_results_silver_distinct_ingest.sql
│       └── Gold/              ← Final aggregated model (materialized: table)
│           ├── final.sql      ← Master JSON-aggregated output across all sources
│           └── final_v2.sql
│
└── DAG/
    └── dag.py                 ← Full Airflow pipeline (Git-driven, zero manual uploads)
```

---

## ⚙️ Pipeline Flow

```
git_clone_repo  (pulls latest code from GitHub on every run)
       │
       ├──► setup_dbt + install_deps + create_dbt_profiles
       │
       └──► [9 extraction tasks — run in PARALLEL]
                   │   ICC Rankings: allrounders, batsmen, bowlers, teams
                   │   Photo: gallery, photos
                   │   Team: international, players, results
                   │
             (ALL_SUCCESS)
                   │
                run_dbt  →  dbt run --select +final+
                   │
             (ALL_SUCCESS)
                   │
            archive_to_gcs  →  moves CSVs to archive-daya/YYYY-MM-DD/
                   │
              (ALL_DONE)
            check_results
             /           \
     email_success    email_failure
         ✅ All passed     ⚠️ Lists failed sources
```

---

## 🗂️ Data Sources (9 total)

| Category | Source | GCS Bucket |
|---|---|---|
| ICC Rankings | Allrounders (T20) | `icc-rank_allrounder` |
| ICC Rankings | Batsmen (T20) | `icc-rank_batsmen` |
| ICC Rankings | Bowlers (T20) | `icc_rank_bolwer` |
| ICC Rankings | Teams (T20) | `icc-rank_teams` |
| Photo | Gallery | `photo-photo_galary` |
| Photo | Photos | `photo-photos` |
| Team | International | `testing-daya` |
| Team | Players | `team-team_players` |
| Team | Results | `team-team_result` |

---

## ☁️ GCP Stack

| Service | Usage |
|---|---|
| **Cloud Composer** | Airflow DAG orchestration |
| **Cloud Storage** | Raw CSV landing + archive bucket (`archive-daya`) |
| **BigQuery** | Dataset: `dayasagar` in project `project-75b24ad0-af2b-4b2b-863` |
| **dbt Core** | Bronze → Silver → Gold transformation |

---

## 🚀 Deployment

Only `dag.py` needs to be uploaded to the Composer bucket **once**:

```bash
gsutil cp DAG/dag.py gs://<your-composer-bucket>/dags/dag.py
```

All other files (`data_extraction.py`, dbt models, `requirements.json`) are pulled automatically from this Git repo at the start of every DAG run via the `git_clone_repo` task.

---

## ⏰ Schedule

Runs automatically on the **1st of every month at 9:00 AM IST** (`30 3 1 * *` UTC).

---

## 📬 Notifications

Email sent to `dayasagarreddy484@gmail.com` after every run:
- ✅ **Success** — *"All DAGs are executed successfully"*
- ⚠️ **Partial failure** — *"All DAGs passed except: [failed task names]"*
