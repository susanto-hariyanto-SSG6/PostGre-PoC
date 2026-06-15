# Copilot Instructions

## Project Overview

This is a **PostgreSQL performance proof-of-concept** — a Flask web app that visually demonstrates the query performance impact of adding a database index on the `attendance` table. The workflow is: run baseline test → inject bulk data → apply index → re-run test → compare results via a generated chart.

## Architecture

```
app.py              # Flask routes — orchestrates the demo workflow
simulator.py        # Simulation logic — 4 functions + standalone __main__ block:
                    #   run_traffic_load(label, total_students, threads)
                    #   execute_db_maintenance(sql, label)
                    #   inject_crowded_data(rows)
                    #   generate_comparison_chart(students)
templates/
  index.html        # Single-page UI with 4 sequential control buttons
static/
  index_comparison_report.png  # Output chart written by generate_comparison_chart()
```

## Database

- **Engine:** PostgreSQL 15 via `psycopg2`
- **Connection string** is hardcoded in `app.py` (matches the Docker Compose credentials):
  ```
  dbname=attendance_system user=admin password=password host=localhost
  ```
- **Schema** (auto-applied via `init.sql` on first container start):
  - `classes` — 20 classes seeded automatically
  - `students` — 300 students seeded across those classes
  - `attendance(id, student_id FK, clock_in, clock_out, status)` — the target table for the performance test
- **Index created by the app at runtime:** `idx_attendance_student ON attendance(student_id)`

## Running the App

```bash
# Build and start both Flask app + PostgreSQL
docker compose up --build

# Access at http://localhost:5000
```

To reset the database to its initial state:
```bash
docker compose down -v && docker compose up --build
```

For local development without Docker:
```bash
docker compose up -d db          # start only Postgres
pip install -r requirements.txt
python app.py                    # starts on http://localhost:5000
```

## Key Conventions

- `simulator.py` is imported but **not included in the repo** — any implementation must expose exactly these four function signatures used in `app.py`:
  - `run_traffic_load(label: str, total_students: int, threads: int)`
  - `execute_db_maintenance(sql: str, label: str)`
  - `inject_crowded_data(rows: int)`
  - `generate_comparison_chart(students: int)`
- The chart is saved to `static/index_comparison_report.png`; the HTML references it with a hardcoded cache-bust query string `?v=1` — increment this when regenerating.
- All routes use `redirect(url_for('index'))` after completing work (POST-redirect-GET style via GET routes).
- `get_row_count()` silently returns `0` on any database error — connection failures don't crash the UI.
