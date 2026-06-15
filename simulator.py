import psycopg2
import pymssql
import time
import os
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from concurrent.futures import ThreadPoolExecutor

# --- Connection config ---
DB_HOST = os.environ.get("DB_HOST", "localhost")
PG_CONFIG = f"dbname=attendance_system user=admin password=password host={DB_HOST}"

MSSQL_HOST = os.environ.get("MSSQL_HOST", "localhost")
MSSQL_DB   = "attendance_mssql"
MSSQL_USER = "SA"
MSSQL_PASS = "Password123!"

# --- Per-run history: each entry is {mean, p95, max} for one test run ---
run_history = {
    "PG Before Index":    [],
    "PG After Index":     [],
    "MSSQL Before Index": [],
    "MSSQL After Index":  [],
}


def get_metrics():
    return {k: list(v) for k, v in run_history.items()}

CHART_META = [
    ("PG Before Index",    "PostgreSQL — Before Index", "#e74c3c"),
    ("PG After Index",     "PostgreSQL — After Index",  "#27ae60"),
    ("MSSQL Before Index", "SQL Server — Before Index", "#c0392b"),
    ("MSSQL After Index",  "SQL Server — After Index",  "#2980b9"),
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_pg_row_count():
    try:
        conn = psycopg2.connect(PG_CONFIG)
        cur  = conn.cursor()
        cur.execute("SELECT count(*) FROM attendance")
        n = cur.fetchone()[0]
        cur.close(); conn.close()
        return n
    except Exception:
        return 0


def get_mssql_row_count():
    try:
        conn = _mssql_conn()
        cur  = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM attendance")
        n = cur.fetchone()[0]
        cur.close(); conn.close()
        return n
    except Exception:
        return 0


def _mssql_conn():
    return pymssql.connect(
        server=MSSQL_HOST, user=MSSQL_USER,
        password=MSSQL_PASS, database=MSSQL_DB
    )


# ── PostgreSQL ────────────────────────────────────────────────────────────────

def run_traffic_load(phase_key, total_students=300, threads=50, log_fn=None):
    if log_fn:
        log_fn(f"[PG] '{phase_key}' — {total_students} students, {threads} threads...")
    run_lat = []
    student_ids = list(range(1, total_students + 1))
    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=threads) as ex:
        ex.map(lambda s: _pg_clock_in(s, run_lat), student_ids)
    elapsed = time.perf_counter() - start
    if run_lat:
        mean = np.mean(run_lat)
        p95  = np.percentile(run_lat, 95)
        mx   = np.max(run_lat)
        run_history[phase_key].append({"mean": round(mean, 2), "p95": round(p95, 2), "max": round(mx, 2), "latencies": run_lat})
        if log_fn:
            log_fn(f"[PG] Done in {elapsed:.2f}s — Mean: {mean:.2f}ms | P95: {p95:.2f}ms | Max: {mx:.2f}ms")


def _pg_clock_in(student_id, lat_list):
    t = time.perf_counter()
    try:
        conn = psycopg2.connect(PG_CONFIG)
        cur  = conn.cursor()
        # Simulate login screen: aggregate student + class info + attendance history
        cur.execute("""
            SELECT s.id, s.name, c.name AS class_name, COUNT(a.id) AS total_checkins
            FROM students s
            JOIN classes c ON s.class_id = c.id
            LEFT JOIN attendance a ON a.student_id = s.id
            WHERE s.id = %s
            GROUP BY s.id, s.name, c.name
        """, (student_id,))
        cur.fetchone()
        # Record clock-in
        cur.execute("INSERT INTO attendance (student_id, status) VALUES (%s, 'present')", (student_id,))
        conn.commit(); cur.close(); conn.close()
        lat_list.append((time.perf_counter() - t) * 1000)
    except Exception:
        pass


def inject_crowded_data(count=5000, log_fn=None):
    if log_fn:
        log_fn(f"[PG] Inserting {count} historical records...")
    conn = psycopg2.connect(PG_CONFIG)
    cur  = conn.cursor()
    cur.execute(f"""
        INSERT INTO attendance (student_id, status, clock_in)
        SELECT (random()*299)+1, 'present', now() - (random() * interval '30 days')
        FROM generate_series(1, {count});
    """)
    conn.commit(); cur.close(); conn.close()
    if log_fn:
        log_fn(f"[PG] Done. {count} records inserted.")


def execute_db_maintenance(sql, label, log_fn=None):
    if log_fn:
        log_fn(f"[PG] {label}...")
    conn = psycopg2.connect(PG_CONFIG)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(sql)
    cur.close(); conn.close()
    if log_fn:
        log_fn(f"[PG] {label} complete.")


# ── SQL Server ────────────────────────────────────────────────────────────────

def init_mssql_schema(log_fn=None):
    """Create database, tables, and seed data if they do not exist."""
    # Create database in master
    conn = pymssql.connect(
        server=MSSQL_HOST, user=MSSQL_USER,
        password=MSSQL_PASS, database="master", autocommit=True
    )
    cur = conn.cursor()
    cur.execute(
        f"IF NOT EXISTS (SELECT name FROM sys.databases WHERE name='{MSSQL_DB}') "
        f"CREATE DATABASE [{MSSQL_DB}]"
    )
    cur.close(); conn.close()

    # Create tables
    conn = _mssql_conn()
    cur  = conn.cursor()
    cur.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='classes')
        CREATE TABLE classes (
            id   INT IDENTITY(1,1) PRIMARY KEY,
            name NVARCHAR(50) NOT NULL
        )
    """)
    cur.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='students')
        CREATE TABLE students (
            id       INT IDENTITY(1,1) PRIMARY KEY,
            name     NVARCHAR(100) NOT NULL,
            class_id INT REFERENCES classes(id)
        )
    """)
    cur.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name='attendance')
        CREATE TABLE attendance (
            id         INT IDENTITY(1,1) PRIMARY KEY,
            student_id INT REFERENCES students(id),
            clock_in   DATETIME2 DEFAULT GETDATE(),
            clock_out  DATETIME2,
            status     NVARCHAR(20)
        )
    """)
    conn.commit()

    cur.execute("SELECT COUNT(*) FROM classes")
    if cur.fetchone()[0] == 0:
        for i in range(1, 21):
            cur.execute("INSERT INTO classes (name) VALUES (%s)", (f"Class {i}",))
        conn.commit()

    cur.execute("SELECT COUNT(*) FROM students")
    if cur.fetchone()[0] == 0:
        for i in range(1, 301):
            cur.execute("INSERT INTO students (name, class_id) VALUES (%s, %s)",
                        (f"Student {i}", (i % 20) + 1))
        conn.commit()

    cur.close(); conn.close()
    if log_fn:
        log_fn("[MSSQL] Schema ready — tables + seed data initialised.")


def run_traffic_load_mssql(phase_key, total_students=300, threads=50, log_fn=None):
    if log_fn:
        log_fn(f"[MSSQL] '{phase_key}' — {total_students} students, {threads} threads...")
    run_lat = []
    student_ids = list(range(1, total_students + 1))
    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=threads) as ex:
        ex.map(lambda s: _mssql_clock_in(s, run_lat), student_ids)
    elapsed = time.perf_counter() - start
    if run_lat:
        mean = np.mean(run_lat)
        p95  = np.percentile(run_lat, 95)
        mx   = np.max(run_lat)
        run_history[phase_key].append({"mean": round(mean, 2), "p95": round(p95, 2), "max": round(mx, 2), "latencies": run_lat})
        if log_fn:
            log_fn(f"[MSSQL] Done in {elapsed:.2f}s — Mean: {mean:.2f}ms | P95: {p95:.2f}ms | Max: {mx:.2f}ms")


def _mssql_clock_in(student_id, lat_list):
    t = time.perf_counter()
    conn = None
    try:
        conn = _mssql_conn()
        cur  = conn.cursor()
        # Simulate login screen: aggregate student + class info + attendance history
        cur.execute("""
            SELECT s.id, s.name, c.name AS class_name, COUNT(a.id) AS total_checkins
            FROM students s
            JOIN classes c ON s.class_id = c.id
            LEFT JOIN attendance a ON a.student_id = s.id
            WHERE s.id = %s
            GROUP BY s.id, s.name, c.name
        """, (student_id,))
        cur.fetchone()
        # Record clock-in
        cur.execute("INSERT INTO attendance (student_id, status) VALUES (%s, 'present')", (student_id,))
        conn.commit()
        lat_list.append((time.perf_counter() - t) * 1000)
    except Exception:
        pass
    finally:
        if conn:
            try: conn.close()
            except Exception: pass


def inject_crowded_data_mssql(count=5000, log_fn=None):
    if log_fn:
        log_fn(f"[MSSQL] Inserting {count} historical records...")
    conn = _mssql_conn()
    cur  = conn.cursor()
    cur.execute(f"""
        WITH nums AS (
            SELECT TOP ({count}) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
            FROM sys.all_objects a CROSS JOIN sys.all_objects b
        )
        INSERT INTO attendance (student_id, status, clock_in)
        SELECT
            (ABS(CHECKSUM(NEWID())) % 300) + 1,
            'present',
            DATEADD(day, -(ABS(CHECKSUM(NEWID())) % 30), GETDATE())
        FROM nums
    """)
    conn.commit(); cur.close(); conn.close()
    if log_fn:
        log_fn(f"[MSSQL] Done. {count} records inserted.")


def execute_db_maintenance_mssql(sql, label, log_fn=None):
    if log_fn:
        log_fn(f"[MSSQL] {label}...")
    conn = _mssql_conn()
    cur  = conn.cursor()
    cur.execute(sql)
    conn.commit(); cur.close(); conn.close()
    if log_fn:
        log_fn(f"[MSSQL] {label} complete.")


# ── Chart ─────────────────────────────────────────────────────────────────────

def generate_comparison_chart(total_students=300, log_fn=None):
    """Fallback PNG chart (used only in __main__ standalone mode)."""
    if log_fn:
        log_fn("Generating comparison chart...")
    os.makedirs("static", exist_ok=True)

    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    fig.suptitle("PostgreSQL vs SQL Server — Index Performance Comparison",
                 fontsize=15, fontweight="bold")

    for ax, (key, title, color) in zip(axes.flatten(), CHART_META):
        runs = run_history[key]
        if runs:
            means = [r["mean"] for r in runs]
            p95s  = [r["p95"]  for r in runs]
            maxs  = [r["max"]  for r in runs]
            labels = ["Mean", "P95", "Max"]
            values = [np.mean(means), np.mean(p95s), np.mean(maxs)]
            ax.bar(labels, values, color=color, alpha=0.85, edgecolor="black")
            ax.set_ylabel("Latency (ms)")
            ax.set_title(f"{title}  ({len(runs)} run(s))")
        else:
            ax.text(0.5, 0.5, "No data yet\nRun test to populate",
                    ha="center", va="center", transform=ax.transAxes,
                    fontsize=12, color="gray")
            ax.set_title(title)

    plt.tight_layout()
    plt.savefig("static/index_comparison_report.png")
    plt.close()
    if log_fn:
        log_fn("Chart saved.")


if __name__ == "__main__":
    init_mssql_schema(log_fn=print)
    run_traffic_load("PG Before Index", log_fn=print)
    inject_crowded_data(5000, log_fn=print)
    execute_db_maintenance(
        "CREATE INDEX idx_attendance_student ON attendance(student_id);",
        "Applying PG Index", log_fn=print)
    run_traffic_load("PG After Index", log_fn=print)
    run_traffic_load_mssql("MSSQL Before Index", log_fn=print)
    inject_crowded_data_mssql(5000, log_fn=print)
    execute_db_maintenance_mssql(
        "IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name='idx_attendance_student' AND object_id=OBJECT_ID('attendance')) CREATE INDEX idx_attendance_student ON attendance(student_id)",
        "Applying MSSQL Index", log_fn=print)
    run_traffic_load_mssql("MSSQL After Index", log_fn=print)
    generate_comparison_chart(300, log_fn=print)


# ── Schema diagram ────────────────────────────────────────────────────────────

def get_all_table_counts_pg():
    result = {"classes": 0, "students": 0, "attendance": 0}
    try:
        conn = psycopg2.connect(PG_CONFIG)
        cur  = conn.cursor()
        for t in result:
            cur.execute(f"SELECT COUNT(*) FROM {t}")
            result[t] = cur.fetchone()[0]
        cur.close(); conn.close()
    except Exception:
        pass
    return result


def get_all_table_counts_mssql():
    result = {"classes": 0, "students": 0, "attendance": 0}
    try:
        conn = _mssql_conn()
        cur  = conn.cursor()
        for t in result:
            cur.execute(f"SELECT COUNT(*) FROM {t}")
            result[t] = cur.fetchone()[0]
        cur.close(); conn.close()
    except Exception:
        pass
    return result


def generate_schema_diagram(pg_counts, mssql_counts, log_fn=None):
    import matplotlib.patches as mp

    SCHEMA = [
        {"name": "classes",
         "cols": [("id", "SERIAL / INT IDENTITY", True, False),
                  ("name", "VARCHAR(50)", False, False)]},
        {"name": "students",
         "cols": [("id", "SERIAL / INT IDENTITY", True, False),
                  ("name", "VARCHAR(100)", False, False),
                  ("class_id", "INT  →  classes.id", False, True)]},
        {"name": "attendance",
         "cols": [("id", "SERIAL / INT IDENTITY", True, False),
                  ("student_id", "INT  →  students.id", False, True),
                  ("clock_in", "TIMESTAMP", False, False),
                  ("clock_out", "TIMESTAMP", False, False),
                  ("status", "VARCHAR(20)", False, False)]},
    ]
    RELATIONS = [
        ("students",   "class_id",   "classes",  "id"),
        ("attendance", "student_id", "students", "id"),
    ]

    fig, axes = plt.subplots(1, 2, figsize=(17, 11))
    fig.patch.set_facecolor("#f5f6fa")
    fig.suptitle("Database Schema & Table Row Counts",
                 fontsize=15, fontweight="bold", y=0.99, color="#2c3e50")

    for ax, db_label, db_color, counts in [
        (axes[0], "PostgreSQL 15",    "#336791", pg_counts),
        (axes[1], "SQL Server 2022",  "#CC2927", mssql_counts),
    ]:
        ax.set_facecolor("#f5f6fa")
        ax.axis("off")

        TW  = 5.2   # table width
        TX  = 0.5   # table left edge x
        RH  = 0.46  # row height
        HH  = 0.58  # header height
        FH  = 0.40  # footer height
        GAP = 1.1   # vertical gap between tables

        # Compute table y positions top-down
        y_cursor = 11.8
        tbl_meta = {}
        for tbl in SCHEMA:
            h = HH + len(tbl["cols"]) * RH + FH
            tbl_meta[tbl["name"]] = {
                "y_top": y_cursor,
                "y_bot": y_cursor - h,
                "col_ys": {},
            }
            y_cursor = y_cursor - h - GAP

        ax.set_xlim(0, 7.0)
        ax.set_ylim(y_cursor - 0.8, 13.2)

        # DB title badge
        ax.text(TX + TW / 2, 12.7, db_label,
                ha="center", va="center", fontsize=13, fontweight="bold", color="white",
                bbox=dict(boxstyle="round,pad=0.5", facecolor=db_color, edgecolor="none"))

        for tbl in SCHEMA:
            m = tbl_meta[tbl["name"]]
            y_top, y_bot = m["y_top"], m["y_bot"]
            body_h = y_top - y_bot - HH - FH

            # Drop shadow
            ax.add_patch(mp.FancyBboxPatch(
                (TX + 0.08, y_bot - 0.08), TW, y_top - y_bot,
                boxstyle="round,pad=0.05", fc="#c8c8c8", ec="none", zorder=1))

            # Header
            ax.add_patch(mp.FancyBboxPatch(
                (TX, y_top - HH), TW, HH,
                boxstyle="round,pad=0.05", fc=db_color, ec="none", zorder=2))
            ax.text(TX + TW / 2, y_top - HH / 2, tbl["name"],
                    ha="center", va="center", fontsize=11, fontweight="bold",
                    color="white", zorder=3)

            # Body
            ax.add_patch(mp.FancyBboxPatch(
                (TX, y_bot + FH), TW, body_h,
                boxstyle="round,pad=0.05", fc="white", ec=db_color, lw=1.5, zorder=2))

            # Columns
            y_cur = y_top - HH
            for col_name, col_type, is_pk, is_fk in tbl["cols"]:
                y_cur -= RH
                cy = y_cur + RH / 2
                m["col_ys"][col_name] = cy

                # Row divider
                ax.plot([TX + 0.06, TX + TW - 0.06], [y_cur + RH, y_cur + RH],
                        color="#eeeeee", lw=0.7, zorder=3)

                # PK / FK badge
                if is_pk:
                    badge_fc, badge_txt = "#c0392b", "PK"
                elif is_fk:
                    badge_fc, badge_txt = "#2980b9", "FK"
                else:
                    badge_fc, badge_txt = None, None

                if badge_fc:
                    ax.text(TX + 0.14, cy, badge_txt,
                            ha="left", va="center", fontsize=7, color="white",
                            fontweight="bold", zorder=3,
                            bbox=dict(facecolor=badge_fc, edgecolor="none",
                                      boxstyle="round,pad=0.2"))

                col_color = "#c0392b" if is_pk else "#2980b9" if is_fk else "#333333"
                ax.text(TX + 0.60, cy, col_name,
                        ha="left", va="center", fontsize=9,
                        color=col_color,
                        fontweight="bold" if (is_pk or is_fk) else "normal", zorder=3)
                ax.text(TX + TW - 0.12, cy, col_type,
                        ha="right", va="center", fontsize=7.5, color="#999999", zorder=3)

            # Footer (row count)
            cnt = counts.get(tbl["name"], 0)
            ax.add_patch(mp.FancyBboxPatch(
                (TX, y_bot), TW, FH,
                boxstyle="round,pad=0.05", fc="#eef2f7", ec=db_color, lw=1.0, zorder=2))
            ax.text(TX + TW / 2, y_bot + FH / 2,
                    f"Total rows:  {cnt:,}",
                    ha="center", va="center", fontsize=9.5,
                    color=db_color, fontweight="bold", zorder=3)

        # FK relationship arrows (right side, curved)
        for (from_tbl, from_col, to_tbl, to_col) in RELATIONS:
            fm  = tbl_meta[from_tbl]
            tm  = tbl_meta[to_tbl]
            fky = fm["col_ys"].get(from_col, fm["y_bot"])
            pky = tm["col_ys"].get(to_col,   tm["y_top"])
            ax_x = TX + TW + 0.08

            ax.annotate("",
                        xy=(ax_x, pky), xytext=(ax_x, fky),
                        arrowprops=dict(
                            arrowstyle="-|>",
                            color="#e67e22", lw=2.2,
                            connectionstyle="arc3,rad=-0.55",
                            mutation_scale=14,
                        ),
                        zorder=5)

        # Legend
        ly = y_cursor - 0.35
        for lx, lc, ll in [
            (TX,       "#c0392b", "PK  Primary Key"),
            (TX + 2.7, "#2980b9", "FK  Foreign Key"),
        ]:
            ax.add_patch(mp.FancyBboxPatch(
                (lx, ly - 0.14), 0.28, 0.28,
                boxstyle="round,pad=0.04", fc=lc, ec="none", zorder=3))
            ax.text(lx + 0.38, ly, ll, va="center", fontsize=8, color="#555555", zorder=3)

    plt.tight_layout(rect=[0, 0, 1, 0.97])
    os.makedirs("static", exist_ok=True)
    plt.savefig("static/schema_diagram.png", dpi=100,
                bbox_inches="tight", facecolor="#f5f6fa")
    plt.close()
    if log_fn:
        log_fn("Schema diagram saved to static/schema_diagram.png")
