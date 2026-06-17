import psycopg2
import pymssql
import time
import os
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mp
import matplotlib.patheffects as pe
from matplotlib.patches import FancyBboxPatch
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
    error_list = []
    student_ids = list(range(1, total_students + 1))
    start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=threads) as ex:
        ex.map(lambda s: _pg_clock_in(s, run_lat, error_list), student_ids)
    elapsed = time.perf_counter() - start
    success = len(run_lat)
    errors  = len(error_list)
    if run_lat:
        mean = np.mean(run_lat)
        p95  = np.percentile(run_lat, 95)
        mx   = np.max(run_lat)
        run_history[phase_key].append({"mean": round(mean, 2), "p95": round(p95, 2), "max": round(mx, 2), "success": success, "errors": errors, "latencies": run_lat})
        if log_fn:
            log_fn(f"[PG] Done in {elapsed:.2f}s — Mean: {mean:.2f}ms | P95: {p95:.2f}ms | Max: {mx:.2f}ms | Success: {success} | Errors: {errors}")


def _pg_clock_in(student_id, lat_list, error_list):
    t = time.perf_counter()
    try:
        conn = psycopg2.connect(PG_CONFIG)
        cur  = conn.cursor()
        # Simulate login screen: aggregate student + class info + attendance history
        cur.execute("""
            SELECT s.id, s.name, s.class_id, c.name AS class_name, COUNT(a.id) AS total_checkins
            FROM students s
            JOIN classes c ON s.class_id = c.id
            LEFT JOIN attendance a ON a.student_id = s.id
            WHERE s.id = %s
            GROUP BY s.id, s.name, s.class_id, c.name
        """, (student_id,))
        row = cur.fetchone()
        class_id = row[2] if row else None
        # Record clock-in with class reference
        cur.execute(
            "INSERT INTO attendance (student_id, class_id, status) VALUES (%s, %s, 'present')",
            (student_id, class_id)
        )
        conn.commit(); cur.close(); conn.close()
        lat_list.append((time.perf_counter() - t) * 1000)
    except Exception as e:
        error_list.append(str(e)) 


def inject_crowded_data(count=5000, log_fn=None):
    if log_fn:
        log_fn(f"[PG] Inserting {count} historical records...")
    conn = psycopg2.connect(PG_CONFIG)
    cur  = conn.cursor()
    cur.execute(f"""
        INSERT INTO attendance (student_id, class_id, status, clock_in)
        SELECT s.id, s.class_id, 'present', now() - (random() * interval '30 days')
        FROM (
            SELECT ((random() * 299) + 1)::int AS sid
            FROM generate_series(1, {count})
        ) t
        JOIN students s ON s.id = t.sid;
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
            class_id   INT REFERENCES classes(id),
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
    error_list = []
    with ThreadPoolExecutor(max_workers=threads) as ex:
        ex.map(lambda s: _mssql_clock_in(s, run_lat, error_list), student_ids)
    elapsed = time.perf_counter() - start
    if run_lat:
        mean = np.mean(run_lat)
        p95  = np.percentile(run_lat, 95)
        mx   = np.max(run_lat)
        success = len(run_lat)
        errors  = len(error_list)
        run_history[phase_key].append({"mean": round(mean, 2), "p95": round(p95, 2), "max": round(mx, 2), "success": success, "errors": errors, "latencies": run_lat})
        if log_fn:
            log_fn(f"[MSSQL] Done in {elapsed:.2f}s — Mean: {mean:.2f}ms | P95: {p95:.2f}ms | Max: {mx:.2f}ms | Success: {success} | Errors: {errors}")


def _mssql_clock_in(student_id, lat_list, error_list):
    t = time.perf_counter()
    conn = None
    try:
        conn = _mssql_conn()
        cur  = conn.cursor()
        # Simulate login screen: aggregate student + class info + attendance history
        cur.execute("""
            SELECT s.id, s.name, s.class_id, c.name AS class_name, COUNT(a.id) AS total_checkins
            FROM students s
            JOIN classes c ON s.class_id = c.id
            LEFT JOIN attendance a ON a.student_id = s.id
            WHERE s.id = %s
            GROUP BY s.id, s.name, s.class_id, c.name
        """, (student_id,))
        row = cur.fetchone()
        class_id = row[2] if row else None
        # Record clock-in with class reference
        cur.execute(
            "INSERT INTO attendance (student_id, class_id, status) VALUES (%s, %s, 'present')",
            (student_id, class_id)
        )
        conn.commit()
        lat_list.append((time.perf_counter() - t) * 1000)
    except Exception as e:
        error_list.append(str(e))
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
        ),
        rand_students AS (
            SELECT (ABS(CHECKSUM(NEWID())) % 300) + 1 AS sid FROM nums
        )
        INSERT INTO attendance (student_id, class_id, status, clock_in)
        SELECT
            s.id,
            s.class_id,
            'present',
            DATEADD(day, -(ABS(CHECKSUM(NEWID())) % 30), GETDATE())
        FROM rand_students r
        JOIN students s ON s.id = r.sid
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
 
 
# ── Palette (module-level so _rounded_rect can reference them) ─────────────────
_BG        = "#F7F8FA"
_CARD      = "#FFFFFF"
_HEADER_PK = "#C0392B"
_HEADER_FK = "#2980B9"
_BORDER    = "#D0D5DD"
_TEXT_MAIN = "#1A1D23"
_TEXT_MUTE = "#6B7280"
_TEXT_TYPE = "#9CA3AF"
_ACCENT_PG = "#336791"
_ACCENT_MS = "#CC2927"
_ROW_ALT   = "#F3F6FA"
_ARROW_CLR = "#E67E22"
 
SCHEMA = [
    {
        "name": "classes",
        "cols": [
            ("id",   "SERIAL / INT IDENTITY", True,  False),
            ("name", "VARCHAR(50)",            False, False),
        ],
    },
    {
        "name": "students",
        "cols": [
            ("id",       "SERIAL / INT IDENTITY", True,  False),
            ("name",     "VARCHAR(100)",           False, False),
            ("class_id", "INT -> classes.id",      False, True),
        ],
    },
    {
        "name": "attendance",
        "cols": [
            ("id",         "SERIAL / INT IDENTITY", True,  False),
            ("student_id", "INT -> students.id",    False, True),
            ("class_id",   "INT -> classes.id",     False, True),
            ("clock_in",   "TIMESTAMP",              False, False),
            ("clock_out",  "TIMESTAMP",              False, False),
            ("status",     "VARCHAR(20)",            False, False),
        ],
    },
]
 
RELATIONS = [
    ("students",   "class_id",   "classes",  "id"),
    ("attendance", "student_id", "students", "id"),
    ("attendance", "class_id",   "classes",  "id"),
]
 
 
def _rounded_rect(ax, x, y, w, h, radius=0.18, fc=_CARD, ec=_BORDER,
                  lw=1.0, zorder=2, shadow=False):
    if shadow:
        ax.add_patch(FancyBboxPatch(
            (x + 0.06, y - 0.07), w, h,
            boxstyle=f"round,pad=0.04,rounding_size={radius}",
            fc="#D0D5DD", ec="none", zorder=zorder - 1, alpha=0.55,
        ))
    ax.add_patch(FancyBboxPatch(
        (x, y), w, h,
        boxstyle=f"round,pad=0.04,rounding_size={radius}",
        fc=fc, ec=ec, lw=lw, zorder=zorder,
    ))
 
 
def generate_schema_diagram(pg_counts, mssql_counts, log_fn=None):
    TW    = 5.2
    RH    = 0.48
    HH    = 0.60
    FH    = 0.52
    GAP_Y = 0.80   # vertical gap between classes & students (left col)
    GAP_X = 2.20   # horizontal gap between left and right column

    # ── Figure ────────────────────────────────────────────────────────────────
    fig, ax = plt.subplots(figsize=(16, 11))
    fig.patch.set_facecolor(_BG)
    ax.set_facecolor(_BG)
    ax.axis("off")
    ax.set_xlim(0, 16)
    ax.set_ylim(0, 11)

    ax.text(8, 10.6, "Database Schema",
            ha="center", va="center", fontsize=16, fontweight="bold", color=_TEXT_MAIN)
    ax.text(8, 10.25, "Row counts:  PostgreSQL  |  SQL Server",
            ha="center", va="center", fontsize=9, color=_TEXT_MUTE)

    # ── Fixed positions ───────────────────────────────────────────────────────
    # Left column: classes (top) and students (below)
    # Right column: attendance (vertically centred)
    LEFT_X  = 1.0
    RIGHT_X = LEFT_X + TW + GAP_X

    def card_height(tbl):
        return HH + len(tbl["cols"]) * RH + FH

    h_classes    = card_height(SCHEMA[0])
    h_students   = card_height(SCHEMA[1])
    h_attendance = card_height(SCHEMA[2])

    students_top  = 9.8
    students_bot  = students_top - h_students

    # Classes now goes below students with a gap
    classes_top   = students_bot - GAP_Y
    classes_bot   = classes_top - h_classes

    # Attendance: vertically centred alongside the two left tables
    left_span_top = students_top
    left_span_bot = classes_bot
    att_mid       = (left_span_top + left_span_bot) / 2
    att_top       = att_mid + h_attendance / 2
    att_bot       = att_mid - h_attendance / 2

    positions = {
        "students":   (LEFT_X,  students_top, students_bot),
        "classes":    (LEFT_X,  classes_top,  classes_bot),
        "attendance": (RIGHT_X, att_top,      att_bot),
    }

    tbl_meta = {}
    for tbl in SCHEMA:
        tx, ty_top, ty_bot = positions[tbl["name"]]
        tbl_meta[tbl["name"]] = {
            "tx": tx, "ty_top": ty_top, "ty_bot": ty_bot, "col_ys": {},
        }

    # ── Draw each table ───────────────────────────────────────────────────────
    for tbl in SCHEMA:
        m      = tbl_meta[tbl["name"]]
        tx     = m["tx"]
        ty_top = m["ty_top"]
        ty_bot = m["ty_bot"]
        total_h = ty_top - ty_bot

        # Shadow
        ax.add_patch(FancyBboxPatch(
            (tx + 0.07, ty_bot - 0.07), TW, total_h,
            boxstyle="round,pad=0.04,rounding_size=0.20",
            fc="#D0D5DD", ec="none", zorder=1, alpha=0.5,
        ))
        # Card
        ax.add_patch(FancyBboxPatch(
            (tx, ty_bot), TW, total_h,
            boxstyle="round,pad=0.04,rounding_size=0.20",
            fc=_CARD, ec=_BORDER, lw=1.2, zorder=2,
        ))

        # Header
        ax.add_patch(FancyBboxPatch(
            (tx, ty_top - HH), TW, HH,
            boxstyle="round,pad=0.04,rounding_size=0.20",
            fc=_TEXT_MAIN, ec="none", zorder=3,
        ))
        ax.add_patch(plt.Rectangle(
            (tx, ty_top - HH), TW, HH / 2,
            fc=_TEXT_MAIN, ec="none", zorder=3,
        ))
        ax.text(tx + 0.45, ty_top - HH / 2, tbl["name"],
                ha="left", va="center", fontsize=12, fontweight="bold",
                color="white", zorder=4)
        ax.text(tx + TW - 0.22, ty_top - HH / 2, "⊞",
                ha="center", va="center", fontsize=10,
                color="white", alpha=0.4, zorder=4)

        # Columns
        y_cur = ty_top - HH
        for i, (col_name, col_type, is_pk, is_fk) in enumerate(tbl["cols"]):
            y_cur -= RH
            cy = y_cur + RH / 2
            m["col_ys"][col_name] = cy

            if i % 2 == 1:
                ax.add_patch(plt.Rectangle(
                    (tx + 0.02, y_cur + 0.02), TW - 0.04, RH - 0.04,
                    fc=_ROW_ALT, ec="none", zorder=2,
                ))
            ax.plot([tx + 0.1, tx + TW - 0.1], [y_cur + RH, y_cur + RH],
                    color=_BORDER, lw=0.6, zorder=3)

            if is_pk or is_fk:
                badge_fc  = _HEADER_PK if is_pk else _HEADER_FK
                badge_txt = "PK" if is_pk else "FK"
                ax.text(tx + 0.22, cy, badge_txt,
                        ha="center", va="center", fontsize=7.5,
                        color="white", fontweight="bold", zorder=4,
                        bbox=dict(facecolor=badge_fc, edgecolor="none",
                                  boxstyle="round,pad=0.22"))

            col_color = _HEADER_PK if is_pk else (_HEADER_FK if is_fk else _TEXT_MAIN)
            ax.text(tx + 0.55, cy, col_name,
                    ha="left", va="center", fontsize=9.5, color=col_color,
                    fontweight="bold" if (is_pk or is_fk) else "normal", zorder=4)
            ax.text(tx + TW - 0.14, cy, col_type,
                    ha="right", va="center", fontsize=7.8, color=_TEXT_TYPE, zorder=4)

        # Footer
        pg_cnt = pg_counts.get(tbl["name"], 0)
        ms_cnt = mssql_counts.get(tbl["name"], 0)

        ax.add_patch(FancyBboxPatch(
            (tx, ty_bot), TW, FH,
            boxstyle="round,pad=0.04,rounding_size=0.20",
            fc="#F0F4FF", ec=_BORDER, lw=0.8, zorder=3,
        ))
        ax.add_patch(plt.Rectangle(
            (tx, ty_bot + FH / 2), TW, FH / 2,
            fc="#F0F4FF", ec="none", zorder=3,
        ))
        cx   = tx + TW / 2
        cy_f = ty_bot + FH / 2
        ax.text(cx - 0.15, cy_f, "Total rows:", ha="right", va="center",
                fontsize=8.5, color=_TEXT_MUTE, zorder=4)
        ax.text(cx + 0.08, cy_f, f"{pg_cnt:,}", ha="left", va="center",
                fontsize=9.5, fontweight="bold", color=_ACCENT_PG, zorder=4,
                bbox=dict(facecolor="#E8F0F9", edgecolor="none", boxstyle="round,pad=0.22"))
        ax.text(cx + 0.82, cy_f, "|", ha="center", va="center",
                fontsize=10, color=_TEXT_MUTE, zorder=4)
        ax.text(cx + 1.05, cy_f, f"{ms_cnt:,}", ha="left", va="center",
                fontsize=9.5, fontweight="bold", color=_ACCENT_MS, zorder=4,
                bbox=dict(facecolor="#FDECEA", edgecolor="none", boxstyle="round,pad=0.22"))

    # ── FK arrows: from right edge of left table → left edge of attendance ────
    for (from_tbl, from_col, to_tbl, to_col) in RELATIONS:
        fm  = tbl_meta[from_tbl]
        tm  = tbl_meta[to_tbl]
        fky = fm["col_ys"].get(from_col, (fm["ty_top"] + fm["ty_bot"]) / 2)
        pky = tm["col_ys"].get(to_col,   (tm["ty_top"] + tm["ty_bot"]) / 2)

        from_tx = fm["tx"]
        to_tx   = tm["tx"]

        same_col = abs(from_tx - to_tx) < 0.5

        if same_col:
            # Both tables on the left column: route arrow on the LEFT side
            # Go left out of the FK col → elbow down/up → into PK row on the right
            exit_x  = from_tx - 0.35          # just left of the table
            enter_x = to_tx   - 0.35          # same x, left side
            land_x  = to_tx + 0.02            # just touching left edge of target (no overlap)

            path_x = [from_tx, exit_x,  enter_x, land_x]
            path_y = [fky,     fky,      pky,     pky]
            ax.plot(path_x, path_y,
                    color=_ARROW_CLR, lw=1.8, zorder=5,
                    solid_capstyle="round", solid_joinstyle="round")
            # Arrowhead pointing right into the table
            ax.annotate("",
                xy=(land_x + 0.01, pky), xytext=(land_x - 0.25, pky),
                arrowprops=dict(arrowstyle="-|>", color=_ARROW_CLR,
                                lw=1.8, mutation_scale=13),
                zorder=6,
            )

        else:
            start_x = from_tx            
            exit_x  = start_x - 0.15          # Step left into the gap
            
            # 2. End at the RIGHT edge of the target table (Class/Student)
            land_x  = to_tx + TW              
            enter_x = land_x + 0.15           # Step left towards the target's right edge

            # Route the path from right to left
            path_x = [start_x, exit_x,  enter_x, land_x]
            path_y = [fky,     fky,     pky,     pky]
            
            # Draw the connecting line
            ax.plot(path_x, path_y,
                    color=_ARROW_CLR, lw=1.8, zorder=5,
                    solid_capstyle="round", solid_joinstyle="round")
            
            # 3. Arrowhead pointing LEFT (<-) into the right edge of the target table
            ax.annotate("",
                xy=(land_x, pky),             # Tip touches the right edge of Class/Student
                xytext=(enter_x, pky),        # Tail comes from the right side of the tip
                arrowprops=dict(arrowstyle="-|>", color=_ARROW_CLR,
                                lw=1.8, mutation_scale=13),
                zorder=6,
            )

    # ── Legend ────────────────────────────────────────────────────────────────
    legend_items = [
        (_HEADER_PK, "PK  Primary Key"),
        (_HEADER_FK, "FK  Foreign Key"),
        (_ACCENT_PG, "PG  PostgreSQL"),
        (_ACCENT_MS, "SQL  SQL Server"),
    ]
    lx = 1.8
    for lc, ll in legend_items:
        ax.add_patch(FancyBboxPatch(
            (lx, 0.28), 0.26, 0.26,
            boxstyle="round,pad=0.03", fc=lc, ec="none", zorder=3,
        ))
        ax.text(lx + 0.38, 0.41, ll, va="center",
                fontsize=9, color=_TEXT_MUTE, zorder=3)
        lx += 3.2

    plt.tight_layout(pad=0.4)
    os.makedirs("static", exist_ok=True)
    out_path = "static/schema_diagram.png"
    plt.savefig(out_path, dpi=120, bbox_inches="tight", facecolor=_BG)
    plt.close()
    if log_fn:
        log_fn(f"Schema diagram saved to {out_path}")