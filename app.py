from flask import Flask, render_template, jsonify, request
import time
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from simulator import (
    run_traffic_load, inject_crowded_data, execute_db_maintenance,
    run_traffic_load_mssql, inject_crowded_data_mssql, execute_db_maintenance_mssql,
    init_mssql_schema,
    get_pg_row_count, get_mssql_row_count, get_metrics,
    get_all_table_counts_pg, get_all_table_counts_mssql, generate_schema_diagram,
)

app = Flask(__name__)

# --- Global state ---
pg_index_applied    = False
mssql_index_applied = False
mssql_ready         = False
log_lines           = []
test_running        = False
log_lock            = threading.Lock()

# --- Docker stats cache ---
PG_CONTAINER    = os.environ.get("PG_CONTAINER",    "poc-postgres")
MSSQL_CONTAINER = os.environ.get("MSSQL_CONTAINER", "poc-mssql")
stats_cache = {
    "pg":    {"cpu": 0.0, "mem_mb": 0.0, "mem_pct": 0.0},
    "mssql": {"cpu": 0.0, "mem_mb": 0.0, "mem_pct": 0.0},
}

try:
    import docker as docker_sdk
    docker_client = docker_sdk.from_env()
    docker_client.ping()
except Exception as e:
    print(f"[startup] Docker SDK unavailable: {e}")
    docker_client = None


def _get_container_stats(container_name):
    try:
        c = docker_client.containers.get(container_name)
        s = c.stats(stream=False)
        cpu_now   = s.get("cpu_stats",    {}).get("cpu_usage", {}).get("total_usage", 0)
        cpu_pre   = s.get("precpu_stats", {}).get("cpu_usage", {}).get("total_usage", 0)
        sys_now   = s.get("cpu_stats",    {}).get("system_cpu_usage", 0)
        sys_pre   = s.get("precpu_stats", {}).get("system_cpu_usage", 0)
        ncpus     = s.get("cpu_stats",    {}).get("online_cpus", 1)
        sys_delta = sys_now - sys_pre
        cpu_pct   = ((cpu_now - cpu_pre) / sys_delta * ncpus * 100.0) if sys_delta > 0 else 0.0
        mem_stats  = s.get("memory_stats", {})
        mem_usage  = mem_stats.get("usage", 0)
        mem_limit  = max(mem_stats.get("limit", 1), 1)
        inner      = mem_stats.get("stats", {})
        cache      = inner.get("cache", inner.get("inactive_file", 0))
        mem_actual = max(0, mem_usage - cache)
        return {
            "cpu":     round(min(cpu_pct, 100.0), 1),
            "mem_mb":  round(mem_actual / 1024 / 1024, 1),
            "mem_pct": round(mem_actual / mem_limit * 100, 1),
        }
    except Exception:
        return {"cpu": 0.0, "mem_mb": 0.0, "mem_pct": 0.0}


def _stats_update_loop():
    if not docker_client:
        return
    while True:
        with ThreadPoolExecutor(max_workers=2) as ex:
            f_pg    = ex.submit(_get_container_stats, PG_CONTAINER)
            f_mssql = ex.submit(_get_container_stats, MSSQL_CONTAINER)
            stats_cache["pg"]    = f_pg.result()
            stats_cache["mssql"] = f_mssql.result()
        time.sleep(1)


threading.Thread(target=_stats_update_loop, daemon=True).start()


# ── Log helpers ───────────────────────────────────────────────────────────────

def add_log(line):
    with log_lock:
        log_lines.append(line)


def start_task(fn):
    """Append a separator, mark running, execute fn in background thread."""
    global test_running
    with log_lock:
        if log_lines:
            log_lines.append("SEP")   # frontend renders this as a divider
    test_running = True

    def wrapper():
        global test_running
        try:
            fn()
        except Exception as e:
            add_log(f"ERROR: {e}")
        finally:
            add_log("__DONE__")
            test_running = False

    threading.Thread(target=wrapper, daemon=True).start()


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template(
        "index.html",
        pg_row_count=get_pg_row_count(),
        mssql_row_count=get_mssql_row_count(),
        pg_index_applied=pg_index_applied,
        mssql_index_applied=mssql_index_applied,
        mssql_ready=mssql_ready,
    )


@app.route("/run_test")
def run_test():
    if test_running:
        return jsonify({"status": "busy"})
    threads = int(request.args.get("threads", 50))

    def task():
        pg_label    = "PG After Index"    if pg_index_applied    else "PG Before Index"
        mssql_label = "MSSQL After Index" if mssql_index_applied else "MSSQL Before Index"
        jobs = [threading.Thread(target=lambda: run_traffic_load(pg_label, 300, threads, add_log))]
        if mssql_ready:
            jobs.append(threading.Thread(target=lambda: run_traffic_load_mssql(mssql_label, 300, threads, add_log)))
        else:
            add_log("[MSSQL] Not ready yet — running PostgreSQL only.")
        for t in jobs: t.start()
        for t in jobs: t.join()

    start_task(task)
    return jsonify({"status": "started"})


@app.route("/inject_history")
def inject_history():
    if test_running:
        return jsonify({"status": "busy"})

    def task():
        jobs = [threading.Thread(target=lambda: inject_crowded_data(5000, add_log))]
        if mssql_ready:
            jobs.append(threading.Thread(target=lambda: inject_crowded_data_mssql(5000, add_log)))
        else:
            add_log("[MSSQL] Not ready yet — injecting PostgreSQL only.")
        for t in jobs: t.start()
        for t in jobs: t.join()

    start_task(task)
    return jsonify({"status": "started"})


@app.route("/apply_index")
def apply_index():
    if test_running:
        return jsonify({"status": "busy"})

    def task():
        global pg_index_applied, mssql_index_applied
        jobs = []

        def _pg():
            execute_db_maintenance(
                "CREATE INDEX IF NOT EXISTS idx_attendance_student ON attendance(student_id);",
                "Applying PG Index", log_fn=add_log)
            global pg_index_applied
            pg_index_applied = True

        def _mssql():
            execute_db_maintenance_mssql(
                "IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name='idx_attendance_student' "
                "AND object_id=OBJECT_ID('attendance')) "
                "CREATE INDEX idx_attendance_student ON attendance(student_id)",
                "Applying MSSQL Index", log_fn=add_log)
            global mssql_index_applied
            mssql_index_applied = True

        jobs.append(threading.Thread(target=_pg))
        if mssql_ready:
            jobs.append(threading.Thread(target=_mssql))
        else:
            add_log("[MSSQL] Not ready yet — applying PG index only.")
        for t in jobs: t.start()
        for t in jobs: t.join()

    start_task(task)
    return jsonify({"status": "started"})


@app.route("/remove_index")
def remove_index():
    if test_running:
        return jsonify({"status": "busy"})

    def task():
        global pg_index_applied, mssql_index_applied

        def _pg():
            execute_db_maintenance(
                "DROP INDEX IF EXISTS idx_attendance_student;",
                "Removing PG Index", log_fn=add_log)
            global pg_index_applied
            pg_index_applied = False

        def _mssql():
            execute_db_maintenance_mssql(
                "DROP INDEX IF EXISTS idx_attendance_student ON attendance;",
                "Removing MSSQL Index", log_fn=add_log)
            global mssql_index_applied
            mssql_index_applied = False

        jobs = [threading.Thread(target=_pg)]
        if mssql_ready:
            jobs.append(threading.Thread(target=_mssql))
        for t in jobs: t.start()
        for t in jobs: t.join()

    start_task(task)
    return jsonify({"status": "started"})


@app.route("/logs")
def get_logs():
    with log_lock:
        return jsonify({"lines": list(log_lines), "running": test_running})


@app.route("/status")
def status():
    return jsonify({
        "pg_index_applied":    pg_index_applied,
        "mssql_index_applied": mssql_index_applied,
        "pg_row_count":        get_pg_row_count(),
        "mssql_row_count":     get_mssql_row_count(),
        "running":             test_running,
        "mssql_ready":         mssql_ready,
    })


@app.route("/docker_stats")
def docker_stats():
    return jsonify(stats_cache)


@app.route("/metrics")
def metrics():
    return jsonify(get_metrics())


@app.route("/refresh_schema")
def refresh_schema():
    pg_counts    = get_all_table_counts_pg()
    mssql_counts = get_all_table_counts_mssql() if mssql_ready else {"classes": 0, "students": 0, "attendance": 0}
    generate_schema_diagram(pg_counts, mssql_counts)
    return jsonify({"status": "ok"})


# __ Startup background threads ────────────────────────────────────────────────

def _mssql_startup_init():
    global mssql_ready
    for attempt in range(6):
        try:
            init_mssql_schema(log_fn=print)
            mssql_ready = True
            print("[startup] MSSQL schema ready.")
            return
        except Exception as e:
            print(f"[startup] MSSQL init attempt {attempt + 1}/6 failed: {e}")
            time.sleep(5)
    print("[startup] MSSQL init gave up after 6 attempts.")


threading.Thread(target=_mssql_startup_init, daemon=True).start()


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000, use_reloader=False)



