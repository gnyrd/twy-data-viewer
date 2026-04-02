"""TWY Data Viewer - Web interface for HeyMarvelous data."""

import os
import sys
import json
import sqlite3
from twy_paths import load_env, marvy_db_path
load_env()
import subprocess
import threading
import time
from datetime import datetime, timezone
from functools import wraps
from pathlib import Path

from flask import Flask, request, jsonify, send_from_directory, session, redirect, url_for, Response

app = Flask(__name__, static_folder="static")
app.secret_key = os.getenv("FLASK_SECRET_KEY", "twy-data-viewer-secret-key")

DB_PATH = str(marvy_db_path())
SYNC_SCRIPT = "/root/twy/marvy/scripts/sync.py"
SYNC_PYTHON = "/root/twy/marvy/.venv/bin/python3"
QUERIES_FILE = "/root/twy/data-viewer/saved_queries.json"

# Track sync state
_sync_lock = threading.Lock()
_sync_status = {"running": False, "last_log": ""}


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("logged_in"):
            if request.path.startswith("/api/"):
                return jsonify({"error": "Not authenticated"}), 401
            return redirect(url_for("login"))
        return f(*args, **kwargs)
    return decorated


LOGIN_PAGE = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>TWY Data Viewer - Sign in</title>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
      background: #0d1117;
      min-height: 100vh;
      display: flex;
      align-items: center;
      justify-content: center;
    }
    .card {
      background: #161b22;
      border: 1px solid #30363d;
      border-radius: 12px;
      padding: 32px 28px;
      width: 100%;
      max-width: 360px;
    }
    .logo { font-size: 22px; font-weight: 800; color: #e6edf3; margin-bottom: 4px; }
    .subtitle { font-size: 13px; color: #8b949e; margin-bottom: 24px; }
    label { display: block; font-size: 13px; font-weight: 600; color: #8b949e; margin-bottom: 6px; }
    input[type=password] {
      width: 100%;
      padding: 11px 13px;
      border: 1.5px solid #30363d;
      border-radius: 7px;
      font-size: 15px;
      margin-bottom: 14px;
      background: #0d1117;
      color: #e6edf3;
      transition: border-color 0.2s;
    }
    input[type=password]:focus { outline: none; border-color: #58a6ff; }
    button {
      width: 100%;
      padding: 11px;
      background: #1f6feb;
      color: white;
      border: none;
      border-radius: 7px;
      font-size: 15px;
      font-weight: 700;
      cursor: pointer;
      transition: background 0.2s;
    }
    button:hover { background: #388bfd; }
    .error { color: #f85149; font-size: 13px; margin-bottom: 12px; }
  </style>
</head>
<body>
  <div class="card">
    <div class="logo">TWY Data Viewer</div>
    <div class="subtitle">Tiffany Wood Yoga</div>
    {error}
    <form method="post">
      <label for="password">Password</label>
      <input id="password" name="password" type="password" autofocus autocomplete="current-password">
      <button type="submit">Sign in</button>
    </form>
  </div>
</body>
</html>"""


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        password = request.form.get("password", "")
        if os.getenv("DASHBOARD_PASS") and password == os.getenv("DASHBOARD_PASS"):
            session["logged_in"] = True
            return redirect(url_for("index"))
        return Response(
            LOGIN_PAGE.replace("{error}", '<div class="error">Invalid password</div>'),
            content_type="text/html"
        )
    return Response(
        LOGIN_PAGE.replace("{error}", ""),
        content_type="text/html"
    )


@app.route("/logout")
def logout():
    session.pop("logged_in", None)
    return redirect(url_for("login"))


def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def load_saved_queries():
    if os.path.exists(QUERIES_FILE):
        with open(QUERIES_FILE) as f:
            return json.load(f)
    return []


def save_queries(queries):
    with open(QUERIES_FILE, "w") as f:
        json.dump(queries, f, indent=2)


@app.route("/")
@login_required
def index():
    return send_from_directory("static", "index.html")


@app.route("/api/query", methods=["POST"])
@login_required
def run_query():
    sql = request.json.get("sql", "").strip()
    if not sql:
        return jsonify({"error": "No SQL provided"}), 400

    # Read-only enforcement
    first_word = sql.split()[0].upper() if sql.split() else ""
    if first_word not in ("SELECT", "WITH", "EXPLAIN", "PRAGMA"):
        return jsonify({"error": "Only SELECT queries allowed"}), 403

    try:
        db = get_db()
        t0 = time.time()
        rows = db.execute(sql).fetchall()
        elapsed = time.time() - t0
        columns = [desc[0] for desc in db.execute(sql).description] if rows else []
        db.close()

        return jsonify({
            "columns": columns,
            "rows": [list(r) for r in rows],
            "count": len(rows),
            "elapsed_ms": round(elapsed * 1000, 1),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/schema")
@login_required
def schema():
    db = get_db()
    tables = {}
    for row in db.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"):
        name = row[0]
        cols = []
        for col in db.execute(f"PRAGMA table_info({name})"):
            cols.append({"name": col[1], "type": col[2], "pk": bool(col[5])})
        count = db.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
        tables[name] = {"columns": cols, "count": count}
    db.close()
    return jsonify(tables)


@app.route("/api/sync-status")
@login_required
def sync_status():
    db = get_db()
    row = db.execute("SELECT * FROM sync_log ORDER BY id DESC LIMIT 1").fetchone()
    db.close()

    last_sync = None
    if row:
        last_sync = {
            "started_at": row["started_at"],
            "finished_at": row["finished_at"],
            "duration_seconds": row["duration_seconds"],
            "customers_synced": row["customers_synced"],
            "purchases_synced": row["purchases_synced"],
            "products_synced": row["products_synced"],
        }

    return jsonify({
        "syncing": _sync_status["running"],
        "last_sync": last_sync,
        "last_log": _sync_status["last_log"],
    })


@app.route("/api/sync", methods=["POST"])
@login_required
def trigger_sync():
    if _sync_status["running"]:
        return jsonify({"error": "Sync already in progress"}), 409

    def do_sync():
        with _sync_lock:
            _sync_status["running"] = True
            _sync_status["last_log"] = ""
            try:
                result = subprocess.run(
                    [SYNC_PYTHON, SYNC_SCRIPT],
                    capture_output=True, text=True, timeout=600,
                    cwd="/root/twy/marvy"
                )
                _sync_status["last_log"] = result.stdout + result.stderr
            except Exception as e:
                _sync_status["last_log"] = f"Error: {e}"
            finally:
                _sync_status["running"] = False

    threading.Thread(target=do_sync, daemon=True).start()
    return jsonify({"status": "started"})


@app.route("/api/saved-queries")
@login_required
def list_saved_queries():
    return jsonify(load_saved_queries())


@app.route("/api/saved-queries", methods=["POST"])
@login_required
def save_query():
    data = request.json
    name = data.get("name", "").strip()
    sql = data.get("sql", "").strip()
    if not name or not sql:
        return jsonify({"error": "Name and SQL required"}), 400

    queries = load_saved_queries()

    # Update existing or append
    for q in queries:
        if q["name"] == name:
            q["sql"] = sql
            q["updated"] = datetime.now(timezone.utc).isoformat()
            break
    else:
        queries.append({
            "name": name,
            "sql": sql,
            "created": datetime.now(timezone.utc).isoformat(),
            "updated": datetime.now(timezone.utc).isoformat(),
        })

    save_queries(queries)
    return jsonify({"status": "saved"})


@app.route("/api/saved-queries/<name>", methods=["DELETE"])
@login_required
def delete_query(name):
    queries = load_saved_queries()
    queries = [q for q in queries if q["name"] != name]
    save_queries(queries)
    return jsonify({"status": "deleted"})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5007, debug=True)
