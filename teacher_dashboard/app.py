
from __future__ import annotations

import csv
import io
import json
import os
import shutil
import sqlite3
import threading
import time
import urllib.parse
import urllib.request
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from dotenv import load_dotenv
from flask import Flask, Response, jsonify, render_template, request, send_file

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env", override=True)

DB_PATH = Path(os.getenv("DB_PATH", "database/class.db"))
if not DB_PATH.is_absolute():
    DB_PATH = BASE_DIR / DB_PATH
DB_PATH = DB_PATH.resolve()

BOT_TOKEN = (os.getenv("BOT_TOKEN") or "").strip()
DEFAULT_AT_RISK_THRESHOLD = 3

CAMPAIGN_TEMPLATES: dict[str, str] = {
    "gentle": (
        "Hi {first_name}, this is a friendly reminder that you currently have "
        "{missing_count} missing assignment(s).\\n\\n"
        "{missing_list}\\n\\n"
        "Please submit what you can today. Open /start for details."
    ),
    "firm": (
        "{first_name}, action needed: you have {missing_count} missing assignment(s).\\n\\n"
        "{missing_list}\\n\\n"
        "Submit as soon as possible to avoid grade impact. Open /start now."
    ),
    "exam": (
        "Exam prep check-in for {first_name}:\\n"
        "You still have {missing_count} missing assignment(s).\\n\\n"
        "{missing_list}\\n\\n"
        "Clearing these will help your readiness. Open /start to plan next steps."
    ),
}

SCHEDULE_OPTIONS: dict[str, str] = {
    "now": "Send now",
    "30m": "In 30 minutes",
    "2h": "In 2 hours",
    "tomorrow_0700": "Tomorrow 07:00",
}

app = Flask(__name__, template_folder="templates", static_folder="static")
try:
    CAMPAIGN_POLL_INTERVAL_SEC = max(15, int(os.getenv("DASH_CAMPAIGN_POLL_SEC", "30")))
except ValueError:
    CAMPAIGN_POLL_INTERVAL_SEC = 30
_campaign_worker_lock = threading.Lock()
_campaign_worker_started = False


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(value)


@contextmanager
def db_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _json_ok(data: Any = None, message: str | None = None):
    payload = {"ok": True}
    if data is not None:
        payload["data"] = data
    if message:
        payload["message"] = message
    return jsonify(payload)


def _json_error(message: str, status_code: int = 400):
    return jsonify({"ok": False, "error": message}), status_code


def _fetch_courses(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """SELECT id, COALESCE(lms_id, '') AS lms_id, name
           FROM courses
           ORDER BY name COLLATE NOCASE"""
    ).fetchall()
    return [dict(row) for row in rows]


def _resolve_student_course_id(
    conn: sqlite3.Connection,
    student_id: int,
    fallback_course_id: int,
) -> int | None:
    if fallback_course_id > 0:
        exists = conn.execute(
            """SELECT 1
               FROM enrollments
               WHERE student_id = ? AND course_id = ?""",
            (student_id, fallback_course_id),
        ).fetchone()
        if exists:
            return fallback_course_id

    row = conn.execute(
        """SELECT course_id
           FROM enrollments
           WHERE student_id = ?
           ORDER BY enrolled_at DESC
           LIMIT 1""",
        (student_id,),
    ).fetchone()
    if row:
        return _safe_int(row["course_id"])

    row = conn.execute(
        """SELECT a.course_id
           FROM submissions sub
           JOIN assignments a ON a.id = sub.assignment_id
           WHERE sub.student_id = ?
           ORDER BY sub.updated_at DESC
           LIMIT 1""",
        (student_id,),
    ).fetchone()
    if row:
        return _safe_int(row["course_id"])

    if fallback_course_id > 0:
        return fallback_course_id
    return None

def _rebuild_summary(conn: sqlite3.Connection, student_id: int, course_id: int) -> None:
    row = conn.execute(
        """
        WITH course_assignments AS (
          SELECT
            a.id AS assignment_id,
            COALESCE(
              a.max_score,
              (
                SELECT MAX(s2.score_max)
                FROM submissions s2
                WHERE s2.assignment_id = a.id
                  AND s2.score_max IS NOT NULL
              ),
              0
            ) AS possible_points
          FROM assignments a
          WHERE a.course_id = ?
        ),
        student_rows AS (
          SELECT
            ca.assignment_id,
            COALESCE(sub.score_points, 0) AS earned_points,
            ca.possible_points             AS possible_points,
            sub.status                     AS status,
            sub.score_points               AS score_points,
            sub.score_pct                  AS score_pct
          FROM course_assignments ca
          LEFT JOIN submissions sub
            ON sub.assignment_id = ca.assignment_id
           AND sub.student_id    = ?
        )
        SELECT
          COUNT(*) AS total_assigned,
          SUM(
            CASE
              WHEN status IS NOT NULL
               AND status != 'Missing'
               AND score_points IS NOT NULL
               AND score_points != 0
              THEN 1
              ELSE 0
            END
          ) AS total_submitted,
          SUM(
            CASE
              WHEN status IS NULL
                OR status = 'Missing'
                OR score_points = 0
                OR (
                     status IN ('Submitted', 'Late', 'Graded')
                     AND score_points IS NULL
                   )
              THEN 1
              ELSE 0
            END
          ) AS total_missing,
          SUM(
            CASE
              WHEN status = 'Late'
               AND score_points IS NOT NULL
               AND score_points != 0
              THEN 1
              ELSE 0
            END
          ) AS total_late,
          SUM(
            CASE
              WHEN score_pct IS NOT NULL
               AND score_points IS NOT NULL
               AND score_points != 0
              THEN 1
              ELSE 0
            END
          ) AS total_graded,
          ROUND(
            AVG(
              CASE
                WHEN score_pct IS NOT NULL
                 AND score_points IS NOT NULL
                 AND score_points != 0
                THEN score_pct
              END
            ),
            2
          ) AS avg_submitted_pct,
          ROUND(
            SUM(earned_points) * 100.0 /
            NULLIF(SUM(possible_points), 0), 2
          ) AS avg_all_pct,
          SUM(earned_points) AS points_earned,
          SUM(possible_points) AS points_possible
        FROM student_rows
        """,
        (course_id, student_id),
    ).fetchone()

    total_assigned = _safe_int(row["total_assigned"] if row else 0)
    total_submitted = _safe_int(row["total_submitted"] if row else 0)
    total_missing = _safe_int(row["total_missing"] if row else 0)
    total_late = _safe_int(row["total_late"] if row else 0)
    total_graded = _safe_int(row["total_graded"] if row else 0)
    avg_submitted_pct = _safe_float(row["avg_submitted_pct"] if row else 0.0)
    avg_all_pct = _safe_float(row["avg_all_pct"] if row else 0.0)
    points_earned = _safe_float(row["points_earned"] if row else 0.0)
    points_possible = _safe_float(row["points_possible"] if row else 0.0)

    conn.execute(
        """
        INSERT INTO course_summaries (
            student_id, course_id, total_assigned, total_submitted, total_missing,
            total_late, total_graded, avg_submitted_pct, avg_all_pct,
            points_earned, points_possible, needs_rebuild, last_synced
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, datetime('now'))
        ON CONFLICT(student_id, course_id) DO UPDATE SET
            total_assigned = excluded.total_assigned,
            total_submitted = excluded.total_submitted,
            total_missing = excluded.total_missing,
            total_late = excluded.total_late,
            total_graded = excluded.total_graded,
            avg_submitted_pct = excluded.avg_submitted_pct,
            avg_all_pct = excluded.avg_all_pct,
            points_earned = excluded.points_earned,
            points_possible = excluded.points_possible,
            needs_rebuild = 0,
            last_synced = excluded.last_synced
        """,
        (
            student_id,
            course_id,
            total_assigned,
            total_submitted,
            total_missing,
            total_late,
            total_graded,
            avg_submitted_pct,
            avg_all_pct,
            points_earned,
            points_possible,
        ),
    )


def _rebuild_all_summaries(conn: sqlite3.Connection, course_id: int = 0) -> int:
    if course_id > 0:
        rows = conn.execute(
            """
            SELECT DISTINCT e.student_id, e.course_id
            FROM enrollments e
            WHERE e.course_id = ?
            UNION
            SELECT DISTINCT sub.student_id, a.course_id
            FROM submissions sub
            JOIN assignments a ON a.id = sub.assignment_id
            WHERE a.course_id = ?
            ORDER BY student_id, course_id
            """,
            (course_id, course_id),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT DISTINCT e.student_id, e.course_id
            FROM enrollments e
            UNION
            SELECT DISTINCT sub.student_id, a.course_id
            FROM submissions sub
            JOIN assignments a ON a.id = sub.assignment_id
            ORDER BY student_id, course_id
            """
        ).fetchall()

    rebuilt = 0
    for row in rows:
        sid = _safe_int(row["student_id"])
        cid = _safe_int(row["course_id"])
        if sid > 0 and cid > 0:
            _rebuild_summary(conn, sid, cid)
            rebuilt += 1
    return rebuilt


def _fetch_overview(
    conn: sqlite3.Connection,
    course_id: int,
    at_risk_threshold: int,
) -> dict[str, Any]:
    course_filter = ""
    params: list[Any] = []
    if course_id > 0:
        course_filter = " AND a.course_id = ?"
        params.append(course_id)

    if course_id > 0:
        total_students = _safe_int(
            conn.execute(
                """SELECT COUNT(DISTINCT student_id)
                   FROM enrollments
                   WHERE course_id = ?""",
                (course_id,),
            ).fetchone()[0]
        )
        registered_students = _safe_int(
            conn.execute(
                """SELECT COUNT(DISTINCT e.student_id)
                   FROM enrollments e
                   JOIN students s ON s.id = e.student_id
                   WHERE e.course_id = ?
                     AND s.telegram_id IS NOT NULL""",
                (course_id,),
            ).fetchone()[0]
        )
        assignments_count = _safe_int(
            conn.execute(
                "SELECT COUNT(*) FROM assignments WHERE course_id = ?",
                (course_id,),
            ).fetchone()[0]
        )
    else:
        total_students = _safe_int(conn.execute("SELECT COUNT(*) FROM students").fetchone()[0])
        registered_students = _safe_int(
            conn.execute(
                "SELECT COUNT(*) FROM students WHERE telegram_id IS NOT NULL"
            ).fetchone()[0]
        )
        assignments_count = _safe_int(conn.execute("SELECT COUNT(*) FROM assignments").fetchone()[0])

    submissions_count = _safe_int(
        conn.execute(
            """SELECT COUNT(*)
               FROM submissions sub
               JOIN assignments a ON a.id = sub.assignment_id
               WHERE 1 = 1"""
            + course_filter,
            tuple(params),
        ).fetchone()[0]
    )

    missing_count = _safe_int(
        conn.execute(
            """SELECT COUNT(*)
               FROM submissions sub
               JOIN assignments a ON a.id = sub.assignment_id
               WHERE (
                      sub.status = 'Missing'
                      OR sub.score_points = 0
                      OR (
                           sub.status IN ('Submitted', 'Late', 'Graded')
                           AND sub.score_points IS NULL
                         )
                    )"""
            + course_filter,
            tuple(params),
        ).fetchone()[0]
    )

    pending_reports = _safe_int(
        conn.execute(
            """SELECT COUNT(*)
               FROM submissions sub
               JOIN assignments a ON a.id = sub.assignment_id
               WHERE sub.flagged_by_student = 1
                 AND sub.flag_verified = 0"""
            + course_filter,
            tuple(params),
        ).fetchone()[0]
    )

    if course_id > 0:
        at_risk_count = _safe_int(
            conn.execute(
                """SELECT COUNT(*)
                   FROM course_summaries
                   WHERE course_id = ? AND total_missing >= ?""",
                (course_id, at_risk_threshold),
            ).fetchone()[0]
        )
        avg_overall = _safe_float(
            conn.execute(
                """SELECT AVG(avg_all_pct)
                   FROM course_summaries
                   WHERE course_id = ? AND avg_all_pct IS NOT NULL""",
                (course_id,),
            ).fetchone()[0]
        )
    else:
        at_risk_count = _safe_int(
            conn.execute(
                "SELECT COUNT(*) FROM course_summaries WHERE total_missing >= ?",
                (at_risk_threshold,),
            ).fetchone()[0]
        )
        avg_overall = _safe_float(
            conn.execute(
                "SELECT AVG(avg_all_pct) FROM course_summaries WHERE avg_all_pct IS NOT NULL"
            ).fetchone()[0]
        )

    status_rows = conn.execute(
        """SELECT
             CASE
               WHEN sub.score_points = 0 THEN 'Missing'
               WHEN sub.status IN ('Submitted', 'Late', 'Graded')
                AND sub.score_points IS NULL THEN 'Missing'
               ELSE sub.status
             END AS status,
             COUNT(*) AS total
           FROM submissions sub
           JOIN assignments a ON a.id = sub.assignment_id
           WHERE 1 = 1"""
        + course_filter
        + """ GROUP BY
               CASE
                 WHEN sub.score_points = 0 THEN 'Missing'
                 WHEN sub.status IN ('Submitted', 'Late', 'Graded')
                  AND sub.score_points IS NULL THEN 'Missing'
                 ELSE sub.status
               END
             ORDER BY total DESC""",
        tuple(params),
    ).fetchall()
    status_breakdown = [
        {"status": str(row["status"]), "total": _safe_int(row["total"])} for row in status_rows
    ]

    top_missing = conn.execute(
        """SELECT
               a.id AS assignment_id,
               a.title,
               c.name AS course_name,
               COUNT(*) AS missing_count
           FROM submissions sub
           JOIN assignments a ON a.id = sub.assignment_id
           JOIN courses c ON c.id = a.course_id
           WHERE (
                  sub.status = 'Missing'
                  OR sub.score_points = 0
                  OR (
                       sub.status IN ('Submitted', 'Late', 'Graded')
                       AND sub.score_points IS NULL
                     )
                )"""
        + (" AND a.course_id = ?" if course_id > 0 else "")
        + " GROUP BY a.id, a.title, c.name ORDER BY missing_count DESC, a.title LIMIT 10",
        (course_id,) if course_id > 0 else (),
    ).fetchall()

    top_missing_assignments = [
        {
            "assignment_id": _safe_int(row["assignment_id"]),
            "title": str(row["title"]),
            "course_name": str(row["course_name"]),
            "missing_count": _safe_int(row["missing_count"]),
        }
        for row in top_missing
    ]

    latest_sync = conn.execute(
        """SELECT synced_at, source, rows_added, rows_updated, notes
           FROM sync_log
           WHERE (? = 0) OR (course_id = ? OR course_id IS NULL)
           ORDER BY synced_at DESC
           LIMIT 1""",
        (course_id, course_id),
    ).fetchone()

    completion_rate = 0.0
    if submissions_count > 0:
        completion_rate = max(0.0, min(100.0, (submissions_count - missing_count) * 100.0 / submissions_count))

    return {
        "totals": {
            "students": total_students,
            "registered": registered_students,
            "assignments": assignments_count,
            "submissions": submissions_count,
            "missing": missing_count,
            "pending_reports": pending_reports,
            "at_risk": at_risk_count,
            "avg_overall": round(avg_overall, 2),
            "completion_rate": round(completion_rate, 2),
        },
        "status_breakdown": status_breakdown,
        "top_missing_assignments": top_missing_assignments,
        "latest_sync": dict(latest_sync) if latest_sync else None,
    }

def _fetch_students(
    conn: sqlite3.Connection,
    course_id: int,
    search: str,
    limit: int,
) -> list[dict[str, Any]]:
    search = search.strip().lower()
    pattern = f"%{search}%"

    rows = conn.execute(
        """
        WITH latest_enrollment AS (
            SELECT e.student_id, e.course_id, e.enrolled_at
            FROM enrollments e
            JOIN (
                SELECT student_id, MAX(enrolled_at) AS max_enrolled_at
                FROM enrollments
                GROUP BY student_id
            ) x
              ON x.student_id = e.student_id
             AND x.max_enrolled_at = e.enrolled_at
        ),
        agg AS (
            SELECT
                sub.student_id,
                a.course_id,
                COUNT(*) AS total_assigned,
                SUM(
                    CASE
                      WHEN sub.status IS NOT NULL
                       AND sub.status != 'Missing'
                       AND sub.score_points IS NOT NULL
                       AND sub.score_points != 0
                      THEN 1
                      ELSE 0
                    END
                ) AS total_submitted,
                SUM(
                    CASE
                      WHEN sub.status = 'Missing'
                        OR sub.score_points = 0
                        OR (
                             sub.status IN ('Submitted', 'Late', 'Graded')
                             AND sub.score_points IS NULL
                           )
                      THEN 1
                      ELSE 0
                    END
                ) AS total_missing,
                ROUND(
                    SUM(COALESCE(sub.score_points, 0)) * 100.0 /
                    NULLIF(SUM(COALESCE(sub.score_max, 0)), 0),
                    2
                ) AS avg_all_pct,
                MAX(sub.updated_at) AS agg_synced
            FROM submissions sub
            JOIN assignments a ON a.id = sub.assignment_id
            GROUP BY sub.student_id, a.course_id
        )
        SELECT
            s.id,
            s.lms_id,
            s.full_name,
            COALESCE(s.telegram_id, '') AS telegram_id,
            COALESCE(s.telegram_username, '') AS telegram_username,
            COALESCE(le.course_id, 0) AS course_id,
            COALESCE(c.name, '') AS course_name,
            COALESCE(cs.total_assigned, agg.total_assigned, 0) AS total_assigned,
            COALESCE(cs.total_submitted, agg.total_submitted, 0) AS total_submitted,
            COALESCE(cs.total_missing, agg.total_missing, 0) AS total_missing,
            COALESCE(cs.avg_all_pct, agg.avg_all_pct, 0) AS avg_all_pct,
            COALESCE(cs.last_synced, agg.agg_synced, '') AS last_synced
        FROM students s
        LEFT JOIN latest_enrollment le
               ON le.student_id = s.id
        LEFT JOIN courses c
               ON c.id = le.course_id
        LEFT JOIN course_summaries cs
               ON cs.student_id = s.id
              AND cs.course_id = le.course_id
        LEFT JOIN agg
               ON agg.student_id = s.id
              AND agg.course_id = le.course_id
        WHERE ((? = '')
           OR LOWER(s.full_name) LIKE ?
           OR LOWER(s.lms_id) LIKE ?
           OR LOWER(COALESCE(s.telegram_id, '')) LIKE ?)
          AND (? = 0 OR le.course_id = ?)
        ORDER BY s.full_name COLLATE NOCASE
        LIMIT ?
        """,
        (search, pattern, pattern, pattern, course_id, course_id, limit),
    ).fetchall()

    data: list[dict[str, Any]] = []
    for row in rows:
        total_assigned = _safe_int(row["total_assigned"])
        total_submitted = _safe_int(row["total_submitted"])
        completion_pct = round((total_submitted * 100.0 / total_assigned), 1) if total_assigned else 0.0
        item = dict(row)
        item["total_assigned"] = total_assigned
        item["total_submitted"] = total_submitted
        item["total_missing"] = _safe_int(row["total_missing"])
        item["avg_all_pct"] = round(_safe_float(row["avg_all_pct"]), 2)
        item["completion_pct"] = completion_pct
        data.append(item)
    return data


def _fetch_student_detail(
    conn: sqlite3.Connection,
    student_id: int,
    course_id: int,
    limit: int,
) -> dict[str, Any] | None:
    student = conn.execute(
        """SELECT id, lms_id, full_name,
                  COALESCE(telegram_id, '') AS telegram_id,
                  COALESCE(telegram_username, '') AS telegram_username
           FROM students
           WHERE id = ?""",
        (student_id,),
    ).fetchone()
    if not student:
        return None

    resolved_course_id = _resolve_student_course_id(conn, student_id, course_id)
    if not resolved_course_id:
        resolved_course_id = 0

    course_name = ""
    if resolved_course_id > 0:
        course_row = conn.execute(
            "SELECT name FROM courses WHERE id = ?",
            (resolved_course_id,),
        ).fetchone()
        if course_row:
            course_name = str(course_row["name"])

    summary = None
    if resolved_course_id > 0:
        _rebuild_summary(conn, student_id, resolved_course_id)
        summary_row = conn.execute(
            """SELECT total_assigned, total_submitted, total_missing, total_late,
                      avg_all_pct, points_earned, points_possible, last_synced
               FROM course_summaries
               WHERE student_id = ? AND course_id = ?""",
            (student_id, resolved_course_id),
        ).fetchone()
        summary = dict(summary_row) if summary_row else None

    work: list[dict[str, Any]] = []
    if resolved_course_id > 0:
        rows = conn.execute(
            """SELECT
                     a.id AS assignment_id,
                     a.title,
                     a.due_date,
                     CASE
                       WHEN sub.score_points = 0 THEN 'Missing'
                       WHEN sub.status IN ('Submitted', 'Late', 'Graded')
                        AND sub.score_points IS NULL THEN 'Missing'
                       ELSE COALESCE(sub.status, 'Missing')
                     END AS status,
                     sub.score_points,
                     COALESCE(sub.score_max, a.max_score, 0) AS score_max,
                     sub.score_pct,
                     COALESCE(sub.flagged_by_student, 0) AS flagged_by_student,
                     COALESCE(sub.flag_verified, 0) AS flag_verified,
                     COALESCE(sub.proof_uploaded_at, '') AS proof_uploaded_at
                 FROM assignments a
                 LEFT JOIN submissions sub
                        ON sub.assignment_id = a.id
                       AND sub.student_id = ?
                 WHERE a.course_id = ?
                 ORDER BY COALESCE(a.due_date, a.created_at) DESC, a.created_at DESC
                 LIMIT ?""",
            (student_id, resolved_course_id, limit),
        ).fetchall()
        work = [dict(row) for row in rows]

    return {
        "student": dict(student),
        "course_id": resolved_course_id,
        "course_name": course_name,
        "summary": summary,
        "work": work,
    }


def _fetch_pending_reports(conn: sqlite3.Connection, course_id: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        """SELECT
                 s.id AS student_id,
                 s.full_name,
                 COALESCE(s.telegram_id, '') AS telegram_id,
                 a.id AS assignment_id,
                 a.title AS assignment_title,
                 c.name AS course_name,
                 COALESCE(sub.flagged_at, '') AS flagged_at,
                 COALESCE(sub.flag_note, '') AS flag_note,
                 COALESCE(sub.proof_file_id, '') AS proof_file_id,
                 COALESCE(sub.proof_file_type, '') AS proof_file_type,
                 COALESCE(sub.proof_caption, '') AS proof_caption,
                 COALESCE(sub.proof_uploaded_at, '') AS proof_uploaded_at
               FROM submissions sub
               JOIN students s ON s.id = sub.student_id
               JOIN assignments a ON a.id = sub.assignment_id
               JOIN courses c ON c.id = a.course_id
               WHERE sub.flagged_by_student = 1
                 AND sub.flag_verified = 0
                 AND (? = 0 OR a.course_id = ?)
               ORDER BY sub.flagged_at ASC""",
        (course_id, course_id),
    ).fetchall()
    return [dict(row) for row in rows]


def _fetch_at_risk(
    conn: sqlite3.Connection,
    course_id: int,
    threshold: int,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """SELECT
                 s.id AS student_id,
                 s.full_name,
                 COALESCE(s.telegram_id, '') AS telegram_id,
                 c.name AS course_name,
                 cs.total_missing,
                 cs.total_assigned,
                 cs.total_submitted,
                 cs.avg_all_pct,
                 cs.avg_submitted_pct,
                 cs.points_earned,
                 cs.points_possible,
                 cs.last_synced
               FROM course_summaries cs
               JOIN students s ON s.id = cs.student_id
               JOIN courses c ON c.id = cs.course_id
               WHERE cs.total_missing >= ?
                 AND (? = 0 OR cs.course_id = ?)
               ORDER BY cs.total_missing DESC, cs.avg_all_pct ASC, s.full_name COLLATE NOCASE""",
        (threshold, course_id, course_id),
    ).fetchall()
    return [dict(row) for row in rows]


def _fetch_campaign_jobs(conn: sqlite3.Connection, limit: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        """SELECT id, template_key, template_text, run_at, schedule_label,
                  status, target_count, sent_count, created_by,
                  created_at, started_at, finished_at, error
           FROM campaign_jobs
           ORDER BY id DESC
           LIMIT ?""",
        (limit,),
    ).fetchall()
    return [dict(row) for row in rows]


def _fetch_due_campaign_jobs(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """SELECT id, template_key, template_text, run_at, schedule_label
           FROM campaign_jobs
           WHERE status = 'pending'
             AND datetime(run_at) <= datetime('now', 'localtime')
           ORDER BY datetime(run_at) ASC, id ASC"""
    ).fetchall()
    return [dict(row) for row in rows]


def _claim_campaign_job(conn: sqlite3.Connection, job_id: int) -> bool:
    result = conn.execute(
        """UPDATE campaign_jobs
           SET status = 'running',
               started_at = datetime('now'),
               error = NULL
           WHERE id = ?
             AND status = 'pending'""",
        (job_id,),
    )
    return result.rowcount > 0


def _complete_campaign_job(
    conn: sqlite3.Connection,
    job_id: int,
    target_count: int,
    sent_count: int,
) -> None:
    conn.execute(
        """UPDATE campaign_jobs
           SET status = 'completed',
               target_count = ?,
               sent_count = ?,
               finished_at = datetime('now'),
               error = NULL
           WHERE id = ?""",
        (target_count, sent_count, job_id),
    )


def _fail_campaign_job(conn: sqlite3.Connection, job_id: int, error: str) -> None:
    conn.execute(
        """UPDATE campaign_jobs
           SET status = 'failed',
               finished_at = datetime('now'),
               error = ?
           WHERE id = ?""",
        (error[:500], job_id),
    )


def _fetch_campaign_targets(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    rows = conn.execute(
        """SELECT
               s.id AS student_id,
               s.full_name,
               s.telegram_id,
               a.title AS assignment_title
           FROM students s
           JOIN submissions sub ON sub.student_id = s.id
           JOIN assignments a ON a.id = sub.assignment_id
           WHERE s.telegram_id IS NOT NULL
             AND (
                    sub.status = 'Missing'
                    OR sub.score_points = 0
                    OR (
                         sub.status IN ('Submitted', 'Late', 'Graded')
                         AND sub.score_points IS NULL
                       )
                  )
           ORDER BY s.id, COALESCE(a.due_date, a.created_at), a.created_at"""
    ).fetchall()

    grouped: dict[int, dict[str, Any]] = {}
    for row in rows:
        student_id = _safe_int(row["student_id"])
        if student_id not in grouped:
            grouped[student_id] = {
                "student_id": student_id,
                "full_name": str(row["full_name"]),
                "telegram_id": str(row["telegram_id"]),
                "missing_titles": [],
            }
        titles: list[str] = grouped[student_id]["missing_titles"]
        if len(titles) < 12:
            titles.append(str(row["assignment_title"]))

    return list(grouped.values())


def _render_campaign_message(template: str, student: dict[str, Any]) -> str:
    first_name = (student.get("full_name") or "Learner").split()[0]
    missing_titles = student.get("missing_titles") or []
    missing_list = "\n".join(f"- {title}" for title in missing_titles) or "- none"

    try:
        text = template.format(
            first_name=first_name,
            full_name=student.get("full_name", "Learner"),
            missing_count=len(missing_titles),
            missing_list=missing_list,
        )
    except Exception:
        text = (
            f"{first_name}, you currently have {len(missing_titles)} missing assignment(s):\n\n"
            f"{missing_list}\n\n"
            "Open /start for details."
        )
    return text[:3900]


def _telegram_send_message(chat_id: str, text: str) -> None:
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is not configured.")

    payload = urllib.parse.urlencode(
        {
            "chat_id": str(chat_id),
            "text": text,
            "disable_web_page_preview": "true",
        }
    ).encode("utf-8")
    req = urllib.request.Request(
        f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
        data=payload,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )

    with urllib.request.urlopen(req, timeout=20) as response:
        data = json.loads(response.read().decode("utf-8"))
    if not data.get("ok"):
        raise RuntimeError(data.get("description", "Telegram API sendMessage failed"))


def _process_campaign_job(job_id: int) -> dict[str, Any]:
    result: dict[str, Any] = {
        "job_id": job_id,
        "processed": False,
        "target_count": 0,
        "sent_count": 0,
        "send_errors": 0,
        "error": None,
    }
    if not BOT_TOKEN:
        result["error"] = "BOT_TOKEN is missing."
        return result

    with db_conn() as conn:
        claimed = _claim_campaign_job(conn, job_id)
    if not claimed:
        result["error"] = "Job is not pending or already claimed."
        return result

    result["processed"] = True

    with db_conn() as conn:
        row = conn.execute(
            """SELECT id, template_key, template_text
               FROM campaign_jobs
               WHERE id = ?""",
            (job_id,),
        ).fetchone()
    if not row:
        with db_conn() as conn:
            _fail_campaign_job(conn, job_id, "Campaign job row not found after claim.")
        result["error"] = "Campaign job row not found."
        return result

    template_key = str(row["template_key"] or "gentle")
    template_text = str(row["template_text"] or "").strip()
    template = (
        template_text
        if template_key == "custom"
        else CAMPAIGN_TEMPLATES.get(template_key, CAMPAIGN_TEMPLATES["gentle"])
    )

    try:
        with db_conn() as conn:
            targets = _fetch_campaign_targets(conn)

        target_count = len(targets)
        sent_count = 0
        send_errors: list[str] = []
        for student in targets:
            try:
                message = _render_campaign_message(template, student)
                _telegram_send_message(str(student["telegram_id"]), message)
                sent_count += 1
            except Exception as exc:
                send_errors.append(str(exc))

        with db_conn() as conn:
            _complete_campaign_job(conn, job_id, target_count, sent_count)
            if send_errors:
                conn.execute(
                    """UPDATE campaign_jobs
                       SET error = ?
                       WHERE id = ?""",
                    (f"{len(send_errors)} send error(s).", job_id),
                )

        result["target_count"] = target_count
        result["sent_count"] = sent_count
        result["send_errors"] = len(send_errors)
        if send_errors:
            result["error"] = f"{len(send_errors)} send error(s)."
        return result
    except Exception as exc:
        with db_conn() as conn:
            _fail_campaign_job(conn, job_id, str(exc))
        result["error"] = str(exc)
        return result


def _process_due_campaign_jobs_once(dry_run: bool = False) -> dict[str, int]:
    stats = {
        "due_jobs": 0,
        "processed_jobs": 0,
        "messages_targeted": 0,
        "messages_sent": 0,
    }
    if not BOT_TOKEN:
        return stats

    with db_conn() as conn:
        due_jobs = _fetch_due_campaign_jobs(conn)
    stats["due_jobs"] = len(due_jobs)
    if not due_jobs:
        return stats

    if dry_run:
        with db_conn() as conn:
            targets = _fetch_campaign_targets(conn)
        stats["processed_jobs"] = len(due_jobs)
        stats["messages_targeted"] = len(targets) * len(due_jobs)
        return stats

    for job in due_jobs:
        job_id = _safe_int(job.get("id"))
        if job_id <= 0:
            continue
        job_result = _process_campaign_job(job_id)
        if not job_result.get("processed"):
            continue
        stats["processed_jobs"] += 1
        stats["messages_targeted"] += _safe_int(job_result.get("target_count"), 0)
        stats["messages_sent"] += _safe_int(job_result.get("sent_count"), 0)

    return stats


def _campaign_worker_loop() -> None:
    while True:
        try:
            stats = _process_due_campaign_jobs_once()
            if stats["processed_jobs"] > 0:
                print(
                    "Campaign worker:",
                    f"processed={stats['processed_jobs']},",
                    f"sent={stats['messages_sent']}/{stats['messages_targeted']}",
                )
        except Exception as exc:
            print(f"Campaign worker error: {exc}")
        time.sleep(CAMPAIGN_POLL_INTERVAL_SEC)


def _ensure_campaign_worker() -> None:
    global _campaign_worker_started
    if _campaign_worker_started:
        return

    # In debug reloader mode, start the thread only in the active child process.
    if app.debug and os.environ.get("WERKZEUG_RUN_MAIN") != "true":
        return

    with _campaign_worker_lock:
        if _campaign_worker_started:
            return
        if not BOT_TOKEN:
            print("Campaign worker disabled: BOT_TOKEN is missing.")
            _campaign_worker_started = True
            return

        thread = threading.Thread(
            target=_campaign_worker_loop,
            name="campaign-worker",
            daemon=True,
        )
        thread.start()
        _campaign_worker_started = True
        print(f"Campaign worker started (poll={CAMPAIGN_POLL_INTERVAL_SEC}s)")


def _fetch_sync_log(
    conn: sqlite3.Connection,
    course_id: int,
    limit: int,
) -> list[dict[str, Any]]:
    rows = conn.execute(
        """SELECT synced_at, source, rows_added, rows_updated,
                  COALESCE(notes, '') AS notes,
                  COALESCE(course_id, 0) AS course_id
           FROM sync_log
           WHERE (? = 0 OR course_id = ? OR course_id IS NULL)
           ORDER BY synced_at DESC
           LIMIT ?""",
        (course_id, course_id, limit),
    ).fetchall()
    return [dict(row) for row in rows]

def _resolve_schedule(schedule_key: str, run_at_raw: str | None) -> tuple[str, str]:
    now = datetime.now()
    if schedule_key == "30m":
        run_at = now + timedelta(minutes=30)
        return run_at.strftime("%Y-%m-%d %H:%M:%S"), SCHEDULE_OPTIONS["30m"]
    if schedule_key == "2h":
        run_at = now + timedelta(hours=2)
        return run_at.strftime("%Y-%m-%d %H:%M:%S"), SCHEDULE_OPTIONS["2h"]
    if schedule_key == "tomorrow_0700":
        run_at = (now + timedelta(days=1)).replace(hour=7, minute=0, second=0, microsecond=0)
        return run_at.strftime("%Y-%m-%d %H:%M:%S"), SCHEDULE_OPTIONS["tomorrow_0700"]
    if schedule_key == "custom" and run_at_raw:
        text = run_at_raw.strip().replace("T", " ")
        try:
            custom_dt = datetime.fromisoformat(text)
        except ValueError:
            custom_dt = now
        return custom_dt.strftime("%Y-%m-%d %H:%M:%S"), "Custom"

    return now.strftime("%Y-%m-%d %H:%M:%S"), SCHEDULE_OPTIONS["now"]


def _create_backup_file() -> Path:
    backup_dir = BASE_DIR / "database" / "backups"
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = backup_dir / f"class_{stamp}.db"
    shutil.copy2(DB_PATH, backup_file)
    return backup_file


def _init_schema(conn: sqlite3.Connection) -> None:
    schema_path = BASE_DIR / "database" / "schema.sql"
    sql_text = schema_path.read_text(encoding="utf-8")
    conn.executescript(sql_text)


def _csv_response(filename: str, headers: list[str], rows: list[list[Any]]) -> Response:
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(headers)
    writer.writerows(rows)
    data = buf.getvalue()
    return Response(
        data,
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


def _telegram_fetch_file(file_id: str) -> tuple[bytes, str, str]:
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is not configured. Cannot fetch Telegram proof files.")

    get_file_url = (
        f"https://api.telegram.org/bot{BOT_TOKEN}/getFile?"
        + urllib.parse.urlencode({"file_id": file_id})
    )
    with urllib.request.urlopen(get_file_url, timeout=20) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if not payload.get("ok"):
        raise RuntimeError("Telegram getFile request failed")

    file_path = payload.get("result", {}).get("file_path")
    if not file_path:
        raise RuntimeError("Telegram file path is missing")

    file_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_path}"
    with urllib.request.urlopen(file_url, timeout=30) as response:
        content = response.read()
        content_type = response.headers.get("Content-Type", "application/octet-stream")

    filename = Path(file_path).name or "proof.bin"
    return content, content_type, filename


@app.route("/")
def index():
    return render_template(
        "index.html",
        db_path=str(DB_PATH),
        now=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )


@app.before_request
def _start_background_workers():
    _ensure_campaign_worker()


@app.route("/api/bootstrap")
def api_bootstrap():
    with db_conn() as conn:
        data = {
            "courses": _fetch_courses(conn),
            "schedule_options": [
                {"key": key, "label": label} for key, label in SCHEDULE_OPTIONS.items()
            ]
            + [{"key": "custom", "label": "Custom date/time"}],
            "campaign_templates": [
                {"key": key, "label": key.capitalize(), "text": text}
                for key, text in CAMPAIGN_TEMPLATES.items()
            ]
            + [{"key": "custom", "label": "Custom", "text": ""}],
            "defaults": {
                "course_id": 0,
                "at_risk_threshold": DEFAULT_AT_RISK_THRESHOLD,
            },
            "db_path": str(DB_PATH),
            "campaign_sender_enabled": bool(BOT_TOKEN),
            "campaign_poll_seconds": CAMPAIGN_POLL_INTERVAL_SEC,
        }
    return _json_ok(data)


@app.route("/api/overview")
def api_overview():
    course_id = _safe_int(request.args.get("course_id"), 0)
    threshold = max(1, _safe_int(request.args.get("threshold"), DEFAULT_AT_RISK_THRESHOLD))
    with db_conn() as conn:
        data = _fetch_overview(conn, course_id, threshold)
    return _json_ok(data)


@app.route("/api/students")
def api_students():
    course_id = _safe_int(request.args.get("course_id"), 0)
    search = request.args.get("search", "")
    limit = max(1, min(1000, _safe_int(request.args.get("limit"), 300)))

    with db_conn() as conn:
        rows = _fetch_students(conn, course_id, search, limit)
    return _json_ok(rows)


@app.route("/api/students/<int:student_id>")
def api_student_detail(student_id: int):
    course_id = _safe_int(request.args.get("course_id"), 0)
    limit = max(10, min(500, _safe_int(request.args.get("limit"), 200)))

    with db_conn() as conn:
        data = _fetch_student_detail(conn, student_id, course_id, limit)
    if not data:
        return _json_error("Student not found", 404)
    return _json_ok(data)


@app.route("/api/students/<int:student_id>/unlink", methods=["POST"])
def api_student_unlink(student_id: int):
    with db_conn() as conn:
        result = conn.execute(
            """UPDATE students
               SET telegram_id = NULL,
                   telegram_username = NULL
               WHERE id = ?
                 AND telegram_id IS NOT NULL""",
            (student_id,),
        )

    if result.rowcount == 0:
        return _json_error("No linked Telegram account found for this learner", 404)
    return _json_ok({"student_id": student_id}, "Telegram account unlinked")


@app.route("/api/students/<int:student_id>/rebuild-summary", methods=["POST"])
def api_student_rebuild_summary(student_id: int):
    body = request.get_json(silent=True) or {}
    course_id = _safe_int(body.get("course_id"), 0)

    with db_conn() as conn:
        resolved_course_id = _resolve_student_course_id(conn, student_id, course_id)
        if not resolved_course_id:
            return _json_error("Course could not be resolved for this learner", 404)
        _rebuild_summary(conn, student_id, resolved_course_id)

    return _json_ok(
        {
            "student_id": student_id,
            "course_id": resolved_course_id,
        },
        "Summary rebuilt",
    )

@app.route("/api/pending-reports")
def api_pending_reports():
    course_id = _safe_int(request.args.get("course_id"), 0)
    with db_conn() as conn:
        rows = _fetch_pending_reports(conn, course_id)
    return _json_ok(rows)


@app.route("/api/reports/verify", methods=["POST"])
def api_verify_report():
    body = request.get_json(silent=True) or {}
    student_id = _safe_int(body.get("student_id"), 0)
    assignment_id = _safe_int(body.get("assignment_id"), 0)
    approved = _as_bool(body.get("approved"))
    reviewer = str(body.get("reviewer") or "Web Dashboard").strip()
    if not reviewer:
        reviewer = "Web Dashboard"

    if student_id <= 0 or assignment_id <= 0:
        return _json_error("student_id and assignment_id are required", 400)

    new_status = "Submitted" if approved else "Missing"

    with db_conn() as conn:
        result = conn.execute(
            """UPDATE submissions
               SET status = ?,
                   flag_verified = 1,
                   flag_verified_at = datetime('now'),
                   flag_verified_by = ?,
                   flagged_by_student = 0
               WHERE student_id = ?
                 AND assignment_id = ?
                 AND flagged_by_student = 1
                 AND flag_verified = 0""",
            (new_status, reviewer, student_id, assignment_id),
        )
        if result.rowcount == 0:
            return _json_error("Report already processed or no longer eligible", 409)

        course_row = conn.execute(
            "SELECT course_id FROM assignments WHERE id = ?",
            (assignment_id,),
        ).fetchone()
        if course_row:
            _rebuild_summary(conn, student_id, _safe_int(course_row["course_id"]))

    return _json_ok(
        {
            "student_id": student_id,
            "assignment_id": assignment_id,
            "new_status": new_status,
        },
        "Report reviewed",
    )


@app.route("/api/at-risk")
def api_at_risk():
    course_id = _safe_int(request.args.get("course_id"), 0)
    threshold = max(1, _safe_int(request.args.get("threshold"), DEFAULT_AT_RISK_THRESHOLD))

    with db_conn() as conn:
        rows = _fetch_at_risk(conn, course_id, threshold)
    return _json_ok(rows)


@app.route("/api/campaign-jobs")
def api_campaign_jobs():
    limit = max(1, min(200, _safe_int(request.args.get("limit"), 40)))
    with db_conn() as conn:
        rows = _fetch_campaign_jobs(conn, limit)
    return _json_ok(rows)


@app.route("/api/campaign-jobs", methods=["POST"])
def api_campaign_create():
    body = request.get_json(silent=True) or {}

    template_key = str(body.get("template_key") or "gentle").strip().lower()
    if template_key not in CAMPAIGN_TEMPLATES and template_key != "custom":
        return _json_error("Invalid template key", 400)

    schedule_key = str(body.get("schedule_key") or "now").strip().lower()
    run_at_raw = body.get("run_at")
    run_at, schedule_label = _resolve_schedule(schedule_key, run_at_raw)
    if schedule_key == "now" and not BOT_TOKEN:
        return _json_error("BOT_TOKEN is missing. Cannot send immediately.", 400)

    created_by = str(body.get("created_by") or "web_dashboard").strip() or "web_dashboard"
    template_text = None
    if template_key == "custom":
        template_text = str(body.get("template_text") or "").strip()
        if not template_text:
            return _json_error("Custom template text is required", 400)

    with db_conn() as conn:
        cursor = conn.execute(
            """INSERT INTO campaign_jobs
                 (created_by, template_key, template_text, run_at, schedule_label, status)
               VALUES (?, ?, ?, ?, ?, 'pending')""",
            (created_by, template_key, template_text, run_at, schedule_label),
        )
        job_id = _safe_int(cursor.lastrowid)

    immediate_result = None
    message = "Campaign scheduled"
    if schedule_key == "now":
        immediate_result = _process_campaign_job(job_id)
        target_count = _safe_int(immediate_result.get("target_count"), 0)
        sent_count = _safe_int(immediate_result.get("sent_count"), 0)
        message = f"Campaign sent now: {sent_count}/{target_count} delivered."
        if immediate_result.get("error"):
            message += f" {immediate_result['error']}"

    return _json_ok(
        {
            "job_id": job_id,
            "template_key": template_key,
            "run_at": run_at,
            "schedule_label": schedule_label,
            "immediate_result": immediate_result,
        },
        message,
    )


@app.route("/api/campaign-jobs/run-due", methods=["POST"])
def api_campaign_run_due():
    if not BOT_TOKEN:
        return _json_error("BOT_TOKEN is missing. Campaign sending is disabled.", 400)

    body = request.get_json(silent=True) or {}
    dry_run = _as_bool(body.get("dry_run")) if body else False
    stats = _process_due_campaign_jobs_once(dry_run=dry_run)
    return _json_ok(
        stats,
        (
            (
                f"Dry run only: {stats['processed_jobs']} due job(s), "
                f"{stats['messages_targeted']} message target(s)."
            )
            if dry_run
            else (
                f"Processed {stats['processed_jobs']} due job(s); "
                f"sent {stats['messages_sent']}/{stats['messages_targeted']} message(s)."
            )
        ),
    )


@app.route("/api/sync-log")
def api_sync_log():
    course_id = _safe_int(request.args.get("course_id"), 0)
    limit = max(1, min(500, _safe_int(request.args.get("limit"), 150)))
    with db_conn() as conn:
        rows = _fetch_sync_log(conn, course_id, limit)
    return _json_ok(rows)


@app.route("/api/maintenance/rebuild-summaries", methods=["POST"])
def api_maintenance_rebuild_summaries():
    body = request.get_json(silent=True) or {}
    course_id = _safe_int(body.get("course_id"), 0)

    with db_conn() as conn:
        rebuilt = _rebuild_all_summaries(conn, course_id)
        conn.execute(
            """INSERT INTO sync_log (course_id, source, rows_added, rows_updated, notes)
               VALUES (?, 'web_dashboard_rebuild', 0, 0, ?)""",
            (
                course_id if course_id > 0 else None,
                f"Rebuilt summaries for {rebuilt} student-course pair(s)",
            ),
        )

    return _json_ok({"rebuilt": rebuilt}, "Summaries rebuilt")


@app.route("/api/maintenance/init-schema", methods=["POST"])
def api_maintenance_init_schema():
    with db_conn() as conn:
        _init_schema(conn)
        conn.execute(
            """INSERT INTO sync_log (course_id, source, rows_added, rows_updated, notes)
               VALUES (NULL, 'web_dashboard_schema_init', 0, 0, 'Schema initialized from schema.sql')"""
        )

    return _json_ok(message="Schema initialized")


@app.route("/api/maintenance/backup", methods=["POST"])
def api_maintenance_backup():
    if not DB_PATH.exists():
        return _json_error(f"Database not found at {DB_PATH}", 404)

    backup_file = _create_backup_file()
    with db_conn() as conn:
        conn.execute(
            """INSERT INTO sync_log (course_id, source, rows_added, rows_updated, notes)
               VALUES (NULL, 'web_dashboard_backup', 0, 0, ?)""",
            (f"Backup created: {backup_file.name}",),
        )

    return _json_ok({"backup_file": str(backup_file)}, "Backup created")


@app.route("/api/proof/<int:student_id>/<int:assignment_id>")
def api_proof(student_id: int, assignment_id: int):
    with db_conn() as conn:
        row = conn.execute(
            """SELECT proof_file_id, proof_file_type
               FROM submissions
               WHERE student_id = ? AND assignment_id = ?""",
            (student_id, assignment_id),
        ).fetchone()

    if not row or not row["proof_file_id"]:
        return _json_error("No proof file available", 404)

    file_id = str(row["proof_file_id"])
    try:
        content, content_type, filename = _telegram_fetch_file(file_id)
    except Exception as exc:
        return _json_error(f"Could not load proof file: {exc}", 502)

    as_attachment = request.args.get("download") == "1"
    return send_file(
        io.BytesIO(content),
        mimetype=content_type,
        download_name=filename,
        as_attachment=as_attachment,
    )


@app.route("/api/export/students.csv")
def api_export_students_csv():
    course_id = _safe_int(request.args.get("course_id"), 0)
    search = request.args.get("search", "")

    with db_conn() as conn:
        rows = _fetch_students(conn, course_id, search, 5000)

    csv_rows = [
        [
            row.get("id", ""),
            row.get("lms_id", ""),
            row.get("full_name", ""),
            row.get("course_name", ""),
            row.get("telegram_id", ""),
            row.get("total_assigned", 0),
            row.get("total_submitted", 0),
            row.get("total_missing", 0),
            row.get("avg_all_pct", 0),
            row.get("completion_pct", 0),
            row.get("last_synced", ""),
        ]
        for row in rows
    ]

    return _csv_response(
        "learners.csv",
        [
            "Student ID",
            "LMS ID",
            "Full Name",
            "Course",
            "Telegram ID",
            "Total Assigned",
            "Total Submitted",
            "Total Missing",
            "Average Overall %",
            "Completion %",
            "Last Synced",
        ],
        csv_rows,
    )


@app.route("/api/export/reports.csv")
def api_export_reports_csv():
    course_id = _safe_int(request.args.get("course_id"), 0)

    with db_conn() as conn:
        rows = _fetch_pending_reports(conn, course_id)

    csv_rows = [
        [
            row.get("student_id", ""),
            row.get("full_name", ""),
            row.get("assignment_id", ""),
            row.get("assignment_title", ""),
            row.get("course_name", ""),
            row.get("flagged_at", ""),
            row.get("proof_file_type", ""),
            row.get("proof_uploaded_at", ""),
            row.get("flag_note", ""),
            row.get("proof_caption", ""),
        ]
        for row in rows
    ]

    return _csv_response(
        "pending_reports.csv",
        [
            "Student ID",
            "Student",
            "Assignment ID",
            "Assignment",
            "Course",
            "Reported At",
            "Proof Type",
            "Proof Uploaded At",
            "Note",
            "Proof Caption",
        ],
        csv_rows,
    )


def run():
    _ensure_campaign_worker()
    app.run(host="127.0.0.1", port=8787, debug=True)


if __name__ == "__main__":
    run()
