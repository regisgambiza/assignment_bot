import asyncio
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from config import DB_PATH

# ── Connection ────────────────────────────────────────────

@contextmanager
def get_db():
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

def init_db():
    """Create all tables from schema.sql"""
    schema = Path(__file__).parent / "schema.sql"
    with get_db() as conn:
        _run_migrations(conn)
        conn.executescript(schema.read_text())
        _run_migrations(conn)
    _run_one_time_summary_backfill()
    print("Database initialized")


def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(row["name"] == column for row in rows)


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone()
    return row is not None


def _run_migrations(conn: sqlite3.Connection) -> None:
    # Backfill proof columns for existing databases.
    if _table_exists(conn, "submissions"):
        if not _column_exists(conn, "submissions", "proof_file_id"):
            conn.execute("ALTER TABLE submissions ADD COLUMN proof_file_id TEXT")
        if not _column_exists(conn, "submissions", "proof_file_type"):
            conn.execute("ALTER TABLE submissions ADD COLUMN proof_file_type TEXT")
        if not _column_exists(conn, "submissions", "proof_caption"):
            conn.execute("ALTER TABLE submissions ADD COLUMN proof_caption TEXT")
        if not _column_exists(conn, "submissions", "proof_uploaded_at"):
            conn.execute("ALTER TABLE submissions ADD COLUMN proof_uploaded_at TEXT")

    if _table_exists(conn, "course_summaries"):
        if not _column_exists(conn, "course_summaries", "needs_rebuild"):
            conn.execute(
                "ALTER TABLE course_summaries ADD COLUMN needs_rebuild INTEGER DEFAULT 1"
            )
        conn.execute(
            "UPDATE course_summaries SET needs_rebuild = 1 WHERE needs_rebuild IS NULL"
        )
    conn.execute(
        """CREATE TABLE IF NOT EXISTS app_meta (
               key        TEXT PRIMARY KEY,
               value      TEXT,
               updated_at TEXT DEFAULT (datetime('now'))
           )"""
    )


def _run_one_time_summary_backfill() -> None:
    marker_key = "summary_backfill_v3_done"
    with get_db() as conn:
        row = conn.execute(
            "SELECT value FROM app_meta WHERE key = ?",
            (marker_key,),
        ).fetchone()
    if row:
        return

    rebuilt = rebuild_all_summaries()
    with get_db() as conn:
        conn.execute(
            """INSERT INTO app_meta (key, value, updated_at)
               VALUES (?, ?, datetime('now'))
               ON CONFLICT(key) DO UPDATE SET
                 value = excluded.value,
                 updated_at = excluded.updated_at""",
            (marker_key, str(rebuilt)),
        )
    print(f"Summary backfill completed: {rebuilt} student-course rows rebuilt.")


def _summary_needs_refresh(
    conn: sqlite3.Connection,
    student_id: int,
    course_id: int,
    summary_row: sqlite3.Row | None,
) -> bool:
    if not summary_row:
        return True
    if int(summary_row["needs_rebuild"] or 0) == 1:
        return True

    last_synced = summary_row["last_synced"] or "1970-01-01 00:00:00"

    max_submission = conn.execute(
        """SELECT MAX(sub.updated_at) AS max_updated
           FROM submissions sub
           JOIN assignments a ON a.id = sub.assignment_id
           WHERE sub.student_id = ? AND a.course_id = ?""",
        (student_id, course_id),
    ).fetchone()
    if max_submission and max_submission["max_updated"]:
        if str(max_submission["max_updated"]) > str(last_synced):
            return True

    max_assignment = conn.execute(
        """SELECT MAX(created_at) AS max_created
           FROM assignments
           WHERE course_id = ?""",
        (course_id,),
    ).fetchone()
    if max_assignment and max_assignment["max_created"]:
        if str(max_assignment["max_created"]) > str(last_synced):
            return True

    return False

# ── Students ──────────────────────────────────────────────

def get_student_by_telegram(telegram_id: str) -> dict | None:
    with get_db() as conn:
        row = conn.execute(
            "SELECT * FROM students WHERE telegram_id = ?",
            (str(telegram_id),)
        ).fetchone()
        return dict(row) if row else None

def find_students_by_id(lms_id: str) -> list[dict]:
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM students WHERE lms_id = ?",
            (lms_id.strip(),)
        ).fetchall()
        return [dict(r) for r in rows]

def find_students_by_name(name: str) -> list[dict]:
    with get_db() as conn:
        pattern = f"%{name.strip()}%"
        rows = conn.execute(
            """SELECT * FROM students
               WHERE LOWER(full_name) LIKE LOWER(?)
               ORDER BY full_name""",
            (pattern,)
        ).fetchall()
        return [dict(r) for r in rows]

def find_student(query: str) -> list[dict]:
    """Try ID first, fall back to name search"""
    if query.strip().isdigit():
        results = find_students_by_id(query.strip())
        if results:
            return results
    return find_students_by_name(query)

def link_student(lms_id: str, telegram_id: str,
                 telegram_username: str = None) -> bool:
    with get_db() as conn:
        result = conn.execute(
            """UPDATE students
               SET telegram_id = ?, telegram_username = ?
               WHERE lms_id = ? AND telegram_id IS NULL""",
            (str(telegram_id), telegram_username, lms_id)
        )
        return result.rowcount > 0

# ── Submissions ───────────────────────────────────────────

def get_missing_work(student_id: int, limit: int | None = None) -> list[dict]:
    with get_db() as conn:
        sql = """SELECT a.title, a.due_date, a.id AS assignment_id,
                        sub.flagged_by_student
                 FROM   submissions sub
                 JOIN   assignments a ON a.id = sub.assignment_id
                 WHERE  sub.student_id = ?
                   AND  (
                          sub.status = 'Missing'
                          OR sub.score_points = 0
                          OR (
                               sub.status IN ('Submitted', 'Late', 'Graded')
                               AND sub.score_points IS NULL
                             )
                        )
                 ORDER  BY a.created_at ASC"""
        params: tuple = (student_id,)
        if limit is not None:
            sql += " LIMIT ?"
            params = (student_id, int(limit))
        rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]

def get_grades(student_id: int, limit: int | None = None) -> list[dict]:
    with get_db() as conn:
        sql = """SELECT a.title, a.due_date, a.id AS assignment_id,
                        sub.status, sub.score_raw, sub.score_pct
                 FROM   submissions sub
                 JOIN   assignments a ON a.id = sub.assignment_id
                 WHERE  sub.student_id = ?
                 ORDER  BY a.created_at DESC"""
        params: tuple = (student_id,)
        if limit is not None:
            sql += " LIMIT ?"
            params = (student_id, int(limit))
        rows = conn.execute(sql, params).fetchall()
        return [dict(r) for r in rows]


def get_submitted_work(student_id: int) -> list[dict]:
    with get_db() as conn:
        rows = conn.execute(
            """SELECT a.title,
                      a.due_date,
                      a.id AS assignment_id,
                      sub.status,
                      sub.score_raw,
                      sub.score_pct
               FROM   submissions sub
               JOIN   assignments a ON a.id = sub.assignment_id
               WHERE  sub.student_id = ?
                 AND  sub.status IN ('Submitted', 'Late', 'Graded')
                 AND  sub.score_points IS NOT NULL
                 AND  sub.score_points != 0
               ORDER  BY COALESCE(a.due_date, a.created_at) DESC, a.created_at DESC""",
            (student_id,)
        ).fetchall()
        return [dict(r) for r in rows]


def get_student_work_filtered(
    student_id: int,
    title_contains: str | None = None,
    due_from: str | None = None,
    due_to: str | None = None,
    limit: int = 50,
) -> list[dict]:
    with get_db() as conn:
        sql = """
            SELECT
                a.id AS assignment_id,
                a.title,
                a.due_date,
                CASE
                    WHEN sub.score_points = 0 THEN 'Missing'
                    WHEN sub.status IN ('Submitted', 'Late', 'Graded')
                      AND sub.score_points IS NULL THEN 'Missing'
                    ELSE COALESCE(sub.status, 'Missing')
                END AS status,
                sub.score_raw,
                sub.score_pct
            FROM assignments a
            JOIN enrollments e
              ON e.course_id = a.course_id
             AND e.student_id = ?
            LEFT JOIN submissions sub
              ON sub.assignment_id = a.id
             AND sub.student_id = ?
            WHERE 1 = 1
        """
        params: list = [student_id, student_id]

        if title_contains:
            sql += " AND LOWER(a.title) LIKE LOWER(?)"
            params.append(f"%{title_contains.strip()}%")

        if due_from:
            sql += " AND date(a.due_date) >= date(?)"
            params.append(due_from)

        if due_to:
            sql += " AND date(a.due_date) <= date(?)"
            params.append(due_to)

        sql += (
            " ORDER BY COALESCE(a.due_date, a.created_at) ASC, a.created_at ASC"
            " LIMIT ?"
        )
        params.append(int(limit))

        rows = conn.execute(sql, tuple(params)).fetchall()
        return [dict(r) for r in rows]


def get_projection_snapshot(
    student_id: int, course_id: int | None = None
) -> dict | None:
    with get_db() as conn:
        resolved_course_id = course_id
        if resolved_course_id is None:
            enrollment = conn.execute(
                """SELECT course_id
                   FROM enrollments
                   WHERE student_id = ?
                   ORDER BY enrolled_at DESC
                   LIMIT 1""",
                (student_id,)
            ).fetchone()
            if not enrollment:
                return None
            resolved_course_id = enrollment["course_id"]

        row = conn.execute(
            """WITH course_assignments AS (
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
                     ca.possible_points,
                     COALESCE(sub.score_points, 0) AS earned_points,
                     CASE
                       WHEN sub.score_points = 0 THEN 'Missing'
                       WHEN sub.status IN ('Submitted', 'Late', 'Graded')
                         AND sub.score_points IS NULL THEN 'Missing'
                       ELSE COALESCE(sub.status, 'Missing')
                     END AS status
                   FROM course_assignments ca
                   LEFT JOIN submissions sub
                     ON sub.assignment_id = ca.assignment_id
                    AND sub.student_id    = ?
                 )
                 SELECT
                   COUNT(*) AS total_assignments,
                   SUM(earned_points) AS earned_points,
                   SUM(possible_points) AS total_possible_points,
                   SUM(
                     CASE
                       WHEN status = 'Missing' THEN possible_points
                       ELSE 0
                     END
                   ) AS remaining_possible_points,
                   SUM(
                     CASE
                       WHEN status = 'Missing' THEN 1
                       ELSE 0
                     END
                   ) AS remaining_assignments
                 FROM student_rows""",
            (resolved_course_id, student_id)
        ).fetchone()

        if not row:
            return None
        data = dict(row)
        data["course_id"] = resolved_course_id
        return data

def get_student_course_id(student_id: int) -> int | None:
    with get_db() as conn:
        row = conn.execute(
            """SELECT course_id
               FROM enrollments
               WHERE student_id = ?
               ORDER BY enrolled_at DESC
               LIMIT 1""",
            (student_id,)
        ).fetchone()
        return int(row["course_id"]) if row else None


def get_student_course_name(student_id: int) -> str | None:
    with get_db() as conn:
        row = conn.execute(
            """SELECT c.name AS course_name
               FROM enrollments e
               JOIN courses c ON c.id = e.course_id
               WHERE e.student_id = ?
               ORDER BY e.enrolled_at DESC
               LIMIT 1""",
            (student_id,),
        ).fetchone()
        return str(row["course_name"]) if row and row["course_name"] else None


def get_summary(student_id: int, course_id: int | None = None) -> dict | None:
    resolved_course_id = course_id
    with get_db() as conn:
        if resolved_course_id is None:
            enrollment = conn.execute(
                """SELECT course_id
                   FROM enrollments
                   WHERE student_id = ?
                   ORDER BY enrolled_at DESC
                   LIMIT 1""",
                (student_id,),
            ).fetchone()
            if not enrollment:
                return None
            resolved_course_id = enrollment["course_id"]

        row = conn.execute(
            """SELECT * FROM course_summaries
               WHERE student_id = ? AND course_id = ?""",
            (student_id, resolved_course_id),
        ).fetchone()
        needs_refresh = _summary_needs_refresh(
            conn, student_id, int(resolved_course_id), row
        )

    if needs_refresh:
        rebuilt = rebuild_summary(student_id, int(resolved_course_id))
        if not rebuilt:
            return None
        with get_db() as conn:
            row = conn.execute(
                """SELECT * FROM course_summaries
                   WHERE student_id = ? AND course_id = ?""",
                (student_id, resolved_course_id),
            ).fetchone()

    return dict(row) if row else None


# ── Flagging ──────────────────────────────────────────────

def flag_submission(student_id: int, assignment_id: int) -> bool:
    with get_db() as conn:
        result = conn.execute(
            """UPDATE submissions
               SET flagged_by_student = 1,
                   flagged_at         = datetime('now'),
                   proof_file_id      = NULL,
                   proof_file_type    = NULL,
                   proof_caption      = NULL,
                   proof_uploaded_at  = NULL
               WHERE student_id   = ?
               AND   assignment_id = ?
               AND   (
                       status = 'Missing'
                       OR score_points = 0
                       OR (
                            status IN ('Submitted', 'Late', 'Graded')
                            AND score_points IS NULL
                          )
                     )""",
            (student_id, assignment_id)
        )
        return result.rowcount > 0


def add_submission_proof(
    student_id: int,
    assignment_id: int,
    file_id: str,
    file_type: str,
    caption: str | None = None,
) -> bool:
    with get_db() as conn:
        result = conn.execute(
            """UPDATE submissions
               SET proof_file_id     = ?,
                   proof_file_type   = ?,
                   proof_caption     = ?,
                   proof_uploaded_at = datetime('now'),
                   updated_at        = datetime('now')
               WHERE student_id          = ?
                 AND assignment_id       = ?
                 AND flagged_by_student  = 1
                 AND flag_verified       = 0""",
            (file_id, file_type, caption, student_id, assignment_id)
        )
        return result.rowcount > 0


def get_submission_evidence(student_id: int, assignment_id: int) -> dict | None:
    with get_db() as conn:
        row = conn.execute(
            """SELECT proof_file_id, proof_file_type,
                      proof_caption, proof_uploaded_at
               FROM submissions
               WHERE student_id = ? AND assignment_id = ?""",
            (student_id, assignment_id)
        ).fetchone()
        return dict(row) if row else None

def verify_flag(student_id: int, assignment_id: int,
                approved: bool, teacher: str) -> bool:
    new_status = "Submitted" if approved else "Missing"
    updated = False
    course_id = None

    with get_db() as conn:
        result = conn.execute(
            """UPDATE submissions
               SET status             = ?,
                   flag_verified      = 1,
                   flag_verified_at   = datetime('now'),
                   flag_verified_by   = ?,
                   flagged_by_student = 0
               WHERE student_id   = ?
               AND   assignment_id = ?""",
            (new_status, teacher, student_id, assignment_id)
        )
        updated = result.rowcount > 0
        if updated:
            row = conn.execute(
                "SELECT course_id FROM assignments WHERE id = ?",
                (assignment_id,)
            ).fetchone()
            course_id = row["course_id"] if row else None

    if updated and course_id is not None:
        rebuild_summary(student_id, int(course_id))

    return updated

# ── Teacher tools ─────────────────────────────────────────

def get_at_risk_students() -> list[dict]:
    with get_db() as conn:
        rows = conn.execute("SELECT * FROM v_at_risk_students").fetchall()
        return [dict(r) for r in rows]

def get_pending_flags() -> list[dict]:
    with get_db() as conn:
        rows = conn.execute(
            """SELECT
                 s.full_name,
                 s.telegram_id,
                 s.id AS student_id,
                 a.title AS assignment_title,
                 a.id AS assignment_id,
                 c.name AS course_name,
                 sub.flagged_at,
                 sub.flag_note,
                 sub.proof_file_id,
                 sub.proof_file_type,
                 sub.proof_caption,
                 sub.proof_uploaded_at
               FROM submissions sub
               JOIN students    s ON s.id = sub.student_id
               JOIN assignments a ON a.id = sub.assignment_id
               JOIN courses     c ON c.id = a.course_id
               WHERE sub.flagged_by_student = 1
                 AND sub.flag_verified      = 0
               ORDER BY sub.flagged_at ASC"""
        ).fetchall()
        return [dict(r) for r in rows]

def get_all_students_with_telegram() -> list[dict]:
    """All registered students — for broadcast"""
    with get_db() as conn:
        rows = conn.execute(
            """SELECT s.*, cs.total_missing
               FROM students s
               LEFT JOIN enrollments e
                      ON e.student_id = s.id
               LEFT JOIN course_summaries cs
                      ON cs.student_id = s.id
                     AND cs.course_id  = e.course_id
               WHERE s.telegram_id IS NOT NULL"""
        ).fetchall()
        return [dict(r) for r in rows]


def create_campaign_job(
    created_by: str,
    template_key: str,
    run_at: str,
    schedule_label: str,
    template_text: str | None = None,
) -> int:
    with get_db() as conn:
        cursor = conn.execute(
            """INSERT INTO campaign_jobs
                 (created_by, template_key, template_text, run_at, schedule_label, status)
               VALUES (?, ?, ?, ?, ?, 'pending')""",
            (created_by, template_key, template_text, run_at, schedule_label)
        )
        return int(cursor.lastrowid)


def get_due_campaign_jobs(now_ts: str | None = None) -> list[dict]:
    with get_db() as conn:
        rows = conn.execute(
            """SELECT *
               FROM campaign_jobs
               WHERE status = 'pending'
                 AND datetime(run_at) <= datetime(COALESCE(?, datetime('now')))
               ORDER BY datetime(run_at) ASC, id ASC""",
            (now_ts,)
        ).fetchall()
        return [dict(r) for r in rows]


def claim_campaign_job(job_id: int) -> bool:
    with get_db() as conn:
        result = conn.execute(
            """UPDATE campaign_jobs
               SET status = 'running',
                   started_at = datetime('now')
               WHERE id = ?
                 AND status = 'pending'""",
            (job_id,)
        )
        return result.rowcount > 0


def complete_campaign_job(job_id: int, target_count: int, sent_count: int) -> None:
    with get_db() as conn:
        conn.execute(
            """UPDATE campaign_jobs
               SET status = 'completed',
                   target_count = ?,
                   sent_count = ?,
                   finished_at = datetime('now'),
                   error = NULL
               WHERE id = ?""",
            (target_count, sent_count, job_id)
        )


def fail_campaign_job(job_id: int, error: str) -> None:
    with get_db() as conn:
        conn.execute(
            """UPDATE campaign_jobs
               SET status = 'failed',
                   finished_at = datetime('now'),
                   error = ?
               WHERE id = ?""",
            (error[:500], job_id)
        )


def list_campaign_jobs(limit: int = 20) -> list[dict]:
    with get_db() as conn:
        rows = conn.execute(
            """SELECT id, template_key, schedule_label, run_at,
                      status, target_count, sent_count, created_at,
                      finished_at, error
               FROM campaign_jobs
               ORDER BY id DESC
               LIMIT ?""",
            (int(limit),)
        ).fetchall()
        return [dict(r) for r in rows]


def rebuild_all_summaries() -> int:
    with get_db() as conn:
        rows = conn.execute(
            """SELECT DISTINCT student_id, course_id
               FROM enrollments
               UNION
               SELECT DISTINCT sub.student_id, a.course_id
               FROM submissions sub
               JOIN assignments a ON a.id = sub.assignment_id
               ORDER BY student_id, course_id"""
        ).fetchall()

    rebuilt = 0
    for row in rows:
        if rebuild_summary(int(row["student_id"]), int(row["course_id"])):
            rebuilt += 1
    return rebuilt


def rebuild_dirty_summaries(limit: int = 200) -> int:
    with get_db() as conn:
        rows = conn.execute(
            """SELECT student_id, course_id
               FROM (
                 SELECT e.student_id, e.course_id
                 FROM enrollments e
                 LEFT JOIN course_summaries cs
                   ON cs.student_id = e.student_id
                  AND cs.course_id  = e.course_id
                 WHERE cs.id IS NULL OR cs.needs_rebuild = 1

                 UNION

                 SELECT sub.student_id, a.course_id
                 FROM submissions sub
                 JOIN assignments a ON a.id = sub.assignment_id
                 LEFT JOIN course_summaries cs
                   ON cs.student_id = sub.student_id
                  AND cs.course_id  = a.course_id
                 WHERE cs.id IS NULL OR cs.needs_rebuild = 1
               )
               ORDER BY student_id, course_id
               LIMIT ?""",
            (int(limit),),
        ).fetchall()

    rebuilt = 0
    for row in rows:
        if rebuild_summary(int(row["student_id"]), int(row["course_id"])):
            rebuilt += 1
    return rebuilt


async def summary_repair_worker(interval_sec: int = 300, batch_size: int = 200):
    print(
        "Summary repair worker started - interval:",
        f"{interval_sec}s, batch_size={batch_size}",
    )
    while True:
        try:
            rebuilt = rebuild_dirty_summaries(batch_size)
            if rebuilt:
                print(f"Summary repair worker rebuilt {rebuilt} row(s).")
        except Exception as exc:
            print(f"Summary repair worker error: {exc}")
        await asyncio.sleep(interval_sec)

def rebuild_summary(student_id: int, course_id: int | None = None) -> bool:
    """Recompute course_summaries for one student"""
    with get_db() as conn:
        resolved_course_id = course_id
        if resolved_course_id is None:
            enrollment = conn.execute(
                """SELECT course_id
                   FROM enrollments
                   WHERE student_id = ?
                   ORDER BY enrolled_at DESC
                   LIMIT 1""",
                (student_id,)
            ).fetchone()
            if not enrollment:
                return False
            resolved_course_id = enrollment["course_id"]

        row = conn.execute(
            """WITH course_assignments AS (
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
                     SUM(earned_points) * 100.0 / NULLIF(SUM(possible_points), 0),
                     2
                   ) AS avg_all_pct,
                   SUM(earned_points)   AS points_earned,
                   SUM(possible_points) AS points_possible
                 FROM student_rows""",
            (resolved_course_id, student_id)
        ).fetchone()

        conn.execute(
            """INSERT INTO course_summaries
                 (student_id, course_id, total_assigned, total_submitted,
                  total_missing, total_late, total_graded,
                  avg_submitted_pct, avg_all_pct,
                  points_earned, points_possible, needs_rebuild, last_synced)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,0,datetime('now'))
               ON CONFLICT(student_id, course_id) DO UPDATE SET
                 total_assigned    = excluded.total_assigned,
                 total_submitted   = excluded.total_submitted,
                 total_missing     = excluded.total_missing,
                 total_late        = excluded.total_late,
                 total_graded      = excluded.total_graded,
                 avg_submitted_pct = excluded.avg_submitted_pct,
                 avg_all_pct       = excluded.avg_all_pct,
                 points_earned     = excluded.points_earned,
                 points_possible   = excluded.points_possible,
                 needs_rebuild     = 0,
                 last_synced       = excluded.last_synced""",
            (student_id, resolved_course_id,
             row["total_assigned"], row["total_submitted"],
             row["total_missing"],  row["total_late"],
             row["total_graded"],   row["avg_submitted_pct"],
             row["avg_all_pct"],    row["points_earned"],
             row["points_possible"])
        )
        return True


