import sqlite3
from contextlib import contextmanager
from pathlib import Path
from config import DB_PATH

# â”€â”€ Connection â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

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
        conn.executescript(schema.read_text())
    print("Database initialized")

# â”€â”€ Students â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

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

# â”€â”€ Submissions â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def get_missing_work(student_id: int) -> list[dict]:
    with get_db() as conn:
        rows = conn.execute(
            """SELECT a.title, a.due_date, a.id AS assignment_id,
                      sub.flagged_by_student
               FROM   submissions sub
               JOIN   assignments a ON a.id = sub.assignment_id
               WHERE  sub.student_id = ? AND sub.status = 'Missing'
               ORDER  BY a.created_at ASC""",
            (student_id,)
        ).fetchall()
        return [dict(r) for r in rows]

def get_grades(student_id: int) -> list[dict]:
    with get_db() as conn:
        rows = conn.execute(
            """SELECT a.title, a.due_date, a.id AS assignment_id,
                      sub.status, sub.score_raw, sub.score_pct
               FROM   submissions sub
               JOIN   assignments a ON a.id = sub.assignment_id
               WHERE  sub.student_id = ?
               ORDER  BY a.created_at DESC""",
            (student_id,)
        ).fetchall()
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
               ORDER  BY COALESCE(a.due_date, a.created_at) DESC, a.created_at DESC""",
            (student_id,)
        ).fetchall()
        return [dict(r) for r in rows]

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


def get_summary(student_id: int, course_id: int | None = None) -> dict | None:
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
            """SELECT * FROM course_summaries
               WHERE student_id = ? AND course_id = ?""",
            (student_id, resolved_course_id)
        ).fetchone()
        return dict(row) if row else None


# â”€â”€ Flagging â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def flag_submission(student_id: int, assignment_id: int) -> bool:
    with get_db() as conn:
        result = conn.execute(
            """UPDATE submissions
               SET flagged_by_student = 1,
                   flagged_at         = datetime('now')
               WHERE student_id   = ?
               AND   assignment_id = ?
               AND   status       = 'Missing'""",
            (student_id, assignment_id)
        )
        return result.rowcount > 0

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

# â”€â”€ Teacher tools â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def get_at_risk_students() -> list[dict]:
    with get_db() as conn:
        rows = conn.execute("SELECT * FROM v_at_risk_students").fetchall()
        return [dict(r) for r in rows]

def get_pending_flags() -> list[dict]:
    with get_db() as conn:
        rows = conn.execute("SELECT * FROM v_pending_flags").fetchall()
        return [dict(r) for r in rows]

def get_all_students_with_telegram() -> list[dict]:
    """All registered students â€” for broadcast"""
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
                       WHEN status IS NOT NULL AND status != 'Missing' THEN 1
                       ELSE 0
                     END
                   ) AS total_submitted,
                   SUM(
                     CASE
                       WHEN status IS NULL OR status = 'Missing' THEN 1
                       ELSE 0
                     END
                   ) AS total_missing,
                   SUM(CASE WHEN status = 'Late' THEN 1 ELSE 0 END) AS total_late,
                   SUM(CASE WHEN score_pct IS NOT NULL THEN 1 ELSE 0 END) AS total_graded,
                   ROUND(
                     AVG(CASE WHEN score_pct IS NOT NULL THEN score_pct END),
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
                  points_earned, points_possible, last_synced)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,datetime('now'))
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
                 last_synced       = excluded.last_synced""",
            (student_id, resolved_course_id,
             row["total_assigned"], row["total_submitted"],
             row["total_missing"],  row["total_late"],
             row["total_graded"],   row["avg_submitted_pct"],
             row["avg_all_pct"],    row["points_earned"],
             row["points_possible"])
        )
        return True

