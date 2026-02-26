import logging
import sqlite3
from pathlib import Path
from typing import Dict, Optional


logger = logging.getLogger("analysis_db_sync")

ALLOWED_STATUSES = {"Missing", "Submitted", "Late", "Graded", "Flagged"}


def _upsert_school(conn: sqlite3.Connection, school_name: str) -> int:
    row = conn.execute("SELECT id FROM schools WHERE name = ?", (school_name,)).fetchone()
    if row:
        return int(row["id"])
    conn.execute("INSERT INTO schools (name) VALUES (?)", (school_name,))
    row = conn.execute("SELECT id FROM schools WHERE name = ?", (school_name,)).fetchone()
    if not row:
        raise RuntimeError(f"Failed to create school: {school_name}")
    logger.debug("Created school name=%s id=%s", school_name, row["id"])
    return int(row["id"])


def _upsert_course(
    conn: sqlite3.Connection,
    course_lms_id: str,
    course_name: str,
    school_id: int,
    stats: Dict[str, int],
) -> int:
    row = conn.execute(
        "SELECT id, name, school_id FROM courses WHERE lms_id = ?",
        (course_lms_id,),
    ).fetchone()
    if not row:
        conn.execute(
            "INSERT INTO courses (lms_id, name, school_id) VALUES (?, ?, ?)",
            (course_lms_id, course_name, school_id),
        )
        stats["courses_added"] += 1
        logger.debug("Inserted course lms_id=%s name=%s", course_lms_id, course_name)
        row = conn.execute(
            "SELECT id FROM courses WHERE lms_id = ?",
            (course_lms_id,),
        ).fetchone()
        if not row:
            raise RuntimeError(f"Failed to create course: {course_lms_id}")
        return int(row["id"])

    if row["name"] != course_name or row["school_id"] != school_id:
        conn.execute(
            "UPDATE courses SET name = ?, school_id = ? WHERE lms_id = ?",
            (course_name, school_id, course_lms_id),
        )
        stats["courses_updated"] += 1
        logger.debug("Updated course lms_id=%s", course_lms_id)

    return int(row["id"])


def _upsert_student(conn: sqlite3.Connection, lms_id: str, full_name: str, stats: Dict[str, int]) -> int:
    row = conn.execute(
        "SELECT id, full_name FROM students WHERE lms_id = ?",
        (lms_id,),
    ).fetchone()
    if not row:
        conn.execute(
            "INSERT INTO students (lms_id, full_name) VALUES (?, ?)",
            (lms_id, full_name),
        )
        stats["students_added"] += 1
        logger.debug("Inserted student lms_id=%s full_name=%s", lms_id, full_name)
        row = conn.execute("SELECT id FROM students WHERE lms_id = ?", (lms_id,)).fetchone()
        if not row:
            raise RuntimeError(f"Failed to create student: {lms_id}")
        return int(row["id"])

    if row["full_name"] != full_name:
        conn.execute(
            "UPDATE students SET full_name = ? WHERE lms_id = ?",
            (full_name, lms_id),
        )
        stats["students_updated"] += 1
        logger.debug("Updated student name lms_id=%s", lms_id)

    return int(row["id"])


def _ensure_enrollment(conn: sqlite3.Connection, student_id: int, course_id: int, stats: Dict[str, int]) -> None:
    row = conn.execute(
        "SELECT id FROM enrollments WHERE student_id = ? AND course_id = ?",
        (student_id, course_id),
    ).fetchone()
    if row:
        return
    conn.execute(
        "INSERT INTO enrollments (student_id, course_id) VALUES (?, ?)",
        (student_id, course_id),
    )
    stats["enrollments_added"] += 1
    logger.debug("Added enrollment student_id=%s course_id=%s", student_id, course_id)


def _build_assignment_map(student_analysis: Dict) -> Dict[str, Dict]:
    assignment_map: Dict[str, Dict] = {}
    for data in student_analysis.values():
        for cw in data.get("coursework", []):
            lms_id = str(cw["id"])
            title = cw.get("title") or lms_id
            created = cw.get("creationTime")
            max_points = cw.get("maxPoints")

            existing = assignment_map.get(lms_id)
            if not existing:
                assignment_map[lms_id] = {
                    "title": title,
                    "created_at": created,
                    "max_score": max_points,
                }
                continue

            if title and len(title) > len(existing.get("title", "")):
                existing["title"] = title
            if existing.get("created_at") in (None, "") and created:
                existing["created_at"] = created
            if existing.get("max_score") is None and max_points is not None:
                existing["max_score"] = max_points
    return assignment_map


def _upsert_assignments(
    conn: sqlite3.Connection,
    course_id: int,
    assignment_map: Dict[str, Dict],
    stats: Dict[str, int],
) -> Dict[str, int]:
    db_ids: Dict[str, int] = {}
    for lms_id, meta in assignment_map.items():
        title = meta.get("title") or lms_id
        created_at = meta.get("created_at") or "1970-01-01T00:00:00Z"
        max_score = meta.get("max_score")

        row = conn.execute(
            "SELECT id, title, max_score, course_id, created_at FROM assignments WHERE lms_id = ?",
            (lms_id,),
        ).fetchone()

        if not row:
            conn.execute(
                """
                INSERT INTO assignments (lms_id, course_id, title, max_score, created_at, is_active)
                VALUES (?, ?, ?, ?, ?, 1)
                """,
                (lms_id, course_id, title, max_score, created_at),
            )
            stats["assignments_added"] += 1
            logger.debug("Inserted assignment lms_id=%s title=%s", lms_id, title)
            row = conn.execute("SELECT id FROM assignments WHERE lms_id = ?", (lms_id,)).fetchone()
            if not row:
                raise RuntimeError(f"Failed to create assignment: {lms_id}")
            db_ids[lms_id] = int(row["id"])
            continue

        updates = {}
        if row["course_id"] != course_id:
            updates["course_id"] = course_id
        if title and len(title) > len(row["title"] or ""):
            updates["title"] = title
        if max_score is not None and row["max_score"] != max_score:
            updates["max_score"] = max_score
        if (row["created_at"] is None or row["created_at"] == "") and created_at:
            updates["created_at"] = created_at

        if updates:
            sets = ", ".join(f"{k} = ?" for k in updates)
            values = list(updates.values()) + [lms_id]
            conn.execute(f"UPDATE assignments SET {sets}, is_active = 1 WHERE lms_id = ?", values)
            stats["assignments_updated"] += 1
            logger.debug("Updated assignment lms_id=%s fields=%s", lms_id, ",".join(updates.keys()))
        else:
            conn.execute("UPDATE assignments SET is_active = 1 WHERE lms_id = ?", (lms_id,))

        db_ids[lms_id] = int(row["id"])

    return db_ids


def _delete_stale_assignments(
    conn: sqlite3.Connection,
    course_id: int,
    active_assignment_lms_ids: set[str],
    stats: Dict[str, int],
    *,
    start_date: str | None = None,
    end_date: str | None = None,
) -> int:
    where = ["course_id = ?"]
    params: list[object] = [course_id]

    # On partial syncs, only prune assignments within the selected window.
    if start_date:
        where.append("date(COALESCE(created_at, '1970-01-01')) >= date(?)")
        params.append(start_date)
    if end_date:
        where.append("date(COALESCE(created_at, '1970-01-01')) <= date(?)")
        params.append(end_date)

    if active_assignment_lms_ids:
        placeholders = ", ".join("?" for _ in active_assignment_lms_ids)
        where.append(f"lms_id NOT IN ({placeholders})")
        params.extend(sorted(active_assignment_lms_ids))

    rows = conn.execute(
        f"SELECT id FROM assignments WHERE {' AND '.join(where)}",
        tuple(params),
    ).fetchall()
    if not rows:
        return 0

    assignment_ids = [int(row["id"]) for row in rows]
    placeholders = ", ".join("?" for _ in assignment_ids)
    conn.execute(
        f"DELETE FROM assignments WHERE id IN ({placeholders})",
        tuple(assignment_ids),
    )
    stats["assignments_deleted"] += len(assignment_ids)
    logger.info(
        "Deleted %d stale assignment(s) for course_id=%s (start=%s, end=%s)",
        len(assignment_ids),
        course_id,
        start_date,
        end_date,
    )
    return len(assignment_ids)


def _compute_submission_status_and_score(cw: Dict) -> Dict[str, Optional[object]]:
    submission = cw.get("submission")
    max_points = cw.get("maxPoints")
    score_max_default = float(max_points) if (max_points is not None and max_points != 0) else None

    if not submission:
        return {
            "status": "Missing",
            "score_raw": None,
            "score_points": None,
            "score_max": score_max_default,
            "score_pct": None,
        }

    state = submission.get("state", "")
    if state in ["NEW", "CREATED"]:
        status = "Missing"
    elif submission.get("late", False):
        status = "Late"
    else:
        status = "Submitted"

    assigned_grade = submission.get("assignedGrade")
    if assigned_grade is None:
        return {
            "status": status,
            "score_raw": None,
            "score_points": None,
            "score_max": score_max_default,
            "score_pct": None,
        }

    if assigned_grade == 0:
        return {
            "status": "Missing",
            "score_raw": None,
            "score_points": None,
            "score_max": score_max_default,
            "score_pct": None,
        }

    score_points = float(assigned_grade)
    if max_points is None or max_points == 0:
        return {
            "status": status,
            "score_raw": str(assigned_grade),
            "score_points": score_points,
            "score_max": None,
            "score_pct": None,
        }

    score_max = float(max_points)
    score_pct = round((score_points / score_max) * 100, 2)
    return {
        "status": status,
        "score_raw": f"{assigned_grade}/{max_points}",
        "score_points": score_points,
        "score_max": score_max,
        "score_pct": score_pct,
    }


def _upsert_submission(
    conn: sqlite3.Connection,
    student_id: int,
    assignment_id: int,
    payload: Dict[str, Optional[object]],
    stats: Dict[str, int],
) -> None:
    status = payload["status"]
    if status not in ALLOWED_STATUSES:
        raise ValueError(f"Unsupported status: {status}")

    row = conn.execute(
        """
        SELECT id, status, score_raw, score_points, score_max, score_pct
        FROM submissions
        WHERE student_id = ? AND assignment_id = ?
        """,
        (student_id, assignment_id),
    ).fetchone()

    if not row:
        conn.execute(
            """
            INSERT INTO submissions
            (student_id, assignment_id, status, score_raw, score_points, score_max, score_pct)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                student_id,
                assignment_id,
                status,
                payload["score_raw"],
                payload["score_points"],
                payload["score_max"],
                payload["score_pct"],
            ),
        )
        stats["submissions_added"] += 1
        logger.debug(
            "Inserted submission student_id=%s assignment_id=%s status=%s",
            student_id,
            assignment_id,
            status,
        )
        return

    changed = (
        row["status"] != status
        or row["score_raw"] != payload["score_raw"]
        or row["score_points"] != payload["score_points"]
        or row["score_max"] != payload["score_max"]
        or row["score_pct"] != payload["score_pct"]
    )
    if not changed:
        return

    conn.execute(
        """
        UPDATE submissions
        SET status = ?,
            score_raw = ?,
            score_points = ?,
            score_max = ?,
            score_pct = ?,
            updated_at = datetime('now')
        WHERE student_id = ? AND assignment_id = ?
        """,
        (
            status,
            payload["score_raw"],
            payload["score_points"],
            payload["score_max"],
            payload["score_pct"],
            student_id,
            assignment_id,
        ),
    )
    stats["submissions_updated"] += 1
    logger.debug(
        "Updated submission student_id=%s assignment_id=%s status=%s",
        student_id,
        assignment_id,
        status,
    )


def _upsert_course_summary(
    conn: sqlite3.Connection,
    student_id: int,
    course_id: int,
    metrics: Dict,
    coursework: list,
    stats: Dict[str, int],
) -> None:
    total_assigned = int(metrics.get("total_assignments", len(coursework)))
    total_missing = int(metrics.get("missing", 0))
    total_late = int(metrics.get("late", 0))
    total_graded = int(metrics.get("graded_count", 0))
    total_submitted = max(total_assigned - total_missing, 0)
    avg_submitted_pct = float(metrics.get("average_submitted", 0.0))
    avg_all_pct = float(metrics.get("average_all", 0.0))

    points_earned = 0.0
    points_possible = 0.0
    for cw in coursework:
        submission = cw.get("submission")
        max_points = cw.get("maxPoints")
        if not submission or max_points is None:
            continue
        assigned_grade = submission.get("assignedGrade")
        if assigned_grade is None or assigned_grade <= 0:
            continue
        points_earned += float(assigned_grade)
        points_possible += float(max_points)

    conn.execute(
        """
        INSERT INTO course_summaries
        (student_id, course_id, total_assigned, total_submitted, total_missing, total_late, total_graded,
         avg_submitted_pct, avg_all_pct, points_earned, points_possible, last_synced)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
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
    stats["summaries_upserted"] += 1
    logger.debug(
        "Upserted summary student_id=%s course_id=%s missing=%s avg_all=%.2f",
        student_id,
        course_id,
        total_missing,
        avg_all_pct,
    )


def sync_course_analysis_to_db(
    course: Dict,
    student_analysis: Dict,
    db_path: str,
    schema_path: str,
    school_name: str = "School",
    source: str = "learner_performance_monitor_direct",
    dry_run: bool = False,
    start_date: str | None = None,
    end_date: str | None = None,
    active_assignment_lms_ids: set[str] | None = None,
) -> Dict[str, int]:
    """
    Sync one analysed course directly to class.db without reading report files.
    """
    if not student_analysis:
        logger.info("No student analysis rows for course=%s. Nothing to sync.", course.get("id"))
        return {
            "courses_added": 0,
            "courses_updated": 0,
            "students_added": 0,
            "students_updated": 0,
            "enrollments_added": 0,
            "assignments_added": 0,
            "assignments_updated": 0,
            "assignments_deleted": 0,
            "submissions_added": 0,
            "submissions_updated": 0,
            "summaries_upserted": 0,
            "sync_logs_added": 0,
        }

    db_path_obj = Path(db_path)
    schema_path_obj = Path(schema_path)
    if not db_path_obj.exists():
        raise FileNotFoundError(f"DB file not found: {db_path_obj}")
    if not schema_path_obj.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path_obj}")

    stats = {
        "courses_added": 0,
        "courses_updated": 0,
        "students_added": 0,
        "students_updated": 0,
        "enrollments_added": 0,
        "assignments_added": 0,
        "assignments_updated": 0,
        "assignments_deleted": 0,
        "submissions_added": 0,
        "submissions_updated": 0,
        "summaries_upserted": 0,
        "sync_logs_added": 0,
    }

    conn = sqlite3.connect(str(db_path_obj))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")

    try:
        schema_sql = schema_path_obj.read_text(encoding="utf-8")
        conn.executescript(schema_sql)
        logger.debug("Schema applied from %s", schema_path_obj)

        school_id = _upsert_school(conn, school_name)
        course_id = _upsert_course(
            conn=conn,
            course_lms_id=str(course["id"]),
            course_name=course["name"],
            school_id=school_id,
            stats=stats,
        )

        assignment_map = _build_assignment_map(student_analysis)
        assignment_db_ids = _upsert_assignments(conn, course_id, assignment_map, stats)
        assignment_ids_for_cleanup = (
            active_assignment_lms_ids if active_assignment_lms_ids is not None else set(assignment_map.keys())
        )
        cleanup_start_date = None if active_assignment_lms_ids is not None else start_date
        cleanup_end_date = None if active_assignment_lms_ids is not None else end_date
        _delete_stale_assignments(
            conn=conn,
            course_id=course_id,
            active_assignment_lms_ids=assignment_ids_for_cleanup,
            stats=stats,
            start_date=cleanup_start_date,
            end_date=cleanup_end_date,
        )

        for sid, data in student_analysis.items():
            profile = data["student"].get("profile", {})
            name_info = profile.get("name", {})
            full_name = " ".join(
                filter(None, [name_info.get("givenName", ""), name_info.get("familyName", "")])
            ).strip() or str(sid)

            student_id = _upsert_student(conn, str(sid), full_name, stats)
            _ensure_enrollment(conn, student_id, course_id, stats)

            coursework = data.get("coursework", [])
            for cw in coursework:
                assignment_lms_id = str(cw["id"])
                assignment_id = assignment_db_ids.get(assignment_lms_id)
                if assignment_id is None:
                    logger.warning(
                        "Skipping submission: missing assignment id for assignment_lms_id=%s",
                        assignment_lms_id,
                    )
                    continue
                payload = _compute_submission_status_and_score(cw)
                _upsert_submission(conn, student_id, assignment_id, payload, stats)

            _upsert_course_summary(
                conn=conn,
                student_id=student_id,
                course_id=course_id,
                metrics=data.get("metrics", {}),
                coursework=coursework,
                stats=stats,
            )

        conn.execute(
            """
            INSERT INTO sync_log (course_id, source, rows_added, rows_updated, notes)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                course_id,
                source,
                stats["submissions_added"],
                stats["submissions_updated"],
                (
                    f"direct_sync course={course.get('name')} students={len(student_analysis)} "
                    f"assignments_deleted={stats['assignments_deleted']}"
                ),
            ),
        )
        stats["sync_logs_added"] += 1

        if dry_run:
            conn.rollback()
            logger.info("Direct DB sync dry-run complete; changes rolled back.")
        else:
            conn.commit()
            logger.info(
                "Direct DB sync committed for course=%s (%s). submissions_added=%d submissions_updated=%d assignments_deleted=%d",
                course.get("name"),
                course.get("id"),
                stats["submissions_added"],
                stats["submissions_updated"],
                stats["assignments_deleted"],
            )
    except Exception:
        conn.rollback()
        logger.exception("Direct DB sync failed; transaction rolled back.")
        raise
    finally:
        conn.close()

    return stats
