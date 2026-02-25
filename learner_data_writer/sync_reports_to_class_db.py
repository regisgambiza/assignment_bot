import argparse
import logging
import os
import re
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple


BASE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = str((BASE_DIR / os.getenv("DB_PATH", "database/class.db")).resolve())
DEFAULT_SCHEMA_PATH = str((BASE_DIR / "database" / "schema.sql").resolve())
DEFAULT_REPORTS_DIR = "reports"
DEFAULT_SOURCE = "learner_performance_monitor_reports"

ALLOWED_STATUSES = {"Missing", "Submitted", "Late", "Graded", "Flagged"}
MISSING_SCORE_MARKERS = {"", "-", "--", "\u2014", "â€”"}


logger = logging.getLogger("report_db_sync")


@dataclass
class AssignmentRecord:
    lms_id: str
    title: str
    status: str
    score_raw: Optional[str]
    score_points: Optional[float]
    score_max: Optional[float]
    score_pct: Optional[float]
    created_at: Optional[str]


@dataclass
class StudentRecord:
    lms_id: str
    full_name: str
    assignments: List[AssignmentRecord]
    total_assigned: Optional[int]
    total_missing: Optional[int]
    total_late: Optional[int]
    total_graded: Optional[int]
    avg_submitted_pct: Optional[float]
    avg_all_pct: Optional[float]
    points_earned: Optional[float]
    points_possible: Optional[float]


@dataclass
class CourseReport:
    source_file: Path
    course_lms_id: str
    course_name: str
    students: List[StudentRecord]


@dataclass
class SyncStats:
    courses_added: int = 0
    courses_updated: int = 0
    students_added: int = 0
    students_updated: int = 0
    enrollments_added: int = 0
    assignments_added: int = 0
    assignments_updated: int = 0
    submissions_added: int = 0
    submissions_updated: int = 0
    summaries_upserted: int = 0
    sync_logs_added: int = 0
    assignments_seen: int = 0
    submissions_seen: int = 0

    def merge(self, other: "SyncStats") -> None:
        self.courses_added += other.courses_added
        self.courses_updated += other.courses_updated
        self.students_added += other.students_added
        self.students_updated += other.students_updated
        self.enrollments_added += other.enrollments_added
        self.assignments_added += other.assignments_added
        self.assignments_updated += other.assignments_updated
        self.submissions_added += other.submissions_added
        self.submissions_updated += other.submissions_updated
        self.summaries_upserted += other.summaries_upserted
        self.sync_logs_added += other.sync_logs_added
        self.assignments_seen += other.assignments_seen
        self.submissions_seen += other.submissions_seen


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s | %(levelname)-5s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Sync learner performance report files into assignment_bot class.db",
    )
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH, help="Path to class.db")
    parser.add_argument("--schema-path", default=DEFAULT_SCHEMA_PATH, help="Path to schema.sql")
    parser.add_argument("--reports-dir", default=DEFAULT_REPORTS_DIR, help="Directory containing report .txt files")
    parser.add_argument(
        "--report-file",
        action="append",
        default=[],
        help="Specific report file to sync. Can be used multiple times.",
    )
    parser.add_argument("--school-name", default="School", help="School name to use when creating courses")
    parser.add_argument("--source", default=DEFAULT_SOURCE, help="sync_log source tag")
    parser.add_argument("--dry-run", action="store_true", help="Parse and execute SQL but rollback before commit")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging verbosity",
    )
    return parser.parse_args()


def discover_report_files(reports_dir: Path, report_files: List[str]) -> List[Path]:
    files: List[Path] = []
    if report_files:
        for raw in report_files:
            path = Path(raw)
            if not path.exists():
                raise FileNotFoundError(f"Report file not found: {path}")
            files.append(path)
    else:
        if not reports_dir.exists():
            raise FileNotFoundError(f"Reports directory not found: {reports_dir}")
        for path in sorted(reports_dir.glob("*.txt")):
            name = path.name.lower()
            if name.endswith("_summary.txt") or name.endswith("_categories.txt"):
                continue
            files.append(path)

    filtered: List[Path] = []
    for path in files:
        text = path.read_text(encoding="utf-8", errors="replace")
        if "Reports for Course:" in text:
            filtered.append(path)
        else:
            logger.debug("Skipping non-report file: %s", path)
    return filtered


def normalize_score_raw(raw: str) -> Optional[str]:
    value = raw.strip()
    if value in MISSING_SCORE_MARKERS:
        return None
    return value


def parse_score(raw: Optional[str]) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    if not raw:
        return None, None, None
    parts = raw.split("/", 1)
    if len(parts) != 2:
        return None, None, None
    try:
        points = float(parts[0].strip())
        max_score = float(parts[1].strip())
        pct = round((points / max_score) * 100, 2) if max_score else None
        return points, max_score, pct
    except ValueError:
        return None, None, None


def parse_int_metric(block: str, label: str) -> Optional[int]:
    pattern = rf"\|\s*{re.escape(label)}\s*\|\s*(\d+)"
    match = re.search(pattern, block, flags=re.IGNORECASE)
    return int(match.group(1)) if match else None


def parse_avg_submitted_metric(block: str) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    pattern = (
        r"\|\s*Average\s*\(submitted\)\s*\|"
        r"\s*([0-9]+(?:\.[0-9]+)?)\s*%"
        r"(?:\s*\(([0-9]+(?:\.[0-9]+)?)\s*/\s*([0-9]+(?:\.[0-9]+)?)\))?"
    )
    match = re.search(pattern, block, flags=re.IGNORECASE)
    if not match:
        return None, None, None
    avg_sub = float(match.group(1))
    points = float(match.group(2)) if match.group(2) else None
    possible = float(match.group(3)) if match.group(3) else None
    return avg_sub, points, possible


def parse_avg_all_metric(block: str) -> Optional[float]:
    pattern = r"\|\s*Average\s*\(all\)\s*\|\s*([0-9]+(?:\.[0-9]+)?)\s*%"
    match = re.search(pattern, block, flags=re.IGNORECASE)
    return float(match.group(1)) if match else None


def parse_assignment_line(line: str) -> Optional[AssignmentRecord]:
    if "|" not in line:
        return None
    parts = [part.strip() for part in line.split("|")]
    if len(parts) < 5:
        return None

    title, lms_id, status, score_raw, created_at = parts[:5]
    if not lms_id.isdigit():
        return None
    if status not in ALLOWED_STATUSES:
        return None

    score_clean = normalize_score_raw(score_raw)
    points, max_score, pct = parse_score(score_clean)
    created_clean = created_at.strip() or None

    return AssignmentRecord(
        lms_id=lms_id,
        title=title,
        status=status,
        score_raw=score_clean,
        score_points=points,
        score_max=max_score,
        score_pct=pct,
        created_at=created_clean,
    )


def parse_student_block(block: str) -> Optional[StudentRecord]:
    name_match = re.search(r"^\s*Student:\s*(.+?)\s*$", block, flags=re.MULTILINE)
    id_match = re.search(r"^\s*Student ID:\s*(\d+)\s*$", block, flags=re.MULTILINE)
    if not name_match or not id_match:
        return None

    full_name = name_match.group(1).strip()
    lms_id = id_match.group(1).strip()

    assignments: List[AssignmentRecord] = []
    for line in block.splitlines():
        record = parse_assignment_line(line)
        if record:
            assignments.append(record)

    if not assignments:
        logger.warning("No assignment rows parsed for %s (%s)", full_name, lms_id)

    total_assigned = parse_int_metric(block, "Total Assigned")
    total_missing = parse_int_metric(block, "Missing")
    total_late = parse_int_metric(block, "Late")
    total_graded = parse_int_metric(block, "Graded Count")
    avg_submitted_pct, points_earned, points_possible = parse_avg_submitted_metric(block)
    avg_all_pct = parse_avg_all_metric(block)

    return StudentRecord(
        lms_id=lms_id,
        full_name=full_name,
        assignments=assignments,
        total_assigned=total_assigned,
        total_missing=total_missing,
        total_late=total_late,
        total_graded=total_graded,
        avg_submitted_pct=avg_submitted_pct,
        avg_all_pct=avg_all_pct,
        points_earned=points_earned,
        points_possible=points_possible,
    )


def parse_course_report(path: Path) -> CourseReport:
    text = path.read_text(encoding="utf-8", errors="replace")
    header_match = re.search(
        r"Reports for Course:\s*(.*?)\s*\(([^()]+)\)",
        text,
        flags=re.IGNORECASE,
    )
    if not header_match:
        raise ValueError(f"Could not parse course header from: {path}")

    course_name = header_match.group(1).strip()
    course_lms_id = header_match.group(2).strip()

    students: List[StudentRecord] = []
    blocks = re.split(r"(?=^Student:\s)", text, flags=re.MULTILINE)
    for block in blocks:
        student = parse_student_block(block)
        if student:
            students.append(student)

    if not students:
        raise ValueError(f"No students parsed from report: {path}")

    logger.info(
        "Parsed %d students from %s for course %s (%s)",
        len(students),
        path.name,
        course_name,
        course_lms_id,
    )
    return CourseReport(
        source_file=path,
        course_lms_id=course_lms_id,
        course_name=course_name,
        students=students,
    )


def apply_schema(conn: sqlite3.Connection, schema_path: Path) -> None:
    schema_sql = schema_path.read_text(encoding="utf-8")
    conn.executescript(schema_sql)
    logger.debug("Schema applied from %s", schema_path)


def get_or_create_school_id(conn: sqlite3.Connection, school_name: str) -> int:
    row = conn.execute("SELECT id FROM schools WHERE name = ?", (school_name,)).fetchone()
    if row:
        return int(row["id"])
    conn.execute("INSERT INTO schools (name) VALUES (?)", (school_name,))
    row = conn.execute("SELECT id FROM schools WHERE name = ?", (school_name,)).fetchone()
    if not row:
        raise RuntimeError("Failed to create school record")
    logger.debug("Created school '%s' with id=%s", school_name, row["id"])
    return int(row["id"])


def maybe_update_value(existing, new_value) -> bool:
    return new_value is not None and existing != new_value


def pick_title(existing: Optional[str], candidate: Optional[str]) -> Optional[str]:
    if not candidate:
        return existing
    if not existing:
        return candidate
    if len(candidate) > len(existing):
        return candidate
    return existing


def upsert_course(
    conn: sqlite3.Connection,
    course_lms_id: str,
    course_name: str,
    school_id: int,
    stats: SyncStats,
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
        stats.courses_added += 1
        logger.debug("Inserted course %s (%s)", course_name, course_lms_id)
        row = conn.execute(
            "SELECT id FROM courses WHERE lms_id = ?",
            (course_lms_id,),
        ).fetchone()
        if not row:
            raise RuntimeError(f"Failed to insert course {course_lms_id}")
        return int(row["id"])

    updates = {}
    if maybe_update_value(row["name"], course_name):
        updates["name"] = course_name
    if maybe_update_value(row["school_id"], school_id):
        updates["school_id"] = school_id

    if updates:
        sets = ", ".join(f"{k} = ?" for k in updates)
        values = list(updates.values()) + [course_lms_id]
        conn.execute(f"UPDATE courses SET {sets} WHERE lms_id = ?", values)
        stats.courses_updated += 1
        logger.debug("Updated course %s (%s): %s", course_name, course_lms_id, ", ".join(updates.keys()))

    return int(row["id"])


def upsert_student(conn: sqlite3.Connection, student: StudentRecord, stats: SyncStats) -> int:
    row = conn.execute(
        "SELECT id, full_name FROM students WHERE lms_id = ?",
        (student.lms_id,),
    ).fetchone()
    if not row:
        conn.execute(
            "INSERT INTO students (lms_id, full_name) VALUES (?, ?)",
            (student.lms_id, student.full_name),
        )
        stats.students_added += 1
        logger.debug("Inserted student %s (%s)", student.full_name, student.lms_id)
        row = conn.execute(
            "SELECT id FROM students WHERE lms_id = ?",
            (student.lms_id,),
        ).fetchone()
        if not row:
            raise RuntimeError(f"Failed to insert student {student.lms_id}")
        return int(row["id"])

    if row["full_name"] != student.full_name:
        conn.execute(
            "UPDATE students SET full_name = ? WHERE lms_id = ?",
            (student.full_name, student.lms_id),
        )
        stats.students_updated += 1
        logger.debug("Updated student name for %s -> %s", student.lms_id, student.full_name)

    return int(row["id"])


def upsert_enrollment(conn: sqlite3.Connection, student_id: int, course_id: int, stats: SyncStats) -> None:
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
    stats.enrollments_added += 1
    logger.debug("Added enrollment student_id=%s course_id=%s", student_id, course_id)


def aggregate_assignments(students: List[StudentRecord]) -> Dict[str, Dict[str, Optional[object]]]:
    assignment_map: Dict[str, Dict[str, Optional[object]]] = {}
    for student in students:
        for assignment in student.assignments:
            existing = assignment_map.get(assignment.lms_id)
            if not existing:
                assignment_map[assignment.lms_id] = {
                    "title": assignment.title,
                    "max_score": assignment.score_max,
                    "created_at": assignment.created_at,
                }
                continue

            existing["title"] = pick_title(existing.get("title"), assignment.title)

            current_max = existing.get("max_score")
            if current_max is None and assignment.score_max is not None:
                existing["max_score"] = assignment.score_max
            elif current_max is not None and assignment.score_max is not None and assignment.score_max > float(current_max):
                existing["max_score"] = assignment.score_max

            existing_created = existing.get("created_at")
            if not existing_created and assignment.created_at:
                existing["created_at"] = assignment.created_at
    return assignment_map


def upsert_assignments(
    conn: sqlite3.Connection,
    course_id: int,
    assignment_meta: Dict[str, Dict[str, Optional[object]]],
    stats: SyncStats,
) -> Dict[str, int]:
    db_ids: Dict[str, int] = {}
    for lms_id, meta in assignment_meta.items():
        row = conn.execute(
            "SELECT id, title, max_score, created_at, course_id FROM assignments WHERE lms_id = ?",
            (lms_id,),
        ).fetchone()

        title = str(meta.get("title") or "")
        max_score = meta.get("max_score")
        created_at = str(meta.get("created_at") or "")

        if not created_at:
            created_at = "1970-01-01T00:00:00Z"

        if not row:
            conn.execute(
                """
                INSERT INTO assignments (lms_id, course_id, title, max_score, created_at, is_active)
                VALUES (?, ?, ?, ?, ?, 1)
                """,
                (lms_id, course_id, title, max_score, created_at),
            )
            stats.assignments_added += 1
            logger.debug("Inserted assignment %s (%s)", title, lms_id)
            row = conn.execute(
                "SELECT id FROM assignments WHERE lms_id = ?",
                (lms_id,),
            ).fetchone()
            if not row:
                raise RuntimeError(f"Failed to insert assignment {lms_id}")
            db_ids[lms_id] = int(row["id"])
            continue

        updates = {}
        preferred_title = pick_title(row["title"], title)
        if preferred_title != row["title"]:
            updates["title"] = preferred_title
        if max_score is not None and row["max_score"] != max_score:
            updates["max_score"] = max_score
        if row["created_at"] in (None, "") and created_at:
            updates["created_at"] = created_at
        if row["course_id"] != course_id:
            updates["course_id"] = course_id

        if updates:
            sets = ", ".join(f"{k} = ?" for k in updates)
            values = list(updates.values()) + [lms_id]
            conn.execute(f"UPDATE assignments SET {sets}, is_active = 1 WHERE lms_id = ?", values)
            stats.assignments_updated += 1
            logger.debug("Updated assignment %s (%s): %s", title, lms_id, ", ".join(updates.keys()))
        else:
            conn.execute("UPDATE assignments SET is_active = 1 WHERE lms_id = ?", (lms_id,))

        db_ids[lms_id] = int(row["id"])

    return db_ids


def floats_equal(a: Optional[float], b: Optional[float], tol: float = 1e-6) -> bool:
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    return abs(float(a) - float(b)) <= tol


def upsert_submission(
    conn: sqlite3.Connection,
    student_id: int,
    assignment_id: int,
    assignment: AssignmentRecord,
    assignment_max_score: Optional[float],
    stats: SyncStats,
) -> None:
    effective_score_max = assignment.score_max if assignment.score_max is not None else assignment_max_score
    effective_score_pct = assignment.score_pct
    if effective_score_pct is None and assignment.score_points is not None and effective_score_max not in (None, 0):
        effective_score_pct = round((float(assignment.score_points) / float(effective_score_max)) * 100, 2)

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
                assignment.status,
                assignment.score_raw,
                assignment.score_points,
                effective_score_max,
                effective_score_pct,
            ),
        )
        stats.submissions_added += 1
        logger.debug(
            "Inserted submission student_id=%s assignment_id=%s status=%s",
            student_id,
            assignment_id,
            assignment.status,
        )
        return

    changed = False
    if row["status"] != assignment.status:
        changed = True
    if row["score_raw"] != assignment.score_raw:
        changed = True
    if not floats_equal(row["score_points"], assignment.score_points):
        changed = True
    if not floats_equal(row["score_max"], effective_score_max):
        changed = True
    if not floats_equal(row["score_pct"], effective_score_pct):
        changed = True

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
            assignment.status,
            assignment.score_raw,
            assignment.score_points,
            effective_score_max,
            effective_score_pct,
            student_id,
            assignment_id,
        ),
    )
    stats.submissions_updated += 1
    logger.debug(
        "Updated submission student_id=%s assignment_id=%s status=%s",
        student_id,
        assignment_id,
        assignment.status,
    )


def derive_summary_fallback(student: StudentRecord) -> Tuple[int, int, int, int, float, float, float, float]:
    total_assigned = student.total_assigned if student.total_assigned is not None else len(student.assignments)

    status_missing = sum(1 for a in student.assignments if a.status == "Missing")
    status_late = sum(1 for a in student.assignments if a.status == "Late")
    graded = sum(1 for a in student.assignments if a.score_pct is not None)
    total_submitted = sum(1 for a in student.assignments if a.status != "Missing")

    total_missing = student.total_missing if student.total_missing is not None else status_missing
    total_late = student.total_late if student.total_late is not None else status_late
    total_graded = student.total_graded if student.total_graded is not None else graded

    points_earned = student.points_earned
    points_possible = student.points_possible

    if points_earned is None:
        points_earned = sum(float(a.score_points) for a in student.assignments if a.score_points is not None)
    if points_possible is None:
        points_possible = sum(float(a.score_max) for a in student.assignments if a.score_max is not None)

    avg_submitted_pct = student.avg_submitted_pct
    if avg_submitted_pct is None:
        if points_possible:
            avg_submitted_pct = round((points_earned / points_possible) * 100, 2)
        else:
            avg_submitted_pct = 0.0

    avg_all_pct = student.avg_all_pct
    if avg_all_pct is None:
        avg_all_pct = avg_submitted_pct

    return (
        total_assigned,
        total_submitted,
        total_missing,
        total_late,
        total_graded,
        float(avg_submitted_pct),
        float(avg_all_pct),
        float(points_earned),
        float(points_possible),
    )


def upsert_course_summary(
    conn: sqlite3.Connection,
    student_id: int,
    course_id: int,
    student: StudentRecord,
    stats: SyncStats,
) -> None:
    (
        total_assigned,
        total_submitted,
        total_missing,
        total_late,
        total_graded,
        avg_submitted_pct,
        avg_all_pct,
        points_earned,
        points_possible,
    ) = derive_summary_fallback(student)

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
    stats.summaries_upserted += 1
    logger.debug(
        "Upserted summary student_id=%s course_id=%s missing=%s avg_all=%.2f",
        student_id,
        course_id,
        total_missing,
        avg_all_pct,
    )


def insert_sync_log(
    conn: sqlite3.Connection,
    course_id: int,
    source: str,
    stats: SyncStats,
    notes: str,
) -> None:
    conn.execute(
        """
        INSERT INTO sync_log (course_id, source, rows_added, rows_updated, notes)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            course_id,
            source,
            stats.submissions_added,
            stats.submissions_updated,
            notes,
        ),
    )
    stats.sync_logs_added += 1


def sync_course_report(
    conn: sqlite3.Connection,
    report: CourseReport,
    school_name: str,
    source: str,
) -> SyncStats:
    stats = SyncStats()
    school_id = get_or_create_school_id(conn, school_name)
    course_id = upsert_course(conn, report.course_lms_id, report.course_name, school_id, stats)

    assignment_meta_map = aggregate_assignments(report.students)
    stats.assignments_seen = len(assignment_meta_map)
    stats.submissions_seen = sum(len(s.assignments) for s in report.students)
    assignment_db_ids = upsert_assignments(conn, course_id, assignment_meta_map, stats)

    for student in report.students:
        student_id = upsert_student(conn, student, stats)
        upsert_enrollment(conn, student_id, course_id, stats)

        for assignment in student.assignments:
            assignment_id = assignment_db_ids.get(assignment.lms_id)
            if assignment_id is None:
                logger.warning(
                    "Skipping submission for unknown assignment lms_id=%s student=%s",
                    assignment.lms_id,
                    student.lms_id,
                )
                continue
            assignment_meta = assignment_meta_map.get(assignment.lms_id, {})
            assignment_max = assignment_meta.get("max_score")
            upsert_submission(conn, student_id, assignment_id, assignment, assignment_max, stats)

        upsert_course_summary(conn, student_id, course_id, student, stats)

    note = (
        f"file={report.source_file.name}; students={len(report.students)}; "
        f"assignments_seen={stats.assignments_seen}; submissions_seen={stats.submissions_seen}"
    )
    insert_sync_log(conn, course_id, source, stats, note)

    db_counts = conn.execute(
        """
        SELECT
            COUNT(sub.id) AS total_rows,
            SUM(CASE WHEN sub.score_points IS NOT NULL THEN 1 ELSE 0 END) AS scored_rows,
            SUM(CASE WHEN sub.score_points IS NULL THEN 1 ELSE 0 END) AS unscored_rows
        FROM assignments a
        LEFT JOIN submissions sub ON sub.assignment_id = a.id
        WHERE a.course_id = ?
        """,
        (course_id,),
    ).fetchone()

    logger.info(
        (
            "Synced course %s (%s): students=%d, assignments_seen=%d, submissions_seen=%d, "
            "assignments_added=%d, assignments_updated=%d, submissions_added=%d, submissions_updated=%d, "
            "db_total_rows=%d, db_scored_rows=%d, db_unscored_rows=%d"
        ),
        report.course_name,
        report.course_lms_id,
        len(report.students),
        stats.assignments_seen,
        stats.submissions_seen,
        stats.assignments_added,
        stats.assignments_updated,
        stats.submissions_added,
        stats.submissions_updated,
        db_counts["total_rows"] or 0,
        db_counts["scored_rows"] or 0,
        db_counts["unscored_rows"] or 0,
    )
    return stats


def sync_reports(
    db_path: Path,
    schema_path: Path,
    reports: List[CourseReport],
    school_name: str,
    source: str,
    dry_run: bool,
) -> SyncStats:
    if not db_path.exists():
        raise FileNotFoundError(f"DB file not found: {db_path}")
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")

    total = SyncStats()
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")

    try:
        apply_schema(conn, schema_path)
        for report in reports:
            course_stats = sync_course_report(
                conn=conn,
                report=report,
                school_name=school_name,
                source=source,
            )
            total.merge(course_stats)

        if dry_run:
            conn.rollback()
            logger.info("Dry-run enabled: rolled back all DB changes")
        else:
            conn.commit()
            logger.info("Committed DB changes")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return total


def main() -> None:
    args = parse_args()
    setup_logging(args.log_level)

    db_path = Path(args.db_path)
    schema_path = Path(args.schema_path)
    reports_dir = Path(args.reports_dir)

    logger.info("Starting report-to-db sync")
    logger.info("Target DB: %s", db_path)
    logger.info("Schema file: %s", schema_path)
    logger.debug("db_path=%s", db_path)
    logger.debug("schema_path=%s", schema_path)
    logger.debug("reports_dir=%s", reports_dir)
    logger.debug("source=%s dry_run=%s", args.source, args.dry_run)

    report_paths = discover_report_files(reports_dir, args.report_file)
    if not report_paths:
        raise RuntimeError("No report files found to sync")

    parsed_reports = [parse_course_report(path) for path in report_paths]
    stats = sync_reports(
        db_path=db_path,
        schema_path=schema_path,
        reports=parsed_reports,
        school_name=args.school_name,
        source=args.source,
        dry_run=args.dry_run,
    )

    logger.info(
        "Done. courses_added=%d courses_updated=%d students_added=%d students_updated=%d "
        "enrollments_added=%d assignments_added=%d assignments_updated=%d submissions_added=%d "
        "submissions_updated=%d summaries_upserted=%d sync_logs_added=%d assignments_seen=%d submissions_seen=%d",
        stats.courses_added,
        stats.courses_updated,
        stats.students_added,
        stats.students_updated,
        stats.enrollments_added,
        stats.assignments_added,
        stats.assignments_updated,
        stats.submissions_added,
        stats.submissions_updated,
        stats.summaries_upserted,
        stats.sync_logs_added,
        stats.assignments_seen,
        stats.submissions_seen,
    )


if __name__ == "__main__":
    main()
