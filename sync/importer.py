"""
sync/importer.py

Import or re-sync from your LMS report export.
Parses the text report format from your data.

Usage:
  python -m sync.importer --file report.txt
  python -m sync.importer --file report.txt --dry-run
"""
import re
import sys
import argparse
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from database.db import get_db, rebuild_summary, init_db

# ── Parse the report text format ─────────────────────────

def parse_report(text: str) -> list[dict]:
    """
    Parses your exact report format into a list of student dicts.
    Each dict has: name, lms_id, assignments[]
    """
    students = []
    # Split by the student separator
    blocks = re.split(r"-{50,}", text)

    for block in blocks:
        block = block.strip()
        if not block:
            continue

        student = {}

        # Name
        name_match = re.search(r"Student:\s*(.+)", block)
        if name_match:
            student["full_name"] = name_match.group(1).strip()

        # LMS ID
        id_match = re.search(r"Student ID:\s*(\d+)", block)
        if id_match:
            student["lms_id"] = id_match.group(1).strip()

        if not student.get("full_name") or not student.get("lms_id"):
            continue

        # Parse assignment rows from the detailed table
        assignments = []
        # Match rows like: Chapter 14 Quiz | 842720561319 | Submitted | 9/14 | 2026-...
        pattern = re.compile(
            r"^(.+?)\s*\|\s*(\d+)\s*\|\s*(Submitted|Missing|Late|Graded)"
            r"\s*\|\s*([\d/—\-]+)?\s*\|\s*([\d\-T:\.Z]+)",
            re.MULTILINE
        )
        for m in pattern.finditer(block):
            title     = m.group(1).strip()
            lms_id    = m.group(2).strip()
            status    = m.group(3).strip()
            score_raw = m.group(4).strip() if m.group(4) else None
            created   = m.group(5).strip()

            if score_raw in ("—", "-", ""):
                score_raw = None

            pts, mx, pct = _parse_score(score_raw)
            assignments.append({
                "lms_id":       lms_id,
                "title":        title,
                "status":       status,
                "score_raw":    score_raw,
                "score_points": pts,
                "score_max":    mx,
                "score_pct":    pct,
                "created_at":   created,
            })

        student["assignments"] = assignments
        students.append(student)

    return students

def _parse_score(raw):
    if not raw:
        return None, None, None
    try:
        pts, mx = raw.split("/")
        pts, mx = float(pts), float(mx)
        pct = round(pts / mx * 100, 1) if mx else None
        return pts, mx, pct
    except Exception:
        return None, None, None

# ── Write parsed data to DB ───────────────────────────────

def import_to_db(students: list[dict], course_id: int = 1,
                 dry_run: bool = False) -> dict:
    stats = {"students": 0, "assignments": 0, "submissions": 0, "updated": 0}

    if dry_run:
        print("🔍 DRY RUN — no changes will be saved\n")

    with get_db() as conn:
        for student in students:
            if dry_run:
                print(f"  Would upsert student: {student['full_name']} ({student['lms_id']})")

            if not dry_run:
                conn.execute(
                    """INSERT INTO students (lms_id, full_name)
                       VALUES (?, ?)
                       ON CONFLICT(lms_id) DO UPDATE SET full_name = excluded.full_name""",
                    (student["lms_id"], student["full_name"])
                )
                # Enroll in course
                row = conn.execute(
                    "SELECT id FROM students WHERE lms_id = ?",
                    (student["lms_id"],)
                ).fetchone()
                student_db_id = row["id"]
                conn.execute(
                    "INSERT OR IGNORE INTO enrollments (student_id, course_id) VALUES (?,?)",
                    (student_db_id, course_id)
                )
            stats["students"] += 1

            for a in student["assignments"]:
                if dry_run:
                    print(f"    {a['status']:10} | {a['title'][:35]:35} | {a['score_raw'] or '—'}")
                    continue

                # Upsert assignment
                conn.execute(
                    """INSERT INTO assignments
                         (lms_id, course_id, title, max_score, created_at)
                       VALUES (?, ?, ?, ?, ?)
                       ON CONFLICT(lms_id) DO UPDATE SET
                         title = excluded.title,
                         max_score = CASE
                           WHEN assignments.max_score IS NULL THEN excluded.max_score
                           WHEN excluded.max_score IS NULL THEN assignments.max_score
                           ELSE MAX(assignments.max_score, excluded.max_score)
                         END""",
                    (a["lms_id"], course_id, a["title"], a["score_max"], a["created_at"])
                )
                assign_row = conn.execute(
                    "SELECT id FROM assignments WHERE lms_id = ?",
                    (a["lms_id"],)
                ).fetchone()
                assign_db_id = assign_row["id"]
                stats["assignments"] += 1

                # Upsert submission
                existing = conn.execute(
                    "SELECT id FROM submissions WHERE student_id=? AND assignment_id=?",
                    (student_db_id, assign_db_id)
                ).fetchone()

                if existing:
                    conn.execute(
                        """UPDATE submissions
                           SET status=?, score_raw=?, score_points=?,
                               score_max=?, score_pct=?, updated_at=datetime('now')
                           WHERE student_id=? AND assignment_id=?""",
                        (a["status"], a["score_raw"], a["score_points"],
                         a["score_max"], a["score_pct"],
                         student_db_id, assign_db_id)
                    )
                    stats["updated"] += 1
                else:
                    conn.execute(
                        """INSERT INTO submissions
                             (student_id, assignment_id, status,
                              score_raw, score_points, score_max, score_pct)
                           VALUES (?,?,?,?,?,?,?)""",
                        (student_db_id, assign_db_id, a["status"],
                         a["score_raw"], a["score_points"],
                         a["score_max"], a["score_pct"])
                    )
                    stats["submissions"] += 1

    # Rebuild summaries
    if not dry_run:
        with get_db() as conn:
            student_ids = conn.execute(
                "SELECT id FROM students"
            ).fetchall()
        for row in student_ids:
            rebuild_summary(row["id"], course_id)

        # Log the sync
        with get_db() as conn:
            conn.execute(
                """INSERT INTO sync_log
                     (course_id, source, rows_added, rows_updated)
                   VALUES (?, 'txt_import', ?, ?)""",
                (course_id, stats["submissions"], stats["updated"])
            )

    return stats

# ── CLI ───────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Import LMS report to DB")
    parser.add_argument("--file",    required=True, help="Path to report .txt file")
    parser.add_argument("--dry-run", action="store_true", help="Preview without saving")
    args = parser.parse_args()

    path = Path(args.file)
    if not path.exists():
        print(f"❌ File not found: {path}")
        sys.exit(1)

    init_db()
    text     = path.read_text(encoding="utf-8")
    students = parse_report(text)

    if not students:
        print("❌ No students parsed. Check the file format.")
        sys.exit(1)

    print(f"📄 Parsed {len(students)} students from {path.name}\n")
    stats = import_to_db(students, dry_run=args.dry_run)

    if not args.dry_run:
        print(f"\n✅ Import complete:")
        print(f"   Students:        {stats['students']}")
        print(f"   Assignments:     {stats['assignments']}")
        print(f"   New submissions: {stats['submissions']}")
        print(f"   Updated:         {stats['updated']}")

if __name__ == "__main__":
    main()
