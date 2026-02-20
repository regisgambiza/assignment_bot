"""
seed.py — Load your real report data into the database.
Run once:  python -m database.seed
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from database.db import init_db, get_db, rebuild_summary

def seed():
    # ── Init schema ───────────────────────────────────────
    init_db()

    with get_db() as conn:

        # ── School & Course ───────────────────────────────
        conn.execute("INSERT OR IGNORE INTO schools (id, name) VALUES (1, 'School')")
        conn.execute("""
            INSERT OR IGNORE INTO courses (id, lms_id, name, school_id)
            VALUES (1, '778493924097', '8/1 Mathematics', 1)
        """)

        # ── Students ──────────────────────────────────────
        students = [
            ("116806481535403601771", "Natcharat Leelamasavat"),
            ("106754724722917820134", "Sunattaya Promfang"),
        ]
        for lms_id, name in students:
            conn.execute(
                "INSERT OR IGNORE INTO students (lms_id, full_name) VALUES (?,?)",
                (lms_id, name)
            )

        # ── Enrollments ───────────────────────────────────
        for i in range(1, len(students) + 1):
            conn.execute(
                "INSERT OR IGNORE INTO enrollments (student_id, course_id) VALUES (?,1)",
                (i,)
            )

        # ── Assignments ───────────────────────────────────
        assignments = [
            # (lms_id, title, max_score, created_at)
            ("838464321662", "Chapter 11 Quiz",                   None, "2026-01-08T15:00:23.643Z"),
            ("824255647570", "12.1 Complementary Events",         9,    "2026-01-09T01:26:27.807Z"),
            ("838854650802", "12.2 Experimental Probability",     25,   "2026-01-12T00:33:43.049Z"),
            ("824371984639", "Chapter 12 Quiz",                   16,   "2026-01-14T07:48:06.222Z"),
            ("793258652101", "13.1 Order of Operations",          28,   "2026-01-19T01:25:05.906Z"),
            ("840501477658", "13.2 Simplifying Calculations",     25,   "2026-01-21T04:05:20.560Z"),
            ("840578412852", "Chapter 13 Quiz",                   20,   "2026-01-21T15:16:49.110Z"),
            ("839194935425", "Consolidation Exercise",            10,   "2026-01-13T13:32:31.353Z"),
            ("840904486990", "14.1 Solving Equations w/ Brackets",21,  "2026-01-23T01:26:11.595Z"),
            ("841457341378", "14.2 Solving Equations w/ Unknowns",12,  "2026-01-27T03:41:39.920Z"),
            ("793447546285", "14.3 Constructing Equations",       None, "2026-01-28T04:08:02.722Z"),
            ("793479159016", "14.4 Inequalities",                 16,   "2026-01-29T03:35:55.596Z"),
            ("842095022390", "Consolidation Exercise (Ch14)",     26,   "2026-01-30T00:22:08.733Z"),
            ("842720561319", "Chapter 14 Quiz",                   14,   "2026-02-03T06:23:34.643Z"),
            ("843174735732", "15.1 Midpoint of a Line Segment",   None, "2026-02-05T03:27:10.576Z"),
        ]
        for lms_id, title, max_score, created_at in assignments:
            conn.execute(
                """INSERT OR IGNORE INTO assignments
                   (lms_id, course_id, title, max_score, created_at)
                   VALUES (?,1,?,?,?)""",
                (lms_id, title, max_score, created_at)
            )

        # ── Helper: parse "9/14" → (9.0, 14.0, 64.3) ─────
        def parse_score(raw):
            if not raw or raw == "—":
                return None, None, None
            try:
                pts, mx = raw.split("/")
                pts, mx = float(pts), float(mx)
                pct = round(pts / mx * 100, 1) if mx else None
                return pts, mx, pct
            except Exception:
                return None, None, None

        # ── Submissions ───────────────────────────────────
        # (student_id, assignment_id, status, score_raw)
        natcharat_subs = [
            (1,  1,  "Submitted", "—"),
            (1,  2,  "Submitted", "9/9"),
            (1,  3,  "Submitted", "14/25"),
            (1,  4,  "Submitted", "14/16"),
            (1,  5,  "Submitted", "28/28"),
            (1,  6,  "Submitted", "25/25"),
            (1,  7,  "Submitted", "17/20"),
            (1,  8,  "Submitted", "10/10"),
            (1,  9,  "Submitted", "19/21"),
            (1,  10, "Submitted", "12/12"),
            (1,  11, "Submitted", "—"),
            (1,  12, "Submitted", "16/16"),
            (1,  13, "Submitted", "25/26"),
            (1,  14, "Submitted", "9/14"),
            (1,  15, "Missing",   None),
        ]
        sunattaya_subs = [
            (2,  1,  "Missing",   None),
            (2,  2,  "Submitted", "9/9"),
            (2,  3,  "Missing",   None),
            (2,  4,  "Submitted", "14/16"),
            (2,  5,  "Submitted", "28/28"),
            (2,  6,  "Missing",   None),
            (2,  7,  "Submitted", "17/20"),
            (2,  8,  "Submitted", "10/10"),
            (2,  9,  "Submitted", "19/21"),
            (2,  10, "Missing",   None),
            (2,  11, "Missing",   None),
            (2,  12, "Missing",   None),
            (2,  13, "Missing",   None),
            (2,  14, "Submitted", "10/14"),
            (2,  15, "Missing",   None),
        ]
        for sid, aid, status, score_raw in natcharat_subs + sunattaya_subs:
            pts, mx, pct = parse_score(score_raw)
            conn.execute(
                """INSERT OR IGNORE INTO submissions
                   (student_id, assignment_id, status,
                    score_raw, score_points, score_max, score_pct)
                   VALUES (?,?,?,?,?,?,?)""",
                (sid, aid, status, score_raw, pts, mx, pct)
            )

    # ── Rebuild summaries ─────────────────────────────────
    rebuild_summary(1, 1)
    rebuild_summary(2, 1)

    print("✅ Seed complete — 2 students, 15 assignments loaded")

if __name__ == "__main__":
    seed()
