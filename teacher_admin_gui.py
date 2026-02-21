"""
Teacher/admin desktop dashboard for Assignment Bot.

Run:
    python teacher_admin_gui.py
"""

from __future__ import annotations

import os
import shutil
import sqlite3
import tkinter as tk
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from tkinter import messagebox, ttk

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional dependency
    load_dotenv = None


def _safe_float(value: object) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _safe_int(value: object) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


class TeacherAdminDashboard(tk.Tk):
    def __init__(self, db_path: Path, course_id: int = 0):
        super().__init__()
        self.db_path = db_path
        self.base_dir = Path(__file__).resolve().parent

        self.title("Assignment Bot - Teacher Admin Dashboard")
        self.geometry("1280x760")
        self.minsize(1120, 640)

        self.course_id_var = tk.IntVar(value=course_id)
        self.search_var = tk.StringVar()
        self.reviewer_var = tk.StringVar(value="Admin GUI")
        self.at_risk_threshold_var = tk.IntVar(value=3)
        self.status_var = tk.StringVar(value="Ready")

        self._configure_style()
        self._build_ui()
        self.refresh_all()

    @contextmanager
    def db_conn(self):
        conn = sqlite3.connect(self.db_path)
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

    def _configure_style(self) -> None:
        style = ttk.Style()
        style.theme_use("clam")
        style.configure("Treeview", rowheight=26, font=("Segoe UI", 10))
        style.configure("Treeview.Heading", font=("Segoe UI", 10, "bold"))
        style.configure("TNotebook.Tab", padding=(12, 8), font=("Segoe UI", 10, "bold"))
        style.configure("Header.TLabel", font=("Segoe UI", 12, "bold"))
        style.configure("Meta.TLabel", font=("Segoe UI", 9))

    def _build_ui(self) -> None:
        root = ttk.Frame(self, padding=12)
        root.pack(fill=tk.BOTH, expand=True)

        self._build_header(root)
        self._build_notebook(root)

        status = ttk.Label(
            root,
            textvariable=self.status_var,
            relief=tk.SUNKEN,
            anchor=tk.W,
            padding=(8, 4),
        )
        status.pack(fill=tk.X, side=tk.BOTTOM, pady=(8, 0))

    def _build_header(self, parent: ttk.Frame) -> None:
        header = ttk.Frame(parent)
        header.pack(fill=tk.X, pady=(0, 10))

        title = ttk.Label(header, text="Teacher Admin Dashboard", style="Header.TLabel")
        title.grid(row=0, column=0, padx=(0, 16), sticky="w")

        ttk.Label(header, text=f"Database: {self.db_path}", style="Meta.TLabel").grid(
            row=0, column=1, padx=(0, 16), sticky="w"
        )

        ttk.Label(header, text="Course ID (0 = all):", style="Meta.TLabel").grid(
            row=0, column=2, padx=(0, 4), sticky="e"
        )
        self.course_spin = ttk.Spinbox(
            header,
            from_=0,
            to=9999,
            textvariable=self.course_id_var,
            width=6,
        )
        self.course_spin.grid(row=0, column=3, padx=(0, 8), sticky="w")

        apply_btn = ttk.Button(header, text="Apply", command=self.refresh_all)
        apply_btn.grid(row=0, column=4, padx=(0, 8))

        refresh_btn = ttk.Button(header, text="Refresh All", command=self.refresh_all)
        refresh_btn.grid(row=0, column=5)

        header.columnconfigure(1, weight=1)

    def _build_notebook(self, parent: ttk.Frame) -> None:
        self.notebook = ttk.Notebook(parent)
        self.notebook.pack(fill=tk.BOTH, expand=True)

        self.students_tab = ttk.Frame(self.notebook, padding=10)
        self.flags_tab = ttk.Frame(self.notebook, padding=10)
        self.at_risk_tab = ttk.Frame(self.notebook, padding=10)
        self.maintenance_tab = ttk.Frame(self.notebook, padding=10)

        self.notebook.add(self.students_tab, text="Students")
        self.notebook.add(self.flags_tab, text="Pending Flags")
        self.notebook.add(self.at_risk_tab, text="At Risk")
        self.notebook.add(self.maintenance_tab, text="Maintenance")

        self._build_students_tab()
        self._build_flags_tab()
        self._build_at_risk_tab()
        self._build_maintenance_tab()

    def _build_students_tab(self) -> None:
        top = ttk.Frame(self.students_tab)
        top.pack(fill=tk.X, pady=(0, 8))

        ttk.Label(top, text="Search (name, LMS ID, Telegram ID):").pack(side=tk.LEFT, padx=(0, 6))
        search_entry = ttk.Entry(top, textvariable=self.search_var, width=44)
        search_entry.pack(side=tk.LEFT, padx=(0, 6))
        search_entry.bind("<Return>", lambda _event: self.refresh_students())

        ttk.Button(top, text="Search", command=self.refresh_students).pack(side=tk.LEFT, padx=(0, 6))
        ttk.Button(top, text="Clear", command=self._clear_student_search).pack(side=tk.LEFT, padx=(0, 16))

        ttk.Button(top, text="Unlink Selected", command=self.unlink_selected_student).pack(
            side=tk.LEFT, padx=(0, 6)
        )
        ttk.Button(top, text="Rebuild Selected Summary", command=self.rebuild_selected_summary).pack(
            side=tk.LEFT
        )

        columns = (
            "id",
            "course_name",
            "lms_id",
            "full_name",
            "telegram_id",
            "telegram_username",
            "missing",
            "avg_all_pct",
            "last_synced",
        )
        self.student_tree = self._build_tree(
            self.students_tab,
            columns=columns,
            headings=(
                "ID",
                "Course",
                "LMS ID",
                "Full Name",
                "Telegram ID",
                "Telegram User",
                "Missing",
                "Avg Overall %",
                "Last Synced",
            ),
            widths=(55, 150, 220, 230, 140, 150, 80, 100, 150),
        )

    def _build_flags_tab(self) -> None:
        top = ttk.Frame(self.flags_tab)
        top.pack(fill=tk.X, pady=(0, 8))

        ttk.Label(top, text="Reviewer name:").pack(side=tk.LEFT, padx=(0, 6))
        ttk.Entry(top, textvariable=self.reviewer_var, width=24).pack(side=tk.LEFT, padx=(0, 16))

        ttk.Button(top, text="Approve Selected", command=lambda: self.verify_selected_flag(True)).pack(
            side=tk.LEFT, padx=(0, 6)
        )
        ttk.Button(top, text="Deny Selected", command=lambda: self.verify_selected_flag(False)).pack(
            side=tk.LEFT, padx=(0, 6)
        )
        ttk.Button(top, text="Refresh", command=self.refresh_flags).pack(side=tk.LEFT)

        columns = (
            "student_id",
            "full_name",
            "assignment_id",
            "assignment_title",
            "flagged_at",
            "flag_note",
        )
        self.flag_tree = self._build_tree(
            self.flags_tab,
            columns=columns,
            headings=("Student ID", "Student", "Assignment ID", "Assignment", "Flagged At", "Note"),
            widths=(95, 220, 110, 320, 150, 260),
        )

    def _build_at_risk_tab(self) -> None:
        top = ttk.Frame(self.at_risk_tab)
        top.pack(fill=tk.X, pady=(0, 8))

        ttk.Label(top, text="Threshold (missing >=):").pack(side=tk.LEFT, padx=(0, 6))
        self.threshold_spin = ttk.Spinbox(
            top,
            from_=1,
            to=30,
            textvariable=self.at_risk_threshold_var,
            width=6,
        )
        self.threshold_spin.pack(side=tk.LEFT, padx=(0, 8))

        ttk.Button(top, text="Apply", command=self.refresh_at_risk).pack(side=tk.LEFT, padx=(0, 8))
        ttk.Button(top, text="Refresh", command=self.refresh_at_risk).pack(side=tk.LEFT)

        columns = (
            "full_name",
            "telegram_id",
            "total_missing",
            "avg_all_pct",
            "avg_submitted_pct",
        )
        self.at_risk_tree = self._build_tree(
            self.at_risk_tab,
            columns=columns,
            headings=("Student", "Telegram ID", "Missing", "Avg Overall %", "Avg Submitted %"),
            widths=(320, 160, 100, 120, 130),
        )

    def _build_maintenance_tab(self) -> None:
        top = ttk.Frame(self.maintenance_tab)
        top.pack(fill=tk.X, pady=(0, 8))

        ttk.Button(top, text="Initialize Schema", command=self.initialize_schema).pack(
            side=tk.LEFT, padx=(0, 6)
        )
        ttk.Button(top, text="Rebuild All Summaries", command=self.rebuild_all_summaries).pack(
            side=tk.LEFT, padx=(0, 6)
        )
        ttk.Button(top, text="Create DB Backup", command=self.create_backup).pack(
            side=tk.LEFT, padx=(0, 6)
        )
        ttk.Button(top, text="Refresh Sync Log", command=self.refresh_sync_log).pack(side=tk.LEFT)

        columns = ("synced_at", "source", "rows_added", "rows_updated", "notes")
        self.sync_tree = self._build_tree(
            self.maintenance_tab,
            columns=columns,
            headings=("Synced At", "Source", "Rows Added", "Rows Updated", "Notes"),
            widths=(170, 170, 110, 110, 520),
        )

    def _build_tree(
        self,
        parent: ttk.Frame,
        columns: tuple[str, ...],
        headings: tuple[str, ...],
        widths: tuple[int, ...],
    ) -> ttk.Treeview:
        frame = ttk.Frame(parent)
        frame.pack(fill=tk.BOTH, expand=True)

        tree = ttk.Treeview(frame, columns=columns, show="headings")
        tree.grid(row=0, column=0, sticky="nsew")

        y_scroll = ttk.Scrollbar(frame, orient=tk.VERTICAL, command=tree.yview)
        y_scroll.grid(row=0, column=1, sticky="ns")
        x_scroll = ttk.Scrollbar(frame, orient=tk.HORIZONTAL, command=tree.xview)
        x_scroll.grid(row=1, column=0, sticky="ew")
        tree.configure(yscrollcommand=y_scroll.set, xscrollcommand=x_scroll.set)

        for idx, col in enumerate(columns):
            tree.heading(col, text=headings[idx])
            anchor = tk.CENTER if col.endswith("id") or "pct" in col or col == "missing" else tk.W
            tree.column(col, width=widths[idx], anchor=anchor)

        frame.rowconfigure(0, weight=1)
        frame.columnconfigure(0, weight=1)
        return tree

    def _clear_tree(self, tree: ttk.Treeview) -> None:
        tree.delete(*tree.get_children())

    def _set_status(self, message: str) -> None:
        self.status_var.set(message)

    def _clear_student_search(self) -> None:
        self.search_var.set("")
        self.refresh_students()

    def _selected_item_values(self, tree: ttk.Treeview) -> tuple[str, ...] | None:
        selected = tree.selection()
        if not selected:
            return None
        item = tree.item(selected[0])
        return tuple(item.get("values", ()))

    def refresh_all(self) -> None:
        try:
            self.refresh_students()
            self.refresh_flags()
            self.refresh_at_risk()
            self.refresh_sync_log()
            self._set_status("Data refreshed")
        except Exception as exc:  # pragma: no cover - UI runtime safety
            self._handle_error("refreshing data", exc)

    def refresh_students(self) -> None:
        self._clear_tree(self.student_tree)
        search = self.search_var.get().strip().lower()
        course_id = _safe_int(self.course_id_var.get())

        query = """
            WITH agg AS (
                SELECT
                    sub.student_id,
                    a.course_id,
                    SUM(
                        CASE
                            WHEN sub.status = 'Missing'
                              OR sub.score_points = 0
                              OR (
                                   sub.status IN ('Submitted', 'Late', 'Graded')
                                   AND sub.score_points IS NULL
                                 )
                            THEN 1 ELSE 0
                        END
                    ) AS total_missing,
                    ROUND(
                        SUM(COALESCE(sub.score_points, 0)) * 100.0 /
                        NULLIF(SUM(COALESCE(sub.score_max, 0)), 0), 2
                    ) AS avg_all_pct,
                    MAX(sub.updated_at) AS agg_synced
                FROM submissions sub
                JOIN assignments a ON a.id = sub.assignment_id
                GROUP BY sub.student_id, a.course_id
            )
            SELECT
                s.id,
                COALESCE(c.name, '') AS course_name,
                s.lms_id,
                s.full_name,
                COALESCE(s.telegram_id, '') AS telegram_id,
                COALESCE(s.telegram_username, '') AS telegram_username,
                COALESCE(cs.total_missing, agg.total_missing, 0) AS total_missing,
                COALESCE(cs.avg_all_pct, agg.avg_all_pct, 0) AS avg_all_pct,
                COALESCE(cs.last_synced, agg.agg_synced, '') AS last_synced
            FROM students s
            LEFT JOIN enrollments e
              ON e.student_id = s.id
            LEFT JOIN courses c
              ON c.id = e.course_id
            LEFT JOIN course_summaries cs
              ON cs.student_id = s.id
             AND cs.course_id = e.course_id
            LEFT JOIN agg
              ON agg.student_id = s.id
             AND agg.course_id = e.course_id
            WHERE ((? = '')
               OR LOWER(s.full_name) LIKE ?
               OR LOWER(s.lms_id) LIKE ?
               OR LOWER(COALESCE(s.telegram_id, '')) LIKE ?)
              AND (? = 0 OR e.course_id = ?)
            ORDER BY s.full_name COLLATE NOCASE
        """
        pattern = f"%{search}%"

        with self.db_conn() as conn:
            rows = conn.execute(
                query, (search, pattern, pattern, pattern, course_id, course_id)
            ).fetchall()

        for row in rows:
            self.student_tree.insert(
                "",
                tk.END,
                values=(
                    row["id"],
                    row["course_name"],
                    row["lms_id"],
                    row["full_name"],
                    row["telegram_id"],
                    row["telegram_username"],
                    row["total_missing"],
                    f"{_safe_float(row['avg_all_pct']):.2f}",
                    str(row["last_synced"])[:19] if row["last_synced"] else "",
                ),
            )

        self._set_status(f"Loaded {len(rows)} students")

    def unlink_selected_student(self) -> None:
        values = self._selected_item_values(self.student_tree)
        if not values:
            messagebox.showinfo("Unlink", "Select a student first.")
            return

        student_id = _safe_int(values[0])
        full_name = str(values[3])
        telegram_id = str(values[4])
        if not telegram_id:
            messagebox.showinfo("Unlink", f"{full_name} is already unlinked.")
            return

        confirmed = messagebox.askyesno(
            "Confirm unlink",
            f"Unlink Telegram account for:\n\n{full_name}\nTelegram ID: {telegram_id}\n\nContinue?",
        )
        if not confirmed:
            return

        with self.db_conn() as conn:
            result = conn.execute(
                """
                UPDATE students
                SET telegram_id = NULL,
                    telegram_username = NULL
                WHERE id = ? AND telegram_id IS NOT NULL
                """,
                (student_id,),
            )

        if result.rowcount:
            self.refresh_students()
            self._set_status(f"Unlinked {full_name}")
        else:
            self._set_status("No rows changed during unlink")

    def rebuild_selected_summary(self) -> None:
        values = self._selected_item_values(self.student_tree)
        if not values:
            messagebox.showinfo("Rebuild summary", "Select a student first.")
            return

        student_id = _safe_int(values[0])
        full_name = str(values[3])
        selected_course_id = _safe_int(self.course_id_var.get())

        with self.db_conn() as conn:
            course_id = self._resolve_student_course_id(conn, student_id, selected_course_id)
            self._rebuild_summary(conn, student_id, course_id)

        self.refresh_students()
        self.refresh_at_risk()
        self._set_status(f"Summary rebuilt for {full_name}")

    def refresh_flags(self) -> None:
        self._clear_tree(self.flag_tree)
        course_id = _safe_int(self.course_id_var.get())

        base_query = """
            SELECT
                s.id AS student_id,
                s.full_name,
                a.id AS assignment_id,
                a.title AS assignment_title,
                COALESCE(sub.flagged_at, '') AS flagged_at,
                COALESCE(sub.flag_note, '') AS flag_note
            FROM submissions sub
            JOIN students s ON s.id = sub.student_id
            JOIN assignments a ON a.id = sub.assignment_id
            WHERE sub.flagged_by_student = 1
              AND sub.flag_verified = 0
        """
        if course_id > 0:
            query = base_query + """
              AND a.course_id = ?
            ORDER BY sub.flagged_at ASC
            """
            params = (course_id,)
        else:
            query = base_query + """
            ORDER BY sub.flagged_at ASC
            """
            params = ()

        with self.db_conn() as conn:
            rows = conn.execute(query, params).fetchall()

        for row in rows:
            self.flag_tree.insert(
                "",
                tk.END,
                values=(
                    row["student_id"],
                    row["full_name"],
                    row["assignment_id"],
                    row["assignment_title"],
                    str(row["flagged_at"])[:19] if row["flagged_at"] else "",
                    row["flag_note"],
                ),
            )

        self._set_status(f"Loaded {len(rows)} pending flags")

    def verify_selected_flag(self, approved: bool) -> None:
        values = self._selected_item_values(self.flag_tree)
        if not values:
            messagebox.showinfo("Verify flag", "Select a flagged submission first.")
            return

        student_id = _safe_int(values[0])
        assignment_id = _safe_int(values[2])
        reviewer = self.reviewer_var.get().strip() or "Admin GUI"
        new_status = "Submitted" if approved else "Missing"
        action_text = "approved" if approved else "denied"

        with self.db_conn() as conn:
            result = conn.execute(
                """
                UPDATE submissions
                SET status = ?,
                    flag_verified = 1,
                    flag_verified_at = datetime('now'),
                    flag_verified_by = ?,
                    flagged_by_student = 0
                WHERE student_id = ?
                  AND assignment_id = ?
                  AND flagged_by_student = 1
                  AND flag_verified = 0
                """,
                (new_status, reviewer, student_id, assignment_id),
            )

            if result.rowcount:
                row = conn.execute(
                    "SELECT course_id FROM assignments WHERE id = ?",
                    (assignment_id,),
                ).fetchone()
                selected_course_id = _safe_int(self.course_id_var.get())
                course_id = _safe_int(row["course_id"]) if row else self._resolve_student_course_id(
                    conn, student_id, selected_course_id
                )
                self._rebuild_summary(conn, student_id, course_id)

        if result.rowcount:
            self.refresh_flags()
            self.refresh_students()
            self.refresh_at_risk()
            self._set_status(f"Flag processed: {action_text}")
        else:
            self._set_status("Flag was already processed or no longer eligible")

    def refresh_at_risk(self) -> None:
        self._clear_tree(self.at_risk_tree)
        course_id = _safe_int(self.course_id_var.get())
        threshold = max(1, _safe_int(self.at_risk_threshold_var.get()))

        query = """
            WITH agg AS (
                SELECT
                    sub.student_id,
                    a.course_id,
                    SUM(
                        CASE
                            WHEN sub.status = 'Missing'
                              OR sub.score_points = 0
                              OR (
                                   sub.status IN ('Submitted', 'Late', 'Graded')
                                   AND sub.score_points IS NULL
                                 )
                            THEN 1 ELSE 0
                        END
                    ) AS total_missing,
                    ROUND(
                        SUM(COALESCE(sub.score_points, 0)) * 100.0 /
                        NULLIF(SUM(COALESCE(sub.score_max, 0)), 0), 2
                    ) AS avg_all_pct,
                    ROUND(
                        AVG(CASE WHEN sub.score_pct IS NOT NULL THEN sub.score_pct END), 2
                    ) AS avg_submitted_pct
                FROM submissions sub
                JOIN assignments a ON a.id = sub.assignment_id
                GROUP BY sub.student_id, a.course_id
            )
            SELECT
                s.full_name,
                COALESCE(s.telegram_id, '') AS telegram_id,
                COALESCE(cs.total_missing, agg.total_missing, 0) AS total_missing,
                COALESCE(cs.avg_all_pct, agg.avg_all_pct, 0) AS avg_all_pct,
                COALESCE(cs.avg_submitted_pct, agg.avg_submitted_pct, 0) AS avg_submitted_pct
            FROM students s
            LEFT JOIN enrollments e
              ON e.student_id = s.id
            LEFT JOIN course_summaries cs
              ON cs.student_id = s.id
             AND cs.course_id = e.course_id
            LEFT JOIN agg
              ON agg.student_id = s.id
             AND agg.course_id = e.course_id
            WHERE (? = 0 OR e.course_id = ?)
              AND COALESCE(cs.total_missing, agg.total_missing, 0) >= ?
            ORDER BY COALESCE(cs.total_missing, agg.total_missing, 0) DESC,
                     s.full_name COLLATE NOCASE
        """

        with self.db_conn() as conn:
            rows = conn.execute(query, (course_id, course_id, threshold)).fetchall()

        for row in rows:
            self.at_risk_tree.insert(
                "",
                tk.END,
                values=(
                    row["full_name"],
                    row["telegram_id"],
                    row["total_missing"],
                    f"{_safe_float(row['avg_all_pct']):.2f}",
                    f"{_safe_float(row['avg_submitted_pct']):.2f}",
                ),
            )

        self._set_status(f"Loaded {len(rows)} at-risk students")

    def initialize_schema(self) -> None:
        schema_path = self._find_schema_path()
        if not schema_path:
            messagebox.showerror("Schema", "schema.sql not found.")
            return

        confirmed = messagebox.askyesno(
            "Initialize schema",
            f"Execute schema file?\n\n{schema_path}",
        )
        if not confirmed:
            return

        sql_text = schema_path.read_text(encoding="utf-8")
        with self.db_conn() as conn:
            conn.executescript(sql_text)

        self.refresh_all()
        self._set_status("Schema initialized")

    def rebuild_all_summaries(self) -> None:
        course_id = _safe_int(self.course_id_var.get())
        with self.db_conn() as conn:
            if course_id > 0:
                rows = conn.execute(
                    """
                    SELECT DISTINCT e.student_id, e.course_id
                    FROM enrollments e
                    WHERE e.course_id = ?
                    ORDER BY e.student_id
                    """,
                    (course_id,),
                ).fetchall()

                if not rows:
                    rows = conn.execute(
                        """
                        SELECT DISTINCT sub.student_id, a.course_id
                        FROM submissions sub
                        JOIN assignments a ON a.id = sub.assignment_id
                        WHERE a.course_id = ?
                        ORDER BY sub.student_id
                        """,
                        (course_id,),
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
                    ORDER BY student_id
                    """
                ).fetchall()

            count = 0
            for row in rows:
                self._rebuild_summary(
                    conn,
                    _safe_int(row["student_id"]),
                    _safe_int(row["course_id"]),
                )
                count += 1

            conn.execute(
                """
                INSERT INTO sync_log (course_id, source, rows_added, rows_updated, notes)
                VALUES (?, ?, 0, 0, ?)
                """,
                (
                    course_id if course_id > 0 else None,
                    "admin_gui_rebuild",
                    f"Rebuilt summaries for {count} student-course pairs",
                ),
            )

        self.refresh_all()
        self._set_status(f"Rebuilt summaries for {count} student-course pairs")

    def create_backup(self) -> None:
        if not self.db_path.exists():
            messagebox.showerror("Backup", f"Database file not found:\n{self.db_path}")
            return

        backup_dir = self.base_dir / "database" / "backups"
        backup_dir.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = backup_dir / f"class_{stamp}.db"
        shutil.copy2(self.db_path, backup_file)
        self._set_status(f"Backup created: {backup_file.name}")
        self.refresh_sync_log()

    def refresh_sync_log(self) -> None:
        self._clear_tree(self.sync_tree)
        course_id = _safe_int(self.course_id_var.get())

        with self.db_conn() as conn:
            if course_id > 0:
                rows = conn.execute(
                    """
                    SELECT synced_at, source, rows_added, rows_updated, COALESCE(notes, '') AS notes
                    FROM sync_log
                    WHERE course_id = ? OR course_id IS NULL
                    ORDER BY synced_at DESC
                    LIMIT 200
                    """,
                    (course_id,),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT synced_at, source, rows_added, rows_updated, COALESCE(notes, '') AS notes
                    FROM sync_log
                    ORDER BY synced_at DESC
                    LIMIT 200
                    """
                ).fetchall()

        for row in rows:
            self.sync_tree.insert(
                "",
                tk.END,
                values=(
                    str(row["synced_at"])[:19] if row["synced_at"] else "",
                    row["source"],
                    row["rows_added"],
                    row["rows_updated"],
                    row["notes"],
                ),
            )

        self._set_status(f"Loaded {len(rows)} sync log entries")

    def _find_schema_path(self) -> Path | None:
        candidates = [
            self.base_dir / "database" / "schema.sql",
            self.base_dir.parent / "database" / "schema.sql",
        ]
        for candidate in candidates:
            if candidate.exists():
                return candidate
        return None

    def _resolve_student_course_id(
        self, conn: sqlite3.Connection, student_id: int, fallback_course_id: int
    ) -> int:
        if fallback_course_id > 0:
            exists = conn.execute(
                """
                SELECT 1
                FROM enrollments
                WHERE student_id = ? AND course_id = ?
                """,
                (student_id, fallback_course_id),
            ).fetchone()
            if exists:
                return fallback_course_id

        row = conn.execute(
            """
            SELECT course_id
            FROM enrollments
            WHERE student_id = ?
            ORDER BY enrolled_at DESC
            LIMIT 1
            """,
            (student_id,),
        ).fetchone()
        if row:
            return _safe_int(row["course_id"])
        return max(1, fallback_course_id)

    def _rebuild_summary(self, conn: sqlite3.Connection, student_id: int, course_id: int) -> None:
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
                points_earned, points_possible, last_synced
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
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

    def _handle_error(self, action: str, exc: Exception) -> None:
        self._set_status(f"Error while {action}: {exc}")
        messagebox.showerror("Error", f"An error occurred while {action}:\n\n{exc}")


def resolve_db_path(base_dir: Path) -> Path:
    if load_dotenv:
        load_dotenv(base_dir / ".env")
    raw = os.getenv("DB_PATH", "database/class.db")
    candidate = Path(raw)
    if not candidate.is_absolute():
        candidate = base_dir / candidate
    return candidate.resolve()


def main() -> None:
    base_dir = Path(__file__).resolve().parent
    db_path = resolve_db_path(base_dir)

    app = TeacherAdminDashboard(db_path=db_path, course_id=0)
    app.mainloop()


if __name__ == "__main__":
    main()
