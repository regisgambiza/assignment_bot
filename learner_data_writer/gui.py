# gui.py
import threading
import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext, filedialog
import os
import sys
import logging
import datetime
import subprocess
import json
import urllib.request
import urllib.error
from pathlib import Path

try:
    from .get_classroom_service import get_classroom_service
    from .get_all_courses import get_all_courses
    from .get_all_students import get_all_students
except ImportError:  # pragma: no cover - script compatibility
    from get_classroom_service import get_classroom_service
    from get_all_courses import get_all_courses
    from get_all_students import get_all_students
import main

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-5s | %(name)s | %(message)s")
logger = logging.getLogger("gui")


class DateSelector(ttk.Frame):
    """A small date selector widget. Uses tkcalendar.DateEntry when installed,
    otherwise falls back to year/month/day comboboxes."""
    def __init__(self, parent, initial_date: str = None):
        super().__init__(parent)

        # ✅ DEFAULT TO TODAY
        if initial_date is None:
            initial_date = datetime.date.today().isoformat()

        try:
            from tkcalendar import DateEntry
        except Exception:
            DateEntry = None

        self._has_dateentry = DateEntry is not None

        if self._has_dateentry:
            self._widget = DateEntry(self, date_pattern='yyyy-mm-dd')
            try:
                y, m, d = [int(x) for x in initial_date.split('-')]
                self._widget.set_date(datetime.date(y, m, d))
            except Exception:
                pass
            self._widget.pack()
        else:
            today = datetime.date.today()
            years = [str(y) for y in range(today.year - 5, today.year + 6)]
            months = [f"{m:02d}" for m in range(1, 13)]
            days = [f"{d:02d}" for d in range(1, 32)]

            self.year_var = tk.StringVar()
            self.month_var = tk.StringVar()
            self.day_var = tk.StringVar()

            self.year_cb = ttk.Combobox(self, values=years, width=6, textvariable=self.year_var, state='readonly')
            self.month_cb = ttk.Combobox(self, values=months, width=4, textvariable=self.month_var, state='readonly')
            self.day_cb = ttk.Combobox(self, values=days, width=4, textvariable=self.day_var, state='readonly')

            try:
                y, m, d = [int(x) for x in initial_date.split('-')]
                self.year_var.set(str(y))
                self.month_var.set(f"{m:02d}")
                self.day_var.set(f"{d:02d}")
            except Exception:
                pass

            self.year_cb.pack(side=tk.LEFT)
            self.month_cb.pack(side=tk.LEFT, padx=(4, 0))
            self.day_cb.pack(side=tk.LEFT, padx=(4, 0))

    def get(self) -> str:
        """Return date as ISO string YYYY-MM-DD, or empty string if not set."""
        if self._has_dateentry:
            try:
                d = self._widget.get_date()
                return d.isoformat()
            except Exception:
                return ""
        else:
            y = self.year_var.get()
            m = self.month_var.get()
            d = self.day_var.get()
            if not (y and m and d):
                return ""
            return f"{y}-{m}-{d}"


class AnalyzerGUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Learner Performance Monitor")
        self.geometry("800x600")
        project_root = Path(__file__).resolve().parents[1]
        default_db_path = (project_root / os.getenv("DB_PATH", "database/class.db")).resolve()
        default_schema_path = (project_root / "database" / "schema.sql").resolve()

        frm = ttk.Frame(self)
        frm.pack(fill=tk.X, padx=8, pady=6)

        ttk.Label(frm, text="Credentials file:").grid(row=0, column=0, sticky=tk.W)
        self.credentials_var = tk.StringVar(value="credentials.json")
        ttk.Entry(frm, textvariable=self.credentials_var, width=40).grid(row=0, column=1, sticky=tk.W)
        ttk.Button(frm, text="Browse", command=self.browse_credentials).grid(row=0, column=2)

        ttk.Label(frm, text="Token file:").grid(row=1, column=0, sticky=tk.W)
        self.token_var = tk.StringVar(value="token.json")
        ttk.Entry(frm, textvariable=self.token_var, width=40).grid(row=1, column=1, sticky=tk.W)
        ttk.Button(frm, text="Browse", command=self.browse_token).grid(row=1, column=2)
        ttk.Button(frm, text="Reauthenticate", command=self.reauthenticate).grid(row=1, column=3)

        ttk.Label(frm, text="Ollama model:").grid(row=2, column=0, sticky=tk.W)
        self.model_var = tk.StringVar(value="gpt-oss:20b")
        self.model_cb = ttk.Combobox(frm, textvariable=self.model_var, width=40, state="readonly")
        self.model_cb.grid(row=2, column=1, sticky=tk.W)
        ttk.Button(frm, text="Load Models", command=self.load_models).grid(row=2, column=2)

        ttk.Label(frm, text="Batch size:").grid(row=3, column=0, sticky=tk.W)
        self.batch_size_var = tk.StringVar(value="2")
        ttk.Entry(frm, textvariable=self.batch_size_var, width=10).grid(row=3, column=1, sticky=tk.W)

        ttk.Label(frm, text="Reports dir:").grid(row=4, column=0, sticky=tk.W)
        self.reports_dir_var = tk.StringVar(value="reports")
        ttk.Entry(frm, textvariable=self.reports_dir_var, width=40).grid(row=4, column=1, sticky=tk.W)

        self.direct_db_sync_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            frm,
            text="Direct sync to DB (no TXT import)",
            variable=self.direct_db_sync_var,
            command=self.on_direct_sync_toggle,
        ).grid(row=5, column=0, columnspan=2, sticky=tk.W)

        ttk.Label(frm, text="DB path:").grid(row=6, column=0, sticky=tk.W)
        self.db_path_var = tk.StringVar(value=str(default_db_path))
        self.db_path_entry = ttk.Entry(frm, textvariable=self.db_path_var, width=40)
        self.db_path_entry.grid(row=6, column=1, sticky=tk.W)

        ttk.Label(frm, text="DB schema path:").grid(row=7, column=0, sticky=tk.W)
        self.db_schema_var = tk.StringVar(value=str(default_schema_path))
        self.db_schema_entry = ttk.Entry(frm, textvariable=self.db_schema_var, width=40)
        self.db_schema_entry.grid(row=7, column=1, sticky=tk.W)

        ttk.Label(frm, text="DB school name:").grid(row=8, column=0, sticky=tk.W)
        self.db_school_var = tk.StringVar(value="School")
        self.db_school_entry = ttk.Entry(frm, textvariable=self.db_school_var, width=40)
        self.db_school_entry.grid(row=8, column=1, sticky=tk.W)

        ttk.Label(frm, text="AI max retries:").grid(row=9, column=0, sticky=tk.W)
        self.ai_retries_var = tk.StringVar(value="5")
        ttk.Entry(frm, textvariable=self.ai_retries_var, width=10).grid(row=9, column=1, sticky=tk.W)

        self.include_teacher_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(frm, text="Include AI Teacher Reports", variable=self.include_teacher_var)\
            .grid(row=10, column=0, columnspan=2, sticky=tk.W)

        ttk.Label(frm, text="Start date (leave blank for no filter):").grid(row=11, column=0, sticky=tk.W)
        self.start_selector = DateSelector(frm)
        self.start_selector.grid(row=11, column=1, sticky=tk.W)

        ttk.Label(frm, text="End date (leave blank for no filter):").grid(row=12, column=0, sticky=tk.W)
        self.end_selector = DateSelector(frm)
        self.end_selector.grid(row=12, column=1, sticky=tk.W)

        self.mode_var = tk.IntVar(value=1)
        modes = [(1, "Analyze all classes"),
                 (2, "Analyze single class"),
                 (3, "Analyze a single student")]
        for i, (val, txt) in enumerate(modes):
            ttk.Radiobutton(frm, text=txt, variable=self.mode_var,
                            value=val, command=self.on_mode_change)\
                .grid(row=13+i, column=0, columnspan=2, sticky=tk.W)

        ttk.Label(frm, text="Course:").grid(row=16, column=0, sticky=tk.W)
        self.course_cb = ttk.Combobox(frm, width=40, state='readonly')
        self.course_cb.grid(row=16, column=1, sticky=tk.W)
        self.load_courses_btn = ttk.Button(frm, text="Load Courses", command=self.load_courses)
        self.load_courses_btn.grid(row=16, column=2)

        ttk.Label(frm, text="Student:").grid(row=17, column=0, sticky=tk.W)
        self.student_cb = ttk.Combobox(frm, width=40, state='readonly')
        self.student_cb.grid(row=17, column=1, sticky=tk.W)
        self.load_students_btn = ttk.Button(frm, text="Load Students", command=self.load_students)
        self.load_students_btn.grid(row=17, column=2)

        ttk.Label(frm, text="Additional context:").grid(row=18, column=0, sticky=tk.NW)
        self.context_txt = scrolledtext.ScrolledText(frm, width=50, height=4)
        self.context_txt.grid(row=18, column=1, columnspan=2, sticky=tk.W)

        btn_frm = ttk.Frame(self)
        btn_frm.pack(pady=10)
        self.run_btn = ttk.Button(btn_frm, text="Run Analysis", command=self.on_run)
        self.run_btn.pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frm, text="Open Reports Folder",
                   command=self.open_reports_folder).pack(side=tk.LEFT, padx=5)

        self.log_txt = scrolledtext.ScrolledText(self, height=10)
        self.log_txt.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)

        self.on_mode_change()
        self.on_direct_sync_toggle()
        self.courses = []
        self.students = []
        self.models = []

    def log(self, msg):
        self.log_txt.insert(tk.END, msg + "\n")
        self.log_txt.see(tk.END)

    def browse_credentials(self):
        path = filedialog.askopenfilename(filetypes=[("JSON files", "*.json")])
        if path:
            self.credentials_var.set(path)

    def browse_token(self):
        path = filedialog.askopenfilename(filetypes=[("JSON files", "*.json")])
        if path:
            self.token_var.set(path)

    def reauthenticate(self):
        token_path = self.token_var.get()
        if os.path.exists(token_path):
            try:
                os.remove(token_path)
                self.log(f"Deleted token file: {token_path}. Please load courses to authenticate again.")
            except Exception as e:
                self.log(f"Error deleting token file: {str(e)}")
                messagebox.showerror("Error", str(e))
        self.load_courses()

    def load_models(self):
        self.log("Loading Ollama models...")
        try:
            with urllib.request.urlopen("http://localhost:11434/api/tags") as resp:
                data = json.loads(resp.read())
                self.models = [m['name'] for m in data.get('models', [])]
                self.model_cb['values'] = self.models
                if self.models:
                    self.model_cb.current(0)
        except Exception as e:
            messagebox.showerror("Error", str(e))

    def on_mode_change(self):
        mode = self.mode_var.get()
        if mode == 1:
            self.course_cb.configure(state=tk.DISABLED)
            self.student_cb.configure(state=tk.DISABLED)
            self.course_cb.set("")
            self.student_cb.set("")
        elif mode == 2:
            self.course_cb.configure(state='readonly')
            self.student_cb.configure(state=tk.DISABLED)
            self.student_cb.set("")
        else:
            self.course_cb.configure(state='readonly')
            self.student_cb.configure(state='readonly')

    def on_direct_sync_toggle(self):
        enabled = self.direct_db_sync_var.get()
        state = "normal" if enabled else "disabled"
        self.db_path_entry.configure(state=state)
        self.db_schema_entry.configure(state=state)
        self.db_school_entry.configure(state=state)

    def load_courses(self):
        service = get_classroom_service(self.credentials_var.get(), self.token_var.get())
        self.courses = get_all_courses(service)
        names = [f"{c['name']} ({c['id']})" for c in self.courses]
        self.course_cb['values'] = names
        if names:
            self.course_cb.current(0)

    def load_students(self):
        sel = self.course_cb.get()
        if not sel:
            return
        course_id = sel.split('(')[-1].strip(')')
        service = get_classroom_service(self.credentials_var.get(), self.token_var.get())
        self.students = get_all_students(service, course_id)
        names = [f"{s['profile']['name']['fullName']} ({s['userId']})" for s in self.students]
        self.student_cb['values'] = names
        if names:
            self.student_cb.current(0)

    # Updated gui.py on_run method to properly extract course_id and student_id

    def on_run(self):
        start_date = self.start_selector.get() or None
        end_date = self.end_selector.get() or None

        mode = self.mode_var.get()
        course_id = None
        student_id = None

        if mode in [2, 3]:
            course_sel = self.course_cb.get()
            if not course_sel:
                messagebox.showerror("Error", "Please select a course.")
                return
            # Extract course_id from "name (id)" format
            try:
                course_id = course_sel.split('(')[-1].rstrip(')')
            except Exception:
                messagebox.showerror("Error", "Invalid course selection.")
                return

        if mode == 3:
            student_sel = self.student_cb.get()
            if not student_sel:
                messagebox.showerror("Error", "Please select a student.")
                return
            # Extract student_id from "name (id)" format
            try:
                student_id = student_sel.split('(')[-1].rstrip(')')
            except Exception:
                messagebox.showerror("Error", "Invalid student selection.")
                return

        def target():
            self.run_btn.configure(state="disabled")
            try:
                if self.direct_db_sync_var.get():
                    if not self.db_path_var.get().strip():
                        raise ValueError("DB path is required when direct DB sync is enabled.")
                    if not self.db_schema_var.get().strip():
                        raise ValueError("DB schema path is required when direct DB sync is enabled.")

                main.run_with_params(
                    credentials=self.credentials_var.get(),
                    token=self.token_var.get(),
                    ollama_model=self.model_var.get(),
                    start_date=start_date,
                    end_date=end_date,
                    mode_choice=mode,
                    course_id=course_id,
                    student_id=student_id,
                    additional_context=self.context_txt.get("1.0", tk.END).strip() or None,
                    reports_dir=self.reports_dir_var.get(),
                    ai_max_retries=self.ai_retries_var.get(),
                    batch_size=self.batch_size_var.get(),
                    include_teacher_reports=self.include_teacher_var.get(),
                    sync_direct_to_db=self.direct_db_sync_var.get(),
                    db_path=self.db_path_var.get().strip(),
                    schema_path=self.db_schema_var.get().strip(),
                    db_school_name=self.db_school_var.get().strip() or "School",
                )
            except Exception as e:
                self.log(f"Error during analysis: {str(e)}")
                messagebox.showerror("Analysis Error", str(e))
            finally:
                self.run_btn.configure(state="normal")

        threading.Thread(target=target, daemon=True).start()
    def open_reports_folder(self):
        reports_dir = os.path.abspath(self.reports_dir_var.get())
        os.makedirs(reports_dir, exist_ok=True)
        if os.name == 'nt':
            subprocess.Popen(['explorer', reports_dir])
        else:
            subprocess.Popen(['xdg-open', reports_dir])


if __name__ == "__main__":
    app = AnalyzerGUI()
    app.mainloop()
