-- ============================================================
-- ASSIGNMENT TRACKER — Database Schema
-- Course: 8/1 Mathematics
-- ============================================================

CREATE TABLE IF NOT EXISTS schools (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    name       TEXT    NOT NULL,
    created_at TEXT    DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS courses (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    lms_id     TEXT    UNIQUE,
    name       TEXT    NOT NULL,
    school_id  INTEGER REFERENCES schools(id),
    created_at TEXT    DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS students (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    lms_id            TEXT    UNIQUE NOT NULL,
    full_name         TEXT    NOT NULL,
    telegram_id       TEXT    UNIQUE,
    telegram_username TEXT,
    phone             TEXT,
    created_at        TEXT    DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS enrollments (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    student_id  INTEGER NOT NULL REFERENCES students(id)  ON DELETE CASCADE,
    course_id   INTEGER NOT NULL REFERENCES courses(id)   ON DELETE CASCADE,
    enrolled_at TEXT    DEFAULT (datetime('now')),
    UNIQUE (student_id, course_id)
);

CREATE TABLE IF NOT EXISTS assignments (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    lms_id     TEXT    UNIQUE NOT NULL,
    course_id  INTEGER NOT NULL REFERENCES courses(id) ON DELETE CASCADE,
    title      TEXT    NOT NULL,
    max_score  REAL,
    due_date   TEXT,
    created_at TEXT    NOT NULL,
    is_active  INTEGER DEFAULT 1
);

CREATE TABLE IF NOT EXISTS submissions (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    student_id          INTEGER NOT NULL REFERENCES students(id)    ON DELETE CASCADE,
    assignment_id       INTEGER NOT NULL REFERENCES assignments(id) ON DELETE CASCADE,
    status              TEXT    NOT NULL DEFAULT 'Missing'
                        CHECK (status IN ('Missing','Submitted','Late','Graded','Flagged')),
    score_raw           TEXT,
    score_points        REAL,
    score_max           REAL,
    score_pct           REAL,
    flagged_by_student  INTEGER DEFAULT 0,
    flagged_at          TEXT,
    flag_note           TEXT,
    flag_verified       INTEGER DEFAULT 0,
    flag_verified_at    TEXT,
    flag_verified_by    TEXT,
    submitted_at        TEXT,
    proof_file_id       TEXT,
    proof_file_type     TEXT
                        CHECK (proof_file_type IN ('photo','document')),
    proof_caption       TEXT,
    proof_uploaded_at   TEXT,
    updated_at          TEXT    DEFAULT (datetime('now')),
    UNIQUE (student_id, assignment_id)
);

CREATE TABLE IF NOT EXISTS course_summaries (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    student_id        INTEGER NOT NULL REFERENCES students(id)  ON DELETE CASCADE,
    course_id         INTEGER NOT NULL REFERENCES courses(id)   ON DELETE CASCADE,
    total_assigned    INTEGER DEFAULT 0,
    total_submitted   INTEGER DEFAULT 0,
    total_missing     INTEGER DEFAULT 0,
    total_late        INTEGER DEFAULT 0,
    total_graded      INTEGER DEFAULT 0,
    avg_submitted_pct REAL,
    avg_all_pct       REAL,
    points_earned     REAL,
    points_possible   REAL,
    needs_rebuild     INTEGER DEFAULT 1,
    last_synced       TEXT    DEFAULT (datetime('now')),
    UNIQUE (student_id, course_id)
);

CREATE TABLE IF NOT EXISTS sync_log (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    course_id    INTEGER REFERENCES courses(id),
    source       TEXT,
    rows_added   INTEGER DEFAULT 0,
    rows_updated INTEGER DEFAULT 0,
    synced_at    TEXT    DEFAULT (datetime('now')),
    notes        TEXT
);

CREATE TABLE IF NOT EXISTS app_meta (
    key         TEXT PRIMARY KEY,
    value       TEXT,
    updated_at  TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS campaign_jobs (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    created_by     TEXT,
    template_key   TEXT    NOT NULL,
    template_text  TEXT,
    run_at         TEXT    NOT NULL,
    status         TEXT    NOT NULL DEFAULT 'pending'
                   CHECK (status IN ('pending','running','completed','failed')),
    target_count   INTEGER DEFAULT 0,
    sent_count     INTEGER DEFAULT 0,
    schedule_label TEXT,
    error          TEXT,
    started_at     TEXT,
    finished_at    TEXT,
    created_at     TEXT    DEFAULT (datetime('now'))
);

-- ── Indexes ───────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_submissions_student  ON submissions(student_id);
CREATE INDEX IF NOT EXISTS idx_submissions_status   ON submissions(status);
CREATE INDEX IF NOT EXISTS idx_submissions_flagged  ON submissions(flagged_by_student);
CREATE INDEX IF NOT EXISTS idx_assignments_course   ON assignments(course_id);
CREATE INDEX IF NOT EXISTS idx_students_telegram    ON students(telegram_id);
CREATE INDEX IF NOT EXISTS idx_campaign_jobs_due    ON campaign_jobs(status, run_at);
CREATE INDEX IF NOT EXISTS idx_course_summaries_dirty ON course_summaries(needs_rebuild);

-- ── Views ─────────────────────────────────────────────────
CREATE VIEW IF NOT EXISTS v_missing_work AS
SELECT s.full_name, s.telegram_id, c.name AS course_name,
       a.title AS assignment_title, a.due_date, a.id AS assignment_id,
       sub.student_id, sub.flagged_by_student
FROM   submissions sub
JOIN   students    s  ON s.id  = sub.student_id
JOIN   assignments a  ON a.id  = sub.assignment_id
JOIN   courses     c  ON c.id  = a.course_id
WHERE  sub.status = 'Missing'
ORDER  BY a.created_at ASC;

CREATE VIEW IF NOT EXISTS v_student_grades AS
SELECT s.full_name, c.name AS course_name,
       a.title, a.due_date, a.id AS assignment_id,
       sub.status, sub.score_raw, sub.score_pct,
       sub.student_id, sub.flagged_by_student
FROM   submissions sub
JOIN   students    s  ON s.id  = sub.student_id
JOIN   assignments a  ON a.id  = sub.assignment_id
JOIN   courses     c  ON c.id  = a.course_id
ORDER  BY a.created_at DESC;

CREATE VIEW IF NOT EXISTS v_at_risk_students AS
SELECT s.full_name, s.telegram_id, c.name AS course_name,
       cs.total_missing, cs.avg_all_pct, cs.avg_submitted_pct
FROM   course_summaries cs
JOIN   students s ON s.id = cs.student_id
JOIN   courses  c ON c.id = cs.course_id
WHERE  cs.total_missing >= 3
ORDER  BY cs.total_missing DESC;

CREATE VIEW IF NOT EXISTS v_pending_flags AS
SELECT s.full_name, s.telegram_id, s.id AS student_id,
       a.title AS assignment_title, a.id AS assignment_id,
       c.name  AS course_name,
       sub.flagged_at, sub.flag_note,
       sub.proof_file_id, sub.proof_file_type,
       sub.proof_caption, sub.proof_uploaded_at
FROM   submissions sub
JOIN   students    s  ON s.id  = sub.student_id
JOIN   assignments a  ON a.id  = sub.assignment_id
JOIN   courses     c  ON c.id  = a.course_id
WHERE  sub.flagged_by_student = 1
AND    sub.flag_verified      = 0
ORDER  BY sub.flagged_at ASC;

-- Dirty-mark triggers to keep summary cache fresh
CREATE TRIGGER IF NOT EXISTS trg_submissions_insert_dirty
AFTER INSERT ON submissions
BEGIN
  UPDATE course_summaries
  SET needs_rebuild = 1
  WHERE student_id = NEW.student_id
    AND course_id = (
      SELECT course_id FROM assignments WHERE id = NEW.assignment_id
    );
END;

CREATE TRIGGER IF NOT EXISTS trg_submissions_update_dirty
AFTER UPDATE ON submissions
BEGIN
  UPDATE course_summaries
  SET needs_rebuild = 1
  WHERE student_id = NEW.student_id
    AND course_id = (
      SELECT course_id FROM assignments WHERE id = NEW.assignment_id
    );

  UPDATE course_summaries
  SET needs_rebuild = 1
  WHERE student_id = OLD.student_id
    AND course_id = (
      SELECT course_id FROM assignments WHERE id = OLD.assignment_id
    );
END;

CREATE TRIGGER IF NOT EXISTS trg_submissions_delete_dirty
AFTER DELETE ON submissions
BEGIN
  UPDATE course_summaries
  SET needs_rebuild = 1
  WHERE student_id = OLD.student_id
    AND course_id = (
      SELECT course_id FROM assignments WHERE id = OLD.assignment_id
    );
END;

CREATE TRIGGER IF NOT EXISTS trg_assignments_insert_dirty
AFTER INSERT ON assignments
BEGIN
  UPDATE course_summaries
  SET needs_rebuild = 1
  WHERE course_id = NEW.course_id;
END;

CREATE TRIGGER IF NOT EXISTS trg_assignments_update_dirty
AFTER UPDATE ON assignments
BEGIN
  UPDATE course_summaries
  SET needs_rebuild = 1
  WHERE course_id = NEW.course_id OR course_id = OLD.course_id;
END;

CREATE TRIGGER IF NOT EXISTS trg_assignments_delete_dirty
AFTER DELETE ON assignments
BEGIN
  UPDATE course_summaries
  SET needs_rebuild = 1
  WHERE course_id = OLD.course_id;
END;
