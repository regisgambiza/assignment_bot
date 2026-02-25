from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from sync.learner_data.classroom import get_all_courses, get_classroom_service
from sync.learner_data.settings import (
    classroom_school_name,
    classroom_sync_source,
    resolve_credentials_path,
    resolve_db_path,
    resolve_schema_path,
    resolve_token_path,
)

ALLOWED_DAY_WINDOWS = {"7", "30", "90", "180", "all", "custom"}


@dataclass
class SyncTotals:
    courses_seen: int = 0
    courses_synced: int = 0
    students_seen: int = 0
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

    def apply_course_stats(self, stats: dict[str, Any]) -> None:
        self.courses_added += _to_int(stats.get("courses_added"))
        self.courses_updated += _to_int(stats.get("courses_updated"))
        self.students_added += _to_int(stats.get("students_added"))
        self.students_updated += _to_int(stats.get("students_updated"))
        self.enrollments_added += _to_int(stats.get("enrollments_added"))
        self.assignments_added += _to_int(stats.get("assignments_added"))
        self.assignments_updated += _to_int(stats.get("assignments_updated"))
        self.submissions_added += _to_int(stats.get("submissions_added"))
        self.submissions_updated += _to_int(stats.get("submissions_updated"))
        self.summaries_upserted += _to_int(stats.get("summaries_upserted"))
        self.sync_logs_added += _to_int(stats.get("sync_logs_added"))


def _to_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def normalize_days(days: str | int | None) -> str:
    text = str(days or "30").strip().lower()
    if text not in ALLOWED_DAY_WINDOWS:
        raise ValueError(f"Unsupported days value: {text}")
    return text


def _normalize_date(value: str | None, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{field_name} is required for custom date range")
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be a valid ISO date (YYYY-MM-DD)") from exc
    return parsed.date().isoformat()


def _resolve_window(
    days: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> tuple[str | None, str | None]:
    if days == "custom":
        start_iso = _normalize_date(start_date, "start_date")
        end_iso = _normalize_date(end_date, "end_date")
        if start_iso > end_iso:
            raise ValueError("start_date cannot be after end_date")
        return start_iso, end_iso

    if days == "all":
        return None, None

    now_utc = datetime.now(timezone.utc)
    start_date = (now_utc - timedelta(days=_to_int(days))).date().isoformat()
    end_date = now_utc.date().isoformat()
    return start_date, end_date


def _course_matches(course: dict[str, Any], include_course_ids: set[str] | None) -> bool:
    if not include_course_ids:
        return True
    return str(course.get("id", "")).strip() in include_course_ids


def sync_all_learners(
    days: str | int = "30",
    *,
    start_date: str | None = None,
    end_date: str | None = None,
    db_path: str | Path | None = None,
    schema_path: str | Path | None = None,
    credentials_file: str | Path | None = None,
    token_file: str | Path | None = None,
    school_name: str | None = None,
    source: str | None = None,
    include_course_ids: list[str] | None = None,
) -> dict[str, Any]:
    normalized_days = str(days or "30").strip().lower()
    normalized_days = normalize_days(normalized_days)
    start_date, end_date = _resolve_window(normalized_days, start_date, end_date)

    db_file = resolve_db_path(db_path)
    schema_file = resolve_schema_path(schema_path)
    credentials_path = resolve_credentials_path(credentials_file)
    token_path = resolve_token_path(token_file)
    source_tag = (source or classroom_sync_source()).strip() or "google_classroom_sync"
    school = (school_name or classroom_school_name()).strip() or "School"

    if not db_file.exists():
        raise FileNotFoundError(f"Database file not found: {db_file}")
    if not schema_file.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_file}")
    if not credentials_path.exists():
        raise FileNotFoundError(
            "Google Classroom credentials file not found: "
            f"{credentials_path}. Set GOOGLE_CLASSROOM_CREDENTIALS_FILE in .env."
        )

    token_path.parent.mkdir(parents=True, exist_ok=True)

    service = get_classroom_service(credentials_file=credentials_path, token_file=token_path)
    courses = sorted(
        get_all_courses(service),
        key=lambda item: str(item.get("name") or "").lower(),
    )

    selected_courses: set[str] | None = None
    if include_course_ids:
        selected_courses = {str(item).strip() for item in include_course_ids if str(item).strip()}

    totals = SyncTotals()
    course_results: list[dict[str, Any]] = []

    from learner_data_writer.analyse_students import analyse_students
    from learner_data_writer.sync_analysis_to_class_db import sync_course_analysis_to_db

    for course in courses:
        if not _course_matches(course, selected_courses):
            continue

        totals.courses_seen += 1
        analysis = analyse_students(
            service=service,
            course=course,
            selected_student_id=None,
            additional_context=None,
            start_date=start_date,
            end_date=end_date,
        )

        student_count = len(analysis)
        totals.students_seen += student_count
        if student_count == 0:
            course_results.append(
                {
                    "course_id": str(course.get("id", "")),
                    "course_name": str(course.get("name", "")),
                    "students": 0,
                    "synced": False,
                }
            )
            continue

        sync_stats = sync_course_analysis_to_db(
            course=course,
            student_analysis=analysis,
            db_path=str(db_file),
            schema_path=str(schema_file),
            school_name=school,
            source=source_tag,
            dry_run=False,
        )
        totals.courses_synced += 1
        totals.apply_course_stats(sync_stats)
        course_results.append(
            {
                "course_id": str(course.get("id", "")),
                "course_name": str(course.get("name", "")),
                "students": student_count,
                "synced": True,
                "stats": sync_stats,
            }
        )

    stats = asdict(totals)
    message = (
        f"Synced {stats['courses_synced']} course(s); "
        f"submissions added={stats['submissions_added']}, "
        f"updated={stats['submissions_updated']}."
    )

    return {
        "ok": True,
        "days": normalized_days,
        "start_date": start_date,
        "end_date": end_date,
        "db_path": str(db_file),
        "schema_path": str(schema_file),
        "source": source_tag,
        "school_name": school,
        "stats": stats,
        "courses": course_results,
        "message": message,
    }
