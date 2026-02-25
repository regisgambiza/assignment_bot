import logging
from datetime import datetime, timezone

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-5s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("main")


def _parse_google_dt(value: str) -> datetime | None:
    text = (value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _coerce_bound(value: str | None, end_of_day: bool = False) -> datetime | None:
    text = (value or "").strip()
    if not text:
        return None
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        if end_of_day:
            dt = dt.replace(hour=23, minute=59, second=59, microsecond=999999)
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def get_all_coursework(service, course_id, start_date=None, end_date=None):
    start_bound = _coerce_bound(start_date)
    end_bound = _coerce_bound(end_date, end_of_day=True)

    coursework = []
    page_token = None
    while True:
        response = service.courses().courseWork().list(
            courseId=course_id, pageToken=page_token, pageSize=100
        ).execute()
        for cw in response.get("courseWork", []):
            created = cw.get("creationTime")  # e.g. "2025-09-28T10:30:00Z"
            if start_bound or end_bound:
                created_ts = _parse_google_dt(created or "")
                if not created_ts:
                    continue
                if start_bound and created_ts < start_bound:
                    continue
                if end_bound and created_ts > end_bound:
                    continue
            coursework.append(cw)
        page_token = response.get("nextPageToken")
        if not page_token:
            break
    return coursework
