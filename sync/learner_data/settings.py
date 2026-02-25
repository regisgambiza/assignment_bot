from __future__ import annotations

import os
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional dependency
    load_dotenv = None


BASE_DIR = Path(__file__).resolve().parents[2]
if load_dotenv:
    load_dotenv(BASE_DIR / ".env", override=False)


def _resolve_path(raw_value: str | Path | None, default_relative: str) -> Path:
    if raw_value is None or str(raw_value).strip() == "":
        path = BASE_DIR / default_relative
    else:
        path = Path(raw_value)
        if not path.is_absolute():
            path = BASE_DIR / path
    return path.resolve()


def resolve_credentials_path(path: str | Path | None = None) -> Path:
    return _resolve_path(
        path or os.getenv("GOOGLE_CLASSROOM_CREDENTIALS_FILE"),
        "learner_data_writer/client_secrets.json",
    )


def resolve_token_path(path: str | Path | None = None) -> Path:
    return _resolve_path(
        path or os.getenv("GOOGLE_CLASSROOM_TOKEN_FILE"),
        "learner_data_writer/token.json",
    )


def resolve_db_path(path: str | Path | None = None) -> Path:
    return _resolve_path(path or os.getenv("DB_PATH"), "database/class.db")


def resolve_schema_path(path: str | Path | None = None) -> Path:
    return _resolve_path(path, "database/schema.sql")


def classroom_school_name() -> str:
    return (os.getenv("CLASSROOM_SYNC_SCHOOL_NAME") or "School").strip() or "School"


def classroom_sync_source() -> str:
    raw = (os.getenv("CLASSROOM_SYNC_SOURCE") or "google_classroom_sync").strip()
    return raw or "google_classroom_sync"
