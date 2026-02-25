from __future__ import annotations

from pathlib import Path

from sync.learner_data.settings import resolve_credentials_path, resolve_token_path


def get_classroom_service(
    credentials_file: str | Path | None = None,
    token_file: str | Path | None = None,
):
    credentials_path = resolve_credentials_path(credentials_file)
    token_path = resolve_token_path(token_file)

    try:
        from learner_data_writer.get_classroom_service import get_classroom_service as _legacy_get_service
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Google Classroom dependencies are missing. "
            "Install requirements before running sync."
        ) from exc

    return _legacy_get_service(str(credentials_path), str(token_path))
