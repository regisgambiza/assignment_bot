from __future__ import annotations


def get_all_coursework(service, course_id: str, start_date: str | None = None, end_date: str | None = None):
    from learner_data_writer.get_all_coursework import get_all_coursework as _legacy_get_all_coursework

    return _legacy_get_all_coursework(service, course_id, start_date=start_date, end_date=end_date)
