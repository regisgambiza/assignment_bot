from __future__ import annotations


def get_all_students(service, course_id: str):
    from learner_data_writer.get_all_students import get_all_students as _legacy_get_all_students

    return _legacy_get_all_students(service, course_id)
