from __future__ import annotations


def get_all_courses(service):
    from learner_data_writer.get_all_courses import get_all_courses as _legacy_get_all_courses

    return _legacy_get_all_courses(service)
