"""
Google Classroom integration components
"""
from .client import get_classroom_service
from .courses import get_all_courses
from .coursework import get_all_coursework
from .students import get_all_students

__all__ = [
    'get_classroom_service',
    'get_all_courses',
    'get_all_coursework',
    'get_all_students'
]