"""
Data models for learner data synchronization
"""
from dataclasses import dataclass
from datetime import datetime

@dataclass
class Course:
    id: str
    name: str
    section: str
    description: str
    owner_id: str

@dataclass
class Student:
    id: str
    full_name: str
    email: str
    course_id: str

@dataclass
class CourseWork:
    id: str
    title: str
    description: str
    due_date: datetime
    course_id: str