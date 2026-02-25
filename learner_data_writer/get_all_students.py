import logging

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-5s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("main")

def get_all_students(service, course_id):
    logger.debug("Fetching students for course_id=%s", course_id)
    students = []
    page_token = None
    while True:
        response = (
            service.courses()
            .students()
            .list(courseId=course_id, pageToken=page_token, pageSize=100)
            .execute()
        )
        students.extend(response.get("students", []))
        logger.debug("Fetched %d students so far for course_id=%s", len(students), course_id)
        page_token = response.get("nextPageToken")
        if not page_token:
            break
    return students