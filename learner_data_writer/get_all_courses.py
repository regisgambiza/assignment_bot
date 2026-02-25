import logging

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-5s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("main")

def get_all_courses(service):
    logger.debug("Fetching all courses...")
    courses = []
    page_token = None
    while True:
        response = service.courses().list(pageToken=page_token, pageSize=100, courseStates=['ACTIVE']).execute()
        courses.extend(response.get("courses", []))
        logger.debug("Fetched %d courses so far", len(courses))
        page_token = response.get("nextPageToken")
        if not page_token:
            break
    return courses