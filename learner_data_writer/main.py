# main.py
import argparse
import logging
from pathlib import Path
try:
    from .get_classroom_service import get_classroom_service
    from .get_all_courses import get_all_courses
    from .get_all_students import get_all_students
    from .get_all_coursework import get_all_coursework
    from .call_ollama_classify import call_ollama_classify
    from .build_batch_prompt import build_batch_prompt
    from .analyse_students import analyse_students
    from .generate_reports import generate_reports
    from .save_reports_to_file import save_reports_to_file
    from .select_course import select_course
    from .select_student import select_student
    from .sync_analysis_to_class_db import sync_course_analysis_to_db
except ImportError:  # pragma: no cover - script compatibility
    from get_classroom_service import get_classroom_service
    from get_all_courses import get_all_courses
    from get_all_students import get_all_students
    from get_all_coursework import get_all_coursework
    from call_ollama_classify import call_ollama_classify
    from build_batch_prompt import build_batch_prompt
    from analyse_students import analyse_students
    from generate_reports import generate_reports
    from save_reports_to_file import save_reports_to_file
    from select_course import select_course
    from select_student import select_student
    from sync_analysis_to_class_db import sync_course_analysis_to_db


# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-5s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("main")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--credentials", default="credentials.json")
    parser.add_argument("--token", default="token.json")
    parser.add_argument("--ollama-model", default="gpt-oss:20b")
    args = parser.parse_args()

    logger.info("Starting learner analysis run")

    # Prompt for start and end dates
    start_date = input("Enter start date (YYYY-MM-DD, e.g., 2025-09-01): ")
    end_date = input("Enter end date (YYYY-MM-DD, e.g., 2025-09-30): ")

    service = get_classroom_service(args.credentials, args.token)
    courses = get_all_courses(service)
    # Sort courses alphabetically by name
    courses = sorted(courses, key=lambda x: x["name"])
    logger.info("Fetched %d courses", len(courses))

    if not courses:
        print("No courses found.")
        return

    print("Options:")
    print("1. Analyze all classes")
    print("2. Analyze a single class")
    print("3. Analyze a single student")
    while True:
        try:
            mode_choice = int(input("Enter choice (1/2/3): "))
            if mode_choice in [1, 2, 3]:
                break
            else:
                print("Invalid choice. Try again.")
        except ValueError:
            print("Please enter a number.")

    selected_student_id = None
    additional_context = None
    if mode_choice == 1:
        target_courses = courses
    elif mode_choice == 2:
        selected_course = select_course(courses)
        target_courses = [selected_course]
    elif mode_choice == 3:
        selected_course = select_course(courses)
        target_courses = [selected_course]
        selected_student_id = select_student(service, selected_course)
        if not selected_student_id:
            return
        additional_context = input("Provide additional context about the student (e.g., attendance, behavior, external factors) for a more accurate report: ")

    categories = ["High Performer", "At Risk", "Average", "Improving", "Emerging", "Needs Review"]

    for course in target_courses:
        logger.info("Analysing course=%s (%s)", course["id"], course["name"])
        student_analysis = analyse_students(service, course, selected_student_id, additional_context, start_date, end_date)
        if not student_analysis:
            logger.warning("No students to analyse in course=%s", course["id"])
            with open("student_reports.txt", "a", encoding="utf-8") as f:
                f.write(f"No students to analyze in course {course['name']} ({course['id']})\n\n")
            continue

        reports = generate_reports(student_analysis, categories, args.ollama_model)
        save_reports_to_file(course, student_analysis, reports)
        for sid, rep in reports.items():
            logger.info("Report for student=%s: %s", sid, rep["ai_response"][:120])

    # Start chatbot after reports are generated
    #run_chatbot(ollama_model=args.ollama_model)

if __name__ == "__main__":
    main()


def run_with_params(credentials: str = "credentials.json",
                    token: str = "token.json",
                    ollama_model: str = "gpt-oss:20b",
                    start_date: str = None,
                    end_date: str = None,
                    mode_choice: int = 1,
                    course_id: str = None,
                    student_id: str = None,
                    additional_context: str = None,
                    reports_dir: str = None,
                    ai_max_retries: str = None,
                    batch_size: str = None,
                    include_teacher_reports: bool = True,
                    sync_direct_to_db: bool = False,
                    db_path: str | None = None,
                    schema_path: str | None = None,
                    db_school_name: str = "School",
                    db_sync_source: str = "learner_performance_monitor_direct"):
    """Non-interactive entrypoint for GUI or automation.

    Parameters mirror the CLI flow. If mode_choice==1 -> all courses; 2 -> single course (course_id required);
    3 -> single course + single student (course_id and student_id required).
    """
    # Set optional env vars
    import os
    if reports_dir:
        os.environ["REPORTS_DIR"] = reports_dir
    if ai_max_retries is not None:
        os.environ["AI_MAX_RETRIES"] = str(ai_max_retries)
    if batch_size is not None:
        os.environ["AI_BATCH_SIZE"] = str(batch_size)

    project_root = Path(__file__).resolve().parents[1]
    if db_path:
        resolved_db_path = Path(db_path)
        if not resolved_db_path.is_absolute():
            resolved_db_path = project_root / resolved_db_path
    else:
        resolved_db_path = project_root / os.getenv("DB_PATH", "database/class.db")

    if schema_path:
        resolved_schema_path = Path(schema_path)
        if not resolved_schema_path.is_absolute():
            resolved_schema_path = project_root / resolved_schema_path
    else:
        resolved_schema_path = project_root / "database" / "schema.sql"

    logger.info("Starting non-interactive analysis run (GUI/automation)")

    service = get_classroom_service(credentials, token)
    courses = get_all_courses(service)
    courses = sorted(courses, key=lambda x: x["name"]) if courses else []

    if not courses:
        logger.warning("No courses found.")
        return

    categories = ["High Performer", "At Risk", "Average", "Improving", "Emerging", "Needs Review"]

    if mode_choice == 1:
        target_courses = courses
    elif mode_choice == 2:
        if not course_id:
            raise ValueError("course_id is required for mode_choice=2")
        target_courses = [c for c in courses if c["id"] == course_id]
        if not target_courses:
            raise ValueError(f"Course id {course_id} not found")
    elif mode_choice == 3:
        if not course_id or not student_id:
            raise ValueError("course_id and student_id are required for mode_choice=3")
        target_courses = [c for c in courses if c["id"] == course_id]
        if not target_courses:
            raise ValueError(f"Course id {course_id} not found")
    else:
        raise ValueError("Invalid mode_choice; expected 1,2, or 3")

    for course in target_courses:
        logger.info("Analysing course=%s (%s)", course["id"], course["name"])
        selected_student_id = student_id if mode_choice == 3 else None
        student_analysis = analyse_students(service, course, selected_student_id, additional_context, start_date, end_date)
        if not student_analysis:
            logger.warning("No students to analyse in course=%s", course["id"])
            with open("student_reports.txt", "a", encoding="utf-8") as f:
                f.write(f"No students to analyze in course {course['name']} ({course['id']})\n\n")
            continue

        if include_teacher_reports:
            reports = generate_reports(student_analysis, categories, ollama_model)
        else:
            reports = {}

        save_reports_to_file(course, student_analysis, reports, include_teacher_reports=include_teacher_reports)
        for sid in student_analysis:
            if sid in reports:
                logger.info("Report for student=%s: %s", sid, reports[sid]["ai_response"][:120])

        if sync_direct_to_db:
            logger.info(
                "Direct DB sync enabled. Syncing course=%s (%s) to db=%s",
                course["id"],
                course["name"],
                db_path,
            )
            sync_stats = sync_course_analysis_to_db(
                course=course,
                student_analysis=student_analysis,
                db_path=str(resolved_db_path.resolve()),
                schema_path=str(resolved_schema_path.resolve()),
                school_name=db_school_name,
                source=db_sync_source,
                dry_run=False,
            )
            logger.info("Direct DB sync stats for course=%s: %s", course["id"], sync_stats)

    logger.info("Non-interactive analysis run complete")
