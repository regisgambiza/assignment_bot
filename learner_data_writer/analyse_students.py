import logging

try:
    from .get_all_students import get_all_students
    from .get_all_coursework import get_all_coursework
except ImportError:  # pragma: no cover - script compatibility
    from get_all_students import get_all_students
    from get_all_coursework import get_all_coursework

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-5s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("main")

def analyse_students(service, course, selected_student_id=None, additional_context=None, start_date=None, end_date=None):
    students = get_all_students(service, course["id"])
    logger.info("Fetched %d students from course=%s", len(students), course["name"])

    if selected_student_id:
        students = [s for s in students if s["userId"] == selected_student_id]
        if not students:
            logger.error("Student %s not found in course %s", selected_student_id, course["id"])
            return {}

    coursework = get_all_coursework(service, course["id"], start_date, end_date)
    logger.info("Fetched %d coursework items from course=%s", len(coursework), course["name"])

    # --- Fetch all submissions in bulk ---
    logger.info("Fetching submissions in bulk for all coursework...")
    submissions_lookup = {}  # (studentId, courseworkId) -> submission
    for j, cw in enumerate(coursework, 1):
        logger.debug("Fetching submissions for coursework %d/%d: %s",
                     j, len(coursework), cw.get("title", cw["id"]))
        try:
            subs_response = service.courses().courseWork().studentSubmissions().list(
                courseId=course["id"],
                courseWorkId=cw["id"],
                pageSize=200
            ).execute()
            subs = subs_response.get("studentSubmissions", [])
            for sub in subs:
                sid = sub.get("userId")
                if sid:
                    submissions_lookup[(sid, cw["id"])] = sub
        except Exception as e:
            logger.warning("Error fetching submissions for coursework=%s: %s",
                           cw.get("id"), str(e))

    logger.info("Finished bulk fetch: %d total submissions cached", len(submissions_lookup))

    # --- Analyse per student ---
    student_analysis = {}
    for idx, s in enumerate(students, 1):
        profile = s.get("profile", {})
        name_info = profile.get("name", {})
        full_name = " ".join(filter(None, [name_info.get("givenName", ""), name_info.get("familyName", "")])).strip() or s["userId"]

        logger.info("Processing student %d/%d: %s", idx, len(students), full_name)

        metrics = {
            "total_assignments": len(coursework),
            "missing": 0,
            "late": 0,
            # average for submitted activities (only graded submissions)
            "average_submitted": 0.0,
            # average including all activities (missing treated as 0 for that activity)
            "average_all": 0.0,
            # backward-compatible single average (set to the 'all' average)
            "average_score": 0.0,
            "graded_count": 0,
            "additional_context": additional_context if selected_student_id else ""
        }
        total_earned_submitted = 0.0  # Sum of assignedGrades for graded submissions
        total_possible_submitted = 0.0  # Sum of maxPoints for graded submissions

        total_earned_all = 0.0  # Sum of assignedGrades (or 0) for all assignments with maxPoints
        total_possible_all = 0.0  # Sum of maxPoints for all assignments with maxPoints

        for cw in coursework:
            sub = submissions_lookup.get((s["userId"], cw["id"]))
            # Determine missing/graded based on score presence rather than submission state
            if not sub:
                metrics["missing"] += 1
                continue

            # Count late submissions when a submission exists
            if sub.get("late", False):
                metrics["late"] += 1

            # Use assignedGrade (score) to determine if the task is graded/completed.
            # If there is no assigned grade or it's explicitly 0, treat as missing.
            assigned = sub.get("assignedGrade")
            max_p = cw.get("maxPoints")

            # For overall averages include only items that have maxPoints defined and positive
            if max_p is not None and max_p > 0:
                total_possible_all += max_p
                if assigned is not None and assigned > 0:
                    total_earned_all += assigned
                else:
                    # missing counts as 0 towards overall earned
                    total_earned_all += 0.0
            else:
                # No maxPoints -> cannot include this item in average calculations
                logger.debug("Coursework %s has no maxPoints, skipping from averages", cw.get("id"))

            if assigned is None or assigned == 0:
                metrics["missing"] += 1
                continue

            # At this point we have a numeric score > 0; include it in submitted averages
            if max_p is not None and max_p > 0:
                total_earned_submitted += assigned
                total_possible_submitted += max_p
                metrics["graded_count"] += 1
            else:
                logger.warning("Skipping grade for coursework %s (no maxPoints)", cw["id"])

        # Compute averages as percentages. Use 0.0 when no applicable items found.
        if total_possible_submitted > 0:
            metrics["average_submitted"] = (total_earned_submitted / total_possible_submitted) * 100
        else:
            metrics["average_submitted"] = 0.0

        if total_possible_all > 0:
            metrics["average_all"] = (total_earned_all / total_possible_all) * 100
        else:
            metrics["average_all"] = 0.0

        # Backward compatibility: set previous single-key average to the 'all' average
        metrics["average_score"] = metrics["average_all"]
        # Else: remains 0.0

        # Attach student, metrics, and detailed coursework/submission info
        student_analysis[s["userId"]] = {
            "student": s,
            "metrics": metrics,
            "coursework": [
                {
                    "id": cw["id"],
                    "title": cw.get("title", ""),
                    "creationTime": cw.get("creationTime"),
                    "maxPoints": cw.get("maxPoints"),
                    "submission": submissions_lookup.get((s["userId"], cw["id"]))
                }
                for cw in coursework
            ]
        }

        logger.info("Finished metrics for %s -> %s", full_name, metrics)

    return student_analysis
