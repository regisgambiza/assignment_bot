import logging
import json
import os
import time
try:
    from .build_batch_prompt import build_batch_prompt
    from .call_ollama_classify import call_ollama_classify
except ImportError:  # pragma: no cover - script compatibility
    from build_batch_prompt import build_batch_prompt
    from call_ollama_classify import call_ollama_classify

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-5s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("main")

def remove_markdown_bold(text):
    """Remove markdown bold markers (**text**) from text."""
    import re
    return re.sub(r'\*\*', '', text)

def generate_reports(student_analysis, categories, ollama_model):
    results = {}
    student_items = list(student_analysis.items())
    try:
        BATCH_SIZE = int(os.getenv("AI_BATCH_SIZE", "2"))
    except Exception:
        BATCH_SIZE = 2

    for start in range(0, len(student_items), BATCH_SIZE):
        batch = student_items[start:start + BATCH_SIZE]
        batch_data = []
        for sid, data in batch:
            profile = data["student"].get("profile", {})
            name_info = profile.get("name", {})
            full_name = " ".join(filter(None, [name_info.get("givenName",""), name_info.get("familyName","")])).strip() or sid
            metrics = data["metrics"]
            # Extract detailed submissions
            detailed_submissions = []
            for cw in data["coursework"]:
                title = cw.get("title", cw["id"])
                max_points = cw.get("maxPoints", "—")
                submission = cw.get("submission")
                if not submission:
                    status = "Missing"
                    score = "—"
                else:
                    state = submission.get("state", "")
                    if state in ["NEW", "CREATED"]:
                        status = "Missing"
                    elif submission.get("late", False):
                        status = "Late"
                    else:
                        status = "Submitted"
                    assigned_grade = submission.get("assignedGrade")
                    if assigned_grade is not None:
                        if assigned_grade == 0:
                            status = "Missing"
                            score = "—"
                        else:
                            score = f"{assigned_grade}/{max_points}" if max_points != "—" else f"{assigned_grade}"
                    else:
                        score = "—"
                detailed_submissions.append({
                    "title": title,
                    "status": status,
                    "score": score
                })
            batch_data.append((full_name, metrics, detailed_submissions))

        logger.info("Building prompt for batch %d-%d learners", start + 1, start + len(batch))
        prompt = build_batch_prompt(batch_data, categories)
        logger.info("Submitting batch %d-%d learners to Ollama", start + 1, start + len(batch))
        ai_response = call_ollama_classify(prompt, model=ollama_model)

        # Split the AI response by ---
        individual_responses = [r.strip() for r in ai_response.split('---') if r.strip()]

        if len(individual_responses) != len(batch):
            logger.warning("Mismatch in responses: got %d, expected %d. Full response: %s", len(individual_responses), len(batch), ai_response)
            # Assign responses to students that did get a response
            for i, (sid, _) in enumerate(batch):
                if i < len(individual_responses):
                    response = individual_responses[i]
                    results[sid] = {"ai_response": remove_markdown_bold(response)}
                else:
                    # Retry logic for missing response
                    attempts = 0
                    valid_response = None
                    try:
                        MAX_RETRIES = int(os.getenv("AI_MAX_RETRIES", "5"))
                    except Exception:
                        MAX_RETRIES = 5
                    INFINITE_RETRIES = MAX_RETRIES <= 0
                    RETRY_BASE_SECONDS = 1
                    while INFINITE_RETRIES or attempts < MAX_RETRIES:
                        attempts += 1
                        logger.info("Attempt %d to reclassify student=%s (missing response)", attempts, sid)
                        single_prompt = build_batch_prompt([batch_data[i]], categories)
                        try:
                            single_ai = call_ollama_classify(single_prompt, model=ollama_model)
                        except Exception as e:
                            logger.exception("Error calling AI on retry for student=%s: %s", sid, e)
                            single_ai = ""
                        single_responses = [r.strip() for r in single_ai.split('---') if r.strip()]
                        if single_responses:
                            candidate = single_responses[0]
                            def extract_category(resp_text):
                                line = next((ln for ln in resp_text.splitlines() if ln.strip().startswith("Category:")), None)
                                if not line:
                                    return None
                                return line.split(":", 1)[1].strip()
                            candidate_cat = extract_category(candidate)
                            if candidate_cat and candidate_cat in categories:
                                valid_response = candidate
                                logger.info("Received valid category '%s' for student=%s on attempt %d", candidate_cat, sid, attempts)
                                break
                            else:
                                logger.warning("Retry attempt %d produced invalid or missing category for student=%s: %s", attempts, sid, candidate_cat)
                        else:
                            logger.warning("Retry attempt %d produced empty AI response for student=%s", attempts, sid)
                        sleep_time = RETRY_BASE_SECONDS * (2 ** (attempts - 1))
                        time.sleep(min(sleep_time, 30))
                    if valid_response:
                        response = valid_response
                    else:
                        logger.warning("Exhausted retries for student=%s, assigning 'Needs Review' (missing response)", sid)
                        response = f"Category: Needs Review\nTeacher Report: Unable to obtain valid category after retrying. Please review student metrics: {json.dumps(batch_data[i][1])}"
                    results[sid] = {"ai_response": remove_markdown_bold(response)}
            continue

        # Assign individual responses with retry logic when AI fails to provide a valid category
        # Configuration: set env var AI_MAX_RETRIES to override default (default 5). Set to 0 or negative for infinite retries.
        try:
            MAX_RETRIES = int(os.getenv("AI_MAX_RETRIES", "5"))
        except Exception:
            MAX_RETRIES = 5
        INFINITE_RETRIES = MAX_RETRIES <= 0
        RETRY_BASE_SECONDS = 1

        for i, (sid, _) in enumerate(batch):
            response = individual_responses[i]

            def extract_category(resp_text):
                line = next((ln for ln in resp_text.splitlines() if ln.strip().startswith("Category:")), None)
                if not line:
                    return None
                return line.split(":", 1)[1].strip()

            category = extract_category(response)

            # If category missing or invalid, retry calling the model for that single student
            if not category or category not in categories:
                attempts = 0
                valid_response = None
                while INFINITE_RETRIES or attempts < MAX_RETRIES:
                    attempts += 1
                    logger.info("Attempt %d to reclassify student=%s", attempts, sid)
                    single_prompt = build_batch_prompt([batch_data[i]], categories)
                    try:
                        single_ai = call_ollama_classify(single_prompt, model=ollama_model)
                    except Exception as e:
                        logger.exception("Error calling AI on retry for student=%s: %s", sid, e)
                        single_ai = ""

                    single_responses = [r.strip() for r in single_ai.split('---') if r.strip()]
                    if single_responses:
                        candidate = single_responses[0]
                        candidate_cat = extract_category(candidate)
                        if candidate_cat and candidate_cat in categories:
                            valid_response = candidate
                            logger.info("Received valid category '%s' for student=%s on attempt %d", candidate_cat, sid, attempts)
                            break
                        else:
                            logger.warning("Retry attempt %d produced invalid or missing category for student=%s: %s", attempts, sid, candidate_cat)
                    else:
                        logger.warning("Retry attempt %d produced empty AI response for student=%s", attempts, sid)

                    # Exponential backoff before next attempt
                    sleep_time = RETRY_BASE_SECONDS * (2 ** (attempts - 1))
                    time.sleep(min(sleep_time, 30))

                if valid_response:
                    response = valid_response
                else:
                    # Exhausted retries: produce a clear 'Needs Review' response but avoid the former terse error message
                    logger.warning("Exhausted retries for student=%s, assigning 'Needs Review'", sid)
                    if category and category not in categories:
                        response = f"Category: Needs Review\nTeacher Report: AI provided an invalid category ('{category}'). Please review student metrics: {json.dumps(batch_data[i][1])}"
                    else:
                        response = f"Category: Needs Review\nTeacher Report: Unable to obtain valid category after retrying. Please review student metrics: {json.dumps(batch_data[i][1])}"

            results[sid] = {"ai_response": remove_markdown_bold(response)}
            logger.debug("Assigned AI response to student=%s", sid)

    return results
