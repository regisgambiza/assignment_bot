"""
services/ai_service.py

Async queue for Ollama â€” only one AI call at a time (GPU constraint).
Everything else in the bot stays fully responsive while Ollama thinks.
"""
import asyncio
import ollama
from dataclasses import dataclass, field
from datetime import datetime
from config import (
    OLLAMA_MODEL,
    OLLAMA_KEEP_ALIVE,
    OLLAMA_NUM_PREDICT,
    OLLAMA_NUM_CTX,
    OLLAMA_TEMPERATURE,
    OLLAMA_TOP_P,
    AI_MAX_MISSING_ITEMS,
    AI_MAX_GRADE_ITEMS,
    AI_TIMEOUT_SEC,
)
from database.db import get_missing_work, get_grades, get_summary

# â”€â”€ Queue item â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

@dataclass
class AIRequest:
    student:   dict
    question:  str
    future:    asyncio.Future
    queued_at: datetime = field(default_factory=datetime.now)

# â”€â”€ Global single queue â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

_ai_queue: asyncio.Queue[AIRequest] = asyncio.Queue()

# â”€â”€ Build rich context from DB â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def build_context(student: dict) -> str:
    missing = get_missing_work(student["id"], limit=AI_MAX_MISSING_ITEMS)
    grades = get_grades(student["id"], limit=AI_MAX_GRADE_ITEMS * 2)
    summary = get_summary(student["id"])

    missing_titles = [m["title"] for m in missing]
    recent_graded = []
    for grade in grades:
        if not grade["score_raw"] or grade["score_raw"] == "â€”":
            continue
        recent_graded.append(
            f"{grade['title']}={grade['score_raw']} ({grade['score_pct'] or '?'}%)"
        )
        if len(recent_graded) >= AI_MAX_GRADE_ITEMS:
            break

    return "\n".join(
        [
            "Role: assignment assistant for mathematics.",
            "Style: answer in 3 short bullets, practical and specific.",
            f"Today: {datetime.now().strftime('%Y-%m-%d')}",
            f"Student: {student['full_name']}",
            (
                "Summary: "
                f"assigned={summary['total_assigned'] if summary else '?'}, "
                f"submitted={summary['total_submitted'] if summary else '?'}, "
                f"missing={summary['total_missing'] if summary else '?'}, "
                f"avg={summary['avg_all_pct'] if summary else '?'}%"
            ),
            (
                "Missing now: "
                + (", ".join(missing_titles) if missing_titles else "none")
            ),
            (
                "Recent graded: "
                + (", ".join(recent_graded) if recent_graded else "none")
            ),
            "If data is missing, say so briefly and give next step.",
        ]
    )


def _chat_options() -> dict:
    return {
        "num_predict": OLLAMA_NUM_PREDICT,
        "num_ctx": OLLAMA_NUM_CTX,
        "temperature": OLLAMA_TEMPERATURE,
        "top_p": OLLAMA_TOP_P,
    }


async def _warmup_model() -> None:
    try:
        await asyncio.to_thread(
            lambda: ollama.chat(
                model=OLLAMA_MODEL,
                messages=[{"role": "user", "content": "ping"}],
                options={"num_predict": 1, "num_ctx": 256},
                keep_alive=OLLAMA_KEEP_ALIVE,
            )
        )
        print("AI model warm-up complete")
    except Exception as exc:
        print(f"AI warm-up skipped: {exc}")

# â”€â”€ Background worker â€” runs for lifetime of the bot â”€â”€â”€â”€â”€â”€

async def ai_worker():
    print("AI worker started - model:", OLLAMA_MODEL)
    print(
        "AI speed profile:",
        f"num_predict={OLLAMA_NUM_PREDICT},",
        f"num_ctx={OLLAMA_NUM_CTX},",
        f"keep_alive={OLLAMA_KEEP_ALIVE},",
        f"timeout={AI_TIMEOUT_SEC}s",
    )
    await _warmup_model()
    while True:
        request: AIRequest = await _ai_queue.get()
        try:
            # asyncio.to_thread keeps the bot responsive while Ollama runs
            response = await asyncio.wait_for(
                asyncio.to_thread(
                    lambda: ollama.chat(
                        model=OLLAMA_MODEL,
                        messages=[
                            {"role": "system", "content": build_context(request.student)},
                            {"role": "user", "content": request.question[:500]},
                        ],
                        options=_chat_options(),
                        keep_alive=OLLAMA_KEEP_ALIVE,
                    )
                ),
                timeout=AI_TIMEOUT_SEC,
            )
            request.future.set_result(response["message"]["content"])

        except asyncio.TimeoutError:
            request.future.set_result(
                "AI took too long this time. Please ask again with a shorter question."
            )
        except Exception as e:
            request.future.set_exception(e)

        finally:
            _ai_queue.task_done()

# â”€â”€ Public API â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

async def ask_ai(question: str, student: dict) -> str:
    """Add to queue, wait for result. Bot stays responsive while waiting."""
    loop = asyncio.get_running_loop()
    future = loop.create_future()
    await _ai_queue.put(AIRequest(student=student, question=question, future=future))
    return await future

def queue_size() -> int:
    """How many requests are waiting â€” used to show position to student"""
    return _ai_queue.qsize()

