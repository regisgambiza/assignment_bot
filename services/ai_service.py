"""
services/ai_service.py

Async queue for Ollama â€” only one AI call at a time (GPU constraint).
Everything else in the bot stays fully responsive while Ollama thinks.
"""
import asyncio
import ollama
from dataclasses import dataclass, field
from datetime import datetime
from config import OLLAMA_MODEL
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
    missing  = get_missing_work(student["id"])
    grades   = get_grades(student["id"])
    summary  = get_summary(student["id"])

    missing_text = "\n".join([
        f"  - {m['title']} (assigned {m['due_date'] or 'unknown'})"
        for m in missing
    ]) or "  None â€” all caught up!"

    graded = [g for g in grades if g["score_raw"] and g["score_raw"] != "â€”"]
    grades_text = "\n".join([
        f"  - {g['title']}: {g['score_raw']} ({g['score_pct'] or '?'}%)"
        for g in graded[:10]
    ]) or "  No graded work yet"

    return f"""
You are a helpful, friendly assistant for a mathematics class (8/1 Mathematics).
Be concise and specific â€” max 3-4 sentences. Use the student's real data below.
Today's date: {datetime.now().strftime('%B %d, %Y')}

Student: {student['full_name']}

Performance summary:
  Total assigned:      {summary['total_assigned']    if summary else '?'}
  Submitted:           {summary['total_submitted']   if summary else '?'}
  Missing:             {summary['total_missing']     if summary else '?'}
  Avg (submitted):     {summary['avg_submitted_pct'] if summary else '?'}%
  Avg (overall):       {summary['avg_all_pct']       if summary else '?'}%
  Points earned:       {summary['points_earned']     if summary else '?'} / {summary['points_possible'] if summary else '?'}

Missing assignments:
{missing_text}

Recent grades:
{grades_text}
""".strip()

# â”€â”€ Background worker â€” runs for lifetime of the bot â”€â”€â”€â”€â”€â”€

async def ai_worker():
    print("AI worker started - model:", OLLAMA_MODEL)
    while True:
        request: AIRequest = await _ai_queue.get()
        try:
            # asyncio.to_thread keeps the bot responsive while Ollama runs
            response = await asyncio.to_thread(
                lambda: ollama.chat(
                    model=OLLAMA_MODEL,
                    messages=[
                        {"role": "system", "content": build_context(request.student)},
                        {"role": "user",   "content": request.question},
                    ]
                )
            )
            request.future.set_result(response["message"]["content"])

        except Exception as e:
            request.future.set_exception(e)

        finally:
            _ai_queue.task_done()

# â”€â”€ Public API â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

async def ask_ai(question: str, student: dict) -> str:
    """Add to queue, wait for result. Bot stays responsive while waiting."""
    loop   = asyncio.get_event_loop()
    future = loop.create_future()
    await _ai_queue.put(AIRequest(student=student, question=question, future=future))
    return await future

def queue_size() -> int:
    """How many requests are waiting â€” used to show position to student"""
    return _ai_queue.qsize()

