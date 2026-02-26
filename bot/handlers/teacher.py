"""
bot/handlers/teacher.py

Teacher-only commands and workflows:
  /teacher     - teacher panel
  /stats       - lookup learner stats by name
  /pending     - review flagged submissions
  /atrisk      - at-risk learner list
  /broadcast   - missing work reminder blast
  /campaign    - create scheduled missing-work campaign
  /campaigns   - list recent campaign jobs
  /links       - learner deep-link generation
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta

from telegram import Update, Bot, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.error import BadRequest
from telegram.ext import ContextTypes

from database.db import (
    find_students_by_name,
    get_summary,
    get_student_course_name,
    get_pending_flags,
    get_at_risk_students,
    get_all_students_with_telegram,
    get_missing_work,
    verify_flag,
    get_db,
    create_campaign_job,
    get_due_campaign_jobs,
    claim_campaign_job,
    complete_campaign_job,
    fail_campaign_job,
    list_campaign_jobs,
    get_submission_evidence,
)
from bot.keyboards import (
    verify_kb,
    broadcast_confirm_kb,
    back_kb,
    campaign_template_kb,
    campaign_schedule_kb,
)
from config import TEACHER_TELEGRAM_ID, COURSE_NAME


CAMPAIGN_TEMPLATES: dict[str, str] = {
    "gentle": (
        "Hi {first_name}, this is a friendly reminder that you currently have "
        "{missing_count} missing assignment(s).\n\n"
        "{missing_list}\n\n"
        "Please submit what you can today. Open /start for details."
    ),
    "firm": (
        "{first_name}, action needed: you have {missing_count} missing assignment(s).\n\n"
        "{missing_list}\n\n"
        "Submit as soon as possible to avoid grade impact. Open /start now."
    ),
    "exam": (
        "Exam prep check-in for {first_name}:\n"
        "You still have {missing_count} missing assignment(s).\n\n"
        "{missing_list}\n\n"
        "Clearing these will help your readiness. Open /start to plan next steps."
    ),
}


def _is_teacher(telegram_id: str) -> bool:
    return str(telegram_id) == str(TEACHER_TELEGRAM_ID)


def _clear_campaign_state(context: ContextTypes.DEFAULT_TYPE) -> None:
    context.user_data.pop("teacher_state", None)
    context.user_data.pop("campaign_template_key", None)
    context.user_data.pop("campaign_template_text", None)


def _campaign_template_preview(template: str) -> str:
    sample = template.format(
        first_name="Learner",
        missing_count=3,
        missing_list="- Example Task 1\n- Example Task 2\n- Example Task 3",
    )
    return sample[:600]


def _split_text_chunks(text: str, limit: int = 3900) -> list[str]:
    if len(text) <= limit:
        return [text]

    chunks: list[str] = []
    remaining = text
    while remaining:
        if len(remaining) <= limit:
            chunks.append(remaining)
            break
        split_at = remaining.rfind("\n", 0, limit)
        if split_at <= 0:
            split_at = limit
        chunks.append(remaining[:split_at])
        remaining = remaining[split_at:].lstrip("\n")
    return chunks


async def _reply_teacher_student_stats(message, student: dict) -> None:
    chunks = _split_text_chunks(_format_teacher_student_stats(student))
    await message.reply_text(chunks[0])
    for chunk in chunks[1:]:
        await message.reply_text(chunk)


async def _edit_teacher_student_stats(query, student: dict) -> None:
    chunks = _split_text_chunks(_format_teacher_student_stats(student))
    await query.edit_message_text(chunks[0])
    for chunk in chunks[1:]:
        await query.message.reply_text(chunk)


def _format_teacher_student_stats(student: dict) -> str:
    summary = get_summary(student["id"])
    course_name = get_student_course_name(student["id"]) or "Unknown class"
    telegram_id = student.get("telegram_id") or "-"
    telegram_username = student.get("telegram_username")
    telegram_label = (
        f"{telegram_id} (@{telegram_username})" if telegram_username else str(telegram_id)
    )

    lines = [
        f"Learner: {student.get('full_name', '-')}",
        f"LMS ID: {student.get('lms_id', '-')}",
        f"Course: {course_name}",
        f"Telegram: {telegram_label}",
    ]

    if not summary:
        lines.append("")
        lines.append("No summary data yet for this learner.")
        return "\n".join(lines)

    total_assigned = int(summary.get("total_assigned") or 0)
    total_missing = int(summary.get("total_missing") or 0)
    total_submitted = summary.get("total_submitted")
    if total_submitted is None:
        total_submitted = max(total_assigned - total_missing, 0)

    points_earned = float(summary.get("points_earned") or 0.0)
    points_possible = float(summary.get("points_possible") or 0.0)
    overall_avg = (
        (points_earned * 100.0 / points_possible) if points_possible > 0 else 0.0
    )
    completion_pct = (
        round((float(total_submitted) / float(total_assigned)) * 100)
        if total_assigned > 0
        else 0
    )

    lines.extend(
        [
            "",
            "Stats:",
            f"- Assigned: {total_assigned}",
            f"- Submitted: {int(total_submitted)}",
            f"- Missing: {total_missing}",
            f"- Late: {int(summary.get('total_late') or 0)}",
            f"- Average: {overall_avg:.2f}%",
            f"- Points: {points_earned:.2f}/{points_possible:.2f}",
            f"- Completion: {completion_pct}%",
        ]
    )

    missing = get_missing_work(student["id"])
    if missing:
        lines.append("")
        lines.append(f"Missing work ({len(missing)}):")
        for idx, item in enumerate(missing, 1):
            lines.append(f"{idx}. {item['title']}")
    else:
        lines.append("")
        lines.append("Missing work: none")

    return "\n".join(lines)


def _teacher_stats_match_kb(students: list[dict]) -> InlineKeyboardMarkup:
    keyboard: list[list[InlineKeyboardButton]] = []
    for student in students:
        keyboard.append(
            [
                InlineKeyboardButton(
                    student.get("full_name", "Unknown learner")[:64],
                    callback_data=f"teacher_stats_pick_{student['id']}",
                )
            ]
        )
    keyboard.append([InlineKeyboardButton("Cancel", callback_data="teacher_stats_cancel")])
    return InlineKeyboardMarkup(keyboard)


def _resolve_schedule(token: str) -> tuple[datetime, str]:
    now = datetime.now()
    if token == "now":
        return now, "Send now"
    if token == "30m":
        return now + timedelta(minutes=30), "In 30 minutes"
    if token == "2h":
        return now + timedelta(hours=2), "In 2 hours"
    if token == "tomorrow_0700":
        run_at = (now + timedelta(days=1)).replace(
            hour=7, minute=0, second=0, microsecond=0
        )
        return run_at, "Tomorrow 07:00"
    return now, "Send now"


def _render_campaign_message(
    template: str, student: dict, missing: list[dict]
) -> str:
    first_name = (student.get("full_name") or "Student").split()[0]
    missing_list = "\n".join(f"- {m['title']}" for m in missing[:12]) or "- none"
    try:
        text = template.format(
            first_name=first_name,
            full_name=student.get("full_name", "Student"),
            missing_count=len(missing),
            missing_list=missing_list,
        )
    except Exception:
        text = (
            f"{first_name}, you have {len(missing)} missing assignment(s):\n\n"
            f"{missing_list}\n\nOpen /start for details."
        )
    return text[:3900]


async def _deny_access(update: Update):
    if update.message:
        await update.message.reply_text("This command is for teachers only.")


async def teacher_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_teacher(update.effective_user.id):
        await _deny_access(update)
        return

    pending = get_pending_flags()
    at_risk = get_at_risk_students()

    await update.message.reply_text(
        f"*Teacher Panel - {COURSE_NAME}*\n\n"
        f"Pending flags: *{len(pending)}*\n"
        f"At-risk learners: *{len(at_risk)}*\n\n"
        "Commands:\n"
        "/stats     - lookup learner stats by name\n"
        "/pending   - review flagged submissions\n"
        "/atrisk    - list at-risk learners\n"
        "/broadcast - send missing-work reminders now\n"
        "/campaign  - schedule template campaign\n"
        "/campaigns - recent campaign jobs\n"
        "/links     - generate registration links",
        parse_mode="Markdown",
    )


async def learner_stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_teacher(update.effective_user.id):
        await _deny_access(update)
        return

    # Prevent registration flow from consuming teacher name lookups.
    context.user_data.pop("state", None)
    context.user_data.pop("candidates", None)
    context.user_data.pop("pending_lms_id", None)
    context.user_data.pop("pending_name", None)
    context.user_data["teacher_state"] = "awaiting_teacher_stats_query"
    await update.message.reply_text(
        "Send learner name to lookup stats.\n"
        "You can type part of the name (example: Maria)."
    )


async def pending_flags(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_teacher(update.effective_user.id):
        await _deny_access(update)
        return

    flags = get_pending_flags()
    if not flags:
        await update.message.reply_text("No pending flags.")
        return

    await update.message.reply_text(f"Pending verification items: {len(flags)}")

    for item in flags:
        details = (
            f"Student: {item['full_name']}\n"
            f"Assignment: {item['assignment_title']}\n"
            f"Course: {item['course_name']}\n"
            f"Flagged: {(item['flagged_at'] or '-')[:16]}"
        )
        if item.get("flag_note"):
            details += f"\nNote: {item['flag_note']}"
        if item.get("proof_uploaded_at"):
            details += f"\nEvidence uploaded: {item['proof_uploaded_at'][:16]}"

        markup = InlineKeyboardMarkup(
            verify_kb(item["student_id"], item["assignment_id"])
        )

        proof_file_id = item.get("proof_file_id")
        proof_type = item.get("proof_file_type")
        proof_caption = item.get("proof_caption")
        if proof_caption:
            details += f"\nProof caption: {proof_caption[:180]}"

        if proof_file_id and proof_type == "photo":
            try:
                await update.message.reply_photo(
                    photo=proof_file_id,
                    caption=details,
                    reply_markup=markup,
                )
                continue
            except Exception as exc:
                details += f"\n(Preview unavailable: {exc})"
        elif proof_file_id and proof_type == "document":
            try:
                await update.message.reply_document(
                    document=proof_file_id,
                    caption=details,
                    reply_markup=markup,
                )
                continue
            except Exception as exc:
                details += f"\n(Preview unavailable: {exc})"

        await update.message.reply_text(details, reply_markup=markup)


async def at_risk(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_teacher(update.effective_user.id):
        await _deny_access(update)
        return

    students = get_at_risk_students()
    if not students:
        await update.message.reply_text("No at-risk learners.")
        return

    lines = []
    for s in students:
        tg = f"@{s['telegram_id']}" if s["telegram_id"] else "not registered"
        lines.append(
            f"{s['full_name']}\n"
            f"Missing: {s['total_missing']} | Overall: {s['avg_all_pct']}%\n"
            f"Telegram: {tg}"
        )

    await update.message.reply_text(
        f"At-risk learners ({len(students)}):\n\n" + "\n\n".join(lines)
    )


async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_teacher(update.effective_user.id):
        await _deny_access(update)
        return

    students = get_all_students_with_telegram()
    targets = [s for s in students if (s.get("total_missing") or 0) > 0]
    if not targets:
        await update.message.reply_text("No learners have missing work.")
        return

    context.user_data["broadcast_targets"] = targets
    await update.message.reply_text(
        f"Broadcast preview:\n"
        f"Will message {len(targets)} learner(s) with missing work.\n\n"
        + "\n".join(
            f"- {s['full_name']} ({s['total_missing']} missing)" for s in targets[:10]
        )
        + (f"\n...and {len(targets)-10} more." if len(targets) > 10 else ""),
        reply_markup=InlineKeyboardMarkup(broadcast_confirm_kb()),
    )


async def campaign_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_teacher(update.effective_user.id):
        await _deny_access(update)
        return

    _clear_campaign_state(context)
    context.user_data["teacher_state"] = "awaiting_campaign_template"
    await update.message.reply_text(
        "Choose a campaign template:",
        reply_markup=InlineKeyboardMarkup(campaign_template_kb()),
    )


async def campaign_jobs_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_teacher(update.effective_user.id):
        await _deny_access(update)
        return

    jobs = list_campaign_jobs(15)
    if not jobs:
        await update.message.reply_text("No campaign jobs yet.")
        return

    lines = []
    for job in jobs:
        lines.append(
            f"#{job['id']} {job['status']} | {job['template_key']} | "
            f"{job['schedule_label'] or '-'} | run_at={job['run_at']} | "
            f"sent={job['sent_count']}/{job['target_count']}"
        )
    await update.message.reply_text("Recent campaign jobs:\n\n" + "\n".join(lines))


async def generate_links(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_teacher(update.effective_user.id):
        await _deny_access(update)
        return

    with get_db() as conn:
        students = conn.execute(
            "SELECT lms_id, full_name, telegram_id FROM students ORDER BY full_name"
        ).fetchall()

    bot_me = await context.bot.get_me()
    username = bot_me.username
    lines = []
    for student in students:
        status = "registered" if student["telegram_id"] else "not yet"
        lines.append(
            f"{student['full_name']} ({status})\n"
            f"t.me/{username}?start={student['lms_id']}"
        )

    await update.message.reply_text("Personal registration links:\n\n" + "\n\n".join(lines))


async def notify_teacher_of_flag(
    bot: Bot,
    student: dict,
    assignment_id: int,
) -> bool:
    teacher_chat_id = str(TEACHER_TELEGRAM_ID or "").strip()
    if not teacher_chat_id or not teacher_chat_id.isdigit():
        print("Could not notify teacher: TEACHER_TELEGRAM_ID missing/invalid.")
        return False

    with get_db() as conn:
        assignment = conn.execute(
            "SELECT title FROM assignments WHERE id = ?",
            (assignment_id,),
        ).fetchone()

    if not assignment:
        return False

    proof = get_submission_evidence(student["id"], assignment_id) or {}
    details = (
        "New flag needs review\n\n"
        f"Student: {student['full_name']}\n"
        f"Assignment: {assignment['title']}"
    )
    if proof.get("proof_uploaded_at"):
        details += f"\nEvidence uploaded: {proof['proof_uploaded_at'][:16]}"

    markup = InlineKeyboardMarkup(verify_kb(student["id"], assignment_id))

    try:
        proof_file_id = proof.get("proof_file_id")
        proof_type = proof.get("proof_file_type")

        if proof_file_id and proof_type == "photo":
            await bot.send_photo(
                chat_id=int(teacher_chat_id),
                photo=proof_file_id,
                caption=details,
                reply_markup=markup,
            )
        elif proof_file_id and proof_type == "document":
            await bot.send_document(
                chat_id=int(teacher_chat_id),
                document=proof_file_id,
                caption=details,
                reply_markup=markup,
            )
        else:
            await bot.send_message(
                chat_id=int(teacher_chat_id),
                text=details,
                reply_markup=markup,
            )
        return True
    except BadRequest as exc:
        if "Chat not found" in str(exc):
            print(
                "Could not notify teacher: chat not found. "
                "Open bot in teacher account and send /start."
            )
        else:
            print(f"Could not notify teacher: {exc}")
    except Exception as exc:
        print(f"Could not notify teacher: {exc}")
    return False


async def handle_teacher_text_input(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> bool:
    if not _is_teacher(update.effective_user.id):
        return False

    state = context.user_data.get("teacher_state")
    if state == "awaiting_teacher_stats_query":
        query_text = (update.message.text or "").strip()
        if len(query_text) < 2:
            await update.message.reply_text("Please enter at least 2 characters.")
            return True

        matches = find_students_by_name(query_text)
        if not matches:
            await update.message.reply_text(
                "No learner found with that name. Try another search."
            )
            return True

        if len(matches) == 1:
            context.user_data.pop("teacher_state", None)
            await _reply_teacher_student_stats(update.message, matches[0])
            return True

        top_matches = matches[:15]
        await update.message.reply_text(
            f"Found {len(matches)} learners. Select one:",
            reply_markup=_teacher_stats_match_kb(top_matches),
        )
        return True

    if state != "awaiting_campaign_custom":
        return False

    template_text = (update.message.text or "").strip()
    if len(template_text) < 10:
        await update.message.reply_text(
            "Template is too short. Send at least 10 characters, or /campaign to restart."
        )
        return True

    context.user_data["campaign_template_key"] = "custom"
    context.user_data["campaign_template_text"] = template_text
    context.user_data["teacher_state"] = "awaiting_campaign_schedule"

    await update.message.reply_text(
        "Custom template saved.\n\n"
        "You can use placeholders:\n"
        "{first_name}, {full_name}, {missing_count}, {missing_list}\n\n"
        "Choose a schedule:",
        reply_markup=InlineKeyboardMarkup(campaign_schedule_kb()),
    )
    return True


async def handle_teacher_buttons(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> bool:
    query = update.callback_query
    data = query.data

    if data == "teacher_stats_cancel":
        if not _is_teacher(query.from_user.id):
            await query.answer("Teacher only", show_alert=True)
            return True
        await query.answer()
        context.user_data.pop("teacher_state", None)
        await query.edit_message_text("Learner stats lookup cancelled.")
        return True

    if data.startswith("teacher_stats_pick_"):
        if not _is_teacher(query.from_user.id):
            await query.answer("Teacher only", show_alert=True)
            return True

        await query.answer()
        try:
            student_id = int(data.replace("teacher_stats_pick_", "", 1))
        except ValueError:
            await query.edit_message_text("Invalid learner selection.")
            return True

        with get_db() as conn:
            row = conn.execute(
                "SELECT * FROM students WHERE id = ?",
                (student_id,),
            ).fetchone()

        if not row:
            await query.edit_message_text("Learner not found.")
            return True

        context.user_data.pop("teacher_state", None)
        await _edit_teacher_student_stats(query, dict(row))
        return True

    if data.startswith("verify_"):
        if not _is_teacher(query.from_user.id):
            await query.answer("Teacher only", show_alert=True)
            return True

        parts = data.split("_")  # verify_approve_sid_aid
        action = parts[1]
        student_id = int(parts[2])
        assignment_id = int(parts[3])
        approved = action == "approve"
        teacher_name = query.from_user.first_name or "Teacher"

        await query.answer()
        success = verify_flag(student_id, assignment_id, approved, teacher_name)

        if success:
            with get_db() as conn:
                row = conn.execute(
                    "SELECT telegram_id FROM students WHERE id = ?",
                    (student_id,),
                ).fetchone()

            status_text = (
                "Verification complete: marked submitted."
                if approved else
                "Verification complete: marked still missing."
            )

            try:
                if query.message.photo or query.message.document:
                    caption = (query.message.caption or "") + f"\n\n{status_text}"
                    await query.edit_message_caption(caption=caption)
                else:
                    await query.edit_message_text(
                        (query.message.text or "") + f"\n\n{status_text}"
                    )
            except Exception:
                await query.message.reply_text(status_text)

            if row and row["telegram_id"]:
                learner_text = (
                    "Your teacher verified this as submitted."
                    if approved else
                    "Your teacher could not verify this yet. Please resubmit and flag again."
                )
                try:
                    await query._bot.send_message(
                        chat_id=row["telegram_id"],
                        text=learner_text,
                        reply_markup=InlineKeyboardMarkup(back_kb()),
                    )
                except Exception as exc:
                    print(f"Could not notify learner: {exc}")
        else:
            try:
                if query.message.photo or query.message.document:
                    caption = (query.message.caption or "") + "\n\nAlready processed."
                    await query.edit_message_caption(caption=caption)
                else:
                    await query.edit_message_text(
                        (query.message.text or "") + "\n\nAlready processed."
                    )
            except Exception:
                await query.message.reply_text("Already processed.")
        return True

    if data == "broadcast_confirm":
        if not _is_teacher(query.from_user.id):
            await query.answer("Teacher only", show_alert=True)
            return True

        await query.answer("Sending...")
        targets = context.user_data.get("broadcast_targets", [])
        sent = 0

        for student in targets:
            if not student.get("telegram_id"):
                continue
            missing = get_missing_work(student["id"])
            if not missing:
                continue
            titles = "\n".join(f"- {m['title']}" for m in missing[:12])
            text = (
                "Reminder: Missing Work\n\n"
                f"Hi {student['full_name'].split()[0]}, "
                f"you have {len(missing)} missing assignment(s):\n\n"
                f"{titles}\n\n"
                "Open /start to review and flag."
            )
            try:
                await query._bot.send_message(chat_id=student["telegram_id"], text=text)
                sent += 1
            except Exception as exc:
                print(f"Could not message {student['full_name']}: {exc}")

        context.user_data.pop("broadcast_targets", None)
        await query.edit_message_text(f"Broadcast complete. Sent to {sent} learner(s).")
        return True

    if data == "broadcast_cancel":
        await query.answer()
        context.user_data.pop("broadcast_targets", None)
        await query.edit_message_text("Broadcast cancelled.")
        return True

    if data.startswith("campaign_tpl_"):
        if not _is_teacher(query.from_user.id):
            await query.answer("Teacher only", show_alert=True)
            return True

        await query.answer()
        key = data.replace("campaign_tpl_", "", 1)
        if key == "custom":
            context.user_data["teacher_state"] = "awaiting_campaign_custom"
            context.user_data["campaign_template_key"] = "custom"
            await query.edit_message_text(
                "Send your custom campaign template as a text message.\n\n"
                "Placeholders: {first_name}, {full_name}, {missing_count}, {missing_list}"
            )
            return True

        template = CAMPAIGN_TEMPLATES.get(key)
        if not template:
            await query.edit_message_text("Unknown template. Send /campaign and try again.")
            return True

        context.user_data["campaign_template_key"] = key
        context.user_data["campaign_template_text"] = template
        context.user_data["teacher_state"] = "awaiting_campaign_schedule"
        await query.edit_message_text(
            "Template selected.\n\nPreview:\n\n"
            f"{_campaign_template_preview(template)}\n\n"
            "Choose schedule:",
            reply_markup=InlineKeyboardMarkup(campaign_schedule_kb()),
        )
        return True

    if data.startswith("campaign_sched_"):
        if not _is_teacher(query.from_user.id):
            await query.answer("Teacher only", show_alert=True)
            return True

        await query.answer()
        if context.user_data.get("teacher_state") != "awaiting_campaign_schedule":
            await query.edit_message_text(
                "No campaign template selected. Send /campaign to start."
            )
            return True

        token = data.replace("campaign_sched_", "", 1)
        run_at, label = _resolve_schedule(token)
        template_key = context.user_data.get("campaign_template_key", "gentle")
        template_text = context.user_data.get("campaign_template_text")
        if not template_text:
            template_text = CAMPAIGN_TEMPLATES.get(template_key, CAMPAIGN_TEMPLATES["gentle"])

        job_id = create_campaign_job(
            created_by=str(query.from_user.id),
            template_key=template_key,
            template_text=template_text,
            run_at=run_at.strftime("%Y-%m-%d %H:%M:%S"),
            schedule_label=label,
        )
        _clear_campaign_state(context)
        await query.edit_message_text(
            f"Campaign scheduled.\n"
            f"Job ID: {job_id}\n"
            f"Run at: {run_at.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"Template: {template_key}"
        )
        return True

    if data == "campaign_cancel":
        if not _is_teacher(query.from_user.id):
            await query.answer("Teacher only", show_alert=True)
            return True
        await query.answer()
        _clear_campaign_state(context)
        await query.edit_message_text("Campaign setup cancelled.")
        return True

    return False


async def _execute_campaign_job(bot: Bot, job: dict) -> tuple[int, int]:
    targets = get_all_students_with_telegram()
    targets = [s for s in targets if (s.get("total_missing") or 0) > 0]

    template_key = job.get("template_key") or "gentle"
    template_text = job.get("template_text") or CAMPAIGN_TEMPLATES.get(
        template_key, CAMPAIGN_TEMPLATES["gentle"]
    )

    sent = 0
    for student in targets:
        if not student.get("telegram_id"):
            continue
        missing = get_missing_work(student["id"])
        if not missing:
            continue

        text = _render_campaign_message(template_text, student, missing)
        try:
            await bot.send_message(chat_id=student["telegram_id"], text=text)
            sent += 1
        except Exception as exc:
            print(f"Campaign send failed for {student['full_name']}: {exc}")

    return len(targets), sent


async def campaign_worker(bot: Bot):
    while True:
        jobs = get_due_campaign_jobs()
        for job in jobs:
            if not claim_campaign_job(job["id"]):
                continue
            try:
                target_count, sent_count = await _execute_campaign_job(bot, job)
                complete_campaign_job(job["id"], target_count, sent_count)
            except Exception as exc:
                fail_campaign_job(job["id"], str(exc))
        await asyncio.sleep(20)
