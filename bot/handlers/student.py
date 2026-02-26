"""
bot/handlers/student.py

All student-facing commands and button interactions.
"""
import asyncio
import re
from datetime import date, timedelta
from telegram import Update, InlineKeyboardMarkup, Message
from telegram.ext import ContextTypes
from database.db import (
    get_student_by_telegram, get_missing_work,
    get_summary, flag_submission, get_submitted_work,
    add_submission_proof, get_projection_snapshot, get_student_work_filtered,
    get_student_course_name
)
from bot.keyboards import (
    main_menu_kb, grades_kb, missing_kb,
    back_kb, ai_followup_kb, flag_proof_kb
)
from services.ai_service import ask_ai, queue_size
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    telegram_id = str(update.effective_user.id)

    # Deep link: t.me/bot?start=STUDENT_LMS_ID
    if context.args:
        from database.db import find_student

        lms_id = context.args[0]
        results = find_student(lms_id)
        if results and len(results) == 1:
            from bot.handlers.registration import _show_confirm

            await _show_confirm(update.message, context, results[0])
            return

    student = get_student_by_telegram(telegram_id)
    if student:
        await show_menu(update.message, student)
        return

    from bot.handlers.registration import ask_for_identity

    await ask_for_identity(update, context)


async def show_menu(message: Message, student: dict, edit: bool = False):
    summary = get_summary(student["id"])
    course_name = get_student_course_name(student["id"]) or "Your enrolled class"
    missing_count = summary["total_missing"] if summary else 0
    first = student["full_name"].split()[0]

    flag = f"WARNING: {missing_count} missing" if missing_count > 0 else "All caught up"
    text = f"Hey *{first}*! {flag}\n_{course_name}_"
    markup = InlineKeyboardMarkup(main_menu_kb(missing_count))

    if edit:
        await message.edit_text(text, parse_mode="Markdown", reply_markup=markup)
    else:
        await message.reply_text(text, parse_mode="Markdown", reply_markup=markup)


async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    from bot.handlers.registration import handle_reg_buttons

    if await handle_reg_buttons(update, context):
        return

    from bot.handlers.teacher import handle_teacher_buttons

    if await handle_teacher_buttons(update, context):
        return

    query = update.callback_query
    await query.answer()
    data = query.data
    telegram_id = str(query.from_user.id)
    student = get_student_by_telegram(telegram_id)

    if not student:
        await query.edit_message_text("You are not registered yet. Type /start to begin.")
        return

    if data == "summary":
        s = get_summary(student["id"])
        if not s:
            await query.edit_message_text("No summary data yet.")
            return

        submitted = s["total_submitted"]
        if submitted is None:
            submitted = s["total_assigned"] - s["total_missing"]

        overall_avg = (
            (float(s["points_earned"] or 0) * 100.0 / float(s["points_possible"]))
            if s["points_possible"] else 0.0
        )
        earned_num = float(s["points_earned"] or 0.0)
        possible_num = float(s["points_possible"] or 0.0)

        pct = round(submitted / s["total_assigned"] * 100) if s["total_assigned"] else 0
        bar = _progress_bar(pct)

        await query.edit_message_text(
            f"*{student['full_name'].split()[0]}'s Summary*\n\n"
            f"Total assigned: *{s['total_assigned']}*\n"
            f"Submitted: *{submitted}*\n"
            f"Missing: *{s['total_missing']}*\n"
            f"Average: *{overall_avg:.2f}%*\n"
            f"Points: *{earned_num:.2f}/{possible_num:.2f}*\n\n"
            f"Completion: {bar} {pct}%",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(back_kb()),
        )

    elif data == "grades":
        submitted = get_submitted_work(student["id"])
        if not submitted:
            await query.edit_message_text(
                "Submitted Work\n\nNo submitted assignments yet.",
                reply_markup=InlineKeyboardMarkup(grades_kb()),
            )
            return

        blocks = []
        for i, g in enumerate(submitted, 1):
            title = g["title"].strip()
            status = g["status"] or "Submitted"
            due = str(g["due_date"])[:10] if g["due_date"] else "-"

            if g["score_raw"]:
                if g["score_pct"] is not None:
                    score = f"{g['score_raw']} ({float(g['score_pct']):.1f}%)"
                else:
                    score = str(g["score_raw"])
            else:
                score = "Pending"

            blocks.append(
                f"{i}. {title}\n"
                f"   Status: {status} | Score: {score} | Due: {due}"
            )

        chunks = _build_chunks(
            header=f"Submitted Work ({len(submitted)})",
            blocks=blocks,
        )

        await query.edit_message_text(
            chunks[0],
            reply_markup=InlineKeyboardMarkup(grades_kb()),
        )
        for chunk in chunks[1:]:
            await query.message.reply_text(chunk)

    elif data == "projection":
        context.user_data["state"] = "awaiting_projection_target"
        await query.edit_message_text(
            "Grade Projection\n\n"
            "Send your target overall percentage (for example: 80).\n"
            "I will calculate what you need on remaining assignments.",
            reply_markup=InlineKeyboardMarkup(back_kb()),
        )

    elif data == "missing":
        missing = get_missing_work(student["id"])
        if not missing:
            await query.edit_message_text(
                "*No missing work!*\nYou are all caught up.",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(back_kb()),
            )
        else:
            lines = []
            for i, m in enumerate(missing, 1):
                marker = " (already reported)" if m["flagged_by_student"] else ""
                lines.append(f"{i}. {m['title']}{marker}")

            await query.edit_message_text(
                f"*Missing Work ({len(missing)} items):*\n\n"
                + "\n".join(lines)
                + "\n\nTap a button below to report any as submitted:",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(missing_kb(missing)),
            )

    elif data.startswith("flag_"):
        assignment_id = int(data.split("_")[1])
        success = flag_submission(student["id"], assignment_id)

        if success:
            context.user_data["state"] = "awaiting_flag_proof"
            context.user_data["pending_flag_assignment_id"] = assignment_id
            await query.edit_message_text(
                "Report saved.\n\n"
                "Upload a screenshot/photo as proof now,\n"
                "or tap Skip Proof to continue without evidence.",
                reply_markup=InlineKeyboardMarkup(flag_proof_kb(assignment_id)),
            )
        else:
            await query.edit_message_text(
                "Could not report that assignment.\n"
                "It may already be reported or marked as submitted.",
                reply_markup=InlineKeyboardMarkup(back_kb()),
            )

    elif data.startswith("proof_skip_"):
        assignment_id = int(data.split("_")[2])
        context.user_data.pop("pending_flag_assignment_id", None)
        context.user_data.pop("state", None)

        from bot.handlers.teacher import notify_teacher_of_flag
        teacher_notified = await notify_teacher_of_flag(query._bot, student, assignment_id)
        text = (
            "Report submitted without proof.\n\n"
            "Your teacher has been notified."
            if teacher_notified else
            "Report saved, but teacher notification failed right now.\n"
            "Teacher can still review this in /pending."
        )
        await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(back_kb()),
        )

    elif data == "ask_ai":
        context.user_data["state"] = "awaiting_ai_question"
        await query.edit_message_text(
            "*Ask me anything about your assignments!*\n\n"
            "_e.g. 'What should I focus on first?'_\n"
            "_or 'How am I doing overall?'_\n"
            "_or 'Give me study tips for the quiz'_",
            parse_mode="Markdown",
        )

    elif data == "back":
        await show_menu(query.message, student, edit=True)


async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    from bot.handlers.registration import handle_search_input
    from bot.handlers.teacher import handle_teacher_text_input, notify_teacher_of_flag

    if await handle_teacher_text_input(update, context):
        return

    if await handle_search_input(update, context):
        return

    if not update.message:
        return

    telegram_id = str(update.effective_user.id)
    student = get_student_by_telegram(telegram_id)

    if context.user_data.get("state") == "awaiting_flag_proof":
        if not student:
            await update.message.reply_text("Type /start to register first.")
            context.user_data.pop("state", None)
            context.user_data.pop("pending_flag_assignment_id", None)
            return

        assignment_id = context.user_data.get("pending_flag_assignment_id")
        if not assignment_id:
            context.user_data.pop("state", None)
            await update.message.reply_text("No pending report found. Use /start to continue.")
            return

        file_id = None
        file_type = None
        if update.message.photo:
            file_id = update.message.photo[-1].file_id
            file_type = "photo"
        elif update.message.document:
            file_id = update.message.document.file_id
            file_type = "document"

        if not file_id:
            await update.message.reply_text(
                "Please upload a photo/screenshot, or tap Skip Proof on the previous message."
            )
            return

        saved = add_submission_proof(
            student_id=student["id"],
            assignment_id=int(assignment_id),
            file_id=file_id,
            file_type=file_type,
            caption=update.message.caption,
        )
        context.user_data.pop("state", None)
        context.user_data.pop("pending_flag_assignment_id", None)

        teacher_notified = await notify_teacher_of_flag(
            update.message.get_bot(), student, int(assignment_id)
        )
        if saved and teacher_notified:
            await update.message.reply_text(
                "Proof received. Your teacher has been notified.",
                reply_markup=InlineKeyboardMarkup(back_kb()),
            )
        elif saved:
            await update.message.reply_text(
                "Proof received, but teacher notification failed right now.\n"
                "Teacher can still review this in /pending.",
                reply_markup=InlineKeyboardMarkup(back_kb()),
            )
        else:
            await update.message.reply_text(
                "Could not attach proof to that report. Please report again from Missing Work."
            )
        return

    if not student:
        await update.message.reply_text("Type /start to register and check your assignments.")
        return

    if context.user_data.get("state") == "awaiting_projection_target":
        text = (update.message.text or "").strip()
        target = _extract_target_percent(text)
        if target is None:
            await update.message.reply_text(
                "Please send a valid percentage between 1 and 100.\n"
                "Example: 85"
            )
            return

        snapshot = get_projection_snapshot(student["id"])
        if not snapshot:
            await update.message.reply_text("Not enough data yet for projection.")
            context.user_data.pop("state", None)
            return

        earned = float(snapshot["earned_points"] or 0.0)
        total_possible = float(snapshot["total_possible_points"] or 0.0)
        remaining_possible = float(snapshot["remaining_possible_points"] or 0.0)
        remaining_assignments = int(snapshot["remaining_assignments"] or 0)

        required_total = (target / 100.0) * total_possible if total_possible else 0.0
        need_points = required_total - earned
        current_pct = (earned * 100.0 / total_possible) if total_possible else 0.0

        context.user_data.pop("state", None)
        if total_possible <= 0:
            await update.message.reply_text("No assignment points are available for projection yet.")
            return

        if need_points <= 0:
            await update.message.reply_text(
                f"Target {target:.2f}%\n"
                f"You are already at {current_pct:.2f}%.\n"
                f"You have already met this target.",
                reply_markup=InlineKeyboardMarkup(back_kb()),
            )
            return

        if remaining_possible <= 0:
            await update.message.reply_text(
                f"Target {target:.2f}%\n"
                f"Current: {current_pct:.2f}%\n"
                "There are no remaining missing assignments to gain points from.",
                reply_markup=InlineKeyboardMarkup(back_kb()),
            )
            return

        needed_avg = (need_points / remaining_possible) * 100.0
        if need_points > remaining_possible:
            await update.message.reply_text(
                f"Target {target:.2f}% is not reachable with current remaining work.\n\n"
                f"Current points: {earned:.2f}/{total_possible:.2f} ({current_pct:.2f}%)\n"
                f"Remaining possible points: {remaining_possible:.2f}\n"
                f"Points needed: {need_points:.2f}\n"
                "Even perfect scores on remaining assignments are not enough.",
                reply_markup=InlineKeyboardMarkup(back_kb()),
            )
            return

        await update.message.reply_text(
            f"Target: {target:.2f}%\n"
            f"Current: {earned:.2f}/{total_possible:.2f} ({current_pct:.2f}%)\n"
            f"Remaining assignments: {remaining_assignments}\n"
            f"Remaining possible points: {remaining_possible:.2f}\n"
            f"Points needed from remaining work: {need_points:.2f}\n"
            f"Required average on remaining work: {needed_avg:.2f}%",
            reply_markup=InlineKeyboardMarkup(back_kb()),
        )
        return

    text = (update.message.text or "").strip()

    if context.user_data.get("state") == "awaiting_ai_question":
        context.user_data["state"] = None
        question = text
        position = queue_size()

        if position == 0:
            thinking = await update.message.reply_text("_Thinking..._", parse_mode="Markdown")
        else:
            thinking = await update.message.reply_text(
                f"You are *#{position + 1}* in line, hang tight...",
                parse_mode="Markdown",
            )

        async def keep_typing():
            while True:
                await update.message.chat.send_action("typing")
                await asyncio.sleep(4)

        typing_task = asyncio.create_task(keep_typing())

        try:
            answer = await ask_ai(question, student)
        except Exception:
            answer = "Sorry, I couldn't reach the AI right now. Please try again."
        finally:
            typing_task.cancel()

        await thinking.delete()
        await update.message.reply_text(
            f"AI: {answer}",
            reply_markup=InlineKeyboardMarkup(ai_followup_kb()),
        )
        return

    if text:
        filter_spec = _parse_natural_filter(text)
        if filter_spec:
            rows = get_student_work_filtered(
                student_id=student["id"],
                title_contains=filter_spec.get("title_contains"),
                due_from=filter_spec.get("due_from"),
                due_to=filter_spec.get("due_to"),
                limit=40,
            )
            if not rows:
                await update.message.reply_text(
                    f"{filter_spec['label']}\n\nNo matching assignments found.",
                    reply_markup=InlineKeyboardMarkup(back_kb()),
                )
                return

            blocks = []
            for i, row in enumerate(rows, 1):
                due = str(row["due_date"])[:10] if row["due_date"] else "-"
                score = row["score_raw"] or "-"
                pct = (
                    f"{float(row['score_pct']):.1f}%"
                    if row["score_pct"] is not None else "-"
                )
                blocks.append(
                    f"{i}. {row['title']}\n"
                    f"   Status: {row['status']} | Due: {due} | Score: {score} ({pct})"
                )

            chunks = _build_chunks(filter_spec["label"], blocks)
            await update.message.reply_text(
                chunks[0], reply_markup=InlineKeyboardMarkup(back_kb())
            )
            for chunk in chunks[1:]:
                await update.message.reply_text(chunk)
            return

    await update.message.reply_text("Type /start to see your assignments.")


def _progress_bar(pct: int, length: int = 10) -> str:
    filled = round(pct / 100 * length)
    return "#" * filled + "-" * (length - filled)


def _build_chunks(header: str, blocks: list[str], limit: int = 3900) -> list[str]:
    chunks = []
    current = header

    for block in blocks:
        candidate = f"{current}\n\n{block}" if current else block
        if len(candidate) <= limit:
            current = candidate
            continue

        if current != header:
            chunks.append(current)
        current = f"{header} (continued)\n\n{block}"
        if len(current) > limit:
            # Safety fallback for extremely long titles.
            current = current[: limit - 3] + "..."

    if current:
        chunks.append(current)

    return chunks


def _extract_target_percent(text: str) -> float | None:
    if not text:
        return None
    match = re.search(r"(\d+(?:\.\d+)?)", text)
    if not match:
        return None
    try:
        value = float(match.group(1))
    except ValueError:
        return None
    if value <= 0 or value > 100:
        return None
    return value


def _parse_natural_filter(text: str) -> dict | None:
    lowered = text.lower()
    title_contains = None
    due_from = None
    due_to = None
    label_parts = []

    if "quiz" in lowered:
        title_contains = "quiz"
        label_parts.append("Quiz work")

    if "this week" in lowered and "due" in lowered:
        today = date.today()
        week_end = today + timedelta(days=6)
        due_from = today.isoformat()
        due_to = week_end.isoformat()
        label_parts.append(f"Due this week ({due_from} to {due_to})")

    if not label_parts:
        return None

    return {
        "title_contains": title_contains,
        "due_from": due_from,
        "due_to": due_to,
        "label": "Filtered Work: " + " + ".join(label_parts),
    }
