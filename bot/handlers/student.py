"""
bot/handlers/student.py

All student-facing commands and button interactions.
"""
import asyncio
from telegram import Update, InlineKeyboardMarkup, Message
from telegram.ext import ContextTypes
from database.db import (
    get_student_by_telegram, get_missing_work,
    get_summary, flag_submission, get_submitted_work
)
from bot.keyboards import (
    main_menu_kb, grades_kb, missing_kb,
    back_kb, ai_followup_kb
)
from services.ai_service import ask_ai, queue_size
from config import COURSE_NAME


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
    missing_count = summary["total_missing"] if summary else 0
    first = student["full_name"].split()[0]

    flag = f"WARNING: {missing_count} missing" if missing_count > 0 else "All caught up"
    text = f"Hey *{first}*! {flag}\n_{COURSE_NAME}_"
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
                flag = " (flagged)" if m["flagged_by_student"] else ""
                lines.append(f"{i}. {m['title']}{flag}")

            await query.edit_message_text(
                f"*Missing Work ({len(missing)} items):*\n\n"
                + "\n".join(lines)
                + "\n\nTap a button below to flag any as submitted:",
                parse_mode="Markdown",
                reply_markup=InlineKeyboardMarkup(missing_kb(missing)),
            )

    elif data.startswith("flag_"):
        assignment_id = int(data.split("_")[1])
        success = flag_submission(student["id"], assignment_id)

        if success:
            from bot.handlers.teacher import notify_teacher_of_flag

            teacher_notified = await notify_teacher_of_flag(query._bot, student, assignment_id)

            if teacher_notified:
                status_text = (
                    "Flagged.\n\n"
                    "Your teacher has been notified.\n"
                    "They will verify and update your status shortly."
                )
            else:
                status_text = (
                    "Flagged.\n\n"
                    "Your flag was saved, but teacher notification could not be delivered right now.\n"
                    "Teacher can still review this via /pending."
                )

            await query.edit_message_text(
                status_text,
                reply_markup=InlineKeyboardMarkup(back_kb()),
            )
        else:
            await query.edit_message_text(
                "Could not flag that assignment.\n"
                "It may already be flagged or marked as submitted.",
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

    if await handle_search_input(update, context):
        return

    telegram_id = str(update.effective_user.id)
    student = get_student_by_telegram(telegram_id)

    if not student:
        await update.message.reply_text("Type /start to register and check your assignments.")
        return

    if context.user_data.get("state") == "awaiting_ai_question":
        context.user_data["state"] = None
        question = update.message.text
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

