"""
bot/handlers/student.py

All student-facing commands and button interactions.
"""
import asyncio
from telegram import Update, InlineKeyboardMarkup, Message
from telegram.ext import ContextTypes
from database.db import (
    get_student_by_telegram, get_missing_work,
    get_grades, get_summary, flag_submission
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

        submitted = s["total_assigned"] - s["total_missing"]
        pct = round(submitted / s["total_assigned"] * 100) if s["total_assigned"] else 0
        bar = _progress_bar(pct)

        await query.edit_message_text(
            f"*{student['full_name'].split()[0]}'s Summary*\n\n"
            f"Total assigned: *{s['total_assigned']}*\n"
            f"Submitted: *{submitted}*\n"
            f"Missing: *{s['total_missing']}*\n"
            f"Avg (submitted): *{s['avg_submitted_pct']}%*\n"
            f"Avg (overall): *{s['avg_all_pct']}%*\n"
            f"Points: *{s['points_earned']}/{s['points_possible']}*\n\n"
            f"Completion: {bar} {pct}%",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(back_kb()),
        )

    elif data == "grades":
        grades = get_grades(student["id"])
        lines = []
        for g in grades[:10]:
            status = "[OK]" if g["status"] == "Submitted" else "[MISSING]"
            score = g["score_raw"] or "-"
            title = g["title"][:22] + "..." if len(g["title"]) > 22 else g["title"]
            lines.append(f"{status} {title} - *{score}*")

        await query.edit_message_text(
            "*Recent Grades* (last 10)\n\n" + "\n".join(lines),
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(grades_kb()),
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
