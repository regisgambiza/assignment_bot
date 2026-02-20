"""
bot/handlers/teacher.py

Teacher-only commands:
  /teacher  â€” teacher panel
  /pending  â€” see flags waiting for review
  /atrisk   â€” see at-risk students
  /broadcast â€” send reminders to all students with missing work
  verify buttons â€” approve or deny flagged submissions
"""
from telegram import Update, Bot, InlineKeyboardMarkup
from telegram.error import BadRequest
from telegram.ext import ContextTypes
from database.db import (
    get_pending_flags, get_at_risk_students,
    get_all_students_with_telegram,
    get_missing_work, verify_flag,
    get_student_by_telegram
)
from bot.keyboards import verify_kb, broadcast_confirm_kb, back_kb
from config import TEACHER_TELEGRAM_ID, COURSE_NAME

# â”€â”€ Guard: teacher only â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def _is_teacher(telegram_id: str) -> bool:
    return str(telegram_id) == str(TEACHER_TELEGRAM_ID)

async def _deny_access(update: Update):
    await update.message.reply_text(
        "â›” This command is for teachers only."
    )

# â”€â”€ /teacher â€” main panel â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

async def teacher_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_teacher(update.effective_user.id):
        await _deny_access(update)
        return

    pending = get_pending_flags()
    at_risk = get_at_risk_students()

    await update.message.reply_text(
        f"ðŸ‘©â€ðŸ« *Teacher Panel â€” {COURSE_NAME}*\n\n"
        f"ðŸš© Pending flags:      *{len(pending)}*\n"
        f"âš ï¸ At-risk students: *{len(at_risk)}*\n\n"
        "Commands:\n"
        "/pending   â€” review flagged submissions\n"
        "/atrisk    â€” list at-risk students\n"
        "/broadcast â€” send missing-work reminders\n"
        "/links     â€” generate registration links",
        parse_mode="Markdown"
    )

# â”€â”€ /pending â€” flags waiting for review â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

async def pending_flags(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_teacher(update.effective_user.id):
        await _deny_access(update)
        return

    flags = get_pending_flags()
    if not flags:
        await update.message.reply_text("âœ… No pending flags â€” all clear!")
        return

    await update.message.reply_text(
        f"ðŸš© *{len(flags)} pending flag(s) to review:*",
        parse_mode="Markdown"
    )

    for f in flags:
        await update.message.reply_text(
            f"ðŸ‘¤ *{f['full_name']}*\n"
            f"ðŸ“ {f['assignment_title']}\n"
            f"ðŸ“š {f['course_name']}\n"
            f"ðŸ• Flagged: {f['flagged_at'][:16] if f['flagged_at'] else 'â€”'}",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(
                verify_kb(f["student_id"], f["assignment_id"])
            )
        )

# â”€â”€ /atrisk â€” students with many missing assignments â”€â”€â”€â”€â”€â”€

async def at_risk(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_teacher(update.effective_user.id):
        await _deny_access(update)
        return

    students = get_at_risk_students()
    if not students:
        await update.message.reply_text(
            "âœ… No at-risk students â€” everyone is on track!"
        )
        return

    lines = []
    for s in students:
        tg = f"@{s['telegram_id']}" if s["telegram_id"] else "not registered"
        lines.append(
            f"âš ï¸ *{s['full_name']}*\n"
            f"   Missing: {s['total_missing']} | Overall avg: {s['avg_all_pct']}%\n"
            f"   Telegram: {tg}"
        )

    await update.message.reply_text(
        f"âš ï¸ *At-Risk Students ({len(students)}):*\n\n"
        + "\n\n".join(lines),
        parse_mode="Markdown"
    )

# â”€â”€ /broadcast â€” send reminders to all with missing work â”€â”€

async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_teacher(update.effective_user.id):
        await _deny_access(update)
        return

    students     = get_all_students_with_telegram()
    needs_remind = [s for s in students if (s["total_missing"] or 0) > 0]

    if not needs_remind:
        await update.message.reply_text("âœ… No students have missing work!")
        return

    context.user_data["broadcast_targets"] = needs_remind
    await update.message.reply_text(
        f"ðŸ“¢ *Broadcast Preview*\n\n"
        f"Will send reminders to *{len(needs_remind)} students* with missing work:\n\n"
        + "\n".join([
            f"â€¢ {s['full_name']} ({s['total_missing']} missing)"
            for s in needs_remind[:10]
        ])
        + (f"\n_...and {len(needs_remind)-10} more_" if len(needs_remind) > 10 else ""),
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(broadcast_confirm_kb())
    )

# â”€â”€ /links â€” generate personal registration links â”€â”€â”€â”€â”€â”€â”€â”€â”€

async def generate_links(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not _is_teacher(update.effective_user.id):
        await _deny_access(update)
        return

    from database.db import get_db
    with get_db() as conn:
        students = conn.execute(
            "SELECT lms_id, full_name, telegram_id FROM students ORDER BY full_name"
        ).fetchall()

    # Get bot username dynamically
    bot_me = await context.bot.get_me()
    username = bot_me.username

    lines = []
    for s in students:
        status = "âœ… registered" if s["telegram_id"] else "â³ not yet"
        lines.append(
            f"*{s['full_name']}* ({status})\n"
            f"`t.me/{username}?start={s['lms_id']}`"
        )

    await update.message.reply_text(
        "ðŸ”— *Personal Registration Links:*\n\n"
        + "\n\n".join(lines),
        parse_mode="Markdown"
    )

# â”€â”€ Notify teacher when student flags a submission â”€â”€â”€â”€â”€â”€â”€â”€

async def notify_teacher_of_flag(bot: Bot, student: dict, assignment_id: int) -> bool:
    teacher_chat_id = str(TEACHER_TELEGRAM_ID or "").strip()
    if not teacher_chat_id or not teacher_chat_id.isdigit():
        print("Could not notify teacher: TEACHER_TELEGRAM_ID is missing or invalid.")
        return False

    from database.db import get_db
    with get_db() as conn:
        assignment = conn.execute(
            "SELECT title FROM assignments WHERE id = ?",
            (assignment_id,)
        ).fetchone()

    if not assignment:
        return False

    try:
        await bot.send_message(
            chat_id=int(teacher_chat_id),
            text=(
                f"New Flag - Needs Review\\n\\n"
                f"Student: {student['full_name']}\\n"
                f"Assignment: {assignment['title']}"
            ),
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(
                verify_kb(student["id"], assignment_id)
            )
        )
        return True
    except BadRequest as e:
        if "Chat not found" in str(e):
            print(
                "Could not notify teacher: chat not found. Open the bot in the "
                "teacher account and send /start once."
            )
        else:
            print(f"Could not notify teacher: {e}")
    except Exception as e:
        print(f"Could not notify teacher: {e}")
    return False
async def handle_teacher_buttons(update: Update,
                                  context: ContextTypes.DEFAULT_TYPE) -> bool:
    query = update.callback_query
    data  = query.data

    # â”€â”€ Verify approve/deny â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    if data.startswith("verify_"):
        if not _is_teacher(query.from_user.id):
            await query.answer("â›” Teacher only", show_alert=True)
            return True

        parts      = data.split("_")   # verify_approve_sid_aid
        action     = parts[1]          # "approve" or "deny"
        student_id = int(parts[2])
        assign_id  = int(parts[3])
        approved   = action == "approve"
        teacher    = query.from_user.first_name or "Teacher"

        await query.answer()
        success = verify_flag(student_id, assign_id, approved, teacher)

        if success:
            # Get student's telegram_id to notify them
            from database.db import get_db
            with get_db() as conn:
                row = conn.execute(
                    "SELECT telegram_id, full_name FROM students WHERE id = ?",
                    (student_id,)
                ).fetchone()

            assignment_title = query.message.text.split("ðŸ“")[1].split("\n")[0].strip() \
                               if "ðŸ“" in query.message.text else "the assignment"

            if approved:
                status_text = "âœ… *Marked as Submitted*"
                student_msg = f"âœ… Your teacher verified *{assignment_title}* as submitted! ðŸŽ‰"
            else:
                status_text = "âŒ *Marked as Still Missing*"
                student_msg = (
                    f"âŒ Your teacher couldn't find *{assignment_title}*.\n"
                    "Please resubmit and flag again."
                )

            await query.edit_message_text(
                query.message.text + f"\n\n{status_text}",
                parse_mode="Markdown"
            )

            # Notify the student
            if row and row["telegram_id"]:
                try:
                    await query._bot.send_message(
                        chat_id=row["telegram_id"],
                        text=student_msg,
                        parse_mode="Markdown",
                        reply_markup=InlineKeyboardMarkup(back_kb())
                    )
                except Exception as e:
                    print(f"âš ï¸  Could not notify student: {e}")
        else:
            await query.edit_message_text(
                query.message.text + "\n\nâš ï¸ Already processed."
            )
        return True

    # â”€â”€ Broadcast confirm â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    if data == "broadcast_confirm":
        if not _is_teacher(query.from_user.id):
            await query.answer("â›” Teacher only", show_alert=True)
            return True

        await query.answer("ðŸ“¢ Sending...")
        targets = context.user_data.get("broadcast_targets", [])
        sent = 0

        for student in targets:
            if not student.get("telegram_id"):
                continue
            missing = get_missing_work(student["id"])
            if not missing:
                continue
            titles = "\n".join([f"  â€¢ {m['title']}" for m in missing])
            try:
                await query._bot.send_message(
                    chat_id=student["telegram_id"],
                    text=(
                        f"ðŸ“¢ *Reminder â€” Missing Work*\n\n"
                        f"Hi *{student['full_name'].split()[0]}*, "
                        f"you have *{len(missing)} missing assignment(s)*:\n\n"
                        f"{titles}\n\n"
                        "Tap /start to check details."
                    ),
                    parse_mode="Markdown"
                )
                sent += 1
            except Exception as e:
                print(f"âš ï¸  Could not message {student['full_name']}: {e}")

        context.user_data.pop("broadcast_targets", None)
        await query.edit_message_text(
            f"âœ… *Broadcast complete!*\n\nSent to {sent} student(s).",
            parse_mode="Markdown"
        )
        return True

    if data == "broadcast_cancel":
        await query.answer()
        context.user_data.pop("broadcast_targets", None)
        await query.edit_message_text("ðŸ“¢ Broadcast cancelled.")
        return True

    return False

