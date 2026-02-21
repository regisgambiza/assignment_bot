"""
bot/handlers/registration.py

Full registration flow:
  1. Student sends name or ID
  2. If multiple matches → show list to pick
  3. Confirm screen: "Is this you?"
  4. Yes → link telegram_id to student record
"""
from telegram import Update, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from database.db import find_student, link_student, get_student_by_telegram
from bot.keyboards import confirm_kb, selection_kb

# ── Step 1: Ask for name/ID ───────────────────────────────

async def ask_for_identity(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data["state"] = "awaiting_search"
    await update.message.reply_text(
        "👋 Welcome to *Assignment Bot!*\n\n"
        "To get started, please send me:\n"
        "• Your *Student ID number*, or\n"
        "• Your *Full Name*\n\n"
        "_You can find your Student ID on your report card._",
        parse_mode="Markdown"
    )

# ── Step 2: Handle the search input ──────────────────────

async def handle_search_input(update: Update,
                               context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Returns True if this message was consumed by registration flow"""
    if context.user_data.get("state") != "awaiting_search":
        return False

    query   = update.message.text.strip()
    results = find_student(query)

    # ── No match ──────────────────────────────────────────
    if not results:
        await update.message.reply_text(
            f"❌ No student found for *\"{query}\"*\n\n"
            "Please check your spelling and try again,\n"
            "or ask your teacher to confirm your details.",
            parse_mode="Markdown"
        )
        return True

    # ── One match → straight to confirm ──────────────────
    if len(results) == 1:
        await _show_confirm(update.message, context, results[0])
        return True

    # ── Multiple matches → let student pick ───────────────
    context.user_data["state"]      = "awaiting_selection"
    context.user_data["candidates"] = {str(s["id"]): s for s in results}

    await update.message.reply_text(
        f"Found *{len(results)} students* matching that name.\n"
        "Please tap yours:",
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(selection_kb(results))
    )
    return True

# ── Step 3: Confirm screen ────────────────────────────────

async def _show_confirm(target, context: ContextTypes.DEFAULT_TYPE,
                        student: dict):
    context.user_data["state"]           = "awaiting_confirm"
    context.user_data["pending_lms_id"]  = student["lms_id"]
    context.user_data["pending_name"]    = student["full_name"]

    masked_id = student["lms_id"][:6] + "..." + student["lms_id"][-3:]
    text = (
        "🎓 *Is this you?*\n\n"
        f"👤 Name:    *{student['full_name']}*\n"
        f"🆔 ID:       `{masked_id}`\n"
        f"📚 Class: your enrolled class"
    )

    # target can be a Message or a CallbackQuery
    if hasattr(target, "reply_text"):
        await target.reply_text(
            text, parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(confirm_kb())
        )
    else:
        await target.edit_message_text(
            text, parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(confirm_kb())
        )

# ── Step 4: Handle buttons ────────────────────────────────

async def handle_reg_buttons(update: Update,
                              context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Returns True if this button was consumed by registration flow"""
    query = update.callback_query
    data  = query.data
    state = context.user_data.get("state")

    # ── Student picks from list ───────────────────────────
    if data.startswith("select_") and state == "awaiting_selection":
        await query.answer()
        student_id = data.split("_")[1]
        candidates = context.user_data.get("candidates", {})
        student    = candidates.get(student_id)

        if not student:
            await query.edit_message_text(
                "⚠️ Something went wrong. Type /start to try again."
            )
            return True

        await _show_confirm(query, context, student)
        return True

    # ── Confirmed: link the account ───────────────────────
    if data == "reg_confirm" and state == "awaiting_confirm":
        await query.answer("✅ Linking your account...")
        telegram_id  = str(query.from_user.id)
        tg_username  = query.from_user.username
        lms_id       = context.user_data.get("pending_lms_id")
        full_name    = context.user_data.get("pending_name", "")
        first_name   = full_name.split()[0]

        success = link_student(lms_id, telegram_id, tg_username)
        context.user_data.clear()

        if success:
            await query.edit_message_text(
                f"✅ *Welcome, {first_name}!* 🎉\n\n"
                "Your account is now linked.\n"
                "Type /start any time to check your assignments.",
                parse_mode="Markdown"
            )
            # Send main menu right away
            student = get_student_by_telegram(telegram_id)
            from bot.handlers.student import show_menu
            await show_menu(query.message, student, edit=False)
        else:
            await query.edit_message_text(
                "⚠️ This account is already linked to another Telegram user.\n\n"
                "Contact your teacher if this is a mistake."
            )
        return True

    # ── Cancelled ─────────────────────────────────────────
    if data == "reg_cancel":
        await query.answer()
        context.user_data.clear()
        context.user_data["state"] = "awaiting_search"
        await query.edit_message_text(
            "No problem! Please send your *Student ID* or *Full Name* again.",
            parse_mode="Markdown"
        )
        return True

    return False   # not a registration button

