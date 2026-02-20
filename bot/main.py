"""
bot/main.py â€” Entry point. Run this to start the bot.
Usage: python -m bot.main
"""
import asyncio
import sys
import platform
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

# â”€â”€ Fix: Windows + Python 3.12+ needs this â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
if platform.system() == "Windows" and sys.version_info < (3, 14):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from telegram.ext import (
    ApplicationBuilder, CommandHandler,
    CallbackQueryHandler, MessageHandler, filters
)
from bot.handlers.student import start, button_handler, message_handler
from bot.handlers.teacher import (
    teacher_panel, pending_flags, at_risk,
    broadcast, generate_links
)
from services.ai_service import ai_worker
from database.db import init_db
from config import BOT_TOKEN

# â”€â”€ Start up â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def ensure_event_loop():
    """
    Python 3.14+ no longer creates a default event loop for get_event_loop().
    PTB 21.x still calls get_event_loop() internally in run_polling().
    """
    try:
        asyncio.get_event_loop()
    except RuntimeError:
        asyncio.set_event_loop(asyncio.new_event_loop())


async def post_init(app):
    """Runs once when bot starts"""
    init_db()
    asyncio.create_task(ai_worker())
    me = await app.bot.get_me()
    print(f"Bot running: @{me.username}")
    print(f"Link: t.me/{me.username}")

# â”€â”€ Error handler â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

async def error_handler(update, context):
    print(f"Error: {context.error}")
    if update and update.effective_message:
        await update.effective_message.reply_text(
            "âš ï¸ Something went wrong. Please try again or type /start."
        )

# â”€â”€ Build and run â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def main():
    ensure_event_loop()

    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .build()
    )

    # â”€â”€ Student commands â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    app.add_handler(CommandHandler("start", start))

    # â”€â”€ Teacher commands â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    app.add_handler(CommandHandler("teacher",   teacher_panel))
    app.add_handler(CommandHandler("pending",   pending_flags))
    app.add_handler(CommandHandler("atrisk",    at_risk))
    app.add_handler(CommandHandler("broadcast", broadcast))
    app.add_handler(CommandHandler("links",     generate_links))

    # â”€â”€ All button taps â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    app.add_handler(CallbackQueryHandler(button_handler))

    # â”€â”€ All free-text messages â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    app.add_handler(MessageHandler(
        filters.TEXT & ~filters.COMMAND,
        message_handler
    ))

    # â”€â”€ Errors â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
    app.add_error_handler(error_handler)

    print("Starting bot...")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()

