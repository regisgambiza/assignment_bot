"""
run_all.py

Start both the Telegram bot and Flask web dashboard in one go.

Usage:
    python run_all.py

This script:
- Starts the bot (Telegram polling) in one thread
- Starts the Flask web server in another thread
- Handles graceful shutdown (Ctrl+C stops both)
"""
import threading
import time
import sys
from pathlib import Path

# Ensure imports work correctly
sys.path.insert(0, str(Path(__file__).parent))


def run_bot():
    """Run the Telegram bot in this thread"""
    try:
        print("\n" + "="*60)
        print("🤖 Starting Telegram Bot...")
        print("="*60)
        from bot.main import main
        main()
    except KeyboardInterrupt:
        print("\n[Bot] Shutdown requested")
    except Exception as e:
        print(f"\n[Bot] Error: {e}", file=sys.stderr)
        raise


def run_dashboard():
    """Run the Flask dashboard in this thread"""
    try:
        # Give bot a moment to initialize
        time.sleep(1)
        print("\n" + "="*60)
        print("🌐 Starting Flask Dashboard...")
        print("   Open: http://127.0.0.1:8787")
        print("="*60)
        from teacher_dashboard.app import app, _ensure_campaign_worker
        _ensure_campaign_worker()
        # Note: debug=False and use_reloader=False required for threading on Windows
        app.run(host="127.0.0.1", port=8787, debug=False, use_reloader=False)
    except KeyboardInterrupt:
        print("\n[Dashboard] Shutdown requested")
    except Exception as e:
        print(f"\n[Dashboard] Error: {e}", file=sys.stderr)
        raise


def main():
    """Start both services in parallel threads"""
    print("\n" + "="*60)
    print("🚀 Assignment Bot - Full Launcher")
    print("="*60)
    print("\nStarting both bot and dashboard...\n")

    # Create threads
    bot_thread = threading.Thread(target=run_bot, daemon=True)
    dashboard_thread = threading.Thread(target=run_dashboard, daemon=True)

    try:
        # Start both
        bot_thread.start()
        dashboard_thread.start()

        print("\n" + "="*60)
        print("✅ Both services running")
        print("="*60)
        print("\n📱 Bot: Check Telegram @botname for messages")
        print("🌐 Dashboard: http://127.0.0.1:8787")
        print("\nPress Ctrl+C to stop all services...\n")

        # Keep main thread alive
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        print("\n\n" + "="*60)
        print("🛑 Shutting down all services...")
        print("="*60)
        # Threads are daemonic, so they'll exit when main exits
        sys.exit(0)


if __name__ == "__main__":
    main()
