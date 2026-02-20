import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# ── Paths ─────────────────────────────────────────────────
BASE_DIR = Path(__file__).parent
DB_PATH  = BASE_DIR / os.getenv("DB_PATH", "database/class.db")

# ── Telegram ──────────────────────────────────────────────
BOT_TOKEN          = os.getenv("BOT_TOKEN")
TEACHER_TELEGRAM_ID = os.getenv("TEACHER_TELEGRAM_ID")

# ── Ollama ────────────────────────────────────────────────
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "gpt-oss:20b")

# ── App ───────────────────────────────────────────────────
COURSE_NAME    = "8/1 Mathematics"
COURSE_ID      = 1
AT_RISK_THRESHOLD = 3   # missing assignments before flagged as at-risk

if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN is missing from .env")
