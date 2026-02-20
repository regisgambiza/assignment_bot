import os
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).parent
# Always load this project's root .env and let it override inherited shell vars.
load_dotenv(BASE_DIR / ".env", override=True)


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _float_env(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return float(raw)
    except ValueError:
        return default

# ── Paths ─────────────────────────────────────────────────
DB_PATH  = BASE_DIR / os.getenv("DB_PATH", "database/class.db")

# ── Telegram ──────────────────────────────────────────────
BOT_TOKEN          = os.getenv("BOT_TOKEN")
TEACHER_TELEGRAM_ID = os.getenv("TEACHER_TELEGRAM_ID")

# ── Ollama ────────────────────────────────────────────────
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "gpt-oss:20b")
OLLAMA_KEEP_ALIVE = os.getenv("OLLAMA_KEEP_ALIVE", "30m")
OLLAMA_NUM_PREDICT = _int_env("OLLAMA_NUM_PREDICT", 96)
OLLAMA_NUM_CTX = _int_env("OLLAMA_NUM_CTX", 1024)
OLLAMA_TEMPERATURE = _float_env("OLLAMA_TEMPERATURE", 0.2)
OLLAMA_TOP_P = _float_env("OLLAMA_TOP_P", 0.9)
AI_MAX_MISSING_ITEMS = _int_env("AI_MAX_MISSING_ITEMS", 6)
AI_MAX_GRADE_ITEMS = _int_env("AI_MAX_GRADE_ITEMS", 6)
AI_TIMEOUT_SEC = _int_env("AI_TIMEOUT_SEC", 45)

# ── App ───────────────────────────────────────────────────
COURSE_NAME    = "8/1 Mathematics"
COURSE_ID      = 1
AT_RISK_THRESHOLD = 3   # missing assignments before flagged as at-risk

if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN is missing from .env")
