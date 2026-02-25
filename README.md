# 🎓 Assignment Bot — 8/1 Mathematics

Telegram bot that gives students real-time access to their assignment status,
grades, missing work, and AI-powered study assistance.

---

## Project Structure

```
assignment_bot/
├── database/
│   ├── schema.sql          # All tables, indexes, views
│   ├── db.py               # All DB queries
│   └── seed.py             # Load initial data
├── bot/
│   ├── main.py             # Entry point ← run this
│   ├── keyboards.py        # All inline keyboards
│   └── handlers/
│       ├── student.py      # Student commands & buttons
│       ├── teacher.py      # Teacher commands & broadcast
│       └── registration.py # Register by name or ID
├── services/
│   └── ai_service.py       # Ollama async queue
├── sync/
│   └── importer.py         # Re-import from LMS report
├── config.py               # All settings from .env
├── requirements.txt
└── .env                    # Your secrets ← fill this in
```

---

## Setup (Step by Step)

### 1. Get your Bot Token
```
1. Open Telegram → search @BotFather
2. Send /newbot
3. Follow prompts → get your token
```

### 2. Get your Telegram ID
```
1. Search @userinfobot on Telegram
2. Send /start → it shows your ID
```

### 3. Fill in .env
```env
BOT_TOKEN=7234567890:AAF...your_token...
TEACHER_TELEGRAM_ID=123456789
OLLAMA_MODEL=llama3.2
DB_PATH=database/class.db
```

### 4. Install dependencies
```bash
pip install -r requirements.txt
```

### 5. Pull Ollama model
```bash
ollama pull llama3.2
```

### 6. Seed the database
```bash
python -m database.seed
```

### 7. Run the bot
```bash
python -m bot.main
```

---

## Student Commands

| Command | What it does |
|---------|-------------|
| `/start` | Register or open main menu |

## Teacher Commands

| Command | What it does |
|---------|-------------|
| `/teacher` | Open teacher panel |
| `/pending` | Review flagged submissions |
| `/atrisk` | See students with 3+ missing |
| `/broadcast` | Send reminders to all missing-work students |
| `/links` | Generate personal registration links |

---

## Re-importing Data

When you export a new report from your LMS:
```bash
# Preview first (no changes)
python -m sync.importer --file new_report.txt --dry-run

# Import for real
python -m sync.importer --file new_report.txt
```

---

## Registration Links

Generate a personal link for each student:
```
/links  (in Telegram, as teacher)
```

Each student gets: `t.me/your_bot?start=STUDENT_ID`
They tap it → confirmed screen → one tap to register. No typing required.

---

## Deploy to Railway (always-on)

```bash
# Install Railway CLI
npm install -g @railway/cli

# Login and deploy
railway login
railway init
railway up
```

Set your .env variables in Railway dashboard under Variables.

---

## Teacher Web Dashboard

Run the new browser dashboard:

```bash
pip install -r requirements.txt
python -m teacher_dashboard
```

Open:

```
http://127.0.0.1:8787
```

Main features:
- Live overview analytics
- Learner search, detail view, unlink, summary rebuild
- Pending report verification with evidence preview/download
- At-risk learner tracking
- Campaign scheduling and job history
  - Scheduled jobs are auto-processed by the dashboard worker
  - `Run Due Now` button can trigger immediate sending
- Maintenance actions (backup, schema init, summary rebuild)
- Maintenance -> Google Classroom Sync (background, non-blocking)
- CSV export for learners and pending reports

Optional `.env` keys for Classroom sync:
- `GOOGLE_CLASSROOM_CREDENTIALS_FILE` (default: `learner_data_writer/client_secrets.json`)
- `GOOGLE_CLASSROOM_TOKEN_FILE` (default: `learner_data_writer/token.json`)
- `CLASSROOM_SYNC_SCHOOL_NAME` (default: `School`)
- `CLASSROOM_SYNC_SOURCE` (default: `google_classroom_sync`)
