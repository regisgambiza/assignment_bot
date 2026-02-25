"""
Enhanced Chatbot with PyQt5 GUI (Modernized like Grok)
- Modern dark theme inspired by Grok/xAI.
- Chat bubbles with sleek styling.
- Removed send button; use Enter to send messages.
- Auto-scrolls to latest message.
- Minimalistic and responsive design.
- Positions: User bubbles on right, Assistant on left.
- Added timestamps, shadows, and modern bubble styles.
- Added clear labels ("You:" and "Assistant:") to distinguish messages.
- Enhanced color contrast for better readability and distinction.
"""

import sys
import logging
import json
import re
from datetime import datetime, UTC
from collections import deque
from typing import Dict, Any, Optional, Tuple

from PyQt5.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QTextEdit, QLineEdit, QStatusBar, QLabel
)
from PyQt5.QtCore import Qt
from PyQt5.QtGui import QFont, QPalette, QColor, QIcon

try:
    from .call_ollama_classify import call_ollama_classify
    LLM_AVAILABLE = True
except Exception:
    try:
        from call_ollama_classify import call_ollama_classify
        LLM_AVAILABLE = True
    except Exception:
        LLM_AVAILABLE = False

        def call_ollama_classify(prompt, model="llama3.1:8b-instruct-q4_0"):
            logging.getLogger("enhanced_chatbot").warning("Using fallback LLM stub.")
            return "(Stub) Please install real LLM backend."

# Additional imports for fetching data
try:
    from .get_classroom_service import get_classroom_service
    from .get_all_courses import get_all_courses
    from .get_all_students import get_all_students
    from .get_all_coursework import get_all_coursework
except ImportError:  # pragma: no cover - script compatibility
    from get_classroom_service import get_classroom_service
    from get_all_courses import get_all_courses
    from get_all_students import get_all_students
    from get_all_coursework import get_all_coursework

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-5s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("enhanced_chatbot")

# ---------------- Memory Manager ----------------
class MemoryManager:
    def __init__(self, max_raw_turns=12, model=None):
        self.raw_history = deque(maxlen=max_raw_turns)
        self.model = model

    def add_turn(self, role, text):
        ts = datetime.now(UTC).isoformat()
        self.raw_history.append({"role": role, "text": text, "time": ts})

    def get_prompt_history(self):
        recent = "\n".join([f"{h['role'].capitalize()}: {h['text']}" for h in self.raw_history])
        return f"Conversation so far:\n{recent if recent else '(none)'}\n"

# ---------------- Chatbot ----------------
class EnhancedChatbot:
    def __init__(self, ollama_model=None):
        self.memory = MemoryManager(model=ollama_model)
        self.ollama_model = ollama_model or 'llama3.1:8b-instruct-q4_0'
        self.data = self._load_data()

    def _load_data(self) -> Dict:
        try:
            with open('classroom_data.json', 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            return self._fetch_and_save_data()

    def _fetch_and_save_data(self) -> Dict:
        logger.info("Fetching all classroom data at launch...")
        service = get_classroom_service()
        courses = get_all_courses(service)
        data: Dict = {"courses": []}

        for course in courses:
            logger.info(f"Collecting submissions for course: {course['name']}")
            students = get_all_students(service, course["id"])
            coursework = get_all_coursework(service, course["id"])

            # Fetch submissions in bulk
            submissions_lookup = {}
            for j, cw in enumerate(coursework, 1):
                logger.debug("Fetching submissions for coursework %d/%d: %s",
                             j, len(coursework), cw.get("title", cw["id"]))
                try:
                    subs_response = service.courses().courseWork().studentSubmissions().list(
                        courseId=course["id"],
                        courseWorkId=cw["id"],
                        pageSize=200
                    ).execute()
                    subs = subs_response.get("studentSubmissions", [])
                    for sub in subs:
                        sid = sub.get("userId")
                        if sid:
                            submissions_lookup[(sid, cw["id"])] = sub
                except Exception as e:
                    logger.warning("Error fetching submissions for coursework=%s: %s",
                                   cw.get("id"), str(e))

            course_dict: Dict = {
                "name": course['name'],
                "id": course['id'],
                "students": [],
                "coursework": [cw for cw in coursework]  # Full coursework info
            }

            # Collect data for each student
            for s in students:
                profile = s.get("profile", {})
                name_info = profile.get("name", {})
                full_name = " ".join(filter(None, [name_info.get("givenName", ""), name_info.get("familyName", "")])).strip() or s["userId"]

                student_dict: Dict = {
                    "name": full_name,
                    "id": s["userId"],
                    "submissions": []
                }

                for cw in coursework:
                    sub = submissions_lookup.get((s["userId"], cw["id"]))
                    sub_dict: Dict = {
                        "coursework_id": cw["id"],
                        "status": "Missing",
                        "score": "N/A",
                        "date": "N/A"
                    }
                    if sub and sub.get("state", "") not in ["NEW", "CREATED"]:
                        sub_dict["status"] = "Late" if sub.get("late", False) else "Submitted"
                        max_p = cw.get("maxPoints", "N/A")
                        if "assignedGrade" in sub:
                            sub_dict["score"] = f"{sub['assignedGrade']}/{max_p}" if max_p != "N/A" else f"{sub['assignedGrade']}"
                        else:
                            sub_dict["score"] = "Ungraded"
                        update_time = sub.get("updateTime", sub.get("creationTime", None))
                        sub_dict["date"] = update_time.split('T')[0] if update_time else "N/A"
                    # Add full submission details if available
                    if sub:
                        sub_dict["full_submission"] = sub

                    student_dict["submissions"].append(sub_dict)

                course_dict["students"].append(student_dict)

            data["courses"].append(course_dict)

        with open('classroom_data.json', 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)

        logger.info("Finished collecting and saving submissions data to classroom_data.json")
        return data

    def _find_student(self, query_name: str) -> Tuple[Optional[Dict], Optional[Dict]]:
        for course in self.data["courses"]:
            for student in course["students"]:
                if query_name.lower() in student["name"].lower():
                    return student, course
        return None, None

    def _compose_prompt(self, user_text, extra_context=""):
        parts = [
            """You are a helpful assistant with access to student submissions data in JSON format.
You must stick strictly to the provided data. Do not invent any names, IDs, scores, dates, or any other information.
Search the provided JSON data for the student mentioned in the user question. If the student name in the query does not match any student in the data (case-insensitive partial match), respond 'I don't have information on that student.' Do not make up data.
If no data matches the query, say 'I don't have that information.' Do not hallucinate or make up details.
The data is a JSON object with 'courses' array, each course has 'name', 'id', 'students' array (with 'name', 'id', 'submissions' array), and 'coursework' array (with details like 'id', 'title', 'description', 'creationTime', 'maxPoints', etc.).
Submissions have 'coursework_id', 'status', 'score', 'date', and optionally 'full_submission'.
To answer, parse the JSON, extract relevant info.
If the query specifies a date like 8/2, interpret as 2025-08-02 (assuming year 2025), and filter submissions where 'date' matches.
If the query is about a class or course, filter by the 'name' in courses.
For best performer, calculate the total earned points over total possible for each student (parse 'score' like '30/40' to sum numerators and denominators, ignore N/A or Ungraded), the one(s) with highest percentage is best. Only use submitted scores.
For list of learners on a date or course, extract unique student names that have submissions matching the filter.
Be accurate and factual. Only answer based on the data given in the prompt.""",
            extra_context,
            self.memory.get_prompt_history(),
            f"User question: {user_text}\nAnswer clearly and professionally."
        ]
        return "\n\n".join([p for p in parts if p.strip()])

    def handle_user_message(self, user_text):
        student, course = self._find_student(user_text)
        if student:
            context = f"Relevant data:\n{json.dumps({'student': student, 'coursework': course['coursework']}, indent=2)}"
        else:
            context = f"All data:\n{json.dumps(self.data, indent=2)}"

        prompt = self._compose_prompt(user_text, context)
        response = call_ollama_classify(prompt, model=self.ollama_model)
        self.memory.add_turn('user', user_text)
        self.memory.add_turn('assistant', response)
        return response

# ---------------- GUI ----------------
class ChatbotGUI(QWidget):
    def __init__(self, chatbot):
        super().__init__()
        self.chatbot = chatbot
        self.setWindowTitle("Student Report Chatbot - Powered by Grok")
        self.setGeometry(200, 200, 800, 600)

        # Modern dark theme like Grok
        palette = QPalette()
        palette.setColor(QPalette.Window, QColor(18, 18, 18))  # Dark background
        palette.setColor(QPalette.WindowText, QColor(240, 240, 240))  # Light text
        palette.setColor(QPalette.Base, QColor(30, 30, 30))
        palette.setColor(QPalette.AlternateBase, QColor(40, 40, 40))
        palette.setColor(QPalette.Text, QColor(240, 240, 240))
        palette.setColor(QPalette.Button, QColor(50, 50, 50))
        palette.setColor(QPalette.ButtonText, QColor(240, 240, 240))
        palette.setColor(QPalette.Highlight, QColor(0, 122, 255))  # Blue accent
        palette.setColor(QPalette.HighlightedText, QColor(255, 255, 255))
        self.setPalette(palette)

        self.setFont(QFont("Arial", 11))  # Modern sans-serif font

        self.layout = QVBoxLayout()
        self.layout.setContentsMargins(20, 20, 20, 20)
        self.layout.setSpacing(15)

        # Header label for modern look
        header = QLabel("Chat with Student Report Assistant")
        header.setStyleSheet("font-size: 18px; font-weight: bold; color: #f0f0f0; padding: 10px;")
        header.setAlignment(Qt.AlignCenter)
        self.layout.addWidget(header)

        self.chat_display = QTextEdit()
        self.chat_display.setReadOnly(True)
        self.chat_display.setStyleSheet("""
            QTextEdit {
                background-color: #1e1e1e;
                border: none;
                color: #f0f0f0;
                font-size: 14px;
                padding: 15px;
                border-radius: 10px;
            }
        """)
        self.layout.addWidget(self.chat_display, stretch=1)

        self.input_box = QLineEdit()
        self.input_box.setPlaceholderText("Ask about a student or report...")
        self.input_box.setStyleSheet("""
            QLineEdit {
                background-color: #2a2a2a;
                border: 1px solid #3a3a3a;
                border-radius: 25px;
                color: #f0f0f0;
                padding: 12px 25px;
                font-size: 14px;
            }
            QLineEdit:focus {
                border: 1px solid #007aff;
                box-shadow: 0 0 5px rgba(0, 122, 255, 0.5);
            }
        """)
        self.input_box.returnPressed.connect(self.send_message)  # Send on Enter
        self.layout.addWidget(self.input_box)

        self.status = QStatusBar()
        self.status.setStyleSheet("color: #a0a0a0; background: transparent; font-size: 12px;")
        self.layout.addWidget(self.status)

        self.setLayout(self.layout)

        # Set window icon for modern touch (assuming you have an icon file, otherwise comment out)
        # self.setWindowIcon(QIcon("path_to_grok_icon.png"))

    def append_message(self, role, message):
        current_time = datetime.now().strftime("%H:%M")
        if role == "user":
            bubble_color = "#333333"  # Slightly lighter dark gray for user to increase contrast
            text_color = "#e0e0e0"  # Softer light text for user
            align_style = "margin-left: auto;"  # Right align
            prefix = '<span style="font-weight: bold; color: #ffffff;">You:</span><br>'
        else:
            bubble_color = "#005fd7"  # Darker blue for assistant for better distinction
            text_color = "#ffffff"
            align_style = "margin-right: auto;"  # Left align
            prefix = '<span style="font-weight: bold; color: #ffffff;">Assistant:</span><br>'

        # Modern bubble styling with shadow and timestamp
        html = f"""
        <div style='background: {bubble_color}; color: {text_color}; padding: 15px; border-radius: 20px; margin: 10px 0; max-width: 70%; {align_style} box-shadow: 0 2px 5px rgba(0,0,0,0.3); width: fit-content;'>
        {prefix}{message.replace('\n', '<br>')}
        <div style='font-size: 10px; color: #b0b0b0; margin-top: 5px; text-align: right;'>{current_time}</div>
        </div>
        """
        self.chat_display.append(html)
        self.chat_display.verticalScrollBar().setValue(self.chat_display.verticalScrollBar().maximum())

    def send_message(self):
        user_text = self.input_box.text().strip()
        if not user_text:
            return
        self.append_message("user", user_text)
        self.input_box.clear()

        try:
            response = self.chatbot.handle_user_message(user_text)
            # Support basic markdown in responses
            formatted_response = response.replace("**", "<b>").replace("*", "<i>").replace("\n", "<br>")
            self.append_message("assistant", formatted_response)
            self.status.showMessage("Response received", 2000)
        except Exception as e:
            self.append_message("assistant", "Sorry, there was an error.")
            self.status.showMessage(f"Error: {str(e)}", 5000)

# ---------------- Main ----------------
if __name__ == "__main__":
    app = QApplication(sys.argv)
    chatbot = EnhancedChatbot()
    gui = ChatbotGUI(chatbot)
    gui.show()
    sys.exit(app.exec_())
