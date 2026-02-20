from telegram import InlineKeyboardButton


def main_menu_kb(missing_count: int = 0) -> list:
    missing_label = (
        f"Missing Work ({missing_count})"
        if missing_count > 0
        else "No Missing Work"
    )
    return [
        [InlineKeyboardButton("My Summary", callback_data="summary")],
        [InlineKeyboardButton("My Grades", callback_data="grades")],
        [InlineKeyboardButton(missing_label, callback_data="missing")],
        [InlineKeyboardButton("Ask AI", callback_data="ask_ai")],
    ]


def grades_kb() -> list:
    return [
        [
            InlineKeyboardButton("Missing Work", callback_data="missing"),
            InlineKeyboardButton("Menu", callback_data="back"),
        ]
    ]


def missing_kb(missing: list[dict]) -> list:
    keyboard = []
    for item in missing:
        aid = item["assignment_id"]
        title = item["title"]
        short = title[:28] + "..." if len(title) > 28 else title
        already_flagged = item.get("flagged_by_student", 0)
        label = f"Already flagged" if already_flagged else f"Flag: {short}"
        if not already_flagged:
            keyboard.append([
                InlineKeyboardButton(label, callback_data=f"flag_{aid}")
            ])

    keyboard.append([
        InlineKeyboardButton("Back to Menu", callback_data="back")
    ])
    return keyboard


def back_kb() -> list:
    return [[InlineKeyboardButton("Back to Menu", callback_data="back")]]


def confirm_kb() -> list:
    return [[
        InlineKeyboardButton("Yes, that's me!", callback_data="reg_confirm"),
        InlineKeyboardButton("Not me", callback_data="reg_cancel"),
    ]]


def selection_kb(students: list[dict]) -> list:
    keyboard = []
    for s in students:
        masked = s["lms_id"][:4] + "****" + s["lms_id"][-3:]
        keyboard.append([
            InlineKeyboardButton(
                f"Student: {s['full_name']} ({masked})",
                callback_data=f"select_{s['id']}",
            )
        ])
    keyboard.append([
        InlineKeyboardButton("None of these", callback_data="reg_cancel")
    ])
    return keyboard


def verify_kb(student_id: int, assignment_id: int) -> list:
    return [[
        InlineKeyboardButton(
            "Mark Submitted",
            callback_data=f"verify_approve_{student_id}_{assignment_id}",
        ),
        InlineKeyboardButton(
            "Still Missing",
            callback_data=f"verify_deny_{student_id}_{assignment_id}",
        ),
    ]]


def broadcast_confirm_kb() -> list:
    return [[
        InlineKeyboardButton("Yes, send now", callback_data="broadcast_confirm"),
        InlineKeyboardButton("Cancel", callback_data="broadcast_cancel"),
    ]]


def ai_followup_kb() -> list:
    return [
        [
            InlineKeyboardButton("Ask Another", callback_data="ask_ai"),
            InlineKeyboardButton("Menu", callback_data="back"),
        ]
    ]
