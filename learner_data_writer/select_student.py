try:
    from .get_all_students import get_all_students
except ImportError:  # pragma: no cover - script compatibility
    from get_all_students import get_all_students

def select_student(service, course):
    students = get_all_students(service, course["id"])
    if not students:
        print("No students in this course.")
        return None
    print("Available students:")
    for i, s in enumerate(students, 1):
        profile = s.get("profile", {})
        name_info = profile.get("name", {})
        full_name = " ".join(filter(None, [name_info.get("givenName",""), name_info.get("familyName","")])).strip() or s["userId"]
        print(f"{i}. {full_name} (ID: {s['userId']})")
    while True:
        try:
            choice = int(input("Select student number: "))
            if 1 <= choice <= len(students):
                return students[choice - 1]["userId"]
            else:
                print("Invalid choice. Try again.")
        except ValueError:
            print("Please enter a number.")
