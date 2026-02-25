def select_course(courses):
    print("Available courses:")
    for i, course in enumerate(courses, 1):
        print(f"{i}. {course['name']} (ID: {course['id']})")
    while True:
        try:
            choice = int(input("Select course number: "))
            if 1 <= choice <= len(courses):
                return courses[choice - 1]
            else:
                print("Invalid choice. Try again.")
        except ValueError:
            print("Please enter a number.")