def sort_student():
    students = [
        {"name": "Alice", "score": 85},
        {"name": "Bob", "score": 92},
        {"name": "Charlie", "score": 78},
        {"name": "David", "score": 95},
        {"name": "Eve", "score": 88}
    ]
    
    sorted_students = sorted(students, key=lambda student: student["score"], reverse=True)
    print("Sorted students based on score: \n", sorted_students)

def even_number():
    numbers = [ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 ]
    filteredNumber= filter(lambda num :num % 2 == 0, numbers)
    
    print("Even Numbers: ", list(filteredNumber))

sort_student()
even_number()