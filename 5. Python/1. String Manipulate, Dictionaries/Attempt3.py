"""students = [
    {"name": "Alice", "age": 25, "score": 92},
    {"name": "Bob", "age": 22, "score": 85},
    {"name": "Charlie", "age": 27, "score": 78},
    {"name": "David", "age": 23, "score": 95},
    {"name": "Eva", "age": 24, "score": 88}
]

def get_age(student):
    return student['age']

sorted_students = sorted(students, key=student['age'])
print(sorted_students)"""

numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
even_filter = filter(lambda anka : anka % 2 != 0, numbers)
print(list(even_filter))

students = [
    {"name": "Alice", "score": 85},
    {"name": "Bob", "score": 92},
    {"name": "Charlie", "score": 78},
    {"name": "David", "score": 95},
    {"name": "Eve", "score": 88}
]

students_sort = sorted(students, key=lambda bidyarthi : bidyarthi ['score'])
print(students_sort)
