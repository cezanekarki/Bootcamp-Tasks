students = [
    {"name": "Alice", "age": 25, "score": 92},
    {"name": "Bob", "age": 22, "score": 85},
    {"name": "Charlie", "age": 27, "score": 78},
    {"name": "David", "age": 23, "score": 95},
    {"name": "Eva", "age": 24, "score": 88}
]


sorted_students = sorted(students, key=lambda X:X["age"])
print(sorted_students)

def get_age(student):
    return student['age']

sorted_student = sorted(students, key=get_age)  
print(sorted_student)

def get_age(student):
    return student['age']


def get_score(student):
    return student['score']
sorted_student = sorted(students, key=get_score, reverse=True)  
print(sorted_student)