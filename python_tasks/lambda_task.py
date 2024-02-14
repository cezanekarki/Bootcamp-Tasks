students = [
    {"name": "Alice", "age": 25, "score": 92},
    {"name": "Bob", "age": 22, "score": 85},
    {"name": "Charlie", "age": 27, "score": 78},
    {"name": "David", "age": 23, "score": 95},
    {"name": "Eva", "age": 24, "score": 88}
]

sorted_students = sorted(students, key = lambda x : x["age"])
print(sorted_students)

def get_age(student):
    return student['age']

sorted_students = sorted(students, key=get_age)
print(sorted_students)


numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
#Using a lambda function and the filter() function, filter out the even numbers from the list. Provide the code to achieve this.

even_numbers = filter(lambda x : x % 2 == 0, numbers)
print(even_numbers)

students = [
    {"name": "Alice", "score": 85},
    {"name": "Bob", "score": 92},
    {"name": "Charlie", "score": 78},
    {"name": "David", "score": 95},
    {"name": "Eve", "score": 88}
]
#Reverse Sort the list of students according to the score.

sorted_students = sorted(students, key=lambda x: x["score"], reverse=True)
for student in sorted_students:
    print(student)
