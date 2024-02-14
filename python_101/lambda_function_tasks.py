students = [
        {"name": "Alice", "age": 25, "score": 92},
        {"name": "Bob", "age": 22, "score": 85},
        {"name": "Charlie", "age": 27, "score": 78},
        {"name": "David", "age": 23, "score": 95},
        {"name": "Eva", "age": 24, "score": 88}
    ]


sort = sorted(students, key=lambda student: student["age"])
print(f'sorted list: {sort}')

'''
You have a list of numbers:
numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
Using a lambda function and the filter() function, filter out the even numbers from the list. Provide the code to achieve this.
students = [
    {"name": "Alice", "score": 85},
    {"name": "Bob", "score": 92},
    {"name": "Charlie", "score": 78},
    {"name": "David", "score": 95},
    {"name": "Eve", "score": 88}
]
Reverse Sort the list of students according to the score.
'''
numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
print(list(filter(lambda x:x%2==0,numbers)))

students = [

    {"name": "Alice", "score": 85},
    
        {"name": "Bob", "score": 92},
    
        {"name": "Charlie", "score": 78},
    
        {"name": "David", "score": 95},
    
        {"name": "Eve", "score": 88}

]

sort = sorted(students, key=lambda student: student["score"], reverse=True)

print(f'sorted list: {sort}')






