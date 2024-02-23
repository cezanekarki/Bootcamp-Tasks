# Databricks notebook source
students = [
    {"name": "Alice", "age": 25, "score": 92},
    {"name": "Bob", "age": 22, "score": 85},
    {"name": "Charlie", "age": 27, "score": 78},
    {"name": "David", "age": 23, "score": 95},
    {"name": "Eva", "age": 24, "score": 88}
]
#sorted_students = sorted(students, key=lambda x: x['age'])
#Bubble sort implementation
n = len(students)
for i in range(n):
    for j in range(0, n-i-1):
        if students[j]['age'] > students[j+1]['age']:
            # Swap the elements
            students[j], students[j+1] = students[j+1], students[j]

# Print the sorted list
for student in students:
    print(student)
# Print the sorted list
for student in sorted_students:
    print(student)

# COMMAND ----------

students = [
    {"name": "Alice", "age": 25, "score": 92},
    {"name": "Bob", "age": 22, "score": 85},
    {"name": "Charlie", "age": 27, "score": 78},
    {"name": "David", "age": 23, "score": 95},
    {"name": "Eva", "age": 24, "score": 88}
]

def get_age(student):
    return student['age']
 
sorted_students = sorted(students, key=get_age)
print(sorted_students)

# COMMAND ----------

"""You have a list of numbers:
numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
Using a lambda function and the filter() function, filter out the even numbers from the list. Provide the code to achieve this."""

numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

# Filter out the even numbers using filter() and lambda function
even_numbers = list(filter(lambda x: x % 2 == 0, numbers))

# Print the filtered list of even numbers
print(even_numbers)



# COMMAND ----------

students = [
    {"name": "Alice", "score": 85},
    {"name": "Bob", "score": 92},
    {"name": "Charlie", "score": 78},
    {"name": "David", "score": 95},
    {"name": "Eve", "score": 88}
]
 
sorted_students = sorted(students, key=lambda student: student["name"],reverse=True)
print("Sorted: ",sorted_students)
 

# COMMAND ----------


