# 7. Create a list of dictionaries named 'models', each dictionary representing a mobile phone model with 'make', 'model', and 'color' keys
# sort based on age
students = [
    {"name": "Alice", "age": 25, "score": 92},
    {"name": "Bob", "age": 22, "score": 85},
    {"name": "Charlie", "age": 27, "score": 78},
    {"name": "David", "age": 23, "score": 95},
    {"name": "Eva", "age": 24, "score": 88}
]

# Display original list of dictionaries
print("Original list of dictionaries:")
print(students)

sorted_students = sorted(students, key=lambda x: x['age'])
print("\nSorting the List of dictionaries:")
print(sorted_students) 