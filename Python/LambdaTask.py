"""You have a list of numbers:
numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
Using a lambda function and the filter() function, filter out the even numbers from the list. Provide the code to achieve this.
2)students = [
    {"name": "Alice", "score": 85},
    {"name": "Bob", "score": 92},
    {"name": "Charlie", "score": 78},
    {"name": "David", "score": 95},
    {"name": "Eve", "score": 88}
]
Reverse Sort the list of students according to the score."""

students = [
    {"name": "Alice", "score": 85},
    {"name": "Bob", "score": 92},
    {"name": "Charlie", "score": 78},
    {"name": "David", "score": 95},
    {"name": "Eve", "score": 88}
]
 
sorted_students = sorted(students, key=lambda student: student["name"],reverse=True)
print("Sorted: ",sorted_students)
 
numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
filteredNumber= filter( lambda num:num%2==0 ,numbers)
 
print("\n Even Numbers: ",list(filteredNumber))